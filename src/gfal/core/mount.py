"""Read-only FUSE mount support built on the existing gfal/fsspec stack."""

from __future__ import annotations

import contextlib
import errno
import itertools
import os
import stat
import sys
from collections.abc import Iterable
from pathlib import Path, PurePosixPath
from typing import Any
from urllib.parse import quote, unquote, urlparse, urlunparse

from gfal.core import fs
from gfal.core.api import GfalClient, local_destination_path

try:
    if sys.platform in {"linux", "darwin"}:
        from mfusepy import FUSE, FuseOSError, Operations
    else:  # pragma: no cover - import is intentionally platform-gated
        raise ImportError
except (
    ImportError,
    OSError,
):  # pragma: no cover - exercised via tests with the fallback shim
    FUSE = None

    class Operations:  # type: ignore[no-redef]
        """Fallback base class so the module stays importable without mfusepy."""

    class FuseOSError(OSError):  # type: ignore[no-redef]
        """Fallback exception that behaves like mfusepy.FuseOSError."""

        def __init__(self, errnum: int):
            super().__init__(errnum, os.strerror(errnum))
            self.errno = errnum


_DEFAULT_BLOCK_SIZE = 4096
_DEFAULT_NAME_MAX = 255
_O_ACCMODE = getattr(os, "O_ACCMODE", os.O_WRONLY | getattr(os, "O_RDWR", 2))
_SUPPORTED_PLATFORMS = {"linux": "Linux", "darwin": "macOS"}


def ensure_mount_supported() -> None:
    """Raise a user-facing error when the current environment cannot mount."""
    if sys.platform not in _SUPPORTED_PLATFORMS:
        raise OSError(
            errno.EOPNOTSUPP,
            "gfal mount is currently supported on Linux and macOS only",
        )
    if FUSE is None:
        raise OSError(
            errno.ENOENT,
            "gfal mount requires the optional mount dependency; install with "
            "pip install -e .[mount]",
        )


def _join_url_path(base: str, child: str) -> str:
    """Append one decoded path segment to *base*, preserving query/fragment."""
    parsed = urlparse(base)
    if parsed.path == "//":
        new_path = f"//{quote(child)}"
    else:
        new_path = parsed.path.rstrip("/") + "/" + quote(child)
    return urlunparse(parsed._replace(path=new_path))


def _entry_name(raw_name: str) -> str:
    """Return the decoded basename for a backend entry name/path/URL."""
    return unquote(PurePosixPath(raw_name.rstrip("/")).name)


def _darwin_fskit_mountpoint(mountpoint: Path) -> str:
    """Return a mountpoint path acceptable to macFUSE's FSKit backend."""
    resolved = mountpoint.resolve()
    resolved_posix = resolved.as_posix()
    if resolved_posix.startswith("/Volumes/") or resolved_posix == "/Volumes":
        return str(PurePosixPath(resolved_posix))

    # FSKit only mounts below /Volumes. "Macintosh HD" is a built-in symlink to
    # "/", so this keeps the user-visible path unchanged while satisfying FSKit.
    return str(PurePosixPath("/Volumes/Macintosh HD") / resolved_posix.lstrip("/"))


def _stat_dict(st: Any, *, inode: int) -> dict[str, Any]:
    """Convert a gfal StatResult into the dict format expected by mfusepy."""
    size = int(getattr(st, "st_size", 0))
    mode = int(getattr(st, "st_mode", stat.S_IFREG | 0o644))
    blocks = max(1, (size + 511) // 512) if stat.S_ISREG(mode) and size > 0 else 0
    nlink = int(getattr(st, "st_nlink", 1))
    if stat.S_ISDIR(mode):
        nlink = max(2, nlink)
    return {
        "st_atime": float(getattr(st, "st_atime", 0.0)),
        "st_ctime": float(getattr(st, "st_ctime", 0.0)),
        "st_gid": int(getattr(st, "st_gid", 0)),
        "st_ino": inode,
        "st_mode": mode,
        "st_mtime": float(getattr(st, "st_mtime", 0.0)),
        "st_nlink": nlink,
        "st_size": size,
        "st_uid": int(getattr(st, "st_uid", 0)),
        "st_blksize": _DEFAULT_BLOCK_SIZE,
        "st_blocks": blocks,
    }


class ReadOnlyFuseOperations(Operations):
    """Minimal read-only FUSE adapter that delegates I/O to GfalClient/fsspec."""

    def __init__(self, source_url: str, client: GfalClient):
        self.source_url = fs.normalize_url(source_url)
        self.client = client
        root_stat = self.client.stat(self.source_url)
        if not root_stat.is_dir():
            raise NotADirectoryError(
                f"mount source must be a directory: {self.source_url}"
            )
        self._handles: dict[int, Any] = {}
        self._next_handle = itertools.count(1)
        self._attr_cache: dict[str, dict[str, Any]] = {
            "/": _stat_dict(root_stat, inode=self._inode_for_path("/"))
        }

    @staticmethod
    def _inode_for_path(path: str) -> int:
        normalized = path or "/"
        return abs(hash(normalized)) & ((1 << 63) - 1)

    def _url_for_path(self, path: str) -> str:
        if path in {"", "/"}:
            return self.source_url
        url = self.source_url
        for part in PurePosixPath(path).parts:
            if part in {"", "/"}:
                continue
            if part in {".", ".."}:
                raise FuseOSError(errno.ENOENT)
            url = _join_url_path(url, part)
        return url

    def _stat_for_path(self, path: str) -> Any:
        try:
            return self.client.stat(self._url_for_path(path))
        except (
            Exception
        ) as exc:  # pragma: no cover - mapped via tests on the public API
            raise self._map_error(exc) from exc

    @staticmethod
    def _map_error(exc: Exception) -> FuseOSError:
        errnum = getattr(exc, "errno", None)
        if isinstance(errnum, int) and errnum > 0:
            return FuseOSError(errnum)
        if isinstance(exc, FileNotFoundError):
            return FuseOSError(errno.ENOENT)
        if isinstance(exc, PermissionError):
            return FuseOSError(errno.EACCES)
        if isinstance(exc, IsADirectoryError):
            return FuseOSError(errno.EISDIR)
        if isinstance(exc, NotADirectoryError):
            return FuseOSError(errno.ENOTDIR)
        return FuseOSError(errno.EIO)

    def access(self, path: str, amode: int) -> int:
        if amode & os.W_OK:
            raise FuseOSError(errno.EROFS)
        self._stat_for_path(path)
        return 0

    def getattr(self, path: str, fh: int | None = None) -> dict[str, Any]:
        if path in self._attr_cache:
            return dict(self._attr_cache[path])
        st = self._stat_for_path(path)
        attrs = _stat_dict(st, inode=self._inode_for_path(path))
        self._attr_cache[path] = attrs
        return dict(attrs)

    def readdir(self, path: str, fh: int) -> Iterable[str]:
        st = self._stat_for_path(path)
        if not st.is_dir():
            raise FuseOSError(errno.ENOTDIR)
        entries = [".", ".."]
        try:
            for entry in self.client.ls(self._url_for_path(path), detail=True):
                raw_name = str(entry.info.get("name", ""))
                name = _entry_name(raw_name)
                if name and name not in entries:
                    entries.append(name)
                    child_path = str(PurePosixPath(path) / name)
                    if not child_path.startswith("/"):
                        child_path = f"/{child_path}"
                    self._attr_cache[child_path] = _stat_dict(
                        entry, inode=self._inode_for_path(child_path)
                    )
        except Exception as exc:
            raise self._map_error(exc) from exc
        return entries

    def open(self, path: str, flags: int) -> int:
        if (flags & _O_ACCMODE) != os.O_RDONLY:
            raise FuseOSError(errno.EROFS)
        st = self._stat_for_path(path)
        if st.is_dir():
            raise FuseOSError(errno.EISDIR)
        try:
            handle = self.client.open(self._url_for_path(path), "rb")
        except Exception as exc:
            raise self._map_error(exc) from exc
        fh = next(self._next_handle)
        self._handles[fh] = handle
        return fh

    def read(self, path: str | None, size: int, offset: int, fh: int) -> bytes:
        try:
            handle = self._handles[fh]
        except KeyError as exc:  # pragma: no cover - defensive kernel/runtime path
            raise FuseOSError(errno.EBADF) from exc
        try:
            handle.seek(offset)
            data = handle.read(size)
        except Exception as exc:
            raise self._map_error(exc) from exc
        return data.tobytes() if isinstance(data, memoryview) else bytes(data)

    def flush(self, path: str | None, fh: int) -> int:
        return 0

    def release(self, path: str | None, fh: int) -> int:
        handle = self._handles.pop(fh, None)
        if handle is not None:
            with contextlib.suppress(Exception):
                handle.close()
        return 0

    def statfs(self, path: str) -> dict[str, Any]:
        _root_fs, root_path = fs.url_to_fs(self.source_url, self.client.storage_options)
        local_root = local_destination_path(self.source_url, root_path)
        if local_root is not None:
            stats = os.statvfs(local_root)
            return {
                "f_bavail": stats.f_bavail,
                "f_bfree": stats.f_bfree,
                "f_blocks": stats.f_blocks,
                "f_bsize": stats.f_bsize,
                "f_favail": stats.f_favail,
                "f_ffree": stats.f_ffree,
                "f_files": stats.f_files,
                "f_frsize": stats.f_frsize,
                "f_namemax": stats.f_namemax,
            }

        root_stat = self.client.stat(self.source_url)
        blocks = max(
            1, (root_stat.st_size + _DEFAULT_BLOCK_SIZE - 1) // _DEFAULT_BLOCK_SIZE
        )
        return {
            "f_bavail": 0,
            "f_bfree": 0,
            "f_blocks": blocks,
            "f_bsize": _DEFAULT_BLOCK_SIZE,
            "f_favail": 0,
            "f_ffree": 0,
            "f_files": 1024,
            "f_frsize": _DEFAULT_BLOCK_SIZE,
            "f_namemax": _DEFAULT_NAME_MAX,
        }

    def destroy(self, path: str) -> None:
        for fh in list(self._handles):
            self.release(path, fh)


def mount_foreground(source_url: str, mountpoint: Path, client: GfalClient) -> None:
    """Mount *source_url* read-only at *mountpoint* in the foreground."""
    ensure_mount_supported()

    if not mountpoint.exists():
        raise FileNotFoundError(
            errno.ENOENT, os.strerror(errno.ENOENT), str(mountpoint)
        )
    if not mountpoint.is_dir():
        raise NotADirectoryError(
            errno.ENOTDIR, os.strerror(errno.ENOTDIR), str(mountpoint)
        )

    operations = ReadOnlyFuseOperations(source_url, client)
    fsname = f"gfal:{urlparse(fs.normalize_url(source_url)).scheme or 'file'}"
    fuse_mountpoint = mountpoint
    fuse_kwargs = {
        "foreground": True,
        "nothreads": True,
        "ro": True,
        "fsname": fsname,
    }
    if sys.platform == "linux":
        fuse_kwargs["default_permissions"] = True
        fuse_kwargs["subtype"] = "gfal"
    elif sys.platform == "darwin":
        fuse_kwargs["backend"] = "fskit"
        fuse_kwargs["volname"] = "gfal"
        fuse_mountpoint = _darwin_fskit_mountpoint(mountpoint)

    FUSE(operations, str(fuse_mountpoint), **fuse_kwargs)  # type: ignore[misc]
