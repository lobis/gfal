"""Native XRootD filesystem adapter used during the backend transition."""

from __future__ import annotations

import io
import posixpath
import stat as stat_module
from typing import Any, Optional


def _load_xrootd_client():
    try:
        from XRootD import client
        from XRootD.client.flags import (
            DirListFlags,
            MkDirFlags,
            OpenFlags,
            QueryCode,
            StatInfoFlags,
        )
    except ImportError as exc:
        raise ModuleNotFoundError(
            "No module named 'XRootD'; install XRootD bindings in your environment, "
            "for example with: conda install -c conda-forge xrootd"
        ) from exc

    return client, DirListFlags, MkDirFlags, OpenFlags, QueryCode, StatInfoFlags


def _status_error(operation: str, status: Any) -> OSError:
    message = getattr(status, "message", str(status))
    return OSError(f"{operation} failed: {message}")


def _flags_to_mode(flags: int, stat_flags: Any) -> int:
    is_dir = bool(flags & stat_flags.IS_DIR)
    is_readable = bool(flags & stat_flags.IS_READABLE)
    is_writable = bool(flags & stat_flags.IS_WRITABLE)

    if is_dir:
        file_type = stat_module.S_IFDIR
        perms = (0o555 if is_readable else 0) | (0o200 if is_writable else 0)
    else:
        file_type = stat_module.S_IFREG
        perms = (0o444 if is_readable else 0) | (0o200 if is_writable else 0)
    return file_type | perms


class XRootDNativeFileSystem:
    """Small fsspec-shaped wrapper around the XRootD Python bindings."""

    root_marker = "/"

    def __init__(self, base_url: str, **storage_options: Any) -> None:
        (
            self._client_mod,
            self._dirlist_flags,
            self._mkdir_flags,
            self._open_flags,
            self._query_code,
            self._stat_flags,
        ) = _load_xrootd_client()
        self.timeout = storage_options.get("timeout", 0)
        self.storage_options = dict(storage_options)
        parsed = self._client_mod.URL(base_url)
        self.protocol = parsed.protocol
        self.hostid = parsed.hostid
        self.base_url = f"{self.protocol}://{self.hostid}"
        self._myclient = self._client_mod.FileSystem(self.base_url)
        if not self._myclient.url.is_valid():
            raise ValueError(f"Invalid XRootD URL: {base_url!r}")

    @classmethod
    def from_url(
        cls,
        url: str,
        storage_options: Optional[dict[str, Any]] = None,
    ) -> tuple[XRootDNativeFileSystem, str]:
        client_mod, *_ = _load_xrootd_client()
        parsed = client_mod.URL(url)
        if not parsed.is_valid():
            raise ValueError(f"Invalid XRootD URL: {url!r}")
        fso = cls(f"{parsed.protocol}://{parsed.hostid}", **(storage_options or {}))
        path = parsed.path_with_params.rstrip("/") or cls.root_marker
        return fso, path

    def unstrip_protocol(self, path: str) -> str:
        if path.startswith(("root://", "xroot://", "https://", "http://")):
            return path
        stripped = path.lstrip("/")
        if path.startswith("/"):
            return f"{self.base_url}/{path}"
        return f"{self.base_url}/{stripped}"

    def invalidate_cache(self, path: Optional[str] = None) -> None:
        return None

    def _info_from_stat(self, path: str, stat_info: Any) -> dict[str, Any]:
        flags = stat_info.flags
        is_dir = bool(flags & self._stat_flags.IS_DIR)
        return {
            "name": path,
            "size": stat_info.size,
            "type": "directory" if is_dir else "file",
            "mtime": stat_info.modtime,
            "atime": stat_info.modtime,
            "ctime": stat_info.modtime,
            "mode": _flags_to_mode(flags, self._stat_flags),
            "uid": 0,
            "gid": 0,
            "nlink": 1,
        }

    def info(self, path: str, **kwargs: Any) -> dict[str, Any]:
        status, stat_info = self._myclient.stat(path, timeout=self.timeout)
        if not status.ok:
            raise _status_error("File stat request", status)
        return self._info_from_stat(path, stat_info)

    def ls(self, path: str, detail: bool = True, **kwargs: Any) -> list[Any]:
        status, entries = self._myclient.dirlist(
            path, self._dirlist_flags.STAT, timeout=self.timeout
        )
        if not status.ok:
            message = getattr(status, "message", "").lower()
            if (
                "not a directory" in message
                or "unable to open directory" in message
                or "no such file or directory" in message
            ):
                info = self.info(path)
                return [info] if detail else [posixpath.basename(path.rstrip("/"))]
            raise _status_error("Directory listing", status)

        listing = []
        for entry in entries:
            child_path = posixpath.join(path.rstrip("/") or "/", entry.name)
            listing.append(self._info_from_stat(child_path, entry.statinfo))
        if detail:
            return listing
        return [posixpath.basename(item["name"].rstrip("/")) for item in listing]

    def isdir(self, path: str) -> bool:
        try:
            return self.info(path).get("type") == "directory"
        except Exception:
            return False

    def mkdir(self, path: str, create_parents: bool = True, **kwargs: Any) -> None:
        if create_parents:
            status, _ = self._myclient.mkdir(
                path, flags=self._mkdir_flags.MAKEPATH, timeout=self.timeout
            )
        else:
            status, _ = self._myclient.mkdir(path, timeout=self.timeout)
        if not status.ok:
            raise _status_error("Directory creation", status)

    def makedirs(self, path: str, exist_ok: bool = False) -> None:
        if not exist_ok and self.exists(path):
            raise OSError("Location already exists and exist_ok is false")
        status, _ = self._myclient.mkdir(
            path, flags=self._mkdir_flags.MAKEPATH, timeout=self.timeout
        )
        if not status.ok:
            if exist_ok and self.exists(path):
                return
            raise _status_error("Directory creation", status)

    def exists(self, path: str) -> bool:
        status, _ = self._myclient.stat(path, timeout=self.timeout)
        if status.ok:
            return True
        message = getattr(status, "message", "").lower()
        if "no such file or directory" in message or "path" in message:
            return False
        raise _status_error("File existence check", status)

    def rm(self, path: str, recursive: bool = False, **kwargs: Any) -> None:
        if recursive and self.isdir(path):
            for item in reversed(self.ls(path, detail=True)):
                self.rm(item["name"], recursive=True)
            self.rmdir(path)
            return

        status, _ = self._myclient.rm(path, timeout=self.timeout)
        if not status.ok:
            raise _status_error("File removal", status)

    def rmdir(self, path: str) -> None:
        status, _ = self._myclient.rmdir(path, timeout=self.timeout)
        if not status.ok:
            raise _status_error("Directory removal", status)

    def mv(
        self,
        path1: str,
        path2: str,
        recursive: bool = False,
        maxdepth: Optional[int] = None,
        **kwargs: Any,
    ) -> None:
        status, _ = self._myclient.mv(path1, path2, timeout=self.timeout)
        if not status.ok:
            raise _status_error("Move operation", status)

    def chmod(self, path: str, mode: int) -> None:
        status, _ = self._myclient.chmod(path, mode, timeout=self.timeout)
        if not status.ok:
            raise _status_error("chmod", status)

    def checksum(self, path: str, algorithm: str = "ADLER32") -> tuple[str, str]:
        status, response = self._myclient.query(
            self._query_code.CHECKSUM, path, timeout=self.timeout
        )
        if not status.ok:
            raise _status_error("Checksum query", status)
        text = response.decode() if isinstance(response, bytes) else response
        parts = text.strip("\x00").strip().split()
        if len(parts) < 2:
            raise OSError(f"Unexpected checksum response: {text!r}")
        return parts[0], parts[1]

    def open(self, path: str, mode: str = "rb", **kwargs: Any) -> Any:
        if "b" not in mode:
            binary_mode = mode.replace("t", "") + "b"
            text_kwargs = {
                key: kwargs.pop(key)
                for key in ["encoding", "errors", "newline"]
                if key in kwargs
            }
            return io.TextIOWrapper(
                self.open(path, binary_mode, **kwargs),
                **text_kwargs,
            )
        return XRootDNativeFile(self, path, mode=mode, **kwargs)


class XRootDNativeFile:
    def __init__(self, fs: XRootDNativeFileSystem, path: str, mode: str = "rb") -> None:
        if mode not in {"rb", "wb", "ab", "r+b"}:
            raise NotImplementedError(f"File mode not supported: {mode}")
        self.fs = fs
        self.path = path
        self.mode = mode
        self.timeout = fs.timeout
        self.loc = 0
        self.closed = False
        self._file = fs._client_mod.File()
        flags = self._flags_for_mode(mode)
        status, _ = self._file.open(
            fs.unstrip_protocol(path), flags, timeout=self.timeout
        )
        if not status.ok:
            raise _status_error("File open", status)
        if mode == "ab":
            status, stat_info = self._file.stat(timeout=self.timeout)
            if not status.ok:
                raise _status_error("File stat", status)
            self.loc = stat_info.size
        elif mode in {"rb", "r+b"}:
            status, stat_info = self._file.stat(timeout=self.timeout)
            if not status.ok:
                raise _status_error("File stat", status)
            self.size = stat_info.size
        else:
            self.size = 0

    def _flags_for_mode(self, mode: str) -> Any:
        if mode == "rb":
            return self.fs._open_flags.READ
        if mode == "wb":
            return self.fs._open_flags.DELETE
        if mode in {"ab", "r+b"}:
            return self.fs._open_flags.UPDATE
        raise NotImplementedError(f"File mode not supported: {mode}")

    def __enter__(self) -> XRootDNativeFile:
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        self.close()

    def readable(self) -> bool:
        return self.mode in {"rb", "r+b"} and not self.closed

    def writable(self) -> bool:
        return self.mode in {"wb", "ab", "r+b"} and not self.closed

    def read(self, length: int = -1) -> bytes:
        if not self.readable():
            raise ValueError("File not in read mode")
        if length is None or length < 0:
            length = max(self.size - self.loc, 0)
        if length == 0:
            return b""
        status, data = self._file.read(self.loc, length, timeout=self.timeout)
        if not status.ok:
            raise _status_error("File read", status)
        self.loc += len(data)
        return data

    def write(self, data: bytes) -> int:
        if not self.writable():
            raise ValueError("File not in write mode")
        status, _ = self._file.write(data, self.loc, len(data), timeout=self.timeout)
        if not status.ok:
            raise _status_error("File write", status)
        self.loc += len(data)
        self.size = max(getattr(self, "size", 0), self.loc)
        return len(data)

    def seek(self, loc: int, whence: int = 0) -> int:
        if whence == 0:
            new_loc = loc
        elif whence == 1:
            new_loc = self.loc + loc
        elif whence == 2:
            new_loc = getattr(self, "size", 0) + loc
        else:
            raise ValueError("invalid whence")
        if new_loc < 0:
            raise ValueError("Seek before start of file")
        self.loc = new_loc
        return self.loc

    def tell(self) -> int:
        return self.loc

    def flush(self) -> None:
        return None

    def close(self) -> None:
        if self.closed:
            return
        status, _ = self._file.close(timeout=self.timeout)
        if not status.ok:
            raise _status_error("File close", status)
        self.closed = True
