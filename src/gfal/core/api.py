from __future__ import annotations

import asyncio
import contextlib
import errno
import hashlib
import os
import stat
import threading
import time
import zlib
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Optional
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

from gfal.core import fs
from gfal.core.errors import (
    GfalError,
    GfalFileExistsError,
    GfalFileNotFoundError,
    GfalIsADirectoryError,
    GfalNotADirectoryError,
    GfalPermissionError,
    GfalTimeoutError,
    is_xrootd_not_found_message,
    is_xrootd_permission_message,
)

WarnCallback = Optional[Callable[[str], None]]
ProgressCallback = Optional[Callable[[int], None]]
StartCallback = Optional[Callable[[], None]]


@dataclass(frozen=True)
class ClientConfig:
    cert: str | None = None
    key: str | None = None
    timeout: int = 1800
    ssl_verify: bool = True
    ipv4_only: bool = False
    ipv6_only: bool = False
    app: Optional[str] = None


@dataclass(frozen=True)
class ChecksumPolicy:
    algorithm: str
    mode: str = "both"
    expected_value: str | None = None


@dataclass(frozen=True)
class CopyOptions:
    overwrite: bool = False
    create_parents: bool = False
    timeout: int | None = None
    checksum: ChecksumPolicy | None = None
    source_space_token: str | None = None
    destination_space_token: str | None = None
    strict: bool = False
    streams: int | None = None
    tpc: str = "never"
    tpc_direction: str = "pull"
    recursive: bool = False
    preserve_times: bool = False
    preserve_times_explicit: bool = False  # True only when user passed --preserve-times
    compare: str | None = None  # None | "size" | "size_mtime" | "checksum" | "none"
    dry_run: bool = False
    just_copy: bool = False
    disable_cleanup: bool = False
    no_delegation: bool = False
    evict: bool = False
    scitag: int | None = None


@dataclass(frozen=True)
class StatResult:
    info: dict[str, Any]
    st_size: int
    st_mode: int
    st_uid: int
    st_gid: int
    st_nlink: int
    st_mtime: float
    st_atime: float
    st_ctime: float

    @classmethod
    def from_info(cls, info: dict[str, Any]) -> StatResult:
        st = fs.StatInfo(info)
        return cls(
            info=dict(info),
            st_size=st.st_size,
            st_mode=st.st_mode,
            st_uid=st.st_uid,
            st_gid=st.st_gid,
            st_nlink=st.st_nlink,
            st_mtime=st.st_mtime,
            st_atime=st.st_atime,
            st_ctime=st.st_ctime,
        )

    @property
    def size(self) -> int:
        return self.st_size

    @property
    def mode(self) -> int:
        return self.st_mode

    @property
    def uid(self) -> int:
        return self.st_uid

    @property
    def gid(self) -> int:
        return self.st_gid

    @property
    def nlink(self) -> int:
        return self.st_nlink

    @property
    def mtime(self) -> float:
        return self.st_mtime

    @property
    def atime(self) -> float:
        return self.st_atime

    @property
    def ctime(self) -> float:
        return self.st_ctime

    def is_dir(self) -> bool:
        return stat.S_ISDIR(self.st_mode)

    def is_file(self) -> bool:
        return stat.S_ISREG(self.st_mode)


class TransferHandle:
    """Background transfer handle shared by sync and async callers."""

    def __init__(
        self,
        thread: threading.Thread,
        cancel_event: threading.Event,
        result_holder: dict[str, Any],
        exc_holder: dict[str, BaseException],
    ):
        self._thread = thread
        self._cancel_event = cancel_event
        self._result_holder = result_holder
        self._exc_holder = exc_holder

    def cancel(self) -> None:
        self._cancel_event.set()

    def done(self) -> bool:
        return not self._thread.is_alive()

    def wait(self, timeout: float | None = None) -> Any:
        self._thread.join(timeout)
        if self._thread.is_alive():
            raise GfalTimeoutError("Transfer handle wait timed out")
        if "error" in self._exc_holder:
            raise self._exc_holder["error"]
        return self._result_holder.get("value")

    async def wait_async(self, timeout: float | None = None) -> Any:
        return await asyncio.to_thread(self.wait, timeout)


class AsyncGfalClient:
    """Async-first library API for GFAL-like operations using fsspec."""

    def __init__(
        self,
        cert: str | None = None,
        key: str | None = None,
        timeout: int = 1800,
        ssl_verify: bool = True,
        ipv4_only: bool = False,
        ipv6_only: bool = False,
        config: ClientConfig | None = None,
        app: str = "python3-gfal-async",
    ):
        if config is None:
            config = ClientConfig(
                cert=cert,
                key=key or cert,
                timeout=timeout,
                ssl_verify=ssl_verify,
                ipv4_only=ipv4_only,
                ipv6_only=ipv6_only,
                app=app,
            )
        self.config = config

        # Preserve the legacy attribute surface for callers/tests that still
        # treat the client as a simple namespace.
        self.cert = config.cert
        self.key = config.key
        self.timeout = config.timeout
        self.ssl_verify = config.ssl_verify
        self.ipv4_only = config.ipv4_only
        self.ipv6_only = config.ipv6_only
        self.app = config.app

    @property
    def storage_options(self) -> dict[str, Any]:
        """Compute storage options for fsspec based on client configuration."""
        return fs.build_storage_options(
            SimpleNamespace(
                cert=self.cert,
                key=self.key,
                timeout=self.timeout,
                ssl_verify=self.ssl_verify,
                ipv4_only=self.ipv4_only,
                ipv6_only=self.ipv6_only,
            )
        )

    def _url(self, url: str) -> str:
        """Return *url* with ``eos.app`` injected when it targets an EOS endpoint."""
        if not self.app or url == "-":
            return url
        return eos_app_url(url, self.app) or url

    @staticmethod
    def _url_path_join(base: str, name: str) -> str:
        """Append *name* to the *path* component of *base*, preserving query/fragment.

        Plain string concatenation breaks when *base* already contains a query
        string (e.g. ``?eos.app=…``): the slash and filename would be appended
        to the query value instead of the URL path.  This helper uses
        ``urllib.parse`` to insert *name* into the correct component.
        """
        parsed = urlparse(base)
        new_path = parsed.path.rstrip("/") + "/" + name
        return urlunparse(parsed._replace(path=new_path))

    async def stat(self, url: str) -> StatResult:
        return await asyncio.to_thread(self._stat_sync, url)

    async def exists(self, url: str) -> bool:
        try:
            await self.stat(url)
        except GfalFileNotFoundError:
            return False
        return True

    async def ls(self, url: str, detail: bool = True) -> list[Any]:
        return await asyncio.to_thread(self._ls_sync, url, detail)

    async def iterdir(self, url: str, detail: bool = True) -> list[Any]:
        return await self.ls(url, detail=detail)

    async def mkdir(self, url: str, mode: int = 0o755, parents: bool = False) -> None:
        await asyncio.to_thread(self._mkdir_sync, url, mode, parents)

    async def rm(self, url: str, recursive: bool = False) -> None:
        await asyncio.to_thread(self._rm_sync, url, recursive)

    async def rmdir(self, url: str) -> None:
        await asyncio.to_thread(self._rmdir_sync, url)

    async def rename(self, src_url: str, dst_url: str) -> None:
        await asyncio.to_thread(self._rename_sync, src_url, dst_url)

    async def chmod(self, url: str, mode: int) -> None:
        await asyncio.to_thread(self._chmod_sync, url, mode)

    async def open(self, url: str, mode: str = "rb") -> Any:
        return await asyncio.to_thread(self._open_sync, url, mode)

    async def getxattr(self, url: str, name: str) -> str:
        return await asyncio.to_thread(self._getxattr_sync, url, name)

    async def setxattr(self, url: str, name: str, value: str) -> None:
        await asyncio.to_thread(self._setxattr_sync, url, name, value)

    async def listxattr(self, url: str) -> list[str]:
        return await asyncio.to_thread(self._listxattr_sync, url)

    async def xattrs(self, url: str) -> dict[str, str]:
        names = await self.listxattr(url)
        return {name: await self.getxattr(url, name) for name in names}

    async def checksum(self, url: str, algorithm: str) -> str:
        return await asyncio.to_thread(self._checksum_sync, url, algorithm)

    async def copy(
        self,
        src_url: str,
        dst_url: str,
        options: CopyOptions | None = None,
        *,
        progress_callback: ProgressCallback = None,
        start_callback: StartCallback = None,
        warn_callback: WarnCallback = None,
        cancel_event: threading.Event | None = None,
    ) -> Any:
        def _runner() -> Any:
            try:
                return self._copy_sync(
                    src_url,
                    dst_url,
                    options or CopyOptions(),
                    progress_callback,
                    start_callback,
                    warn_callback,
                    cancel_event,
                )
            except Exception as e:
                raise self._map_error(e, src_url) from e

        return await asyncio.to_thread(_runner)

    def start_copy(
        self,
        src_url: str,
        dst_url: str,
        options: CopyOptions | None = None,
        *,
        progress_callback: ProgressCallback = None,
        start_callback: StartCallback = None,
        warn_callback: WarnCallback = None,
    ) -> TransferHandle:
        cancel_event = threading.Event()
        result_holder: dict[str, Any] = {}
        exc_holder: dict[str, BaseException] = {}

        def _runner() -> None:
            try:
                result_holder["value"] = self._copy_sync(
                    src_url,
                    dst_url,
                    options or CopyOptions(),
                    progress_callback,
                    start_callback,
                    warn_callback,
                    cancel_event,
                )
            except Exception as exc:  # pragma: no cover - exercised via handle.wait
                exc_holder["error"] = self._map_error(exc, src_url)
            except BaseException as exc:  # pragma: no cover - loop/thread edge path
                exc_holder["error"] = exc

        thread = threading.Thread(target=_runner, daemon=True)
        thread.start()
        return TransferHandle(thread, cancel_event, result_holder, exc_holder)

    def _stat_sync(self, url: str) -> StatResult:
        try:
            fso, path = fs.url_to_fs(url, self.storage_options)
            info = fso.info(path)
            info = fs.xrootd_enrich(info, fso)
            return StatResult.from_info(info)
        except Exception as e:
            raise self._map_error(e, url) from e

    def _ls_sync(self, url: str, detail: bool = True) -> list[Any]:
        try:
            fso, path = fs.url_to_fs(url, self.storage_options)
            try:
                raw_entries = fs.xrootd_ls_enrich(fso, path)
            except OSError as e:
                msg = str(e).lower()
                if (
                    any(
                        marker in msg
                        for marker in ["not a directory", "unable to open directory"]
                    )
                    or getattr(e, "errno", None) == errno.ENOTDIR
                ):
                    raw_entries = [fs.xrootd_enrich(fso.info(path), fso)]
                else:
                    raise
        except Exception as e:
            raise self._map_error(e, url) from e

        if not detail:
            return [Path(entry["name"].rstrip("/")).name for entry in raw_entries]
        return [StatResult.from_info(entry) for entry in raw_entries]

    def _mkdir_sync(self, url: str, mode: int = 0o755, parents: bool = False) -> None:
        try:
            fso, path = fs.url_to_fs(url, self.storage_options)
            if parents:
                if hasattr(fso, "makedirs"):
                    fso.makedirs(path, exist_ok=True)
                else:
                    with contextlib.suppress(FileExistsError):
                        fso.mkdir(path, create_parents=True)
            else:
                fso.mkdir(path, create_parents=False)

            with contextlib.suppress(Exception):
                fso.chmod(path, mode)
        except Exception as e:
            raise self._map_error(e, url) from e

    def _rm_sync(self, url: str, recursive: bool = False) -> None:
        try:
            fso, path = fs.url_to_fs(url, self.storage_options)
            fso.rm(path, recursive=recursive)
        except Exception as e:
            raise self._map_error(e, url) from e

    def _rmdir_sync(self, url: str) -> None:
        try:
            fso, path = fs.url_to_fs(url, self.storage_options)
            fso.rmdir(path)
        except Exception as e:
            raise self._map_error(e, url) from e

    def _rename_sync(self, src_url: str, dst_url: str) -> None:
        try:
            src_fs, src_path = fs.url_to_fs(src_url, self.storage_options)
            dst_fs, dst_path = fs.url_to_fs(dst_url, self.storage_options)
            if type(src_fs) is not type(dst_fs):
                raise GfalError(
                    "Rename across different filesystem types is not supported",
                    errno.EXDEV,
                )

            if hasattr(src_fs, "_myclient"):
                status, _ = src_fs._myclient.mv(src_path, dst_path)
                if not status.ok:
                    raise OSError(status.errno or 1, status.message)
                return

            src_fs.mv(src_path, dst_path)
        except Exception as e:
            raise self._map_error(e, src_url) from e

    def _chmod_sync(self, url: str, mode: int) -> None:
        try:
            fso, path = fs.url_to_fs(url, self.storage_options)
            fso.chmod(path, mode)
        except Exception as e:
            raise self._map_error(e, url) from e

    def _open_sync(self, url: str, mode: str = "rb") -> Any:
        try:
            fso, path = fs.url_to_fs(url, self.storage_options)
            return fso.open(path, mode)
        except Exception as e:
            raise self._map_error(e, url) from e

    def _getxattr_sync(self, url: str, name: str) -> str:
        try:
            fso, path = fs.url_to_fs(url, self.storage_options)
            if not hasattr(fso, "getxattr"):
                raise GfalError(
                    f"xattr not supported by filesystem for {url}",
                    errno.EOPNOTSUPP,
                )
            return str(fso.getxattr(path, name))
        except Exception as e:
            raise self._map_error(e, url) from e

    def _setxattr_sync(self, url: str, name: str, value: str) -> None:
        try:
            fso, path = fs.url_to_fs(url, self.storage_options)
            if not hasattr(fso, "setxattr"):
                raise GfalError(
                    f"xattr not supported by filesystem for {url}",
                    errno.EOPNOTSUPP,
                )
            fso.setxattr(path, name, value)
        except Exception as e:
            raise self._map_error(e, url) from e

    def _listxattr_sync(self, url: str) -> list[str]:
        try:
            fso, path = fs.url_to_fs(url, self.storage_options)
            if not hasattr(fso, "listxattr"):
                raise GfalError(
                    f"xattr not supported by filesystem for {url}",
                    errno.EOPNOTSUPP,
                )
            return fso.listxattr(path)
        except Exception as e:
            raise self._map_error(e, url) from e

    def _checksum_sync(self, url: str, algorithm: str) -> str:
        try:
            fso, path = fs.url_to_fs(url, self.storage_options)
            return compute_checksum(fso, path, algorithm.upper())
        except Exception as e:
            raise self._map_error(e, url) from e

    def _copy_sync(
        self,
        src_url: str,
        dst_url: str,
        options: CopyOptions,
        progress_callback: ProgressCallback,
        start_callback: StartCallback,
        warn_callback: WarnCallback,
        cancel_event: threading.Event | None,
    ) -> Any:
        src_url = self._url(src_url)
        dst_url = self._url(dst_url)
        opts = self.storage_options

        src_fs, src_path = fs.url_to_fs(src_url, opts)
        dst_fs, dst_path = fs.url_to_fs(dst_url, opts)

        src_info = src_fs.info(src_path)
        src_st = StatResult.from_info(src_info)
        src_isdir = src_st.is_dir()

        if options.tpc == "only" and not tpc_applicable(src_url, dst_url):
            src_scheme = urlparse(src_url).scheme.lower()
            dst_scheme = urlparse(dst_url).scheme.lower()
            raise OSError(
                "Third-party copy required (--tpc-only) but not available: "
                f"TPC not supported for {src_scheme}:// -> {dst_scheme}://"
            )

        dst_exists = False
        dst_isdir = False
        try:
            dst_info = dst_fs.info(dst_path)
            dst_exists = True
            dst_isdir = StatResult.from_info(dst_info).is_dir()
        except Exception:
            pass

        if dst_exists and not dst_isdir and src_isdir:
            raise IsADirectoryError("Cannot copy a directory over a file")

        if src_isdir:
            if not options.recursive:
                if warn_callback is not None:
                    warn_callback(
                        f"Skipping directory {src_url} (use recursive=True to copy recursively)"
                    )
                return None
            if not dst_exists:
                dst_fs.mkdir(dst_path, create_parents=options.create_parents)
            self._recursive_copy(
                src_url,
                src_fs,
                src_path,
                dst_url,
                dst_fs,
                dst_path,
                options,
                progress_callback,
                start_callback,
                warn_callback,
                cancel_event,
            )
            self._preserve_times(
                src_st,
                dst_url,
                dst_path,
                options,
                warn_callback,
            )
            return None

        if dst_isdir:
            dst_url = self._url_path_join(dst_url, Path(src_path.rstrip("/")).name)
            dst_fs, dst_path = fs.url_to_fs(dst_url, opts)
            dst_exists = False
            dst_isdir = False
            try:
                dst_info = dst_fs.info(dst_path)
                dst_exists = True
                dst_isdir = StatResult.from_info(dst_info).is_dir()
            except Exception:
                pass

        if (
            not options.just_copy
            and dst_exists
            and not dst_isdir
            and not options.overwrite
            and not is_special_file(dst_path)
        ):
            if options.compare is None:
                raise GfalFileExistsError(
                    f"Destination {dst_url} exists and overwrite is not set"
                )
            if self._existing_file_matches_source(
                src_fs,
                src_path,
                src_st,
                dst_fs,
                dst_path,
                dst_url,
                options,
                warn_callback,
                cancel_event,
            ):
                return None

        tpc_supported = tpc_applicable(src_url, dst_url)
        explicit_tpc = options.tpc in {"auto", "only"} and tpc_supported
        auto_tpc = options.tpc == "auto" and tpc_supported
        if explicit_tpc:
            tpc_dst_url = self._transfer_destination_url(dst_url, src_st, options)
            try:
                from gfal.core import tpc as tpc_module  # noqa: PLC0415

                tpc_module.do_tpc(
                    src_url,
                    tpc_dst_url,
                    opts,
                    mode=options.tpc_direction,
                    timeout=options.timeout,
                    verbose=False,
                    scitag=options.scitag,
                    progress_callback=progress_callback,
                    start_callback=start_callback,
                )
                return None
            except ImportError as e:
                if options.tpc == "only":
                    raise OSError(
                        "Third-party copy required but the tpc module is not available"
                    ) from e
            except NotImplementedError as e:
                if options.tpc == "only":
                    raise OSError(
                        f"Third-party copy required but not available: {e}"
                    ) from e
            except Exception:
                if not auto_tpc:
                    raise

        self._copy_file(
            src_url,
            src_fs,
            src_path,
            dst_url,
            dst_fs,
            dst_path,
            src_st,
            options,
            progress_callback,
            start_callback,
            warn_callback,
            cancel_event,
        )
        return None

    def _recursive_copy(
        self,
        src_url: str,
        src_fs: Any,
        src_path: str,
        dst_url: str,
        dst_fs: Any,
        dst_path: str,
        options: CopyOptions,
        progress_callback: ProgressCallback,
        start_callback: StartCallback,
        warn_callback: WarnCallback,
        cancel_event: threading.Event | None,
    ) -> None:
        entries = src_fs.ls(src_path, detail=False)

        for entry_path in entries:
            if cancel_event is not None and cancel_event.is_set():
                raise GfalError("Transfer cancelled", errno.ECANCELED)

            name = Path(entry_path.rstrip("/")).name
            if name in (".", ".."):
                continue
            self._copy_sync(
                self._url_path_join(src_url, name),
                self._url_path_join(dst_url, name),
                options,
                progress_callback,
                start_callback,
                warn_callback,
                cancel_event,
            )

    def _copy_file(
        self,
        src_url: str,
        src_fs: Any,
        src_path: str,
        dst_url: str,
        dst_fs: Any,
        dst_path: str,
        src_st: StatResult,
        options: CopyOptions,
        progress_callback: ProgressCallback,
        start_callback: StartCallback,
        warn_callback: WarnCallback,
        cancel_event: threading.Event | None,
    ) -> None:
        write_dst_url = self._transfer_destination_url(dst_url, src_st, options)
        write_remote_times = write_dst_url != dst_url
        write_dst_fs, write_dst_path = fs.url_to_fs(write_dst_url, self.storage_options)

        if options.create_parents:
            parent = str(Path(dst_path).parent)
            if parent:
                with contextlib.suppress(Exception):
                    dst_fs.mkdir(parent, create_parents=True)

        src_checksum = None
        if (
            options.checksum
            and not options.just_copy
            and options.checksum.mode in {"source", "both"}
        ):
            src_checksum = checksum_fs(
                src_fs, src_path, options.checksum.algorithm.upper()
            )
            expected = options.checksum.expected_value
            if expected and src_checksum != expected.lower():
                raise OSError(
                    f"Source checksum mismatch: expected {expected}, got {src_checksum}"
                )

        if start_callback is not None:
            start_callback()

        start = time.monotonic()
        transferred = 0
        dst_checksum_hasher = None
        checksum_algorithm = None

        if (
            options.checksum
            and not options.just_copy
            and options.checksum.mode in {"target", "both"}
        ):
            checksum_algorithm = options.checksum.algorithm.upper()
            dst_checksum_hasher = make_hasher(checksum_algorithm)

        try:
            with (
                src_fs.open(src_path, "rb") as src_f,
                write_dst_fs.open(write_dst_path, "wb") as dst_f,
            ):
                while True:
                    if cancel_event is not None and cancel_event.is_set():
                        raise GfalError("Transfer cancelled", errno.ECANCELED)
                    if options.timeout and time.monotonic() - start > options.timeout:
                        raise GfalTimeoutError(
                            f"Transfer timed out after {options.timeout}s: {src_url}"
                        )

                    chunk = src_f.read(fs.CHUNK_SIZE)
                    if not chunk:
                        break

                    dst_f.write(chunk)
                    transferred += len(chunk)
                    if (
                        dst_checksum_hasher is not None
                        and checksum_algorithm is not None
                    ):
                        update_hasher(dst_checksum_hasher, checksum_algorithm, chunk)
                    if progress_callback is not None:
                        progress_callback(transferred)
        except Exception:
            if not options.disable_cleanup:
                with contextlib.suppress(Exception):
                    dst_fs.rm(dst_path, recursive=False)
            raise

        if dst_checksum_hasher is not None and checksum_algorithm is not None:
            dst_checksum = finalise_hasher(dst_checksum_hasher, checksum_algorithm)
            if src_checksum and dst_checksum != src_checksum:
                raise OSError(
                    "Checksum mismatch after transfer: "
                    f"src={src_checksum} dst={dst_checksum}"
                )

        self._preserve_times(
            src_st,
            dst_url,
            dst_path,
            options,
            warn_callback,
            remote_already_preserved=write_remote_times,
        )

    def _existing_file_matches_source(
        self,
        src_fs: Any,
        src_path: str,
        src_st: StatResult,
        dst_fs: Any,
        dst_path: str,
        dst_url: str,
        options: CopyOptions,
        warn_callback: WarnCallback = None,
        cancel_event: threading.Event | None = None,
    ) -> bool:
        compare = options.compare
        if compare is None:
            return False

        if compare == "none":
            if warn_callback is not None:
                warn_callback(f"Skipping existing file {dst_url} (--compare none)")
            return True

        if compare == "size":
            try:
                dst_info = dst_fs.info(dst_path)
                dst_st = StatResult.from_info(dst_info)
                if src_st.st_size == dst_st.st_size:
                    if warn_callback is not None:
                        warn_callback(
                            f"Skipping existing file {dst_url} (matching size)"
                        )
                    return True
            except Exception:
                if warn_callback is not None:
                    warn_callback(
                        f"size compare failed for {dst_url}; "
                        "proceeding with transfer. "
                        "Use --compare=checksum for reliable deduplication "
                        "or --compare=none to skip unconditionally."
                    )
            return False

        if compare == "size_mtime":
            try:
                dst_info = dst_fs.info(dst_path)
                dst_st = StatResult.from_info(dst_info)
                if (
                    src_st.st_size == dst_st.st_size
                    and abs(src_st.st_mtime - dst_st.st_mtime) < 1.0
                ):
                    if warn_callback is not None:
                        warn_callback(
                            f"Skipping existing file {dst_url} (matching mtime and size)"
                        )
                    return True
            except Exception:
                if warn_callback is not None:
                    warn_callback(
                        f"size_mtime compare failed for {dst_url}; "
                        "proceeding with transfer. "
                        "Use --compare=checksum for reliable deduplication "
                        "or --compare=none to skip unconditionally."
                    )
            return False

        if compare == "checksum":
            algorithm = "ADLER32"
            if options.checksum is not None:
                algorithm = options.checksum.algorithm.upper()
            src_checksum = checksum_fs(src_fs, src_path, algorithm, cancel_event)
            dst_checksum = checksum_fs(dst_fs, dst_path, algorithm, cancel_event)
            if src_checksum != dst_checksum:
                return False
            if warn_callback is not None:
                warn_callback(
                    f"Skipping existing file {dst_url} (matching {algorithm} checksum)"
                )
            return True

        return False

    def _transfer_destination_url(
        self,
        dst_url: str,
        src_st: StatResult,
        options: CopyOptions,
    ) -> str:
        if not options.preserve_times:
            return dst_url
        return eos_mtime_url(dst_url, src_st.st_mtime) or dst_url

    def _preserve_times(
        self,
        src_st: StatResult,
        dst_url: str,
        dst_path: str,
        options: CopyOptions,
        warn_callback: WarnCallback,
        *,
        remote_already_preserved: bool = False,
    ) -> None:
        if not options.preserve_times or remote_already_preserved:
            return

        dst_local = local_destination_path(dst_url, dst_path)
        if dst_local is None:
            if warn_callback is not None and options.preserve_times_explicit:
                normalized = fs.normalize_url(dst_url)
                scheme = urlparse(normalized).scheme.lower() or "unknown"
                warn_callback(
                    "--preserve-times is only supported for local destinations; "
                    f"skipping for {scheme} targets"
                )
            return

        try:
            os.utime(
                dst_local,
                ns=(
                    int(src_st.st_atime * 1_000_000_000),
                    int(src_st.st_mtime * 1_000_000_000),
                ),
            )
        except OSError as e:
            if warn_callback is not None:
                warn_callback(f"could not preserve times for {dst_url}: {e}")

    def _map_error(self, e: Exception, url: str) -> GfalError:
        if isinstance(e, GfalError):
            return e

        msg = str(e) or f"({type(e).__name__})"

        # Walk the exception cause/context chain to find aiohttp connection errors
        # that fsspec wraps in FileNotFoundError or other generic exceptions.
        # Must be checked before the FileNotFoundError branch below.
        with contextlib.suppress(ImportError):
            import aiohttp as _aiohttp

            cause: BaseException | None = e.__cause__ or e.__context__
            _seen: set[int] = set()
            while cause is not None and id(cause) not in _seen:
                _seen.add(id(cause))
                if isinstance(cause, _aiohttp.ClientSSLError):
                    return GfalError(msg, errno.EHOSTDOWN)
                if isinstance(cause, _aiohttp.ClientConnectionError):
                    ec = getattr(cause, "errno", None) or errno.ECONNREFUSED
                    if isinstance(ec, int) and ec > 0:
                        return GfalError(msg, ec)
                    return GfalError(msg, errno.ECONNREFUSED)
                cause = cause.__cause__ or cause.__context__

        if isinstance(e, FileNotFoundError):
            return GfalFileNotFoundError(msg)
        if isinstance(e, PermissionError):
            return GfalPermissionError(msg)
        if isinstance(e, FileExistsError):
            return GfalFileExistsError(msg)
        if isinstance(e, IsADirectoryError):
            return GfalIsADirectoryError(msg)
        if isinstance(e, NotADirectoryError):
            return GfalNotADirectoryError(msg)
        if isinstance(e, TimeoutError):
            return GfalTimeoutError(msg)
        status = getattr(e, "status", None)
        if status == 403:
            return GfalPermissionError(msg)
        if status == 404:
            return GfalFileNotFoundError(msg)
        if isinstance(getattr(e, "errno", None), int):
            if e.errno == 0:
                return GfalError(msg, errno.EIO)
            if e.errno == errno.ENOENT:
                return GfalFileNotFoundError(msg)
            if e.errno == errno.EACCES:
                return GfalPermissionError(msg)
            if e.errno == errno.EEXIST:
                return GfalFileExistsError(msg)
            if e.errno == errno.EISDIR:
                return GfalIsADirectoryError(msg)
            if e.errno == errno.ENOTDIR:
                return GfalNotADirectoryError(msg)
            if e.errno == errno.ETIMEDOUT:
                return GfalTimeoutError(msg)

        if is_xrootd_not_found_message(msg):
            return GfalFileNotFoundError(msg)

        if is_xrootd_permission_message(msg):
            return GfalPermissionError(msg)

        return GfalError(msg, getattr(e, "errno", None))


class GfalClient:
    """Synchronous facade over :class:`AsyncGfalClient`."""

    def __init__(
        self,
        cert: str | None = None,
        key: str | None = None,
        timeout: int = 1800,
        ssl_verify: bool = True,
        ipv4_only: bool = False,
        ipv6_only: bool = False,
        config: ClientConfig | None = None,
        app: str = "python3-gfal-sync",
    ):
        self._async_client = AsyncGfalClient(
            cert=cert,
            key=key,
            timeout=timeout,
            ssl_verify=ssl_verify,
            ipv4_only=ipv4_only,
            ipv6_only=ipv6_only,
            config=config,
            app=app,
        )
        self.config = self._async_client.config
        self.cert = self._async_client.cert
        self.key = self._async_client.key
        self.timeout = self._async_client.timeout
        self.ssl_verify = self._async_client.ssl_verify
        self.ipv4_only = self._async_client.ipv4_only
        self.ipv6_only = self._async_client.ipv6_only
        self.app = self._async_client.app

    @property
    def storage_options(self) -> dict[str, Any]:
        return self._async_client.storage_options

    def stat(self, url: str) -> StatResult:
        return run_sync(self._async_client.stat, url)

    def exists(self, url: str) -> bool:
        return run_sync(self._async_client.exists, url)

    def ls(self, url: str, detail: bool = True) -> list[Any]:
        return run_sync(self._async_client.ls, url, detail)

    def iterdir(self, url: str, detail: bool = True) -> Iterator[Any]:
        return iter(self.ls(url, detail=detail))

    def mkdir(self, url: str, mode: int = 0o755, parents: bool = False) -> None:
        run_sync(self._async_client.mkdir, url, mode, parents)

    def rm(self, url: str, recursive: bool = False) -> None:
        run_sync(self._async_client.rm, url, recursive)

    def rmdir(self, url: str) -> None:
        run_sync(self._async_client.rmdir, url)

    def rename(self, src_url: str, dst_url: str) -> None:
        run_sync(self._async_client.rename, src_url, dst_url)

    def chmod(self, url: str, mode: int) -> None:
        run_sync(self._async_client.chmod, url, mode)

    def open(self, url: str, mode: str = "rb") -> Any:
        return run_sync(self._async_client.open, url, mode)

    def getxattr(self, url: str, name: str) -> str:
        return run_sync(self._async_client.getxattr, url, name)

    def setxattr(self, url: str, name: str, value: str) -> None:
        run_sync(self._async_client.setxattr, url, name, value)

    def listxattr(self, url: str) -> list[str]:
        return run_sync(self._async_client.listxattr, url)

    def xattrs(self, url: str) -> dict[str, str]:
        return run_sync(self._async_client.xattrs, url)

    def checksum(self, url: str, algorithm: str) -> str:
        return run_sync(self._async_client.checksum, url, algorithm)

    def copy(
        self,
        src_url: str,
        dst_url: str,
        options: CopyOptions | None = None,
        *,
        progress_callback: ProgressCallback = None,
        start_callback: StartCallback = None,
        warn_callback: WarnCallback = None,
        cancel_event: threading.Event | None = None,
    ) -> Any:
        return run_sync(
            self._async_client.copy,
            src_url,
            dst_url,
            options,
            progress_callback=progress_callback,
            start_callback=start_callback,
            warn_callback=warn_callback,
            cancel_event=cancel_event,
        )

    def start_copy(
        self,
        src_url: str,
        dst_url: str,
        options: CopyOptions | None = None,
        *,
        progress_callback: ProgressCallback = None,
        start_callback: StartCallback = None,
        warn_callback: WarnCallback = None,
    ) -> TransferHandle:
        return self._async_client.start_copy(
            src_url,
            dst_url,
            options,
            progress_callback=progress_callback,
            start_callback=start_callback,
            warn_callback=warn_callback,
        )

    def _map_error(self, e: Exception, url: str) -> GfalError:
        return self._async_client._map_error(e, url)


def run_sync(func: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    """Run an async client method from synchronous code."""
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(func(*args, **kwargs))

    result_holder: dict[str, Any] = {}
    exc_holder: dict[str, BaseException] = {}

    def _runner() -> None:
        try:
            result_holder["value"] = asyncio.run(func(*args, **kwargs))
        except BaseException as exc:  # pragma: no cover - loop/thread edge path
            exc_holder["error"] = exc

    thread = threading.Thread(target=_runner, daemon=True)
    thread.start()
    thread.join()
    if "error" in exc_holder:
        raise exc_holder["error"]
    return result_holder.get("value")


def parse_checksum_arg(arg: str) -> tuple[str, str | None]:
    parts = arg.split(":", 1)
    algorithm = parts[0].upper()
    expected = parts[1].lower() if len(parts) > 1 else None
    return algorithm, expected


def make_hasher(algorithm: str) -> Any:
    algorithm = algorithm.upper()
    if algorithm in ("ADLER32", "CRC32"):
        return [algorithm, 1 if algorithm == "ADLER32" else 0]
    return hashlib.new(algorithm.lower().replace("-", ""))


def update_hasher(hasher: Any, algorithm: str, chunk: bytes) -> None:
    algorithm = algorithm.upper()
    if algorithm == "ADLER32":
        hasher[1] = zlib.adler32(chunk, hasher[1]) & 0xFFFFFFFF
    elif algorithm == "CRC32":
        hasher[1] = zlib.crc32(chunk, hasher[1]) & 0xFFFFFFFF
    else:
        hasher.update(chunk)


def finalise_hasher(hasher: Any, algorithm: str) -> str:
    algorithm = algorithm.upper()
    if algorithm in ("ADLER32", "CRC32"):
        return f"{hasher[1]:08x}"
    return hasher.hexdigest()


def is_special_file(path: str) -> bool:
    try:
        mode = Path(path).stat().st_mode
        return stat.S_ISFIFO(mode) or stat.S_ISCHR(mode) or stat.S_ISSOCK(mode)
    except OSError:
        return False


def checksum_fs(
    fso: Any,
    path: str,
    algorithm: str,
    cancel_event: threading.Event | None = None,
) -> str:
    hasher = make_hasher(algorithm)
    with fso.open(path, "rb") as handle:
        while True:
            if cancel_event is not None and cancel_event.is_set():
                raise GfalError("Transfer cancelled", errno.ECANCELED)
            chunk = handle.read(fs.CHUNK_SIZE)
            if not chunk:
                break
            update_hasher(hasher, algorithm, chunk)
    return finalise_hasher(hasher, algorithm)


def compute_checksum(fso: Any, path: str, algorithm: str) -> str:
    return fs.compute_checksum(fso, path, algorithm)


def tpc_applicable(src_url: str, dst_url: str) -> bool:
    src_scheme = urlparse(src_url).scheme.lower()
    dst_scheme = urlparse(dst_url).scheme.lower()
    http = {"http", "https"}
    xrootd = {"root", "xroot"}
    return (src_scheme in http and dst_scheme in http) or (
        src_scheme in xrootd and dst_scheme in xrootd
    )


def split_timestamp_ns(timestamp: float) -> tuple[int, int]:
    seconds = int(timestamp)
    nanoseconds = int(round((timestamp - seconds) * 1_000_000_000))
    if nanoseconds >= 1_000_000_000:
        seconds += 1
        nanoseconds -= 1_000_000_000
    return seconds, nanoseconds


def _is_eos_host(hostname: str | None) -> bool:
    """Return True if *hostname* matches an EOS endpoint (``eos*.cern.ch``).

    The glob pattern ``eos*.cern.ch`` is matched literally: the hostname must
    start with ``eos`` and end with ``.cern.ch``.  Both ``eos.cern.ch`` and
    ``eospilot.cern.ch`` are valid EOS hostnames.  Hostnames that merely contain
    "eos" (e.g. ``myeos.example.org``) are intentionally excluded.
    """
    if not hostname:
        return False
    h = hostname.lower()
    return h.startswith("eos") and h.endswith(".cern.ch")


def eos_app_url(url: str, app: str) -> str | None:
    """Return *url* with ``eos.app=<app>`` added to the query string.

    :param url: The URL to annotate.  Must use one of the ``http``, ``https``,
        ``root``, or ``xroot`` schemes and target an EOS endpoint
        (hostname matching ``eos*.cern.ch``).
    :param app: The application name to set, e.g. ``python3-gfal-cli``.

    Returns ``None`` when the URL does not point to an EOS endpoint.
    An existing ``eos.app`` value is never overwritten.
    """
    normalized = fs.normalize_url(url)
    parsed = urlparse(normalized)
    if parsed.scheme.lower() not in {"http", "https", "root", "xroot"}:
        return None
    if not _is_eos_host(parsed.hostname):
        return None
    params = dict(parse_qsl(parsed.query, keep_blank_values=True))
    if "eos.app" not in params:
        params["eos.app"] = app
    return urlunparse(parsed._replace(query=urlencode(params)))


def eos_mtime_url(url: str, timestamp: float) -> str | None:
    normalized = fs.normalize_url(url)
    parsed = urlparse(normalized)
    if parsed.scheme.lower() not in {"http", "https", "root", "xroot"}:
        return None
    if "eos" not in (parsed.hostname or "").lower():
        return None

    seconds, nanoseconds = split_timestamp_ns(timestamp)
    params = dict(parse_qsl(parsed.query, keep_blank_values=True))
    params["eos.mtime"] = (
        str(seconds) if nanoseconds == 0 else f"{seconds}.{nanoseconds:09d}"
    )
    return urlunparse(parsed._replace(query=urlencode(params)))


def local_destination_path(dst_url: str, dst_path: str) -> Path | None:
    normalized = fs.normalize_url(dst_url)
    if urlparse(normalized).scheme.lower() != "file":
        return None
    return Path(dst_path)
