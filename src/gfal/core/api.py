from __future__ import annotations

from typing import Any, Optional

from gfal.core import fs
from gfal.core.errors import (
    GfalError,
    GfalFileExistsError,
    GfalFileNotFoundError,
    GfalIsADirectoryError,
    GfalNotADirectoryError,
    GfalPermissionError,
    GfalTimeoutError,
)


class GfalClient:
    """A high-level library API for GFAL-like operations using fsspec."""

    def __init__(
        self,
        cert: Optional[str] = None,
        key: Optional[str] = None,
        timeout: int = 1800,
        ssl_verify: bool = True,
    ):
        """
        Initialize the GFAL client.

        Args:
            cert: Path to client certificate (X.509 PEM or proxy).
            key: Path to private key (defaults to cert if omitted).
            timeout: Operation timeout in seconds.
            ssl_verify: Whether to verify SSL certificates.
        """
        self.cert = cert
        self.key = key or cert
        self.timeout = timeout
        self.ssl_verify = ssl_verify

    @property
    def storage_options(self) -> dict[str, Any]:
        """Compute storage options for fsspec based on client configuration."""
        options = {
            "client_cert": self.cert,
            "client_key": self.key,
            "timeout": self.timeout,
            "ssl_verify": self.ssl_verify,
        }
        # Filter out None values to avoid passing them to fsspec backends that don't expect them
        return {k: v for k, v in options.items() if v is not None}

    def stat(self, url: str) -> fs.StatInfo:
        """Get file status."""
        try:
            fso, path = fs.url_to_fs(url, self.storage_options)
            info = fso.info(path)
            info = fs.xrootd_enrich(info, fso)
            return fs.StatInfo(info)
        except Exception as e:
            raise self._map_error(e, url) from e

    def ls(self, url: str, detail: bool = True) -> list[Any]:
        """List directory contents."""
        try:
            fso, path = fs.url_to_fs(url, self.storage_options)
            try:
                raw_entries = fso.ls(path, detail=True)
            except OSError as e:
                # XRootD/fsspec fallback for files instead of directories
                msg = str(e).lower()
                if (
                    any(
                        m in msg
                        for m in ["not a directory", "unable to open directory"]
                    )
                    or getattr(e, "errno", None) == 20
                ):
                    raw_entries = [fso.info(path)]
                else:
                    raise
        except Exception as e:
            raise self._map_error(e, url) from e

        if not detail:
            # Return list of basenames or full paths? fsspec returns absolute-ish paths.
            # Use basenames to match gfal-ls default.
            import pathlib

            return [pathlib.Path(e["name"].rstrip("/")).name for e in raw_entries]

        return [fs.StatInfo(e) for e in raw_entries]

    def mkdir(self, url: str, mode: int = 0o755, parents: bool = False):
        """Create a directory."""
        try:
            fso, path = fs.url_to_fs(url, self.storage_options)
            if parents:
                if hasattr(fso, "makedirs"):
                    fso.makedirs(path, exist_ok=True)
                else:
                    import contextlib

                    with contextlib.suppress(FileExistsError):
                        fso.mkdir(path, create_parents=True)
            else:
                fso.mkdir(path, create_parents=False)

            # Apply mode Best-effort
            import contextlib

            with contextlib.suppress(Exception):
                fso.chmod(path, mode)
        except Exception as e:
            raise self._map_error(e, url) from e

    def rm(self, url: str, recursive: bool = False):
        """Remove a file or directory."""
        try:
            fso, path = fs.url_to_fs(url, self.storage_options)
            fso.rm(path, recursive=recursive)
        except Exception as e:
            raise self._map_error(e, url) from e

    def rmdir(self, url: str):
        """Remove an empty directory."""
        try:
            fso, path = fs.url_to_fs(url, self.storage_options)
            fso.rmdir(path)
        except Exception as e:
            raise self._map_error(e, url) from e

    def rename(self, src_url: str, dst_url: str):
        """Rename/move a file or directory."""
        try:
            src_fs, src_path = fs.url_to_fs(src_url, self.storage_options)
            dst_fs, dst_path = fs.url_to_fs(dst_url, self.storage_options)
            if type(src_fs) is not type(dst_fs):
                # Import errno locally to avoid top-level dependency if not needed elsewhere
                import errno

                raise GfalError(
                    "Rename across different filesystem types is not supported",
                    errno.EXDEV,
                )
            # XRootD optimization: use native mv to avoid NotImplementedError from fsspec.mv()
            if hasattr(src_fs, "_myclient"):
                status, _ = src_fs._myclient.mv(src_path, dst_path)
                if not status.ok:
                    raise OSError(status.errno or 1, status.message)
                return

            src_fs.mv(src_path, dst_path)
        except Exception as e:
            raise self._map_error(e, src_url) from e

    def chmod(self, url: str, mode: int):
        """Change file permissions."""
        try:
            fso, path = fs.url_to_fs(url, self.storage_options)
            fso.chmod(path, mode)
        except Exception as e:
            raise self._map_error(e, url) from e

    def open(self, url: str, mode: str = "rb"):
        """
        Open a file for reading or writing.

        Args:
            url: The URI of the file.
            mode: The mode to open the file in (e.g., 'rb', 'wb').

        Returns:
            A file-like object.
        """
        try:
            fso, path = fs.url_to_fs(url, self.storage_options)
            return fso.open(path, mode)
        except Exception as e:
            raise self._map_error(e, url) from e

    def getxattr(self, url: str, name: str) -> str:
        """Get an extended attribute."""
        try:
            fso, path = fs.url_to_fs(url, self.storage_options)
            if not hasattr(fso, "getxattr"):
                import errno

                raise GfalError(
                    f"xattr not supported by filesystem for {url}", errno.EOPNOTSUPP
                )
            return str(fso.getxattr(path, name))
        except Exception as e:
            raise self._map_error(e, url) from e

    def setxattr(self, url: str, name: str, value: str):
        """Set an extended attribute."""
        try:
            fso, path = fs.url_to_fs(url, self.storage_options)
            if not hasattr(fso, "setxattr"):
                import errno

                raise GfalError(
                    f"xattr not supported by filesystem for {url}", errno.EOPNOTSUPP
                )
            fso.setxattr(path, name, value)
        except Exception as e:
            raise self._map_error(e, url) from e

    def listxattr(self, url: str) -> list[str]:
        """List extended attributes."""
        try:
            fso, path = fs.url_to_fs(url, self.storage_options)
            if not hasattr(fso, "listxattr"):
                import errno

                raise GfalError(
                    f"xattr not supported by filesystem for {url}", errno.EOPNOTSUPP
                )
            return fso.listxattr(path)
        except Exception as e:
            raise self._map_error(e, url) from e

    def checksum(self, url: str, algorithm: str) -> str:
        """Compute a file checksum."""
        try:
            fso, path = fs.url_to_fs(url, self.storage_options)
            return fs.compute_checksum(fso, path, algorithm.upper())
        except Exception as e:
            raise self._map_error(e, url) from e

    def _map_error(self, e: Exception, url: str) -> GfalError:
        """Map generic exceptions to GfalError subclasses."""
        if isinstance(e, GfalError):
            return e

        msg = str(e)
        if not msg:
            msg = f"({type(e).__name__})"

        # Mapping standard library exceptions
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

        # HTTP status mapping (aiohttp style)
        status = getattr(e, "status", None)
        if status == 403:
            return GfalPermissionError(f"{url}: Permission denied")
        if status == 404:
            return GfalFileNotFoundError(f"{url}: No such file or directory")

        # Fallback
        import errno

        code = getattr(e, "errno", None)
        if not isinstance(code, int) or code == 0:
            code = errno.EIO
        return GfalError(msg, code=code)
