"""
WebDAV filesystem adapter for HTTP/HTTPS endpoints.

Provides directory listing (PROPFIND), directory creation (MKCOL), and
deletion (DELETE/MOVE) on top of fsspec's HTTPFileSystem, which only handles
file reads and writes natively.

All "path" arguments here are full URLs (e.g. ``https://server/dir/``),
matching the convention used by fsspec's HTTPFileSystem.
"""

from __future__ import annotations

import contextlib
import io
import re
import ssl
import stat as stat_module
import tempfile
from email.utils import parsedate_to_datetime
from typing import Literal, Optional
from urllib.parse import unquote, urlparse, urlunparse
from xml.etree import ElementTree as ET

import fsspec
import requests as _requests
from fsspec import AbstractFileSystem
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

_DAV = "{DAV:}"


def _norm_url(url: str) -> Literal[b""]:
    """Normalize a URL for comparison by collapsing repeated slashes in the path.

    EOS WebDAV returns hrefs with a single leading slash (e.g. /eos/...)
    while callers may supply double-slash URLs (https://host//eos/...).

    Stripping trailing slashes and collapsing ``//`` \u2192 ``/`` in the path
    component ensures the self-entry is always identified correctly in ls().
    """
    p = urlparse(url.rstrip("/"))
    return urlunparse(p._replace(path=re.sub(r"/+", "/", p.path)))


_PROPFIND_BODY = (
    '<?xml version="1.0" encoding="utf-8"?>'
    '<D:propfind xmlns:D="DAV:"><D:allprop/></D:propfind>'
)


# ---------------------------------------------------------------------------
# Session factory
# ---------------------------------------------------------------------------


def _make_ssl_context(verify: bool) -> ssl.SSLContext:
    """Return an SSL context for the requests session.

    Sets ``ssl.OP_IGNORE_UNEXPECTED_EOF`` (Python 3.12+) so that servers
    like EOS HTTPS that close the TLS connection without a proper
    ``close_notify`` alert do not trigger ``SSLEOFError``.
    """
    if verify:
        ctx = ssl.create_default_context()
    else:
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
    # Python 3.12 raises SSLEOFError when a server closes the TLS connection
    # without sending close_notify (EOS does this after large PUT uploads).
    # OP_IGNORE_UNEXPECTED_EOF suppresses that behaviour.
    with contextlib.suppress(AttributeError):
        ctx.options |= ssl.OP_IGNORE_UNEXPECTED_EOF  # type: ignore[attr-defined]
    return ctx


class _SSLContextAdapter(HTTPAdapter):
    """HTTPAdapter that injects a custom SSL context into urllib3's pool manager."""

    def __init__(self, ssl_context: ssl.SSLContext, **kwargs):
        self._ssl_context = ssl_context
        super().__init__(**kwargs)

    def init_poolmanager(self, *args, **kwargs):
        kwargs["ssl_context"] = self._ssl_context
        return super().init_poolmanager(*args, **kwargs)


# WebDAV methods that are safe to retry on connection errors.
# Standard idempotent methods plus the WebDAV-specific ones.
_RETRY_METHODS = frozenset(
    [
        "DELETE",
        "GET",
        "HEAD",
        "OPTIONS",
        "PUT",
        "TRACE",
        "PROPFIND",
        "MKCOL",
        "MOVE",
        "COPY",
    ]
)


def _make_session(storage_options):
    """Build a ``requests.Session`` with cert / SSL-verify config.

    A ``Retry`` adapter is mounted for both http:// and https:// to
    transparently retry transient connection errors (e.g. WinError 10053 /
    ECONNABORTED on Windows when the server closes a keep-alive socket before
    the client reuses it).  HTTP error status codes are *not* retried so that
    real 4xx/5xx responses are never silently hidden.
    """
    verify = storage_options.get("ssl_verify", True)
    ssl_ctx = _make_ssl_context(verify)
    _retry = Retry(
        total=10,
        connect=5,
        read=5,
        status=0,
        backoff_factor=0.5,
        raise_on_status=False,
        allowed_methods=_RETRY_METHODS,
    )
    _adapter = _SSLContextAdapter(ssl_context=ssl_ctx, max_retries=_retry)
    session = _requests.Session()
    session.mount("http://", _adapter)
    session.mount("https://", _adapter)

    # Note: We do NOT set session.verify = False even when verify is False.
    # Instead, we rely on the custom SSLContext (with verify_mode=CERT_NONE
    # and the OP_IGNORE_UNEXPECTED_EOF flag) to handle the connection correctly.
    session.verify = True

    cert = storage_options.get("client_cert")
    key = storage_options.get("client_key")
    if cert:
        session.cert = (cert, key) if key else cert

    if not verify:
        import urllib3

        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    bearer_token = storage_options.get("bearer_token")
    if bearer_token:
        session.headers.update({"Authorization": f"Bearer {bearer_token}"})
    return session


def _http_fs_opts(storage_options):
    """Convert storage_options to kwargs for fsspec's HTTPFileSystem."""
    from functools import partial

    from gfal.core.fs import _no_verify_get_client, _verify_get_client

    opts = {k: v for k, v in storage_options.items() if k != "ssl_verify"}
    verify = storage_options.get("ssl_verify", True)
    ipv4_only = storage_options.get("ipv4_only", False)
    ipv6_only = storage_options.get("ipv6_only", False)
    timeout = storage_options.get("timeout")
    # Pull client cert out of opts: we load it directly into the aiohttp SSL
    # context via get_client so it doesn't conflict with our custom SSL context.
    client_cert = opts.pop("client_cert", None)
    client_key = opts.pop("client_key", None)

    if not verify:
        opts["get_client"] = partial(
            _no_verify_get_client,
            client_cert=client_cert,
            client_key=client_key,
            ipv4_only=ipv4_only,
            ipv6_only=ipv6_only,
            timeout=timeout,
        )
    else:
        opts["get_client"] = partial(
            _verify_get_client,
            verify=True,
            client_cert=client_cert,
            client_key=client_key,
            ipv4_only=ipv4_only,
            ipv6_only=ipv6_only,
            timeout=timeout,
        )
    return opts


# ---------------------------------------------------------------------------
# PROPFIND XML parser
# ---------------------------------------------------------------------------


def _parse_propfind(xml_bytes: bytes, base_url: str) -> list[dict]:
    """Parse a WebDAV PROPFIND response body into fsspec-style info dicts."""
    try:
        root = ET.fromstring(xml_bytes)
    except ET.ParseError:
        return []

    parsed_base = urlparse(base_url)
    entries = []

    for resp_el in root.findall(f"{_DAV}response"):
        href_el = resp_el.find(f"{_DAV}href")
        if href_el is None or not href_el.text:
            continue
        href = href_el.text.strip()

        # Find the propstat with HTTP 200 status
        prop = None
        for ps in resp_el.findall(f"{_DAV}propstat"):
            st_el = ps.find(f"{_DAV}status")
            if st_el is not None and " 200 " in (st_el.text or ""):
                prop = ps.find(f"{_DAV}prop")
                break
        if prop is None:
            # Accept first propstat regardless of status
            ps0 = resp_el.find(f"{_DAV}propstat")
            if ps0 is not None:
                prop = ps0.find(f"{_DAV}prop")
        if prop is None:
            continue

        # Reconstruct full URL
        if href.startswith(("http://", "https://")):
            entry_url = href
        else:
            entry_url = urlunparse(parsed_base._replace(path=href))

        # Directory?
        rt = prop.find(f"{_DAV}resourcetype")
        is_dir = rt is not None and rt.find(f"{_DAV}collection") is not None

        # File size
        size = 0
        sz_el = prop.find(f"{_DAV}getcontentlength")
        if sz_el is not None and sz_el.text:
            with contextlib.suppress(ValueError):
                size = int(sz_el.text)

        # Modification time
        mtime = 0.0
        mt_el = prop.find(f"{_DAV}getlastmodified")
        if mt_el is not None and mt_el.text:
            with contextlib.suppress(Exception):
                mtime = parsedate_to_datetime(mt_el.text).timestamp()

        entries.append(
            {
                "name": entry_url,
                "size": size,
                "type": "directory" if is_dir else "file",
                "mtime": mtime,
                "mode": (stat_module.S_IFDIR | 0o755)
                if is_dir
                else (stat_module.S_IFREG | 0o644),
            }
        )

    return entries


# ---------------------------------------------------------------------------
# Error mapping
# ---------------------------------------------------------------------------


def _raise_for_status(resp, url: str) -> None:
    """Map HTTP error responses to Python exceptions."""
    sc = resp.status_code
    if sc == 404:
        raise FileNotFoundError(
            f"[Errno 2] No such file or directory: {unquote(url)!r}"
        )
    if sc == 403:
        err = PermissionError(f"[Errno 13] Permission denied: {unquote(url)!r}")
        err.errno = 13
        raise err
    if sc == 401:
        err = PermissionError(f"[Errno 13] Authentication required: {unquote(url)!r}")
        err.errno = 13
        raise err
    if sc == 405:
        raise NotImplementedError(
            f"Server does not support this WebDAV method (HTTP 405): {url}"
        )
    if sc >= 400:
        resp.raise_for_status()


# ---------------------------------------------------------------------------
# Write-mode file object (HTTP PUT)
# ---------------------------------------------------------------------------


class _RequestsPutFile(io.RawIOBase):
    """Write-only file object that buffers data and sends an HTTP PUT on close.

    Up to 64 MiB is kept in memory (via SpooledTemporaryFile); beyond that the
    data is spilled to a temporary file on disk.  The PUT is issued as a single
    streaming request when close() is called, so the server never sees a
    partial upload unless close() raises an exception.
    """

    def __init__(self, session, url: str, timeout: Optional[float] = None) -> None:
        self._session = session
        self._url = url
        self._timeout = timeout
        self._buf: tempfile.SpooledTemporaryFile = tempfile.SpooledTemporaryFile(  # noqa: SIM115
            max_size=64 * 1024 * 1024
        )

    # io.RawIOBase interface
    def readable(self) -> bool:
        return False

    def writable(self) -> bool:
        return True

    def write(self, b) -> int:  # type: ignore[override]
        return self._buf.write(b)

    def close(self) -> None:
        if not self.closed:
            try:
                self._buf.seek(0)
                # Read the entire buffer. Since we use SpooledTemporaryFile with
                # max_size=64MiB, this is safe and allows requests/urllib3 to
                # perform retries automatically (which it cannot easily do with
                # a raw file-like object that doesn't support automatic rewinding).
                data = self._buf.read()
                # Manual retry for SSL/Connection errors that urllib3 might not
                # catch or retry aggressively enough (e.g. some SSLEOFError cases).
                import random
                import time

                from requests.exceptions import ConnectionError, SSLError

                for attempt in range(5):
                    try:
                        resp = self._session.put(
                            self._url, data=data, timeout=self._timeout
                        )
                        _raise_for_status(resp, self._url)
                        break
                    except _requests.exceptions.Timeout as e:
                        raise TimeoutError(self._url) from e
                    except (SSLError, ConnectionError):
                        if attempt == 4:
                            raise
                        delay = (attempt + 1) * 0.5 + random.random()
                        # session.close() is a bit too harsh; it closes the entire adapter.
                        # We just want to allow a retry on a fresh connection.
                        time.sleep(delay)
            finally:
                self._buf.close()
                super().close()


# ---------------------------------------------------------------------------
# WebDAV filesystem
# ---------------------------------------------------------------------------


class WebDAVFileSystem(AbstractFileSystem):
    """
    Filesystem adapter for HTTP/HTTPS/WebDAV endpoints.

    - ``ls`` / ``info``    \u2014 WebDAV PROPFIND (falls back to HEAD for info on
                             non-WebDAV servers so plain-HTTP file access works)
    - ``mkdir``            \u2014 WebDAV MKCOL
    - ``makedirs``         \u2014 iterative MKCOL from the root down
    - ``rm`` / ``rmdir``   \u2014 HTTP DELETE
    - ``mv``               \u2014 WebDAV MOVE
    - ``open``             \u2014 delegated to fsspec's HTTPFileSystem (GET/PUT)
    - ``chmod``            \u2014 no-op (HTTP has no permission model)
    """

    def __init__(self, storage_options: Optional[dict] = None) -> None:
        self._opts = dict(storage_options or {})
        self._verify = self._opts.get("ssl_verify", True)
        self._timeout = self._opts.get("timeout")
        self._session = _make_session(self._opts)
        self._http_fs = fsspec.filesystem("http", **_http_fs_opts(self._opts))

    # ------------------------------------------------------------------
    # PROPFIND helpers
    # ------------------------------------------------------------------

    def _propfind(self, url: str, depth: int = 0) -> list[dict]:
        """Send PROPFIND and return parsed entries."""
        resp = self._session.request(
            "PROPFIND",
            url,
            headers={
                "Depth": str(depth),
                "Content-Type": "application/xml; charset=utf-8",
            },
            data=_PROPFIND_BODY.encode(),
            timeout=self._timeout,
        )
        _raise_for_status(resp, url)
        return _parse_propfind(resp.content, url)

    # ------------------------------------------------------------------
    # stat / ls
    # ------------------------------------------------------------------

    def info(self, path: str) -> dict:
        """Return an info dict for *path* (file or directory)."""
        # Try PROPFIND Depth:0 first \u2014 works for both files and directories.
        try:
            entries = self._propfind(path, depth=0)
            if entries:
                return entries[0]
        except (_requests.exceptions.SSLError, _requests.exceptions.ConnectionError):
            # Re-raise only when the user has NOT opted out of SSL verification.
            # With --no-verify (ssl_verify=False) fall through to _http_fs.info()
            # which uses aiohttp with a fully-disabled SSL context.
            if self._verify:
                raise
        except NotImplementedError:
            pass  # 405: server doesn't support WebDAV; fall through to HEAD
        except Exception:
            # For other errors (e.g. 403, 500 on PROPFIND), we fall back to HEAD
            # but ONLY if we haven't already failed SSL.
            pass
        # Fall back to fsspec's HTTP HEAD request (works for any plain-HTTP file)
        result = dict(self._http_fs.info(path))
        # Heuristic: plain HTTP servers can't tell us a resource is a directory,
        # but we can infer it from the URL (trailing slash) or Content-Type.
        mimetype = str(result.get("mimetype") or "")
        if path.endswith("/") or "text/html" in mimetype:
            result["type"] = "directory"
            result.setdefault("mode", stat_module.S_IFDIR | 0o755)
        return result

    def ls(self, path: str, detail: bool = True):
        """List directory contents via PROPFIND Depth:1."""
        # Use a trailing slash so the server knows we mean the collection
        url = path.rstrip("/") + "/"
        try:
            entries = self._propfind(url, depth=1)
        except NotImplementedError:
            # Non-WebDAV server: fall back to returning the single resource
            # info so that ``gfal-ls <file-url>`` still works on plain HTTP.
            info = self.info(path)
            return [info] if detail else [info["name"]]

        # Separate the self-entry (the collection itself) from its children.
        # EOS returns href paths with a single slash (e.g. /eos/...) while the
        # request URL may use double slashes (https://host//eos/...).  Normalise
        # both sides by collapsing consecutive slashes in the URL path component
        # before comparing so the self-entry is always filtered out correctly.
        path_norm = _norm_url(path)
        self_entries = [e for e in entries if _norm_url(e["name"]) == path_norm]
        children = [e for e in entries if _norm_url(e["name"]) != path_norm]

        # If PROPFIND returned only the self-entry AND it is a file (not a
        # collection), the path refers to a single file \u2014 return it as-is.
        if not children and self_entries and self_entries[0].get("type") != "directory":
            return self_entries if detail else [e["name"] for e in self_entries]

        # Normal case: return children (may be empty for an empty directory)
        return children if detail else [e["name"] for e in children]

    def isdir(self, path: str) -> bool:
        try:
            return self.info(path).get("type") == "directory"
        except Exception:
            return False

    # ------------------------------------------------------------------
    # mkdir
    # ------------------------------------------------------------------

    def mkdir(self, path: str, create_parents: bool = False, **kwargs) -> None:
        """Create a directory via WebDAV MKCOL."""
        if create_parents:
            self.makedirs(path, exist_ok=True)
            return
        resp = self._session.request("MKCOL", path, timeout=self._timeout)
        if resp.status_code == 201:
            return
        if resp.status_code in (301, 405):
            raise FileExistsError(f"[Errno 17] File exists: {path!r}")
        if resp.status_code == 409:
            raise FileNotFoundError(
                f"[Errno 2] Intermediate directory does not exist: {path!r}"
            )
        _raise_for_status(resp, path)

    def makedirs(self, path: str, exist_ok: bool = False) -> None:
        """Create *path* and all missing ancestors via MKCOL."""
        parsed = urlparse(path)
        # Split path into components, rebuild from the root down
        parts = [p for p in parsed.path.rstrip("/").split("/") if p]
        for i in range(1, len(parts) + 1):
            partial_path = "/" + "/".join(parts[:i])
            partial_url = urlunparse(parsed._replace(path=partial_path))
            resp = self._session.request("MKCOL", partial_url, timeout=self._timeout)
            sc = resp.status_code
            if sc == 201:
                continue  # created
            if sc in (301, 405):
                continue  # already exists \u2014 fine
            if sc == 409:
                # Conflict: intermediate missing \u2192 shouldn't happen top-down but skip
                continue
            if sc == 403:
                # Might not have permission to create ancestors; try to continue
                continue
            if sc >= 400:
                resp.raise_for_status()

    # ------------------------------------------------------------------
    # rm / rmdir
    # ------------------------------------------------------------------

    def rm(self, path: str, recursive: bool = False) -> None:
        """Delete a file or directory via HTTP DELETE."""
        resp = self._session.delete(path, timeout=self._timeout)
        _raise_for_status(resp, path)

    def rmdir(self, path: str) -> None:
        self.rm(path)

    def rm_file(self, path: str) -> None:
        self.rm(path)

    # ------------------------------------------------------------------
    # rename / move
    # ------------------------------------------------------------------

    def mv(self, path1: str, path2: str, **kwargs) -> None:
        """Rename/move via WebDAV MOVE."""
        resp = self._session.request(
            "MOVE",
            path1,
            headers={"Destination": path2, "Overwrite": "T"},
            timeout=self._timeout,
        )
        resp.raise_for_status()

    # ------------------------------------------------------------------
    # permissions
    # ------------------------------------------------------------------

    def chmod(self, path: str, mode: int) -> None:
        pass  # HTTP has no permission model

    # ------------------------------------------------------------------
    # file I/O \u2014 delegate to fsspec's HTTPFileSystem
    # ------------------------------------------------------------------

    def open(self, path: str, mode: str = "rb", **kwargs):
        if "w" in mode:
            return _RequestsPutFile(self._session, path, self._timeout)
        return self._http_fs.open(path, mode, **kwargs)

    def checksum(self, path: str, algorithm: str) -> str:
        """Fetch server-side checksum via HTTP HEAD and the Digest header."""
        alg_lower = algorithm.lower()

        # Ask the server to return the digest (RFC 3230)
        headers = {"Want-Digest": alg_lower}
        resp = self._session.head(path, headers=headers, timeout=self._timeout)
        _raise_for_status(resp, path)

        digest_header = resp.headers.get("Digest")
        if not digest_header:
            raise NotImplementedError(
                "Server-side checksum is not available (no Digest header returned)"
            )

        # Digest can be a comma-separated list: "md5=X, adler32=Y"
        for piece in digest_header.split(","):
            piece = piece.strip()
            if "=" in piece:
                name, val = piece.split("=", 1)
                if name.lower() == alg_lower:
                    return val

        raise NotImplementedError(
            f"Server returned Digest header but missing requested algorithm {algorithm}: {digest_header}"
        )
