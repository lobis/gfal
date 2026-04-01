"""
fsspec integration layer: URL normalization, filesystem acquisition,
and a stat-like wrapper around fsspec info() dicts.
"""

import contextlib
import hashlib
import os
import stat as stat_module
import sys
import zlib
from pathlib import Path
from typing import Any
from urllib.parse import urlparse, urlunparse

import fsspec

CHUNK_SIZE = 4 * 1024 * 1024  # 4 MiB


# ---------------------------------------------------------------------------
# XRootD plugin path fix (macOS)
# ---------------------------------------------------------------------------


def _fix_xrootd_plugin_path():
    """No-op: the DYLD_LIBRARY_PATH fix is handled at startup in shell.py."""


# ---------------------------------------------------------------------------
# SSL helpers
# ---------------------------------------------------------------------------


def get_ssl_context(verify=True):
    """Return an ssl.SSLContext.

    Uses the `truststore` library (if available) to leverage the system
    trust store on macOS and Windows for verified connections.
    """
    import ssl

    if not verify:
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        return ctx

    try:
        import truststore

        return truststore.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    except (ImportError, AttributeError):
        return ssl.create_default_context()


async def _no_verify_get_client(loop=None, **kwargs):
    """aiohttp client factory for fsspec without SSL verification."""
    return await _verify_get_client(loop=loop, verify=False, **kwargs)


async def _verify_get_client(
    loop=None,
    verify=True,
    client_cert=None,
    client_key=None,
    ipv4_only=False,
    ipv6_only=False,
    **kwargs,
):
    """aiohttp client factory for fsspec with system trust (truststore) and IP family support."""
    import socket

    import aiohttp

    ctx = get_ssl_context(verify=verify)
    if client_cert:
        ctx.load_cert_chain(client_cert, client_key or client_cert)
    family = socket.AF_INET if ipv4_only else (socket.AF_INET6 if ipv6_only else 0)
    return aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=ctx, family=family))


# ---------------------------------------------------------------------------
# URL helpers
# ---------------------------------------------------------------------------


def normalize_url(url):
    """
    Convert bare local paths to file:// URLs.
    Maps dav:// -> http:// and davs:// -> https://.
    """
    if url == "-":
        return url
    parsed = urlparse(url)
    scheme = parsed.scheme.lower()
    # A single-char scheme is a Windows drive letter (e.g. "C:"), not a real URL scheme
    if not scheme or len(scheme) == 1:
        p = Path(url)
        if not p.is_absolute():
            p = Path.cwd() / p
        return p.as_uri()
    if scheme == "dav":
        return urlunparse(parsed._replace(scheme="http"))
    if scheme == "davs":
        return urlunparse(parsed._replace(scheme="https"))
    return url


def url_to_fs(url, storage_options=None, **kwargs):
    """
    Return (AbstractFileSystem, path) for a URL.

    storage_options are forwarded to the filesystem constructor.
    For HTTP(S) these may include 'client_cert'/'client_key'.
    For XRootD auth is handled via X509_* environment variables.
    """
    storage_options = {} if storage_options is None else dict(storage_options)

    storage_options.update(kwargs)

    url = normalize_url(url)
    parsed = urlparse(url)
    scheme = parsed.scheme.lower()

    if scheme in ("http", "https"):
        from gfal.core.webdav import WebDAVFileSystem

        return WebDAVFileSystem(storage_options), url

    if scheme in ("root", "xroot"):
        _fix_xrootd_plugin_path()
        try:
            fs, path = fsspec.url_to_fs(url, **storage_options)
        except Exception as e:
            cause = e.__cause__ or e
            raise RuntimeError(
                f"Cannot load XRootD filesystem: {cause}\n"
                "Install the XRootD Python bindings: python3 -m pip install xrootd"
            ) from e
        return fs, path

    if scheme == "file":
        fso = fsspec.filesystem("file")
        path = parsed.path
        # On Windows urlparse gives "/C:/..." — strip the leading slash
        if (
            sys.platform == "win32"
            and len(path) > 2
            and path[0] == "/"
            and path[2] == ":"
        ):
            path = path[1:]
        return fso, path

    # fallback
    fs, path = fsspec.url_to_fs(url, **storage_options)
    return fs, path


def build_storage_options(params):
    """Build fsspec storage_options from parsed CLI params.

    Picks up the X.509 proxy auto-detected by base.py (X509_USER_PROXY) so
    that HTTP/HTTPS sessions also present the client certificate when no
    explicit --cert flag was given.
    """
    opts = {}
    cert = getattr(params, "cert", None)
    key = getattr(params, "key", None)
    if not cert:
        # Fall back to the proxy auto-detected (or user-set) in the environment.
        proxy = os.environ.get("X509_USER_PROXY")
        if proxy and Path(proxy).is_file():
            cert = proxy
            key = proxy
    if cert:
        opts["client_cert"] = cert
        opts["client_key"] = key or cert
    if getattr(params, "ipv4_only", False):
        opts["ipv4_only"] = True
    if getattr(params, "ipv6_only", False):
        opts["ipv6_only"] = True
    if not getattr(params, "ssl_verify", True):
        opts["ssl_verify"] = False
    # Bearer token / macaroon: read from standard WLCG env vars.
    # BEARER_TOKEN takes priority; fall back to BEARER_TOKEN_FILE.
    token = os.environ.get("BEARER_TOKEN")
    if not token:
        token_file = os.environ.get("BEARER_TOKEN_FILE")
        if token_file:
            with contextlib.suppress(OSError):
                token = Path(token_file).read_text().strip()
    if token:
        opts["bearer_token"] = token
    return opts


# ---------------------------------------------------------------------------
# Stat wrapper
# ---------------------------------------------------------------------------


class StatInfo:
    """
    Wraps an fsspec info() dict as a POSIX stat-like object.

    Fields that the underlying filesystem doesn't provide are filled with
    sensible defaults so the rest of the code can always access them.
    """

    __slots__ = (
        "_info",
        "st_size",
        "st_mode",
        "st_uid",
        "st_gid",
        "st_nlink",
        "st_mtime",
        "st_atime",
        "st_ctime",
    )

    def __init__(self, info):
        self._info = info

        self.st_size = int(info.get("size") or 0)

        raw_mode = info.get("mode")
        if raw_mode is not None:
            self.st_mode = int(raw_mode)
        elif info.get("type") == "directory":
            self.st_mode = stat_module.S_IFDIR | 0o755
        else:
            self.st_mode = stat_module.S_IFREG | 0o644

        self.st_uid = int(info.get("uid") or 0)
        self.st_gid = int(info.get("gid") or 0)
        self.st_nlink = int(info.get("nlink") or 1)
        self.st_mtime = float(info.get("mtime") or 0)
        self.st_atime = float(info.get("atime") or self.st_mtime)
        self.st_ctime = float(info.get("ctime") or self.st_mtime)

    @property
    def info(self) -> dict[str, Any]:
        """Return the raw fsspec info dict."""
        return self._info


def _xrootd_flags_to_mode(flags):
    """Convert XRootD StatInfoFlags to a POSIX file mode integer."""
    from XRootD.client.flags import StatInfoFlags

    is_dir = bool(flags & StatInfoFlags.IS_DIR)
    is_readable = bool(flags & StatInfoFlags.IS_READABLE)
    is_writable = bool(flags & StatInfoFlags.IS_WRITABLE)

    if is_dir:
        ftype = stat_module.S_IFDIR
        perms = (0o555 if is_readable else 0) | (0o200 if is_writable else 0)
    else:
        ftype = stat_module.S_IFREG
        perms = (0o444 if is_readable else 0) | (0o200 if is_writable else 0)
    return ftype | perms


def xrootd_enrich(info, fso):
    """
    Enrich a single XRootD info dict with mtime and mode.

    fsspec-xrootd's _info() discards modtime and flags; we recover them
    via a direct _myclient.stat() call and add them back.
    """
    if not hasattr(fso, "_myclient"):
        return info
    try:
        from XRootD.client.flags import StatInfoFlags  # noqa: F401
    except ImportError:
        return info

    path = info.get("name", "")
    timeout = getattr(fso, "timeout", 30)
    status, st = fso._myclient.stat(path, timeout=timeout)
    if not status.ok:
        return info

    enriched = dict(info)
    enriched["mtime"] = st.modtime
    enriched["mode"] = _xrootd_flags_to_mode(st.flags)
    return enriched


def xrootd_ls_enrich(fso, path):
    """
    Directory listing for XRootD with mtime and mode included.

    Calls _myclient.dirlist(DirListFlags.STAT) directly to capture the
    statinfo fields that fsspec-xrootd discards in its _ls() method.
    Falls back to fso.ls(path, detail=True) on any error.
    """
    if not hasattr(fso, "_myclient"):
        return fso.ls(path, detail=True)
    try:
        from XRootD.client.flags import DirListFlags, StatInfoFlags  # noqa: F401
    except ImportError:
        return fso.ls(path, detail=True)

    timeout = getattr(fso, "timeout", 30)
    status, deets = fso._myclient.dirlist(path, DirListFlags.STAT, timeout=timeout)
    if not status.ok:
        return fso.ls(path, detail=True)

    entries = []
    for item in deets:
        flags = item.statinfo.flags
        is_dir = bool(flags & StatInfoFlags.IS_DIR)
        entries.append(
            {
                "name": path + "/" + item.name,
                "size": item.statinfo.size,
                "type": "directory" if is_dir else "file",
                "mtime": item.statinfo.modtime,
                "mode": _xrootd_flags_to_mode(flags),
                "nlink": 0,
                "uid": 0,
                "gid": 0,
            }
        )
    return entries


def stat(url, storage_options=None):
    """Stat a URL, returning a StatInfo."""
    fs, path = url_to_fs(url, storage_options)
    return StatInfo(fs.info(path))


def isdir(url, storage_options=None):
    fs, path = url_to_fs(url, storage_options)
    try:
        return fs.isdir(path)
    except Exception:
        return False


# ---------------------------------------------------------------------------
# Checksum computation
# ---------------------------------------------------------------------------


def compute_checksum(fso, path, alg):
    """Compute a checksum for a file.

    Tries server-side computation first (if the filesystem supports it and
    accepts an algorithm argument). Falls back to client-side computation
    by reading the file.
    """
    alg_upper = alg.upper()

    # 1. Try server-side checksum(path, algorithm)
    # This is a fsspec-xrootd and WebDAVFileSystem extension.
    if hasattr(fso, "checksum"):
        try:
            # Check if it accepts the second argument (algorithm)
            # WebDAVFileSystem and fsspec-xrootd's _checksum do.
            # fsspec's LocalFileSystem.checksum only takes 'path'.
            import inspect

            sig = inspect.signature(fso.checksum)
            if len(sig.parameters) >= 2 or any(
                p.kind == p.VAR_KEYWORD for p in sig.parameters.values()
            ):
                result = fso.checksum(path, alg_upper)
                if result:
                    # If it returns (alg, value), verify it's what we asked for
                    if isinstance(result, (list, tuple)) and len(result) == 2:
                        raw_alg = str(result[0]).upper()
                        if raw_alg != alg_upper:
                            # Algorithm mismatch (e.g. server only does ADLER32)
                            pass
                        else:
                            return _format_checksum_result(result)
                    else:
                        # Single value result — assume it's the requested algorithm
                        return _format_checksum_result(result)
        except Exception:
            pass  # Fall back to client-side

    # 2. Client-side computation
    if alg_upper == "ADLER32":
        value = 1  # zlib.adler32 initial value
        with fso.open(path, "rb") as f:
            while True:
                chunk = f.read(CHUNK_SIZE)
                if not chunk:
                    break
                value = zlib.adler32(chunk, value) & 0xFFFFFFFF
        return f"{value:08x}"

    if alg_upper == "CRC32":
        value = 0
        with fso.open(path, "rb") as f:
            while True:
                chunk = f.read(CHUNK_SIZE)
                if not chunk:
                    break
                value = zlib.crc32(chunk, value) & 0xFFFFFFFF
        return f"{value:08x}"

    if alg_upper == "CRC32C":
        value = _crc32c_file(fso, path)
        return f"{value:08x}"

    # For MD5, SHA*, etc. use hashlib
    name = alg_upper.lower().replace("-", "")  # sha256, md5, …
    try:
        h = hashlib.new(name)
    except ValueError as err:
        raise ValueError(f"unsupported checksum algorithm: {alg}") from err

    with fso.open(path, "rb") as f:
        while True:
            chunk = f.read(CHUNK_SIZE)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def _format_checksum_result(result):
    """Ensure checksum result is a hex string."""
    if isinstance(result, bytes):
        return result.hex()
    if isinstance(result, (list, tuple)) and len(result) == 2:
        return str(result[1])
    return str(result)


def _crc32c_file(fso, path):
    """Compute CRC32C checksum. Uses the crc32c package if available, otherwise
    falls back to crcmod (if installed) or a pure-Python polynomial."""
    try:
        import crc32c as _crc32c

        value = 0
        with fso.open(path, "rb") as f:
            while True:
                chunk = f.read(CHUNK_SIZE)
                if not chunk:
                    break
                value = _crc32c.crc32c(chunk, value)
        return value & 0xFFFFFFFF
    except ImportError:
        pass

    try:
        import crcmod

        crc_fn = crcmod.predefined.mkCrcFun("crc-32c")
        crc = crc_fn(b"")  # initialise
        with fso.open(path, "rb") as f:
            while True:
                chunk = f.read(CHUNK_SIZE)
                if not chunk:
                    break
                crc = crc_fn(chunk, crc)
        return crc & 0xFFFFFFFF
    except (ImportError, Exception):
        pass

    # Pure-Python fallback (slow but correct; no external deps)
    return _crc32c_pure(fso, path)


def _crc32c_pure(fso, path):
    """Pure-Python CRC32C using the Castagnoli polynomial 0x82F63B78."""
    # Build lookup table
    poly = 0x82F63B78
    table = []
    for i in range(256):
        crc = i
        for _ in range(8):
            if crc & 1:
                crc = (crc >> 1) ^ poly
            else:
                crc >>= 1
        table.append(crc)

    crc = 0xFFFFFFFF
    with fso.open(path, "rb") as f:
        while True:
            chunk = f.read(CHUNK_SIZE)
            if not chunk:
                break
            for byte in chunk:
                crc = (crc >> 8) ^ table[(crc ^ byte) & 0xFF]
    return (~crc) & 0xFFFFFFFF
