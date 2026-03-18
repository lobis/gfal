"""
fsspec integration layer: URL normalisation, filesystem acquisition,
and a stat-like wrapper around fsspec info() dicts.
"""

import os
import stat as stat_module
import sys
from pathlib import Path
from urllib.parse import urlparse

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


async def _no_verify_get_client(loop=None, **kwargs):
    """aiohttp client factory that skips SSL certificate verification."""
    import ssl as _ssl

    import aiohttp

    ctx = _ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = _ssl.CERT_NONE
    return aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=ctx))


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
        return "http" + url[3:]
    if scheme == "davs":
        return "https" + url[4:]
    return url


def url_to_fs(url, storage_options=None):
    """
    Return (AbstractFileSystem, path) for a URL.

    storage_options are forwarded to the filesystem constructor.
    For HTTP(S) these may include 'client_cert'/'client_key'.
    For XRootD auth is handled via X509_* environment variables.
    """
    if storage_options is None:
        storage_options = {}

    url = normalize_url(url)
    parsed = urlparse(url)
    scheme = parsed.scheme.lower()

    if scheme in ("http", "https"):
        from gfal_cli.webdav import WebDAVFileSystem

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
        if proxy:
            cert = proxy
            key = proxy
    if cert:
        opts["client_cert"] = cert
        opts["client_key"] = key or cert
    if not getattr(params, "ssl_verify", True):
        opts["ssl_verify"] = False
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
