"""
fsspec integration layer: URL normalisation, filesystem acquisition,
and a stat-like wrapper around fsspec info() dicts.
"""

import stat as stat_module
from pathlib import Path
from urllib.parse import urlparse, urlunparse

import fsspec

CHUNK_SIZE = 4 * 1024 * 1024  # 4 MiB


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
    if not scheme:
        return urlunparse(("file", "", str(Path(url).resolve()), "", "", ""))
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
        fs = fsspec.filesystem("http", **storage_options)
        return fs, url

    if scheme in ("root", "xroot"):
        fs = fsspec.filesystem("root", **storage_options)
        return fs, url

    if scheme == "file":
        fs = fsspec.filesystem("file")
        return fs, parsed.path

    # fallback
    fs, path = fsspec.url_to_fs(url, **storage_options)
    return fs, path


def build_storage_options(params):
    """Build fsspec storage_options from parsed CLI params."""
    opts = {}
    if getattr(params, "cert", None):
        opts["client_cert"] = params.cert
        opts["client_key"] = params.key or params.cert
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
