"""
gfal-ls implementation.
"""

import errno
import math
import os
import re
import stat
import sys
from datetime import datetime
from pathlib import Path

from gfal_cli import base, fs
from gfal_cli.api import GfalClient
from gfal_cli.utils import file_mode_str

# ---------------------------------------------------------------------------
# Time formatters (same choices as gfal2-util)
# ---------------------------------------------------------------------------


def _fmt_full_iso(ts):
    # gfal2-util uses local time for all formats and appends "+0000" for
    # full-iso regardless of the actual timezone.  We replicate this so that
    # output on UTC servers (lxplus) matches the reference exactly.
    return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S.%f +0000")


def _fmt_long_iso(ts):
    return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M")


def _fmt_iso(ts):
    dt = datetime.fromtimestamp(ts)
    diff_days = (datetime.now() - dt).days
    if diff_days < 180:
        return dt.strftime("%m-%d %H:%M")
    return dt.strftime("%Y-%m-%d")


def _fmt_locale(ts):
    dt = datetime.fromtimestamp(ts)
    diff_days = (datetime.now() - dt).days
    day = dt.strftime("%d").lstrip("0").rjust(2)
    if diff_days < 180:
        return dt.strftime("%b " + day + " %H:%M")
    return dt.strftime("%b " + day + "  %Y")


_TIME_FORMATS = {
    "full-iso": _fmt_full_iso,
    "long-iso": _fmt_long_iso,
    "iso": _fmt_iso,
    "locale": _fmt_locale,
}

# ---------------------------------------------------------------------------
# LS_COLORS parsing
# ---------------------------------------------------------------------------

_color_dict = {}
_ls_colors = os.environ.get("LS_COLORS", "")
for _entry in _ls_colors.split(":"):
    if "=" in _entry:
        try:
            _typ, _col = _entry.split("=", 1)
            _color_dict[_typ] = _col
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Sorting helpers
# ---------------------------------------------------------------------------


def _version_key(name):
    """Natural / version sort key: '10' sorts after '9', not before."""
    return [int(p) if p.isdigit() else p.lower() for p in re.split(r"(\d+)", name)]


def _apply_sort(entries, sort_by, reverse_flag):
    """Return entries sorted according to sort_by and reverse_flag.

    Default directions match GNU ls:
      name      — A-Z        (reverse: Z-A)
      size      — largest first  (reverse: smallest first)
      time      — newest first   (reverse: oldest first)
      extension — A-Z by ext, then name  (reverse: Z-A)
      version   — natural ascending      (reverse: descending)
      none      — directory order        (reverse: reversed directory order)
    """
    if sort_by == "none":
        return list(reversed(entries)) if reverse_flag else list(entries)

    if sort_by == "size":
        key = lambda e: fs.StatInfo(e).st_size  # noqa: E731
        default_reverse = True  # largest first
    elif sort_by == "time":
        key = lambda e: fs.StatInfo(e).st_mtime  # noqa: E731
        default_reverse = True  # newest first
    elif sort_by == "extension":

        def key(e):
            name = Path(e.get("name", "").rstrip("/")).name
            p = Path(name)
            return (p.suffix.lower(), p.name.lower())

        default_reverse = False
    elif sort_by == "version":
        key = lambda e: _version_key(Path(e.get("name", "").rstrip("/")).name)  # noqa: E731
        default_reverse = False
    else:  # "name"
        key = lambda e: Path(e.get("name", "").rstrip("/")).name  # noqa: E731
        default_reverse = False

    actual_reverse = default_reverse ^ reverse_flag
    return sorted(entries, key=key, reverse=actual_reverse)


class CommandLs(base.CommandBase):
    @base.arg("-a", "--all", action="store_true", help="show hidden files")
    @base.arg("-l", "--long", action="store_true", help="long listing format")
    @base.arg(
        "-d",
        "--directory",
        action="store_true",
        help="list directory entry itself, not its contents",
    )
    @base.arg(
        "-H",
        "--human-readable",
        action="store_true",
        help="with -l, print sizes in human-readable form (e.g. 1K 2M)",
    )
    @base.arg(
        "--time-style",
        type=str,
        default="locale",
        choices=list(_TIME_FORMATS.keys()),
        help="timestamp format",
    )
    @base.arg("--full-time", action="store_true", help="same as --time-style=full-iso")
    @base.arg(
        "--color",
        type=str,
        choices=["always", "never", "auto"],
        default="auto",
        help="colorise output",
    )
    @base.arg(
        "-r",
        "--reverse",
        action="store_true",
        help="reverse sort order",
    )
    @base.arg(
        "--xattr",
        type=str,
        action="append",
        default=None,
        metavar="ATTR",
        help="query this extended attribute and display it in long output "
        "(can be specified multiple times; only shown with -l)",
    )
    @base.arg(
        "--sort",
        dest="sort",
        type=str,
        choices=["name", "size", "time", "extension", "version", "none"],
        default="name",
        help="sort by: name (default), size, time, extension, version, none",
    )
    @base.arg(
        "-S",
        dest="sort",
        action="store_const",
        const="size",
        help="sort by file size, largest first (same as --sort=size)",
    )
    @base.arg(
        "-U",
        dest="sort",
        action="store_const",
        const="none",
        help="do not sort; list entries in directory order (same as --sort=none)",
    )
    @base.arg("file", nargs="+", type=base.surl, help="URI(s) to list")
    def execute_ls(self):
        """List directory contents."""
        if self.params.full_time:
            self.params.time_style = "long-iso"

        client = GfalClient(
            cert=self.params.cert,
            key=self.params.key,
            timeout=self.params.timeout,
            ssl_verify=getattr(self.params, "ssl_verify", True),
        )

        multi = len(self.params.file) > 1
        rc = 0
        first = True
        for url in self.params.file:
            try:
                r = self._list_one(url, client, print_header=multi, first=first)
            except Exception as e:
                if isinstance(e, OSError) and e.errno == errno.EPIPE:
                    raise
                self._print_error(e)
                rc = 1
            else:
                if r:
                    rc = r
            first = False
        return rc

    def _list_one(self, url, client, *, print_header, first):
        st = client.stat(url)
        if self.params.directory:
            if print_header:
                if not first:
                    sys.stdout.write("\n")
                sys.stdout.write(f"{url}:\n")
            self._print_entry(url, st, self._fetch_xattrs(client, url))
            return 0

        entries_st = client.ls(url, detail=True)
        # Convert StatInfo back to dict-like for _apply_sort (or refactor _apply_sort)
        # Actually _apply_sort uses e.get("name") and fs.StatInfo(e)
        # I'll update entries to be compatible with _apply_sort
        raw_entries = [st.info for st in entries_st]
        entries = _apply_sort(raw_entries, self.params.sort, self.params.reverse)

        # Normalize paths for comparison (especially for Windows)
        _, norm_path = fs.url_to_fs(url)
        norm_path = norm_path.rstrip("/")

        def _get_norm_entry_path(e):
            return e.get("name", "").replace("\\", "/").rstrip("/")

        is_self_only = entries and all(
            _get_norm_entry_path(e) == norm_path for e in entries
        )

        if is_self_only:
            # fsspec returns [the_entry_itself] when path is a file (local/XRootD)
            if print_header:
                if not first:
                    sys.stdout.write("\n")
                sys.stdout.write(f"{url}:\n")
            self._print_entry(url, st, self._fetch_xattrs(client, url))
        elif not entries:
            if not stat.S_ISDIR(st.st_mode):
                # HTTP file: ls() returned nothing; fall back to showing the entry
                if print_header:
                    if not first:
                        sys.stdout.write("\n")
                    sys.stdout.write(f"{url}:\n")
                self._print_entry(url, st, self._fetch_xattrs(client, url))
            # else: genuinely empty directory — print header only
            elif print_header:
                if not first:
                    sys.stdout.write("\n")
                sys.stdout.write(f"{url}:\n")
        else:
            visible = [
                e
                for e in entries
                if self.params.all
                or not Path(e["name"].rstrip("/")).name.startswith(".")
            ]
            max_size_w = max(
                (len(str(fs.StatInfo(e).st_size)) for e in visible),
                default=1,
            )
            if print_header:
                if not first:
                    sys.stdout.write("\n")
                sys.stdout.write(f"{url}:\n")
            for entry_info in entries:
                name = Path(entry_info["name"].rstrip("/")).name
                if not self.params.all and name.startswith("."):
                    continue
                entry_st = fs.StatInfo(entry_info)
                xattrs = self._fetch_xattrs(client, entry_info["name"])
                self._print_entry(name, entry_st, xattrs, size_width=max_size_w)

        return 0

    def _fetch_xattrs(self, client, url):
        """Fetch the extended attributes named by ``--xattr`` for *url*.

        Returns a dict ``{attr_name: value_str_or_None}``.  An empty dict is
        returned when ``--xattr`` was not given, ``-l`` is not active, or the
        filesystem does not support extended attributes.
        """
        if not self.params.long or not self.params.xattr:
            return {}
        result = {}
        import contextlib

        for attr in self.params.xattr:
            with contextlib.suppress(Exception):
                result[attr] = client.getxattr(url, attr)
        return result

    def _print_entry(self, name, st, xattrs=None, *, size_width=None):
        if self.params.long:
            size_val = st.st_size
            if self.params.human_readable:
                size_str = _human_size(size_val)
                size_field = size_str.rjust(5)
            else:
                # Use a minimum width of 8 for size, matching standard ls -l
                w = max(8, size_width if size_width is not None else 0)
                size_field = str(size_val).rjust(w)

            date = _TIME_FORMATS[self.params.time_style](st.st_mtime)

            xattr_suffix = ""
            if xattrs:
                parts = [
                    f"{k}={v}" if v is not None else f"{k}=<error>"
                    for k, v in xattrs.items()
                ]
                xattr_suffix = "  " + "  ".join(parts)

            # Format: mode nlink uid gid size date name [xattrs]
            # gfal2-util uses right-justified UID/GID (usually 5-8 chars)
            # and a trailing tab before the newline.
            sys.stdout.write(
                f"{file_mode_str(st.st_mode)} {st.st_nlink:3d} {st.st_uid:5d} {st.st_gid:5d}"
                f" {size_field} {str(date).ljust(11)} {self._colorize(name, st.st_mode)}"
                f"{xattr_suffix}\t\n"
            )
        else:
            sys.stdout.write(f"{self._colorize(name, st.st_mode)}\n")

    def _colorize(self, name, mode):
        apply = self.params.color == "always" or (
            self.params.color == "auto" and sys.stdout.isatty()
        )
        if not apply:
            return name

        color = None
        if mode is None:
            color = _color_dict.get("no")
        elif stat.S_ISDIR(mode):
            color = _color_dict.get("di")
        elif stat.S_ISLNK(mode):
            color = _color_dict.get("ln")
        elif mode & (stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH):
            color = _color_dict.get("ex")
        else:
            # Regular file: try extension-based color first, then "fi"
            ext = Path(name).suffix  # e.g. ".txt"
            if ext:
                color = _color_dict.get(f"*{ext}")
            if color is None:
                color = _color_dict.get("fi")

        if not color:
            return name
        return f"\033[{color}m{name}\033[0m"


def _human_size(size):
    symbols = ["", "K", "M", "G", "T", "P"]
    deg = 0
    f = float(size)
    while f >= 1024.0 and deg < len(symbols) - 1:
        f /= 1024.0
        deg += 1
    if f < 10.0:
        return f"{math.ceil(f * 10) / 10:0.1f}{symbols[deg]}"
    return f"{math.ceil(f):0.0f}{symbols[deg]}"
