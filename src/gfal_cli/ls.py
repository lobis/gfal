"""
gfal-ls implementation.
"""

import math
import os
import re
import stat
import sys
from datetime import datetime
from pathlib import Path

from gfal_cli import base, fs
from gfal_cli.utils import file_mode_str

# ---------------------------------------------------------------------------
# Time formatters (same choices as gfal2-util)
# ---------------------------------------------------------------------------


def _fmt_full_iso(ts):
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
        key = lambda e: Path(e.get("name", "").rstrip("/")).name.lower()  # noqa: E731
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
            self.params.time_style = "full-iso"

        opts = fs.build_storage_options(self.params)
        multi = len(self.params.file) > 1
        rc = 0
        first = True
        for url in self.params.file:
            try:
                r = self._list_one(url, opts, print_header=multi, first=first)
            except Exception as e:
                sys.stderr.write(f"{self.progr}: {self._format_error(e)}\n")
                rc = 1
            else:
                if r:
                    rc = r
            first = False
        return rc

    def _list_one(self, url, opts, *, print_header, first):
        fso, path = fs.url_to_fs(url, opts)

        info = fso.info(path)
        st = fs.StatInfo(info)

        if self.params.directory:
            if print_header:
                if not first:
                    sys.stdout.write("\n")
                sys.stdout.write(f"{url}:\n")
            self._print_entry(url, st)
            return 0

        # Always attempt ls() — errors (e.g. 403 on HTTP directories that don't
        # support listing) propagate as real errors rather than silently showing
        # just the URL.  This also handles HTTP where info() always returns
        # type='file' even for directories.
        entries = _apply_sort(
            fso.ls(path, detail=True),
            self.params.sort,
            self.params.reverse,
        )

        path_norm = path.rstrip("/")
        is_self_only = entries and all(
            e.get("name", "").rstrip("/") == path_norm for e in entries
        )

        if is_self_only:
            # fsspec returns [the_entry_itself] when path is a file (local/XRootD)
            if print_header:
                if not first:
                    sys.stdout.write("\n")
                sys.stdout.write(f"{url}:\n")
            self._print_entry(url, st)
        elif not entries:
            if not stat.S_ISDIR(st.st_mode):
                # HTTP file: ls() returned nothing; fall back to showing the entry
                if print_header:
                    if not first:
                        sys.stdout.write("\n")
                    sys.stdout.write(f"{url}:\n")
                self._print_entry(url, st)
            # else: genuinely empty directory — print header only
            elif print_header:
                if not first:
                    sys.stdout.write("\n")
                sys.stdout.write(f"{url}:\n")
        else:
            if print_header:
                if not first:
                    sys.stdout.write("\n")
                sys.stdout.write(f"{url}:\n")
            for entry_info in entries:
                name = Path(entry_info["name"].rstrip("/")).name
                if not self.params.all and name.startswith("."):
                    continue
                entry_st = fs.StatInfo(entry_info)
                self._print_entry(name, entry_st)

        return 0

    def _print_entry(self, name, st):
        if self.params.long:
            size = st.st_size
            if self.params.human_readable:
                size_str = _human_size(size)
                size_field = size_str.rjust(4)
            else:
                size_field = str(size).rjust(9)

            date = _TIME_FORMATS[self.params.time_style](st.st_mtime)

            sys.stdout.write(
                f"{file_mode_str(st.st_mode)} {st.st_nlink:3d} {st.st_uid:<5d} {st.st_gid:<5d}"
                f" {size_field} {str(date).ljust(11)} {self._colorize(name, st.st_mode)}\n"
            )
        else:
            sys.stdout.write(f"{self._colorize(name, None)}\n")

    def _colorize(self, name, mode):
        apply = self.params.color == "always" or (
            self.params.color == "auto" and sys.stdout.isatty()
        )
        if not apply:
            return name

        color = "037"
        if mode is None:
            color = _color_dict.get("no", color)
        elif stat.S_ISDIR(mode):
            color = _color_dict.get("di", color)
        elif stat.S_ISLNK(mode):
            color = _color_dict.get("ln", color)
        elif mode & (stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH):
            color = _color_dict.get("ex", color)
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
