"""
gfal-ls implementation.
"""

import math
import os
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
    @base.arg("file", type=base.surl, help="URI to list")
    def execute_ls(self):
        """List directory contents."""
        if self.params.full_time:
            self.params.time_style = "long-iso"

        opts = fs.build_storage_options(self.params)
        fso, path = fs.url_to_fs(self.params.file, opts)

        info = fso.info(path)
        st = fs.StatInfo(info)

        if self.params.directory:
            self._print_entry(self.params.file, st)
            return 0

        # Always attempt ls() — errors (e.g. 403 on HTTP directories that don't
        # support listing) propagate as real errors rather than silently showing
        # just the URL.  This also handles HTTP where info() always returns
        # type='file' even for directories.
        entries = fso.ls(path, detail=True)

        path_norm = path.rstrip("/")
        is_self_only = entries and all(
            e.get("name", "").rstrip("/") == path_norm for e in entries
        )

        if is_self_only:
            # fsspec returns [the_entry_itself] when path is a file (local/XRootD)
            self._print_entry(self.params.file, st)
        elif not entries:
            if not stat.S_ISDIR(st.st_mode):
                # HTTP file: ls() returned nothing; fall back to showing the entry
                self._print_entry(self.params.file, st)
            # else: genuinely empty directory — no output
        else:
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
