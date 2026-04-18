"""
gfal cp implementation.
"""

import contextlib
import errno
import stat
import sys
import threading
import time
from collections import deque
from pathlib import Path
from urllib.parse import urlparse, urlunparse

from rich.text import Text

from gfal.cli import base
from gfal.cli.base import exception_exit_code
from gfal.cli.progress import (
    CountProgress,
    Progress,
    Spinner,
    _final_status_text,
    has_live_progress,
    print_live_message,
)
from gfal.core import api as core_api
from gfal.core import fs
from gfal.core.api import (
    ChecksumPolicy,
    CopyOptions,
    GfalClient,
)
from gfal.core.api import (
    parse_checksum_arg as _parse_checksum_arg,
)
from gfal.core.api import (
    tpc_applicable as _tpc_applicable,
)
from gfal.core.errors import GfalError, GfalPartialFailureError

_make_hasher = core_api.make_hasher
_update_hasher = core_api.update_hasher
_finalise_hasher = core_api.finalise_hasher
_is_special_file = core_api.is_special_file
_eos_mtime_url = core_api.eos_mtime_url

_DEFAULT_RECURSIVE_PARALLELISM = 1
_TRANSFER_MODE_LABELS = {
    "streamed": "streamed",
    "tpc-pull": "TPC pull",
    "tpc-push": "TPC push",
    "tpc-xrootd": "TPC xrootd",
}


def _format_count(value):
    return f"{value:,}"


def _short_elapsed_text(seconds):
    return f"{max(0.0, seconds):.1f}s"


def _average_rate_text(bytes_transferred, elapsed):
    if bytes_transferred <= 0 or elapsed <= 0:
        return "?"
    return f"{_TransferDisplay._size_text(bytes_transferred / elapsed)}/s"


def _file_count_text(count):
    noun = "file" if count == 1 else "files"
    return f"{_format_count(count)} {noun}"


def _truncate_middle(value, max_width):
    if len(value) <= max_width:
        return value
    if max_width <= 3:
        return value[:max_width]
    keep = max_width - 3
    left = keep // 2
    right = keep - left
    return f"{value[:left]}...{value[-right:]}"


def _url_path_join(base_url, name):
    parsed = urlparse(base_url)
    path = parsed.path.rstrip("/") + "/" + name
    return urlunparse(parsed._replace(path=path))


class _TransferDisplay:
    def __init__(
        self,
        src_url,
        dst_url,
        *,
        quiet=False,
        verbose=False,
        src_size=None,
        transfer_mode=None,
        history_only=False,
        transfer_index=None,
        transfer_total=None,
        rich_history=False,
    ):
        self.src_url = src_url
        self.dst_url = dst_url
        self.quiet = quiet
        self.verbose = verbose
        self.src_size = src_size
        self.show_progress = sys.stdout.isatty() and not verbose and not quiet
        self.history_only = history_only
        self.progress_bar = None
        self.progress_started = False
        self.transfer_start = time.monotonic()
        self.transfer_mode = transfer_mode
        self.final_status = None
        self.transfer_index = transfer_index
        self.transfer_total = transfer_total
        self.rich_history = rich_history
        self._suppress_output = False
        self._lock = threading.Lock()

    def _transfer_label(self):
        if self.transfer_mode is None:
            return f"Copying {Path(self.src_url).name}"
        mode = _TRANSFER_MODE_LABELS.get(self.transfer_mode, self.transfer_mode)
        return f"Copying {Path(self.src_url).name} ({mode})"

    @staticmethod
    def _size_text(size):
        if size is None:
            return None
        value = float(size)
        units = ["B", "KB", "MB", "GB", "TB", "PB"]
        unit = units[0]
        for unit in units:
            if value < 1024.0 or unit == units[-1]:
                break
            value /= 1024.0
        if unit == "B":
            return f"{int(size)} B"
        return f"{value:.1f} {unit}"

    @classmethod
    def _rate_text(cls, size, elapsed):
        if size is None or elapsed <= 0:
            return None
        return f"{cls._size_text(size / elapsed)}/s"

    def _history_status_line(self, success):
        line = _final_status_text(
            self._transfer_label(),
            success,
            self.final_status,
        )
        elapsed = max(0.0, time.monotonic() - self.transfer_start)
        size_text = self._size_text(self.src_size)
        rate_text = self._rate_text(self.src_size, elapsed)
        elapsed_text = time.strftime("%H:%M:%S", time.gmtime(elapsed))
        details = [part for part in (size_text, rate_text, elapsed_text) if part]
        if details:
            return f"{line}  {'  '.join(details)}"
        return line

    def _history_status_renderable(self, success):
        elapsed = max(0.0, time.monotonic() - self.transfer_start)
        size_text = self._size_text(self.src_size) or "-"
        rate_text = self._rate_text(self.src_size, elapsed) if success else None
        mode_text = (
            _TRANSFER_MODE_LABELS.get(self.transfer_mode, self.transfer_mode) or "-"
        )
        name_text = _truncate_middle(Path(self.src_url).name, 48)
        total = self.transfer_total or 0
        index = self.transfer_index or 0
        index_text = f"[{index}/{total}]" if total else "[?/?]"
        status_text = (
            "✓ copied"
            if success and self.final_status != "skipped"
            else ("↷ skipped" if self.final_status == "skipped" else "✗ failed")
        )
        status_style = (
            "green"
            if success and self.final_status != "skipped"
            else "yellow"
            if self.final_status == "skipped"
            else "red"
        )
        row = Text()
        row.append(f"{index_text:<7}", style="bold blue")
        row.append("  ")
        row.append(f"{name_text:<48}")
        row.append("  ")
        row.append(f"{size_text:>8}")
        row.append("  ")
        row.append(f"{str(mode_text):<10}", style="cyan")
        row.append("  ")
        row.append(f"{status_text:<9}", style=status_style)
        row.append("  ")
        row.append(
            f"{(rate_text or '-'):>12}", style="dim" if rate_text is None else ""
        )
        row.append("  ")
        row.append(f"{_short_elapsed_text(elapsed):>6}")
        return row

    def start(self):
        with self._lock:
            if self._suppress_output:
                return
            if self.progress_started:
                return
            self.progress_started = True
            if self.show_progress:
                if self.history_only:
                    return
                self.progress_bar = Progress(self._transfer_label())
                if self.src_size is not None:
                    self.progress_bar.update(total_size=self.src_size)
                self.progress_bar.start()
                return
            if not self.quiet:
                print(
                    f"{self._transfer_label()} {self.src_size or 0} bytes  "
                    f"{self.src_url}  =>  {self.dst_url}"
                )

    def update(self, bytes_transferred):
        self.start()
        with self._lock:
            if self._suppress_output:
                return
            if self.show_progress and self.progress_bar is not None and self.src_size:
                self.progress_bar.update(
                    curr_size=bytes_transferred,
                    total_size=self.src_size,
                    elapsed=time.monotonic() - self.transfer_start,
                )

    def set_mode(self, mode):
        with self._lock:
            if self._suppress_output:
                return
            self.transfer_mode = mode
            if self.show_progress and self.progress_bar is not None:
                label = self._transfer_label()
                if hasattr(self.progress_bar, "label"):
                    self.progress_bar.label = label
                if hasattr(self.progress_bar, "set_description"):
                    self.progress_bar.set_description(label)

    def set_total_size(self, total_size):
        with self._lock:
            if self._suppress_output:
                return
            self.src_size = total_size
            if self.show_progress and self.progress_bar is not None and total_size:
                self.progress_bar.update(total_size=total_size)

    def mark_skipped(self):
        with self._lock:
            if self._suppress_output:
                return
            self.final_status = "skipped"
            self.transfer_mode = None
            if self.show_progress and self.progress_bar is not None:
                label = self._transfer_label()
                if hasattr(self.progress_bar, "label"):
                    self.progress_bar.label = label
                if hasattr(self.progress_bar, "set_description"):
                    self.progress_bar.set_description(label)

    def finish(self, success):
        with self._lock:
            if self._suppress_output:
                return
            if not self.progress_started or (
                not self.show_progress and self.progress_bar is None
            ):
                return
            if self.show_progress and self.history_only:
                if self.rich_history:
                    print_live_message(self._history_status_renderable(success))
                else:
                    print_live_message(self._history_status_line(success))
                return
            if self.progress_bar is None:
                return
            if success and self.src_size and self.final_status != "skipped":
                self.progress_bar.update(
                    curr_size=self.src_size,
                    total_size=self.src_size,
                    elapsed=time.monotonic() - self.transfer_start,
                )
            self.progress_bar.stop(success, status=self.final_status)

    def suppress_output(self):
        with self._lock:
            self._suppress_output = True


class CommandCopy(base.CommandBase):
    @base.arg(
        "-f", "--force", action="store_true", help="overwrite destination if it exists"
    )
    @base.arg(
        "-p",
        "--parent",
        action="store_true",
        help="create destination parent directories as needed",
    )
    @base.arg(
        "-K",
        "--checksum",
        type=str,
        default=None,
        help="verify transfer with this checksum algorithm (e.g. ADLER32, MD5) "
        "or algorithm:expected_value",
    )
    @base.arg(
        "--checksum-mode",
        type=str,
        default="both",
        choices=["source", "target", "both"],
        help="which side(s) to verify the checksum on",
    )
    @base.arg(
        "--compare",
        type=str,
        default=None,
        choices=["size", "size_mtime", "checksum", "none"],
        help="when destination exists and --force is not set, how to decide whether "
        "to skip: size = compare file size only, "
        "size_mtime = compare both size and mtime, "
        "checksum = compare checksums, none = skip unconditionally without any checks. "
        "Default (unset): error if destination exists",
    )
    @base.arg(
        "-r", "--recursive", action="store_true", help="copy directories recursively"
    )
    @base.arg(
        "--parallel",
        type=int,
        default=_DEFAULT_RECURSIVE_PARALLELISM,
        metavar="N",
        help="maximum number of concurrent child transfers during recursive copy "
        f"(default: {_DEFAULT_RECURSIVE_PARALLELISM})",
    )
    @base.arg(
        "--limit",
        type=int,
        default=None,
        metavar="N",
        help="limit the number of queued copy jobs. Useful for testing "
        "recursive copies or source lists without processing the full set.",
    )
    @base.arg(
        "--preserve-times",
        action="store_true",
        default=True,
        help="preserve source access and modification times when supported",
    )
    @base.arg(
        "--no-preserve-times",
        action="store_false",
        dest="preserve_times",
        help="do not preserve source timestamps at the destination",
    )
    @base.arg(
        "--from-file",
        type=str,
        default=None,
        help="read source URIs from a file; destination is first positional arg",
    )
    @base.arg(
        "--dry-run",
        action="store_true",
        help="print what would be done without copying",
    )
    @base.arg(
        "--abort-on-failure",
        action="store_true",
        help="stop immediately on first error",
    )
    @base.arg(
        "-T",
        "--transfer-timeout",
        type=int,
        default=0,
        metavar="TRANSFER_TIMEOUT",
        help="per-file transfer timeout in seconds (0 = no per-file timeout)",
    )
    @base.arg(
        "--tpc",
        action="store_true",
        help="attempt third-party copy (data flows server-to-server); "
        "falls back to streaming if the server does not support it",
    )
    @base.arg(
        "--tpc-only",
        action="store_true",
        help="require third-party copy; fail without streaming fallback",
    )
    @base.arg(
        "--tpc-mode",
        type=str,
        choices=["pull", "push"],
        default="pull",
        help="TPC direction: pull = dst pulls from src (default), "
        "push = src pushes to dst",
    )
    @base.arg(
        "--copy-mode",
        type=str,
        choices=["pull", "push", "streamed"],
        default=None,
        help="copy mode (gfal2-util compatible): pull/push = TPC with that direction; "
        "streamed = force client-side streaming. By default, HTTP->HTTP and "
        "root->root copies try pull-mode TPC first with fallback to streaming. "
        "Overrides --tpc/--tpc-only/--tpc-mode when specified.",
    )
    @base.arg(
        "--just-copy",
        action="store_true",
        help="skip all preparation steps (checksum verification, overwrite checks, "
        "parent directory creation) and just perform the raw copy",
    )
    @base.arg(
        "--disable-cleanup",
        action="store_true",
        help="disable removal of partially-written destination files on transfer failure",
    )
    @base.arg(
        "--no-delegation",
        action="store_true",
        help="disable proxy delegation for TPC transfers",
    )
    @base.arg(
        "--evict",
        action="store_true",
        help="evict the source file from its disk buffer after a successful transfer "
        "(requires gfal2; accepted for compatibility, currently a no-op)",
    )
    @base.arg(
        "--scitag",
        type=int,
        default=None,
        metavar="N",
        help="SciTag flow identifier [65-65535] forwarded as HTTP header "
        "(HTTP TPC only; for WLCG network monitoring)",
    )
    @base.arg(
        "-n",
        "--nbstreams",
        type=int,
        default=None,
        metavar="NBSTREAMS",
        help="maximum number of parallel streams (GridFTP only; accepted for compatibility; ignored)",
    )
    @base.arg(
        "--tcp-buffersize",
        type=int,
        default=None,
        metavar="BYTES",
        help="TCP buffer size in bytes (GridFTP only; accepted for compatibility; ignored)",
    )
    @base.arg(
        "-s",
        "--src-spacetoken",
        type=str,
        default=None,
        metavar="TOKEN",
        dest="src_spacetoken",
        help="source space token (SRM/GridFTP only; accepted for compatibility; ignored)",
    )
    @base.arg(
        "-S",
        "--dst-spacetoken",
        type=str,
        default=None,
        metavar="TOKEN",
        dest="dst_spacetoken",
        help="destination space token (SRM/GridFTP only; accepted for compatibility; ignored)",
    )
    @base.arg("src", type=base.surl, nargs="?", help="source URI")
    @base.arg(
        "dst",
        type=base.surl,
        nargs="+",
        help="destination URI(s). Multiple destinations are chained: "
        "src->dst1, dst1->dst2, ...",
    )
    def execute_cp(self):
        """Copy files or directories."""
        if self.params.from_file and self.params.src:
            sys.stderr.write("Cannot combine --from-file with a positional source\n")
            return 1
        if self.params.parallel < 1:
            sys.stderr.write(f"{self.prog}: --parallel must be at least 1\n")
            return 1
        if self.params.limit is not None and self.params.limit < 1:
            sys.stderr.write(f"{self.prog}: --limit must be at least 1\n")
            return 1

        # --copy-mode overrides --tpc/--tpc-only/--tpc-mode for backwards compatibility
        if self.params.copy_mode is not None:
            if self.params.copy_mode == "streamed":
                self.params.tpc = False
                self.params.tpc_only = False
            else:
                self.params.tpc = True
                self.params.tpc_mode = self.params.copy_mode  # "pull" or "push"

        # Validate --scitag range [65, 65535] per WLCG spec
        if self.params.scitag is not None and not (65 <= self.params.scitag <= 65535):
            sys.stderr.write(
                f"{self.prog}: invalid --scitag value {self.params.scitag}: "
                "must be in range [65, 65535]\n"
            )
            return 1

        # Warn about accepted-but-ignored GridFTP/SRM flags
        _ignored = {
            "--nbstreams": self.params.nbstreams,
            "--tcp-buffersize": self.params.tcp_buffersize,
            "--src-spacetoken": self.params.src_spacetoken,
            "--dst-spacetoken": self.params.dst_spacetoken,
        }
        for flag, val in _ignored.items():
            if val is not None and not self._is_quiet():
                sys.stderr.write(
                    f"{self.prog}: warning: {flag} is not supported in this "
                    "implementation and will be ignored\n"
                )

        opts = fs.build_storage_options(self.params)
        self._preserve_times_warned = set()

        # Build list of (source, destination) pairs
        jobs = []
        if self.params.from_file:
            dst = self.params.dst[0]
            with Path(self.params.from_file).open() as fh:
                for line in fh:
                    src = line.strip()
                    if src:
                        jobs.append((src, dst))
        elif self.params.src:
            s = self.params.src
            for idx, dst in enumerate(self.params.dst):
                jobs.append((s, dst))
                if idx == len(self.params.dst) - 1:
                    continue
                # Chain: if dst is a dir the actual destination will be dst/basename(s),
                # otherwise s becomes dst for the next hop.
                try:
                    dst_st = fs.stat(dst, opts)
                    if stat.S_ISDIR(dst_st.st_mode):
                        s = dst.rstrip("/") + "/" + Path(s.rstrip("/")).name
                    else:
                        s = dst
                except Exception:
                    s = dst
        else:
            sys.stderr.write("Missing source\n")
            return 1

        if self.params.limit is not None:
            jobs = jobs[: self.params.limit]

        rc = 0
        for src, dst in jobs:
            try:
                if dst == "-":
                    self._copy_to_stdout(src, opts)
                else:
                    self._do_copy(src, dst, opts)
            except Exception as e:
                if not getattr(e, "already_reported", False):
                    self._print_error(e)
                rc = exception_exit_code(getattr(e, "first_error", e))
                if self.params.abort_on_failure:
                    return rc

        return rc

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _copy_to_stdout(self, src_url, opts):
        """Stream *src_url* to sys.stdout.buffer (the ``-`` destination).

        Using sys.stdout.buffer directly is cross-platform; the
        ``file:///dev/stdout`` approach only works on Unix.
        """
        src_fs, src_path = fs.url_to_fs(src_url, opts)
        with src_fs.open(src_path, "rb") as f:
            while True:
                chunk = f.read(fs.CHUNK_SIZE)
                if not chunk:
                    break
                sys.stdout.buffer.write(chunk)
        sys.stdout.buffer.flush()

    def _build_client(self):
        return GfalClient(**base.build_client_kwargs(self.params))

    def _build_copy_options(self):
        checksum = None
        if self.params.checksum:
            algorithm, expected = _parse_checksum_arg(self.params.checksum)
            checksum = ChecksumPolicy(
                algorithm=algorithm,
                mode=self.params.checksum_mode,
                expected_value=expected,
            )

        tpc = "auto"
        if getattr(self.params, "copy_mode", None) == "streamed":
            tpc = "never"
        elif getattr(self.params, "tpc_only", False):
            tpc = "only"
        elif getattr(self.params, "tpc", False):
            tpc = "auto"

        argv = self.argv or []
        preserve_times_explicit = "--preserve-times" in argv

        return CopyOptions(
            overwrite=getattr(self.params, "force", False),
            create_parents=getattr(self.params, "parent", False),
            timeout=getattr(self.params, "transfer_timeout", 0) or None,
            checksum=checksum,
            source_space_token=getattr(self.params, "src_spacetoken", None),
            destination_space_token=getattr(self.params, "dst_spacetoken", None),
            streams=getattr(self.params, "nbstreams", None),
            tpc=tpc,
            tpc_direction=getattr(self.params, "tpc_mode", "pull"),
            recursive=getattr(self.params, "recursive", False),
            abort_on_failure=getattr(self.params, "abort_on_failure", False),
            preserve_times=getattr(self.params, "preserve_times", True),
            preserve_times_explicit=preserve_times_explicit,
            compare=getattr(self.params, "compare", None),
            just_copy=getattr(self.params, "just_copy", False),
            disable_cleanup=getattr(self.params, "disable_cleanup", False),
            no_delegation=getattr(self.params, "no_delegation", False),
            evict=getattr(self.params, "evict", False),
            scitag=getattr(self.params, "scitag", None),
        )

    def _warn_copy_message(self, message, dst_url):
        if self._is_quiet():
            return
        live_progress = has_live_progress()
        if message.startswith("Skipping existing file ") or message.startswith(
            "Skipping directory "
        ):
            if live_progress:
                return
            print_live_message(message)
            return
        normalized = fs.normalize_url(dst_url)
        scheme = urlparse(normalized).scheme.lower() or "unknown"
        if message.startswith("--preserve-times"):
            if scheme in self._preserve_times_warned:
                return
            self._preserve_times_warned.add(scheme)
        warning = f"{self.prog}: warning: {message}"
        if live_progress:
            print_live_message(warning)
            return
        sys.stderr.write(f"{warning}\n")

    def _child_error_callback(self, _child_src_url, _child_dst_url, error):
        self._reported_child_errors.add(self._child_error_key(error))
        self._print_error(error)

    @staticmethod
    def _child_error_key(error):
        return (type(error), getattr(error, "errno", None), str(error))

    def _traverse_callback(self, dir_src_url, dir_dst_url):
        if self._is_quiet():
            return
        print_live_message(f"Scanning {dir_src_url}  =>  {dir_dst_url}")

    @staticmethod
    def _is_skip_message(message):
        return message.startswith("Skipping existing file ") or message.startswith(
            "Skipping directory "
        )

    def _predicted_transfer_mode(self, src_url, dst_url):
        copy_options = self._build_copy_options()
        if copy_options.tpc == "never":
            return "streamed"
        if not _tpc_applicable(src_url, dst_url):
            return "streamed"
        src_scheme = urlparse(src_url).scheme.lower()
        dst_scheme = urlparse(dst_url).scheme.lower()
        if src_scheme == "root" and dst_scheme == "root":
            return "tpc-xrootd"
        return f"tpc-{copy_options.tpc_direction}"

    def _recursive_parallelism(self, src_url, dst_url):
        del src_url, dst_url
        if getattr(self.params, "abort_on_failure", False):
            return 1
        return max(1, getattr(self.params, "parallel", _DEFAULT_RECURSIVE_PARALLELISM))

    @staticmethod
    def _entry_name(entry):
        value = entry.get("name", "") if isinstance(entry, dict) else str(entry)
        return Path(str(value).rstrip("/")).name

    @staticmethod
    def _entry_mtime(entry):
        if not isinstance(entry, dict):
            return None
        for key in ("mtime", "LastModified", "last_modified"):
            value = entry.get(key)
            if value is not None:
                try:
                    return float(value)
                except (TypeError, ValueError):
                    return None
        return None

    def _prioritize_recursive_child_jobs(
        self,
        src_entries,
        dst_entries,
        compare_mode,
    ):
        jobs, _summary = self._classify_recursive_child_jobs(
            src_entries,
            dst_entries,
            compare_mode,
        )
        return jobs

    def _classify_recursive_child_jobs(
        self,
        src_entries,
        dst_entries,
        compare_mode,
    ):
        if not src_entries:
            return [], {
                "total": 0,
                "queued_first": 0,
                "likely_skipped": 0,
                "deferred_existing": 0,
                "compare_mode": compare_mode,
            }

        if not dst_entries:
            jobs = [
                (child_src_url, child_dst_url)
                for _name, child_src_url, child_dst_url, _src_info in src_entries
            ]
            return jobs, {
                "total": len(src_entries),
                "queued_first": len(jobs),
                "likely_skipped": 0,
                "deferred_existing": 0,
                "compare_mode": compare_mode,
            }

        dst_by_name = {self._entry_name(entry): entry for entry in dst_entries}
        preferred = []
        deferred = []
        likely_skipped = 0

        for name, child_src_url, child_dst_url, src_info in src_entries:
            dst_info = dst_by_name.get(name)
            if dst_info is None:
                preferred.append((child_src_url, child_dst_url))
                continue

            if compare_mode == "none":
                deferred.append((child_src_url, child_dst_url))
                likely_skipped += 1
                continue

            if compare_mode == "size":
                src_size = src_info.get("size") if isinstance(src_info, dict) else None
                dst_size = dst_info.get("size") if isinstance(dst_info, dict) else None
                if (
                    src_size is not None
                    and dst_size is not None
                    and src_size == dst_size
                ):
                    deferred.append((child_src_url, child_dst_url))
                    likely_skipped += 1
                else:
                    preferred.append((child_src_url, child_dst_url))
                continue

            if compare_mode == "size_mtime":
                src_size = src_info.get("size") if isinstance(src_info, dict) else None
                dst_size = dst_info.get("size") if isinstance(dst_info, dict) else None
                src_mtime = self._entry_mtime(src_info)
                dst_mtime = self._entry_mtime(dst_info)
                matches = (
                    src_size is not None
                    and dst_size is not None
                    and src_size == dst_size
                    and src_mtime is not None
                    and dst_mtime is not None
                    and abs(src_mtime - dst_mtime) < 1.0
                )
                if matches:
                    deferred.append((child_src_url, child_dst_url))
                    likely_skipped += 1
                else:
                    preferred.append((child_src_url, child_dst_url))
                continue

            if compare_mode == "checksum":
                deferred.append((child_src_url, child_dst_url))
                continue

            preferred.append((child_src_url, child_dst_url))

        jobs = preferred + deferred
        return jobs, {
            "total": len(src_entries),
            "queued_first": len(preferred),
            "likely_skipped": likely_skipped,
            "deferred_existing": len(deferred),
            "compare_mode": compare_mode,
        }

    @staticmethod
    def _recursive_scan_summary(summary):
        total = summary["total"]
        queued_first = summary["queued_first"]
        likely_skipped = summary["likely_skipped"]
        deferred_existing = summary["deferred_existing"]
        compare_mode = summary["compare_mode"]

        if compare_mode == "none":
            return (
                f"Recursive scan complete: {total} files, {queued_first} queued to copy, "
                f"{likely_skipped} already present and likely skipped"
            )
        if compare_mode in {"size", "size_mtime"}:
            return (
                f"Recursive scan complete: {total} files, {queued_first} queued first, "
                f"{likely_skipped} likely already up to date"
            )
        if compare_mode == "checksum":
            return (
                f"Recursive scan complete: {total} files, {queued_first} missing files "
                f"queued first, {deferred_existing} existing files deferred for checksum comparison"
            )
        return f"Recursive scan complete: {total} files queued"

    def _apply_job_limit(self, jobs, summary):
        if self.params.limit is None or len(jobs) <= self.params.limit:
            updated = dict(summary)
            updated["selected"] = len(jobs)
            return jobs, updated

        limited = jobs[: self.params.limit]
        updated = dict(summary)
        updated["queued_first"] = min(summary["queued_first"], len(limited))
        updated["limited_to"] = len(limited)
        updated["selected"] = len(limited)
        return limited, updated

    @staticmethod
    def _recursive_result_summary(copied, skipped, failed, elapsed=None):
        parts = [f"Recursive copy complete: {copied} copied"]
        if skipped:
            parts.append(f"{skipped} skipped")
        if failed:
            parts.append(f"{failed} failed")
        if elapsed is not None:
            elapsed_text = time.strftime("%H:%M:%S", time.gmtime(max(0.0, elapsed)))
            parts.append(f"elapsed {elapsed_text}")
        return ", ".join(parts)

    def _use_recursive_rich_layout(self):
        return (
            sys.stdout.isatty()
            and not self.params.verbose
            and not self._is_quiet()
            and not base.is_gfal2_compat()
        )

    @staticmethod
    def _estimated_recursive_scan_matches(summary):
        compare_mode = summary["compare_mode"]
        if compare_mode in {"size", "size_mtime", "none"}:
            return summary["likely_skipped"]
        return 0

    def _render_recursive_intro(self, src_url, dst_url):
        intro = Text()
        intro.append("Source      ", style="bold")
        intro.append(src_url, style="cyan")
        intro.append("\n")
        intro.append("Destination ", style="bold")
        intro.append(dst_url, style="cyan")
        intro.append("\n")
        return intro

    def _render_recursive_scan_summary(self, summary):
        compare_mode = summary["compare_mode"]
        total = summary["total"]
        selected = summary.get("selected", total)
        likely_skipped = summary["likely_skipped"]
        eligible = (
            total - likely_skipped
            if compare_mode in {"size", "size_mtime", "none"}
            else summary["queued_first"]
        )
        match_label = {
            "size": "Already up to date (size match)",
            "size_mtime": "Already up to date (size/mtime match)",
            "none": "Already present at destination",
        }.get(compare_mode)

        block = Text()
        block.append("● Scan complete", style="bold blue")
        block.append("\n")
        block.append("  Files discovered")
        block.append(" : ", style="dim")
        block.append(_format_count(total), style="bold")
        block.append("\n")
        block.append("  Eligible to copy")
        block.append(" : ", style="dim")
        block.append(_format_count(eligible), style="green")
        if match_label is not None:
            block.append("\n")
            block.append(f"  {match_label}")
            block.append(" : ", style="dim")
            block.append(_format_count(likely_skipped), style="yellow")
            block.append(" (scan estimate)", style="dim")
        if summary.get("limited_to") is not None:
            block.append("\n")
            block.append("  Copy limit applied")
            block.append(" : ", style="dim")
            block.append(f"{_format_count(selected)} files", style="bold yellow")
        else:
            block.append("\n")
            block.append("  Selected to transfer")
            block.append(" : ", style="dim")
            block.append(f"{_format_count(selected)} files", style="green")
        if compare_mode == "checksum" and summary["deferred_existing"]:
            block.append("\n")
            block.append("  Existing files requiring checksum")
            block.append(" : ", style="dim")
            block.append(_format_count(summary["deferred_existing"]), style="yellow")
        block.append("\n")
        return block

    @staticmethod
    def _render_recursive_transfer_start():
        start = Text("▶ Starting transfers", style="bold blue")
        start.append("\n")
        return start

    def _render_recursive_final_summary(
        self,
        copied,
        copied_bytes,
        skipped,
        failed,
        elapsed,
        scan_summary,
        *,
        cancelled=False,
    ):
        matched = self._estimated_recursive_scan_matches(scan_summary)
        compare_mode = scan_summary["compare_mode"]
        skip_note = {
            "size": "files matched by size during scan",
            "size_mtime": "files matched by size/mtime during scan",
            "none": "files already present at destination during scan",
        }.get(compare_mode)
        skipped_total = matched + skipped

        block = Text()
        block.append("\n")
        if cancelled:
            block.append("⚠ Copy interrupted", style="bold yellow")
        else:
            block.append("✓ Copy complete", style="bold green")
        block.append("\n")
        block.append("  Copied")
        block.append("  : ", style="dim")
        block.append(_file_count_text(copied), style="green")
        if copied_bytes:
            block.append("   ", style="dim")
            block.append(_TransferDisplay._size_text(copied_bytes), style="green")
        block.append("\n")
        block.append("  Skipped")
        block.append(" : ", style="dim")
        if skip_note is not None and skipped_total:
            block.append(f"{_format_count(matched)} {skip_note}", style="yellow")
            if skipped:
                block.append("; ", style="dim")
                block.append(
                    f"{_format_count(skipped)} skipped during transfer",
                    style="yellow",
                )
        elif skipped_total:
            block.append(
                f"{_format_count(skipped_total)} skipped during transfer",
                style="yellow",
            )
        else:
            block.append("0", style="yellow")
        block.append("\n")
        block.append("  Failed")
        block.append("  : ", style="dim")
        block.append(_file_count_text(failed), style="red" if failed else "")
        block.append("\n")
        block.append("  Avg rate")
        block.append(": ", style="dim")
        block.append(_average_rate_text(copied_bytes, elapsed), style="bold")
        block.append("\n")
        block.append("  Elapsed")
        block.append(" : ", style="dim")
        block.append(_short_elapsed_text(elapsed), style="bold")
        return block

    def _render_single_final_summary(
        self,
        copied,
        copied_bytes,
        skipped,
        failed,
        elapsed,
        *,
        cancelled=False,
    ):
        block = Text()
        block.append("\n")
        if cancelled:
            block.append("⚠ Copy interrupted", style="bold yellow")
        else:
            block.append("✓ Copy complete", style="bold green")
        block.append("\n")
        block.append("  Copied")
        block.append("  : ", style="dim")
        block.append(_file_count_text(copied), style="green")
        if copied_bytes:
            block.append("   ", style="dim")
            block.append(_TransferDisplay._size_text(copied_bytes), style="green")
        block.append("\n")
        block.append("  Skipped")
        block.append(" : ", style="dim")
        if skipped:
            block.append(_file_count_text(skipped), style="yellow")
        else:
            block.append("0", style="yellow")
        block.append("\n")
        block.append("  Failed")
        block.append("  : ", style="dim")
        block.append(_file_count_text(failed), style="red" if failed else "")
        block.append("\n")
        block.append("  Avg rate")
        block.append(": ", style="dim")
        block.append(_average_rate_text(copied_bytes, elapsed), style="bold")
        block.append("\n")
        block.append("  Elapsed")
        block.append(" : ", style="dim")
        block.append(_short_elapsed_text(elapsed), style="bold")
        return block

    def _copy_directory_parallel(self, client, src_url, dst_url, opts, src_st):
        recursive_start = time.monotonic()
        copy_options = self._build_copy_options()
        self._reported_child_errors = set()
        src_fs, src_path = fs.url_to_fs(src_url, opts)
        _dst_fs, dst_path = fs.url_to_fs(dst_url, opts)
        rich_recursive_layout = self._use_recursive_rich_layout()

        if rich_recursive_layout:
            print_live_message(self._render_recursive_intro(src_url, dst_url))

        try:
            client.stat(dst_url)
        except Exception as exc:
            if exception_exit_code(exc) != errno.ENOENT:
                raise
            client.mkdir(dst_url, parents=copy_options.create_parents)

        scan_spinner = None
        if not self._is_quiet():
            scan_spinner = Spinner("Scanning files")
            scan_spinner.start()
        try:
            entries = src_fs.ls(src_path, detail=True)
            dst_entries = []
            with contextlib.suppress(Exception):
                dst_entries = _dst_fs.ls(dst_path, detail=True)
        except Exception:
            if scan_spinner is not None:
                scan_spinner.stop(False)
            raise
        else:
            if scan_spinner is not None:
                scan_spinner.stop(True)

        scan_progress = None
        if (
            entries
            and sys.stdout.isatty()
            and not self.params.verbose
            and not self._is_quiet()
        ):
            scan_progress = CountProgress(
                "Scanning files", len(entries), transient=False
            )
            scan_progress.start()
        child_entries = []
        scanned_count = 0
        for entry in entries:
            name = self._entry_name(entry)
            if name in (".", ".."):
                continue
            scanned_count += 1
            child_entries.append(
                (_url_path_join(src_url, name), _url_path_join(dst_url, name), entry)
            )
            if scan_progress is not None and (
                scanned_count == 1 or scanned_count % 250 == 0
            ):
                scan_progress.update(completed=scanned_count)
        if scan_progress is not None:
            scan_progress.update(completed=scanned_count)
            scan_progress.stop(True)
        child_jobs, child_summary = self._classify_recursive_child_jobs(
            [
                (self._entry_name(entry), child_src_url, child_dst_url, entry)
                for child_src_url, child_dst_url, entry in child_entries
            ],
            dst_entries,
            copy_options.compare,
        )
        child_jobs, child_summary = self._apply_job_limit(child_jobs, child_summary)
        if not self._is_quiet():
            if rich_recursive_layout:
                print_live_message(self._render_recursive_scan_summary(child_summary))
            else:
                summary = self._recursive_scan_summary(child_summary)
                if child_summary.get("limited_to") is not None:
                    summary = f"{summary} (limited to {child_summary['limited_to']})"
                print_live_message(summary)

        max_parallel = min(
            self._recursive_parallelism(src_url, dst_url), len(child_jobs)
        )
        if max_parallel <= 0:
            max_parallel = 1

        active = []
        failures = []
        pending = deque(child_jobs)
        copied_count = 0
        copied_bytes = 0
        skipped_count = 0
        finished_count = 0
        aggregate_progress = None
        cancelled = False
        if rich_recursive_layout and child_jobs:
            print_live_message(self._render_recursive_transfer_start())
        if (
            child_jobs
            and sys.stdout.isatty()
            and not self.params.verbose
            and not self._is_quiet()
        ):
            aggregate_progress = CountProgress(
                "Copying files",
                len(child_jobs),
                transient=rich_recursive_layout,
            )
            aggregate_progress.start()

        def _update_aggregate_progress():
            if aggregate_progress is not None:
                aggregate_progress.update(
                    completed=finished_count,
                    bytes_completed=copied_bytes,
                )

        def _cancel_active_transfers():
            nonlocal cancelled
            cancelled = True
            for _, _, active_handle, active_display in active:
                active_display.suppress_output()
                active_handle.cancel()
            deadline = time.monotonic() + min(5.0, 0.2 * len(active) + 0.5)
            while active and time.monotonic() < deadline:
                if all(active_handle.done() for _, _, active_handle, _ in active):
                    break
                time.sleep(0.05)

        def _start_child(child_src_url, child_dst_url):
            if self._cancel_event.is_set():
                raise GfalError("Transfer cancelled", errno.ECANCELED)
            display = _TransferDisplay(
                child_src_url,
                child_dst_url,
                quiet=self._is_quiet(),
                verbose=self.params.verbose,
                transfer_mode=self._predicted_transfer_mode(
                    child_src_url, child_dst_url
                ),
                history_only=True,
                transfer_index=finished_count + len(active) + 1,
                transfer_total=len(child_jobs),
                rich_history=rich_recursive_layout,
            )
            if display.show_progress:
                display.start()
            with contextlib.suppress(Exception):
                child_st = client._async_client._stat_sync(child_src_url)
                if not child_st.is_dir():
                    display.set_total_size(child_st.st_size)

            def _handle_warn(msg, dst=child_dst_url, child_display=display):
                if self._is_skip_message(msg):
                    child_display.mark_skipped()
                    if child_display.show_progress:
                        return
                self._warn_copy_message(msg, dst)

            handle = client.start_copy(
                child_src_url,
                child_dst_url,
                options=copy_options,
                progress_callback=display.update,
                start_callback=display.start,
                warn_callback=_handle_warn,
                transfer_mode_callback=display.set_mode,
                error_callback=self._child_error_callback,
                traverse_callback=self._traverse_callback,
                cancel_event=self._cancel_event,
            )
            active.append((child_src_url, child_dst_url, handle, display))

        try:
            while pending or active:
                if self._cancel_event.is_set():
                    _cancel_active_transfers()
                    raise GfalError("Transfer cancelled", errno.ECANCELED)
                while pending and len(active) < max_parallel:
                    child_src_url, child_dst_url = pending.popleft()
                    _start_child(child_src_url, child_dst_url)

                completed_any = False
                for child_src_url, child_dst_url, handle, display in list(active):
                    if not handle.done():
                        continue
                    completed_any = True
                    active.remove((child_src_url, child_dst_url, handle, display))
                    try:
                        display.transfer_index = finished_count + 1
                        handle.wait()
                        display.finish(True)
                        if getattr(display, "final_status", None) == "skipped":
                            skipped_count += 1
                        else:
                            copied_count += 1
                            display_size = getattr(display, "src_size", None)
                            if display_size:
                                copied_bytes += display_size
                        finished_count += 1
                        _update_aggregate_progress()
                    except Exception as exc:
                        display.transfer_index = finished_count + 1
                        display.finish(False)
                        if (
                            self._child_error_key(exc)
                            not in self._reported_child_errors
                        ):
                            self._print_error(exc)
                        failures.append(exc)
                        finished_count += 1
                        _update_aggregate_progress()
                        if copy_options.abort_on_failure:
                            for _, _, active_handle, active_display in active:
                                active_handle.cancel()
                                active_display.finish(False)
                            if aggregate_progress is not None:
                                aggregate_progress.stop(False)
                            raise exc

                if not completed_any:
                    time.sleep(0.05)

            client._async_client._preserve_times(
                src_st,
                dst_url,
                dst_path,
                copy_options,
                lambda msg: self._warn_copy_message(msg, dst_url),
            )
        except Exception as exc:
            if exception_exit_code(exc) == errno.ECANCELED:
                cancelled = True
            raise
        finally:
            if aggregate_progress is not None:
                aggregate_progress.stop(not failures and not cancelled)
            if not self._is_quiet():
                if rich_recursive_layout:
                    print_live_message(
                        self._render_recursive_final_summary(
                            copied_count,
                            copied_bytes,
                            skipped_count,
                            len(failures),
                            time.monotonic() - recursive_start,
                            child_summary,
                            cancelled=cancelled,
                        )
                    )
                else:
                    print_live_message(
                        self._recursive_result_summary(
                            copied_count,
                            skipped_count,
                            len(failures),
                            time.monotonic() - recursive_start,
                        )
                    )

        if failures:
            raise GfalPartialFailureError(
                f"{len(failures)} recursive transfer(s) failed",
                failures,
            )

    def _do_copy(self, src_url, dst_url, opts):
        """High-level copy wrapper over the library client."""
        client = self._build_client()

        if self.params.dry_run:
            src_st = client.stat(src_url)
            if src_st.is_dir():
                if not self.params.recursive:
                    print(f"Skipping directory {src_url} (use -r to copy recursively)")
                    return
                if not client.exists(dst_url):
                    print(f"Mkdir {dst_url}")
                print(f"Copy {src_url} => {dst_url}")
                return
            print(f"Copy {src_url} => {dst_url}")
            return

        if getattr(self.params, "tpc_only", False) and not _tpc_applicable(
            src_url, dst_url
        ):
            src_scheme = urlparse(src_url).scheme.lower()
            dst_scheme = urlparse(dst_url).scheme.lower()
            raise OSError(
                "Third-party copy required (--tpc-only) but not available: "
                f"TPC not supported for {src_scheme}:// -> {dst_scheme}://"
            )

        src_st = None
        try:
            src_st = client.stat(src_url)
            dst_st = client.stat(dst_url)
            if src_st.is_dir() and not dst_st.is_dir():
                raise IsADirectoryError("Cannot copy a directory over a file")
        except IsADirectoryError:
            raise
        except FileNotFoundError:
            pass
        except Exception:
            pass

        if src_st is not None and src_st.is_dir() and self.params.recursive:
            self._copy_directory_parallel(client, src_url, dst_url, opts, src_st)
            return

        quiet = self._is_quiet()
        rich_single_layout = self._use_recursive_rich_layout() and not quiet
        if rich_single_layout:
            print_live_message(self._render_recursive_intro(src_url, dst_url))
            print_live_message(self._render_recursive_transfer_start())
        display = _TransferDisplay(
            src_url,
            dst_url,
            quiet=quiet,
            verbose=self.params.verbose,
            transfer_mode=self._predicted_transfer_mode(src_url, dst_url),
        )
        if display.show_progress:
            display.start()
        self.progress_bar = display.progress_bar
        with contextlib.suppress(Exception):
            display.set_total_size(client.stat(src_url).st_size)

        def _handle_warn(message):
            if self._is_skip_message(message):
                display.mark_skipped()
                if display.show_progress:
                    return
            self._warn_copy_message(message, dst_url)

        copy_failed = True
        cancelled = False
        try:
            client.copy(
                src_url,
                dst_url,
                options=self._build_copy_options(),
                progress_callback=display.update,
                start_callback=display.start,
                warn_callback=_handle_warn,
                transfer_mode_callback=display.set_mode,
                error_callback=None if quiet else self._child_error_callback,
                traverse_callback=None if quiet else self._traverse_callback,
                cancel_event=self._cancel_event,
            )
            copy_failed = False
        except Exception as exc:
            if exception_exit_code(exc) == errno.ECANCELED:
                cancelled = True
            raise
        finally:
            display.finish(not copy_failed)
            if rich_single_layout:
                skipped = int(getattr(display, "final_status", None) == "skipped")
                copied = int(not copy_failed and not skipped)
                failed = int(copy_failed and not cancelled)
                copied_bytes = display.src_size if copied and display.src_size else 0
                print_live_message(
                    self._render_single_final_summary(
                        copied,
                        copied_bytes,
                        skipped,
                        failed,
                        time.monotonic() - display.transfer_start,
                        cancelled=cancelled,
                    )
                )
            elif display.show_progress:
                print()


# ---------------------------------------------------------------------------
# Checksum helpers
# ---------------------------------------------------------------------------
