"""
gfal cp implementation.
"""

import contextlib
import errno
import stat
import sys
import threading
import time
from pathlib import Path
from urllib.parse import urlparse, urlunparse

from gfal.cli import base
from gfal.cli.base import exception_exit_code
from gfal.cli.progress import Progress, Spinner, print_live_message
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

_DEFAULT_RECURSIVE_PARALLELISM = 5


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
    ):
        self.src_url = src_url
        self.dst_url = dst_url
        self.quiet = quiet
        self.verbose = verbose
        self.src_size = src_size
        self.show_progress = sys.stdout.isatty() and not verbose and not quiet
        self.progress_bar = None
        self.progress_started = False
        self.transfer_start = time.monotonic()
        self.transfer_mode = transfer_mode
        self.final_status = None
        self._lock = threading.Lock()

    def _transfer_label(self):
        mode_labels = {
            "streamed": "streamed",
            "tpc-pull": "TPC pull",
            "tpc-push": "TPC push",
            "tpc-xrootd": "TPC xrootd",
        }
        if self.transfer_mode is None:
            return f"Copying {Path(self.src_url).name}"
        mode = mode_labels.get(self.transfer_mode, self.transfer_mode)
        return f"Copying {Path(self.src_url).name} ({mode})"

    def start(self):
        with self._lock:
            if self.progress_started:
                return
            self.progress_started = True
            if self.show_progress:
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
            if self.show_progress and self.progress_bar is not None and self.src_size:
                self.progress_bar.update(
                    curr_size=bytes_transferred,
                    total_size=self.src_size,
                    elapsed=time.monotonic() - self.transfer_start,
                )

    def set_mode(self, mode):
        with self._lock:
            self.transfer_mode = mode
            if self.show_progress and self.progress_bar is not None:
                label = self._transfer_label()
                if hasattr(self.progress_bar, "label"):
                    self.progress_bar.label = label
                if hasattr(self.progress_bar, "set_description"):
                    self.progress_bar.set_description(label)

    def set_total_size(self, total_size):
        with self._lock:
            self.src_size = total_size
            if self.show_progress and self.progress_bar is not None and total_size:
                self.progress_bar.update(total_size=total_size)

    def mark_skipped(self):
        with self._lock:
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
            if (
                not self.progress_started
                or not self.show_progress
                or self.progress_bar is None
            ):
                return
            if success and self.src_size and self.final_status != "skipped":
                self.progress_bar.update(
                    curr_size=self.src_size,
                    total_size=self.src_size,
                    elapsed=time.monotonic() - self.transfer_start,
                )
            self.progress_bar.stop(success, status=self.final_status)


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
        if message.startswith("Skipping existing file ") or message.startswith(
            "Skipping directory "
        ):
            print_live_message(message)
            return
        normalized = fs.normalize_url(dst_url)
        scheme = urlparse(normalized).scheme.lower() or "unknown"
        if message.startswith("--preserve-times"):
            if scheme in self._preserve_times_warned:
                return
            self._preserve_times_warned.add(scheme)
        sys.stderr.write(f"{self.prog}: warning: {message}\n")

    def _child_error_callback(self, _child_src_url, _child_dst_url, error):
        self._print_error(error)

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

    def _copy_directory_parallel(self, client, src_url, dst_url, opts, src_st):
        copy_options = self._build_copy_options()
        src_fs, src_path = fs.url_to_fs(src_url, opts)
        dst_fs, dst_path = fs.url_to_fs(dst_url, opts)

        try:
            client.stat(dst_url)
        except Exception as exc:
            if exception_exit_code(exc) != errno.ENOENT:
                raise
            client.mkdir(dst_url, parents=copy_options.create_parents)

        scan_spinner = None
        if not self._is_quiet():
            scan_spinner = Spinner(f"Scanning {src_url}  =>  {dst_url}")
            scan_spinner.start()
        try:
            entries = src_fs.ls(src_path, detail=False)
        finally:
            if scan_spinner is not None:
                scan_spinner.stop(True)
        child_jobs = []
        for entry_path in entries:
            name = Path(entry_path.rstrip("/")).name
            if name in (".", ".."):
                continue
            child_jobs.append(
                (_url_path_join(src_url, name), _url_path_join(dst_url, name))
            )

        max_parallel = min(
            self._recursive_parallelism(src_url, dst_url), len(child_jobs)
        )
        if max_parallel <= 0:
            max_parallel = 1

        active = []
        failures = []
        pending = list(child_jobs)

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
            )
            if display.show_progress:
                display.start()
            with contextlib.suppress(Exception):
                display.set_total_size(client.stat(child_src_url).st_size)

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

        while pending or active:
            if self._cancel_event.is_set():
                for _, _, active_handle, active_display in active:
                    active_handle.cancel()
                    active_display.finish(False)
                raise GfalError("Transfer cancelled", errno.ECANCELED)
            while pending and len(active) < max_parallel:
                child_src_url, child_dst_url = pending.pop(0)
                _start_child(child_src_url, child_dst_url)

            completed_any = False
            for child_src_url, child_dst_url, handle, display in list(active):
                if not handle.done():
                    continue
                completed_any = True
                active.remove((child_src_url, child_dst_url, handle, display))
                try:
                    handle.wait()
                    display.finish(True)
                except Exception as exc:
                    display.finish(False)
                    self._print_error(exc)
                    failures.append(exc)
                    if copy_options.abort_on_failure:
                        for _, _, active_handle, active_display in active:
                            active_handle.cancel()
                            active_display.finish(False)
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
        finally:
            display.finish(not copy_failed)
            if display.show_progress:
                print()


# ---------------------------------------------------------------------------
# Checksum helpers
# ---------------------------------------------------------------------------
