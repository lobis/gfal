"""
gfal cp implementation.
"""

import contextlib
import hashlib
import stat
import sys
import threading
import time
import zlib
from pathlib import Path
from urllib.parse import urlparse

from gfal.cli import base
from gfal.cli.progress import Progress
from gfal.core import fs


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
        "-r", "--recursive", action="store_true", help="copy directories recursively"
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
        "streamed = force client-side streaming. Overrides --tpc/--tpc-only/--tpc-mode "
        "when specified.",
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
            if val is not None:
                sys.stderr.write(
                    f"{self.prog}: warning: {flag} is not supported in this "
                    "implementation and will be ignored\n"
                )

        opts = fs.build_storage_options(self.params)

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
                sys.stderr.write(f"ERROR: {e}\n")
                rc = 1
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

    def _do_copy(self, src_url, dst_url, opts):
        """High-level copy: handle directories, overwrite checks, etc."""
        src_fs, src_path = fs.url_to_fs(src_url, opts)
        dst_fs, dst_path = fs.url_to_fs(dst_url, opts)

        src_info = src_fs.info(src_path)
        src_st = fs.StatInfo(src_info)
        src_isdir = stat.S_ISDIR(src_st.st_mode)

        # Fail fast for explicit --tpc-only when the protocol pair can never
        # support third-party copy. This avoids slow destination probes for
        # cases like file:// -> https:// that should be rejected immediately.
        if (
            getattr(self.params, "tpc_only", False)
            and not self.params.dry_run
            and not _tpc_applicable(src_url, dst_url)
        ):
            src_scheme = urlparse(src_url).scheme.lower()
            dst_scheme = urlparse(dst_url).scheme.lower()
            raise OSError(
                "Third-party copy required (--tpc-only) but not available: "
                f"TPC not supported for {src_scheme}:// -> {dst_scheme}://"
            )

        dst_isdir = False
        dst_exists = False
        try:
            dst_info = dst_fs.info(dst_path)
            dst_st = fs.StatInfo(dst_info)
            dst_isdir = stat.S_ISDIR(dst_st.st_mode)
            dst_exists = True
        except (FileNotFoundError, Exception):
            pass

        if not self.params.just_copy:
            if dst_exists and not dst_isdir and not self.params.force:
                # Pipes and character devices (e.g. /dev/stdout) can always be
                # written to without --force — they don't hold persistent data.
                _is_special = _is_special_file(dst_path)
                if not _is_special:
                    raise FileExistsError(
                        f"Destination '{dst_url}' exists and --force not set"
                    )

            if dst_exists and not dst_isdir and src_isdir:
                raise IsADirectoryError("Cannot copy a directory over a file")

        if src_isdir:
            if not self.params.recursive:
                print(f"Skipping directory {src_url} (use -r to copy recursively)")
                return
            if not dst_exists:
                if self.params.dry_run:
                    print(f"Mkdir {dst_url}")
                else:
                    dst_fs.mkdir(dst_path, create_parents=self.params.parent)
            self._recursive_copy(
                src_url, src_fs, src_path, dst_url, dst_fs, dst_path, opts
            )
            return

        # Resolve destination if it is a directory
        if dst_isdir:
            name = Path(src_path.rstrip("/")).name
            dst_url = dst_url.rstrip("/") + "/" + name
            dst_fs, dst_path = fs.url_to_fs(dst_url, opts)

        # ------------------------------------------------------------------
        # Third-party copy attempt
        # ------------------------------------------------------------------
        explicit_tpc = getattr(self.params, "tpc", False) or getattr(
            self.params, "tpc_only", False
        )
        use_streamed = getattr(self.params, "copy_mode", None) == "streamed"
        # Auto-TPC: mirrors gfal2's default behaviour — for HTTP<->HTTP and
        # root<->root transfers, attempt TPC first and fall back to streaming
        # unless the user explicitly requested streaming with --copy-mode=streamed.
        auto_tpc = (
            not explicit_tpc and not use_streamed and _tpc_applicable(src_url, dst_url)
        )
        use_tpc = explicit_tpc or auto_tpc
        if use_tpc and not self.params.dry_run:
            tpc_timeout = getattr(self.params, "transfer_timeout", 0) or None

            # ------------------------------------------------------------------
            # Progress display for TPC
            #
            # Always lazy: progress only starts when the first WLCG perf-marker
            # is received.  This means a TPC attempt that fails before any bytes
            # move (wrong protocol pair, 405, auth error) produces no output —
            # streaming then takes over and shows its own clean progress line.
            # For XRootD TPC (no perf-markers) we start progress explicitly
            # just before the blocking CopyProcess.run() call.
            # ------------------------------------------------------------------
            show_progress = sys.stdout.isatty() and not self.params.verbose
            tpc_start = time.monotonic()
            tpc_progress_shown = [False]

            def _start_tpc_progress():
                if tpc_progress_shown[0]:
                    return
                tpc_progress_shown[0] = True
                if show_progress:
                    self.progress_bar = Progress(f"Copying {Path(src_url).name}")
                    self.progress_bar.update(
                        total_size=src_st.st_size if src_st.st_size else None
                    )
                    self.progress_bar.start()
                else:
                    print(
                        f"Copying {src_st.st_size or 0} bytes  {src_url}  =>  {dst_url}"
                    )

            def _stop_tpc_progress(success):
                if not tpc_progress_shown[0]:
                    return
                if show_progress:
                    self.progress_bar.stop(success)
                    print()

            def _tpc_progress(bytes_transferred):
                _start_tpc_progress()
                if show_progress and src_st.st_size:
                    self.progress_bar.update(
                        curr_size=bytes_transferred,
                        total_size=src_st.st_size,
                        elapsed=time.monotonic() - tpc_start,
                    )

            try:
                from gfal.core import (
                    tpc as _tpc,  # lazy: tpc.py may not be installed  # noqa: PLC0415
                )

                _tpc.do_tpc(
                    src_url,
                    dst_url,
                    opts,
                    mode=getattr(self.params, "tpc_mode", "pull"),
                    timeout=tpc_timeout,
                    verbose=bool(self.params.verbose),
                    scitag=getattr(self.params, "scitag", None),
                    progress_callback=_tpc_progress,
                    start_callback=_start_tpc_progress,
                )
                _stop_tpc_progress(True)
                return  # TPC succeeded — nothing more to do
            except ImportError as e:
                _stop_tpc_progress(False)
                if getattr(self.params, "tpc_only", False):
                    raise OSError(
                        "Third-party copy required (--tpc-only) but the tpc "
                        "module is not available in this installation"
                    ) from e
                if self.params.verbose:
                    sys.stderr.write(
                        "TPC module not available, falling back to streaming\n"
                    )
            except NotImplementedError as e:
                _stop_tpc_progress(False)
                if getattr(self.params, "tpc_only", False):
                    raise OSError(
                        f"Third-party copy required (--tpc-only) but not available: {e}"
                    ) from e
                # Fall through to streaming copy
                if self.params.verbose:
                    sys.stderr.write(
                        f"TPC not available ({e}), falling back to streaming\n"
                    )
            except Exception:
                _stop_tpc_progress(False)
                if auto_tpc:
                    # Auto-TPC failed (e.g. server returned error, auth issue);
                    # fall back to client-side streaming silently unless verbose.
                    if self.params.verbose:
                        sys.stderr.write("Auto-TPC failed, falling back to streaming\n")
                else:
                    raise  # explicit --tpc: propagate the error

        # ------------------------------------------------------------------
        # Streaming (client-side) copy with optional per-file timeout
        # ------------------------------------------------------------------
        transfer_timeout = getattr(self.params, "transfer_timeout", 0)
        if transfer_timeout and transfer_timeout > 0:
            exc_holder = [None]

            def _run():
                try:
                    self._copy_file(
                        src_url,
                        src_fs,
                        src_path,
                        dst_url,
                        dst_fs,
                        dst_path,
                        src_st.st_size,
                    )
                except Exception as e:
                    exc_holder[0] = e

            t = threading.Thread(target=_run, daemon=True)
            t.start()
            t.join(transfer_timeout)
            if t.is_alive():
                raise TimeoutError(
                    f"Transfer timed out after {transfer_timeout}s: {src_url}"
                )
            if exc_holder[0] is not None:
                raise exc_holder[0]
        else:
            self._copy_file(
                src_url, src_fs, src_path, dst_url, dst_fs, dst_path, src_st.st_size
            )

    def _recursive_copy(
        self, src_url, src_fs, src_path, dst_url, dst_fs, dst_path, opts
    ):
        entries = src_fs.ls(src_path, detail=False)
        src_path.rstrip("/") + "/"
        dst_path.rstrip("/") + "/"
        src_url_base = src_url.rstrip("/") + "/"
        dst_url_base = dst_url.rstrip("/") + "/"

        for entry_path in entries:
            name = Path(entry_path.rstrip("/")).name
            if name in (".", ".."):
                continue
            child_src_url = src_url_base + name
            child_dst_url = dst_url_base + name
            try:
                self._do_copy(child_src_url, child_dst_url, opts)
            except Exception as e:
                sys.stderr.write(f"ERROR copying {child_src_url}: {e}\n")
                if self.params.abort_on_failure:
                    raise

    def _copy_file(
        self, src_url, src_fs, src_path, dst_url, dst_fs, dst_path, src_size
    ):
        """Stream a single file from src to dst with optional progress and checksum."""
        if self.params.dry_run:
            print(f"Copy {src_url} => {dst_url}")
            return

        # Create parent directories if requested
        if self.params.parent:
            parent = str(Path(dst_path).parent)
            if parent:
                with contextlib.suppress(Exception):
                    dst_fs.mkdir(parent, create_parents=True)

        # Compute source checksum before transfer if requested (skipped with --just-copy)
        src_checksum = None
        if (
            self.params.checksum
            and not self.params.just_copy
            and self.params.checksum_mode in ("source", "both")
        ):
            alg, expected = _parse_checksum_arg(self.params.checksum)
            src_checksum = _checksum_fs(src_fs, src_path, alg)
            if expected and src_checksum != expected.lower():
                raise OSError(
                    f"Source checksum mismatch: expected {expected}, got {src_checksum}"
                )

        # Set up progress bar
        show_progress = sys.stdout.isatty() and not self.params.verbose
        if show_progress:
            self.progress_bar = Progress(f"Copying {Path(src_url).name}")
            self.progress_bar.update(total_size=src_size if src_size else None)
            self.progress_bar.start()
        else:
            print(f"Copying {src_size or 0} bytes  {src_url}  =>  {dst_url}")

        start = time.monotonic()
        transferred = 0
        dst_checksum_hasher = None

        alg_for_dst = None
        if (
            self.params.checksum
            and not self.params.just_copy
            and self.params.checksum_mode in ("target", "both")
        ):
            alg_for_dst, _ = _parse_checksum_arg(self.params.checksum)
            dst_checksum_hasher = _make_hasher(alg_for_dst)

        try:
            with (
                src_fs.open(src_path, "rb") as src_f,
                dst_fs.open(dst_path, "wb") as dst_f,
            ):
                while True:
                    chunk = src_f.read(fs.CHUNK_SIZE)
                    if not chunk:
                        break
                    dst_f.write(chunk)
                    transferred += len(chunk)
                    if dst_checksum_hasher is not None:
                        _update_hasher(dst_checksum_hasher, alg_for_dst, chunk)
                    if show_progress and src_size:
                        elapsed = time.monotonic() - start
                        self.progress_bar.update(
                            curr_size=transferred,
                            total_size=src_size,
                            elapsed=elapsed,
                        )
        except Exception:
            if show_progress:
                self.progress_bar.stop(False)
                print()
            # Remove the partially-written destination file unless the caller
            # explicitly opted out with --disable-cleanup.
            if not self.params.disable_cleanup:
                with contextlib.suppress(Exception):
                    dst_fs.rm(dst_path, recursive=False)
            raise

        if show_progress:
            self.progress_bar.stop(True)
            print()

        # Verify destination checksum
        if dst_checksum_hasher is not None:
            dst_checksum = _finalise_hasher(dst_checksum_hasher, alg_for_dst)
            if src_checksum and dst_checksum != src_checksum:
                raise OSError(
                    f"Checksum mismatch after transfer: src={src_checksum} dst={dst_checksum}"
                )


# ---------------------------------------------------------------------------
# Checksum helpers
# ---------------------------------------------------------------------------


def _parse_checksum_arg(arg):
    """Return (algorithm, expected_value_or_None)."""
    parts = arg.split(":", 1)
    alg = parts[0].upper()
    expected = parts[1].lower() if len(parts) > 1 else None
    return alg, expected


def _make_hasher(alg):
    alg = alg.upper()
    if alg in ("ADLER32", "CRC32"):
        return [alg, 1 if alg == "ADLER32" else 0]
    return hashlib.new(alg.lower().replace("-", ""))


def _update_hasher(h, alg, chunk):
    alg = alg.upper()
    if alg == "ADLER32":
        h[1] = zlib.adler32(chunk, h[1]) & 0xFFFFFFFF
    elif alg == "CRC32":
        h[1] = zlib.crc32(chunk, h[1]) & 0xFFFFFFFF
    else:
        h.update(chunk)


def _finalise_hasher(h, alg):
    alg = alg.upper()
    if alg in ("ADLER32", "CRC32"):
        return f"{h[1]:08x}"
    return h.hexdigest()


def _tpc_applicable(src_url, dst_url):
    """Return True when TPC should be attempted automatically.

    TPC is applicable when both endpoints use the same transport family:
    - HTTP/HTTPS  <->  HTTP/HTTPS  (WebDAV COPY)
    - root/xroot  <->  root/xroot  (XRootD CopyProcess)

    This mirrors gfal2's behavior of attempting TPC by default for these
    protocol pairs and falling back to streaming if TPC is unavailable.
    """
    src_s = urlparse(src_url).scheme.lower()
    dst_s = urlparse(dst_url).scheme.lower()
    http = {"http", "https"}
    xrd = {"root", "xroot"}
    return (src_s in http and dst_s in http) or (src_s in xrd and dst_s in xrd)


def _is_special_file(path):
    """Return True if *path* is a FIFO, character device, or socket.

    These can always be written to without ``--force`` because they don't
    hold persistent data that would be lost on overwrite (e.g. /dev/stdout).
    Returns False on any OS error (e.g. path is on a remote filesystem).
    """
    try:
        m = Path(path).stat().st_mode
        return stat.S_ISFIFO(m) or stat.S_ISCHR(m) or stat.S_ISSOCK(m)
    except OSError:
        return False


def _checksum_fs(fso, path, alg):
    """Compute checksum by reading the file."""
    h = _make_hasher(alg)
    with fso.open(path, "rb") as f:
        while True:
            chunk = f.read(fs.CHUNK_SIZE)
            if not chunk:
                break
            _update_hasher(h, alg, chunk)
    return _finalise_hasher(h, alg)
