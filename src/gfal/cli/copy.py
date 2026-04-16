"""
gfal cp implementation.
"""

import stat
import sys
import time
from pathlib import Path
from urllib.parse import urlparse

from gfal.cli import base
from gfal.cli.base import exception_exit_code
from gfal.cli.progress import Progress
from gfal.core import api as core_api
from gfal.core import fs
from gfal.core.api import (
    ChecksumPolicy,
    CopyOptions,
    GfalClient,
)
from gfal.core.api import (
    checksum_fs as _checksum_fs,
)
from gfal.core.api import (
    parse_checksum_arg as _parse_checksum_arg,
)
from gfal.core.api import (
    tpc_applicable as _tpc_applicable,
)

_make_hasher = core_api.make_hasher
_update_hasher = core_api.update_hasher
_finalise_hasher = core_api.finalise_hasher
_is_special_file = core_api.is_special_file
_eos_mtime_url = core_api.eos_mtime_url


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
        "--skip-if-same",
        action="store_true",
        help="when destination exists and --force is not set, compare checksums "
        "and skip the copy if source and destination already match",
    )
    @base.arg(
        "-r", "--recursive", action="store_true", help="copy directories recursively"
    )
    @base.arg(
        "--preserve-times",
        action="store_true",
        help="preserve source access and modification times when supported",
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
                self._print_error(e)
                rc = exception_exit_code(e)
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

        tpc = "never"
        if getattr(self.params, "tpc_only", False):
            tpc = "only"
        elif getattr(self.params, "tpc", False):
            tpc = "auto"

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
            preserve_times=getattr(self.params, "preserve_times", False),
            skip_if_same=getattr(self.params, "skip_if_same", False),
            just_copy=getattr(self.params, "just_copy", False),
            disable_cleanup=getattr(self.params, "disable_cleanup", False),
            no_delegation=getattr(self.params, "no_delegation", False),
            evict=getattr(self.params, "evict", False),
            scitag=getattr(self.params, "scitag", None),
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

        if getattr(self.params, "skip_if_same", False) and not getattr(
            self.params, "force", False
        ):
            src_fs, src_path = fs.url_to_fs(src_url, opts)
            dst_fs, dst_path = fs.url_to_fs(dst_url, opts)
            try:
                dst_info = dst_fs.info(dst_path)
                dst_mode = fs.StatInfo(dst_info).st_mode
                if (
                    stat.S_ISREG(dst_mode)
                    and not _is_special_file(src_path)
                    and not _is_special_file(dst_path)
                ):
                    algorithm = "ADLER32"
                    if self.params.checksum:
                        algorithm, _ = _parse_checksum_arg(self.params.checksum)
                    if _checksum_fs(src_fs, src_path, algorithm) == _checksum_fs(
                        dst_fs, dst_path, algorithm
                    ):
                        print(
                            f"Skipping existing file {dst_url} "
                            f"(matching {algorithm} checksum)"
                        )
                        return
            except Exception:
                pass

        src_size = None
        show_progress = sys.stdout.isatty() and not self.params.verbose
        progress_started = [False]
        transfer_start = time.monotonic()

        try:
            src_size = client.stat(src_url).st_size
        except Exception:
            src_size = None

        def _start_progress():
            if progress_started[0]:
                return
            progress_started[0] = True
            if show_progress:
                self.progress_bar = Progress(f"Copying {Path(src_url).name}")
                self.progress_bar.update(total_size=src_size if src_size else None)
                self.progress_bar.start()
            else:
                print(f"Copying {src_size or 0} bytes  {src_url}  =>  {dst_url}")

        def _progress(bytes_transferred):
            _start_progress()
            if show_progress and src_size:
                self.progress_bar.update(
                    curr_size=bytes_transferred,
                    total_size=src_size,
                    elapsed=time.monotonic() - transfer_start,
                )

        def _warn(message):
            if message.startswith("Skipping existing file ") or message.startswith(
                "Skipping directory "
            ):
                print(message)
                return
            normalized = fs.normalize_url(dst_url)
            scheme = urlparse(normalized).scheme.lower() or "unknown"
            if message.startswith("--preserve-times"):
                if scheme in self._preserve_times_warned:
                    return
                self._preserve_times_warned.add(scheme)
            sys.stderr.write(f"{self.prog}: warning: {message}\n")

        copy_failed = True
        try:
            client.copy(
                src_url,
                dst_url,
                options=self._build_copy_options(),
                progress_callback=_progress,
                start_callback=_start_progress,
                warn_callback=_warn,
            )
            copy_failed = False
        finally:
            if progress_started[0] and show_progress:
                self.progress_bar.stop(not copy_failed)
                print()


# ---------------------------------------------------------------------------
# Checksum helpers
# ---------------------------------------------------------------------------
