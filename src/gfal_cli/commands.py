"""
Simple commands: mkdir, save, cat, stat, rename, chmod, sum, xattr.
"""

import contextlib
import errno
import stat
import sys
from datetime import datetime

from gfal_cli import base, fs
from gfal_cli.utils import file_mode_str, file_type_str


class GfalCommands(base.CommandBase):
    # ------------------------------------------------------------------
    # mkdir
    # ------------------------------------------------------------------

    @base.arg(
        "-m",
        "--mode",
        type=str,
        default="755",
        metavar="MODE",
        help="directory permissions in octal (default: 755)",
    )
    @base.arg(
        "-p",
        "--parents",
        action="store_true",
        help="no error if existing, create parent directories as needed",
    )
    @base.arg("directory", nargs="+", type=base.surl, help="directory URI(s)")
    def execute_mkdir(self):
        """Create directories."""
        try:
            mode_int = int(self.params.mode, 8)
        except ValueError:
            sys.stderr.write(
                f"{self.progr}: invalid mode '{self.params.mode}': must be an octal number (e.g. 755, 0755)\n"
            )
            return 1

        opts = fs.build_storage_options(self.params)
        rc = 0
        for d in self.params.directory:
            try:
                fso, path = fs.url_to_fs(d, opts)
                if self.params.parents:
                    # makedirs is idempotent; fall back to mkdir if not available
                    if hasattr(fso, "makedirs"):
                        fso.makedirs(path, exist_ok=True)
                    else:
                        with contextlib.suppress(FileExistsError):
                            fso.mkdir(path, create_parents=True)
                else:
                    fso.mkdir(path, create_parents=False)
                # Apply mode if the filesystem supports chmod (best effort —
                # not all backends honour permissions, e.g. HTTP/XRootD readonly)
                with contextlib.suppress(Exception):
                    fso.chmod(path, mode_int)
            except Exception as e:
                sys.stderr.write(f"{self.progr}: {self._format_error(e)}\n")
                ecode = getattr(e, "errno", None)
                rc = ecode if ecode and 0 < ecode <= 255 else 1
        return rc

    # ------------------------------------------------------------------
    # save  (stdin → remote file)
    # ------------------------------------------------------------------

    @base.arg("file", type=base.surl, help="URI of the file to write")
    def execute_save(self):
        """Read from stdin and write to a remote file."""
        opts = fs.build_storage_options(self.params)
        fso, path = fs.url_to_fs(self.params.file, opts)
        with fso.open(path, "wb") as f:
            while True:
                chunk = sys.stdin.buffer.read(fs.CHUNK_SIZE)
                if not chunk:
                    break
                f.write(chunk)

    # ------------------------------------------------------------------
    # cat  (remote file → stdout)
    # ------------------------------------------------------------------

    @base.arg(
        "-b",
        "--bytes",
        action="store_true",
        help="handle file contents as raw bytes (no-op in Python 3; always binary)",
    )
    @base.arg("file", nargs="+", type=base.surl, help="URI(s) to display")
    def execute_cat(self):
        """Print file contents to stdout."""
        opts = fs.build_storage_options(self.params)
        rc = 0
        for url in self.params.file:
            try:
                fso, path = fs.url_to_fs(url, opts)
                with fso.open(path, "rb") as f:
                    while True:
                        chunk = f.read(fs.CHUNK_SIZE)
                        if not chunk:
                            break
                        sys.stdout.buffer.write(chunk)
                sys.stdout.buffer.flush()
            except Exception as e:
                if isinstance(e, OSError) and e.errno == errno.EPIPE:
                    raise
                sys.stderr.write(f"{self.progr}: {self._format_error(e)}\n")
                ecode = getattr(e, "errno", None)
                rc = ecode if ecode and 0 < ecode <= 255 else 1
        return rc

    # ------------------------------------------------------------------
    # stat
    # ------------------------------------------------------------------

    @base.arg("file", nargs="+", type=base.surl, help="URI(s) to stat")
    def execute_stat(self):
        """Display file status."""
        opts = fs.build_storage_options(self.params)
        rc = 0
        first = True
        for url in self.params.file:
            try:
                if not first:
                    print()
                self._stat_one(url, opts)
                first = False
            except Exception as e:
                if isinstance(e, OSError) and e.errno == errno.EPIPE:
                    raise
                sys.stderr.write(f"{self.progr}: {self._format_error(e)}\n")
                ecode = getattr(e, "errno", None)
                rc = ecode if ecode and 0 < ecode <= 255 else 1
                first = False
        return rc

    def _stat_one(self, url, opts):
        fso, path = fs.url_to_fs(url, opts)
        info = fso.info(path)
        info = fs.xrootd_enrich(info, fso)
        st = fs.StatInfo(info)
        print(f"  File: '{url}'")
        print(f"  Size: {st.st_size}\t{file_type_str(stat.S_IFMT(st.st_mode))}")
        print(
            f"Access: ({stat.S_IMODE(st.st_mode):04o}/{file_mode_str(st.st_mode)})\t"
            f"Uid: {st.st_uid}\tGid: {st.st_gid}\t"
        )
        print(
            "Access: {}".format(
                datetime.fromtimestamp(st.st_atime).strftime("%Y-%m-%d %H:%M:%S.%f")
            )
        )
        print(
            "Modify: {}".format(
                datetime.fromtimestamp(st.st_mtime).strftime("%Y-%m-%d %H:%M:%S.%f")
            )
        )
        print(
            "Change: {}".format(
                datetime.fromtimestamp(st.st_ctime).strftime("%Y-%m-%d %H:%M:%S.%f")
            )
        )

    # ------------------------------------------------------------------
    # rename
    # ------------------------------------------------------------------

    @base.arg("source", type=base.surl, help="original URI")
    @base.arg("destination", type=base.surl, help="new URI")
    def execute_rename(self):
        """Rename a file or directory."""
        opts = fs.build_storage_options(self.params)
        src_fs, src_path = fs.url_to_fs(self.params.source, opts)
        dst_fs, dst_path = fs.url_to_fs(self.params.destination, opts)
        if type(src_fs) is not type(dst_fs):
            raise OSError("rename across different filesystem types is not supported")
        src_fs.mv(src_path, dst_path)

    # ------------------------------------------------------------------
    # chmod
    # ------------------------------------------------------------------

    @base.arg("mode", type=str, help="new permissions in octal (e.g. 0755)")
    @base.arg("file", nargs="+", type=base.surl, help="URI(s) of the file(s)")
    def execute_chmod(self):
        """Change file permissions."""
        try:
            mode = int(self.params.mode, base=8)
        except ValueError:
            self.parser.error("Mode must be an octal number (e.g. 0755)")
            return 1
        opts = fs.build_storage_options(self.params)
        rc = 0
        for url in self.params.file:
            try:
                fso, path = fs.url_to_fs(url, opts)
                fso.chmod(path, mode)
            except Exception as e:
                sys.stderr.write(f"{self.progr}: {self._format_error(e)}\n")
                ecode = getattr(e, "errno", None)
                rc = ecode if ecode and 0 < ecode <= 255 else 1
        return rc

    # ------------------------------------------------------------------
    # sum  (checksum)
    # ------------------------------------------------------------------

    @base.arg("file", type=base.surl, help="URI of the file")
    @base.arg(
        "checksum_type",
        type=str,
        help="algorithm: ADLER32, CRC32, CRC32C, MD5, SHA1, SHA256, ...",
    )
    def execute_sum(self):
        """Compute a file checksum."""
        opts = fs.build_storage_options(self.params)
        alg = self.params.checksum_type.upper()
        fso, path = fs.url_to_fs(self.params.file, opts)

        try:
            checksum = fs.compute_checksum(fso, path, alg)
            sys.stdout.write(f"{self.params.file} {checksum}\n")
        except Exception as e:
            sys.stderr.write(f"{self.progr}: {self._format_error(e)}\n")
            return 1

    # ------------------------------------------------------------------
    # xattr
    # ------------------------------------------------------------------

    @base.arg("file", type=base.surl, help="file URI")
    @base.arg(
        "attribute",
        nargs="?",
        type=str,
        help="attribute to get or set (use key=value to set)",
    )
    def execute_xattr(self):
        """Get or set extended attributes."""
        opts = fs.build_storage_options(self.params)
        fso, path = fs.url_to_fs(self.params.file, opts)

        if not hasattr(fso, "getxattr"):
            sys.stderr.write("xattr is not supported by this filesystem\n")
            return 1

        if self.params.attribute is not None:
            if "=" in self.params.attribute:
                i = self.params.attribute.index("=")
                key = self.params.attribute[:i]
                val = self.params.attribute[i + 1 :]
                fso.setxattr(path, key, val)
            else:
                val = fso.getxattr(path, self.params.attribute)
                sys.stdout.write(f"{val}\n")
        else:
            attrs = fso.listxattr(path)
            for attr in attrs:
                try:
                    val = fso.getxattr(path, attr)
                    sys.stdout.write(f"{attr} = {val}\n\n")
                except Exception as e:
                    sys.stdout.write(f"{attr} FAILED: {e}\n\n")
