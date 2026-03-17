"""
Simple commands: mkdir, save, cat, stat, rename, chmod, sum, xattr.
"""

import contextlib
import hashlib
import stat
import sys
import zlib
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
        type=int,
        default=755,
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
        with contextlib.suppress(ValueError):
            int(str(self.params.mode), 8)

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
                sys.stderr.write(f"{self.progr}: {self._format_error(e)}\n")
                ecode = getattr(e, "errno", None)
                rc = ecode if ecode and 0 < ecode <= 255 else 1
                first = False
        return rc

    def _stat_one(self, url, opts):
        st = fs.stat(url, opts)
        print(f"  File: '{url}'")
        print(f"  Size: {st.st_size}\t{file_type_str(stat.S_IFMT(st.st_mode))}")
        print(
            f"Access: ({stat.S_IMODE(st.st_mode):04o}/{file_mode_str(st.st_mode)})\tUid: {st.st_uid}\tGid: {st.st_gid}"
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

        # Try server-side checksum first (XRootD exposes this)
        if hasattr(fso, "checksum"):
            try:
                result = fso.checksum(path, alg.lower())
                # fsspec-xrootd returns (algorithm, value)
                if isinstance(result, tuple | list):
                    result = result[1]
                sys.stdout.write(f"{self.params.file} {result}\n")
                return
            except Exception:
                pass  # fall through to client-side computation

        checksum = _compute_checksum(fso, path, alg)
        sys.stdout.write(f"{self.params.file} {checksum}\n")

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
                    sys.stdout.write(f"{attr} = {val}\n")
                except Exception as e:
                    sys.stdout.write(f"{attr} FAILED: {e}\n")


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _compute_checksum(fso, path, alg):
    """Read the file and compute a checksum client-side."""
    alg_upper = alg.upper()

    if alg_upper == "ADLER32":
        value = 1  # zlib.adler32 initial value
        with fso.open(path, "rb") as f:
            while True:
                chunk = f.read(fs.CHUNK_SIZE)
                if not chunk:
                    break
                value = zlib.adler32(chunk, value) & 0xFFFFFFFF
        return f"{value:08x}"

    if alg_upper == "CRC32":
        value = 0
        with fso.open(path, "rb") as f:
            while True:
                chunk = f.read(fs.CHUNK_SIZE)
                if not chunk:
                    break
                value = zlib.crc32(chunk, value) & 0xFFFFFFFF
        return f"{value:08x}"

    if alg_upper == "CRC32C":
        value = _crc32c_file(fso, path)
        return f"{value:08x}"

    # For MD5, SHA*, etc. use hashlib
    name = alg_upper.lower().replace("-", "")  # sha256, md5, …
    try:
        h = hashlib.new(name)
    except ValueError as err:
        raise ValueError(f"unsupported checksum algorithm: {alg}") from err

    with fso.open(path, "rb") as f:
        while True:
            chunk = f.read(fs.CHUNK_SIZE)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def _crc32c_file(fso, path):
    """Compute CRC32C checksum. Uses the crc32c package if available, otherwise
    falls back to crcmod (if installed) or a pure-Python polynomial."""
    try:
        import crc32c as _crc32c

        value = 0
        with fso.open(path, "rb") as f:
            while True:
                chunk = f.read(fs.CHUNK_SIZE)
                if not chunk:
                    break
                value = _crc32c.crc32c(chunk, value)
        return value & 0xFFFFFFFF
    except ImportError:
        pass

    try:
        import crcmod

        crc_fn = crcmod.predefined.mkCrcFun("crc-32c")
        crc = crc_fn(b"")  # initialise
        with fso.open(path, "rb") as f:
            while True:
                chunk = f.read(fs.CHUNK_SIZE)
                if not chunk:
                    break
                crc = crcmod.predefined.mkCrcFun("crc-32c")(chunk, crc)
        return crc & 0xFFFFFFFF
    except (ImportError, Exception):
        pass

    # Pure-Python fallback (slow but correct; no external deps)
    return _crc32c_pure(fso, path)


def _crc32c_pure(fso, path):
    """Pure-Python CRC32C using the Castagnoli polynomial 0x82F63B78."""
    # Build lookup table
    poly = 0x82F63B78
    table = []
    for i in range(256):
        crc = i
        for _ in range(8):
            if crc & 1:
                crc = (crc >> 1) ^ poly
            else:
                crc >>= 1
        table.append(crc)

    crc = 0xFFFFFFFF
    with fso.open(path, "rb") as f:
        while True:
            chunk = f.read(fs.CHUNK_SIZE)
            if not chunk:
                break
            for byte in chunk:
                crc = (crc >> 8) ^ table[(crc ^ byte) & 0xFF]
    return (~crc) & 0xFFFFFFFF
