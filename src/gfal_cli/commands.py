"""
Simple commands: mkdir, save, cat, stat, rename, chmod, sum, xattr.
"""

import errno
import stat
import sys
from datetime import datetime

from gfal_cli import base, fs
from gfal_cli.api import GfalClient
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
            msg = f"invalid mode '{self.params.mode}': must be an octal number (e.g. 755, 0755)"
            if base.is_gfal2_compat():
                sys.stderr.write(f"{self.prog}: {msg}\n")
            else:
                self.err_console.print(f"[bold red]{self.prog}[/]: {msg}")
            return 1

        client = GfalClient(
            cert=self.params.cert,
            key=self.params.key,
            timeout=self.params.timeout,
            ssl_verify=getattr(self.params, "ssl_verify", True),
        )

        rc = 0
        for d in self.params.directory:
            try:
                with self.spinner(f"Creating directory {d}..."):
                    client.mkdir(d, mode=mode_int, parents=self.params.parents)
            except Exception as e:
                self._print_error(e)
                rc = getattr(e, "errno", 1)
        return rc

    # ------------------------------------------------------------------
    # save  (stdin → remote file)
    # ------------------------------------------------------------------

    @base.arg("file", type=base.surl, help="URI of the file to write")
    def execute_save(self):
        """Read from stdin and write to a remote file."""
        client = GfalClient(
            cert=self.params.cert,
            key=self.params.key,
            timeout=self.params.timeout,
            ssl_verify=getattr(self.params, "ssl_verify", True),
        )
        with client.open(self.params.file, "wb") as f:
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
        client = GfalClient(
            cert=self.params.cert,
            key=self.params.key,
            timeout=self.params.timeout,
            ssl_verify=getattr(self.params, "ssl_verify", True),
        )
        rc = 0
        for url in self.params.file:
            try:
                with client.open(url, "rb") as f:
                    while True:
                        chunk = f.read(fs.CHUNK_SIZE)
                        if not chunk:
                            break
                        sys.stdout.buffer.write(chunk)
                sys.stdout.buffer.flush()
            except Exception as e:
                if isinstance(e, OSError) and e.errno == errno.EPIPE:
                    raise
                self._print_error(e)
                rc = getattr(e, "errno", 1)
        return rc

    # ------------------------------------------------------------------
    # stat
    # ------------------------------------------------------------------

    @base.arg("file", nargs="+", type=base.surl, help="URI(s) to stat")
    def execute_stat(self):
        """Display file status."""
        client = GfalClient(
            cert=self.params.cert,
            key=self.params.key,
            timeout=self.params.timeout,
            ssl_verify=getattr(self.params, "ssl_verify", True),
        )
        rc = 0
        first = True
        for url in self.params.file:
            try:
                if not first:
                    print()
                self._stat_one(url, client)
                first = False
            except Exception as e:
                if isinstance(e, OSError) and e.errno == errno.EPIPE:
                    raise
                self._print_error(e)
                rc = getattr(e, "errno", 1)
                first = False
        return rc

    def _stat_one(self, url, client):
        with self.spinner(f"Statting {url}..."):
            st = client.stat(url)
        if base.is_gfal2_compat():
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
        else:
            from rich.panel import Panel
            from rich.table import Table

            table = Table.grid(padding=(0, 2))
            table.add_column(style="bold cyan")
            table.add_column()

            table.add_row("File", f"'{url}'")
            table.add_row(
                "Size", f"{st.st_size} bytes ({file_type_str(stat.S_IFMT(st.st_mode))})"
            )
            table.add_row(
                "Access",
                f"{stat.S_IMODE(st.st_mode):04o} ({file_mode_str(st.st_mode)})",
            )
            table.add_row("Uid/Gid", f"{st.st_uid} / {st.st_gid}")
            table.add_row(
                "Access",
                datetime.fromtimestamp(st.st_atime).strftime("%Y-%m-%d %H:%M:%S"),
            )
            table.add_row(
                "Modify",
                datetime.fromtimestamp(st.st_mtime).strftime("%Y-%m-%d %H:%M:%S"),
            )
            table.add_row(
                "Change",
                datetime.fromtimestamp(st.st_ctime).strftime("%Y-%m-%d %H:%M:%S"),
            )

            self.console.print(
                Panel(table, title="[bold white]File Metadata[/]", expand=False)
            )

    # ------------------------------------------------------------------
    # rename
    # ------------------------------------------------------------------

    @base.arg("source", type=base.surl, help="original URI")
    @base.arg("destination", type=base.surl, help="new URI")
    def execute_rename(self):
        """Rename a file or directory."""
        client = GfalClient(
            cert=self.params.cert,
            key=self.params.key,
            timeout=self.params.timeout,
            ssl_verify=getattr(self.params, "ssl_verify", True),
        )
        with self.spinner(f"Renaming {self.params.source}..."):
            client.rename(self.params.source, self.params.destination)

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
            msg = "mode must be an octal number (e.g. 0755)"
            if base.is_gfal2_compat():
                sys.stderr.write(f"{self.prog}: {msg}\n")
            else:
                self.err_console.print(f"[bold red]{self.prog}[/]: {msg}")
            return 1

        client = GfalClient(
            cert=self.params.cert,
            key=self.params.key,
            timeout=self.params.timeout,
            ssl_verify=getattr(self.params, "ssl_verify", True),
        )

        rc = 0
        for url in self.params.file:
            try:
                with self.spinner(f"Changing permissions of {url}..."):
                    client.chmod(url, mode)
            except Exception as e:
                self._print_error(e)
                rc = getattr(e, "errno", 1)
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
        client = GfalClient(
            cert=self.params.cert,
            key=self.params.key,
            timeout=self.params.timeout,
            ssl_verify=getattr(self.params, "ssl_verify", True),
        )
        alg = self.params.checksum_type.upper()

        try:
            with self.spinner(f"Computing {alg} checksum..."):
                checksum = client.checksum(self.params.file, alg)
            sys.stdout.write(f"{self.params.file} {checksum}\n")
        except Exception as e:
            self._print_error(e)
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
        client = GfalClient(
            cert=self.params.cert,
            key=self.params.key,
            timeout=self.params.timeout,
            ssl_verify=getattr(self.params, "ssl_verify", True),
        )

        try:
            if self.params.attribute is not None:
                if "=" in self.params.attribute:
                    i = self.params.attribute.index("=")
                    key = self.params.attribute[:i]
                    val = self.params.attribute[i + 1 :]
                    with self.spinner(f"Setting xattr {key}..."):
                        client.setxattr(self.params.file, key, val)
                else:
                    with self.spinner(f"Getting xattr {self.params.attribute}..."):
                        val = client.getxattr(self.params.file, self.params.attribute)
                    sys.stdout.write(f"{val}\n")
            else:
                with self.spinner("Listing xattrs..."):
                    attrs = client.listxattr(self.params.file)
                for attr in attrs:
                    try:
                        val = client.getxattr(self.params.file, attr)
                        sys.stdout.write(f"{attr} = {val}\n\n")
                    except Exception as e:
                        sys.stdout.write(f"{attr} FAILED: {e}\n\n")
        except Exception as e:
            self._print_error(e)
            return getattr(e, "errno", 1)
        return 0
