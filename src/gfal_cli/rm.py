"""
gfal-rm implementation.
"""

import errno
import stat
import sys
from pathlib import Path

from gfal_cli import base
from gfal_cli.api import GfalClient
from gfal_cli.errors import GfalFileNotFoundError, GfalIsADirectoryError


class CommandRm(base.CommandBase):
    def __init__(self):
        super().__init__()
        self.return_code = 0

    @base.arg(
        "-r",
        "-R",
        "--recursive",
        action="store_true",
        help="remove directories and their contents recursively",
    )
    @base.arg(
        "--dry-run",
        action="store_true",
        help="print what would be deleted without doing it",
    )
    @base.arg(
        "--just-delete",
        action="store_true",
        help="skip stat check and delete directly (useful for signed URLs)",
    )
    @base.arg(
        "--from-file",
        type=str,
        default=None,
        help="read URIs from a file, one per line",
    )
    @base.arg(
        "--bulk",
        action="store_true",
        help="use bulk deletion (accepted for compatibility; currently performs sequential deletion)",
    )
    @base.arg("file", nargs="*", type=base.surl, help="URI(s) to delete")
    def execute_rm(self):
        """Remove files or directories."""
        if self.params.from_file and self.params.file:
            sys.stderr.write(
                "--from-file and positional arguments cannot be combined\n"
            )
            return errno.EINVAL

        if self.params.file:
            urls = self.params.file
        elif self.params.from_file:
            with Path(self.params.from_file).open() as fh:
                urls = [line.strip() for line in fh if line.strip()]
        else:
            sys.stderr.write("No URI specified\n")
            return errno.EINVAL

        client = GfalClient(
            cert=self.params.cert,
            key=self.params.key,
            timeout=self.params.timeout,
            ssl_verify=getattr(self.params, "ssl_verify", True),
        )

        for url in urls:
            self._do_rm(url, client)

        return self.return_code

    def _do_rm(self, url, client):
        try:
            if not self.params.just_delete:
                st = client.stat(url)
                if stat.S_ISDIR(st.st_mode):
                    self._do_rmdir(url, client)
                    return

            if self.params.dry_run:
                print(f"{url}\tSKIP")
                return

            client.rm(url)
            print(f"{url}\tDELETED")
        except (IsADirectoryError, GfalIsADirectoryError) as e:
            sys.stderr.write(f"{self.progr}: {self._format_error(e)}\n")
            self._set_error(1)
        except GfalFileNotFoundError:
            self._set_error(errno.ENOENT)
            print(f"{url}\tMISSING")
        except Exception as e:
            self._set_error(1)
            print(f"{url}\tFAILED: {e}")

    def _do_rmdir(self, url, client):
        if not self.params.recursive:
            raise IsADirectoryError(f"Cannot remove '{url}': is a directory")

        # Remove contents first
        try:
            entries = client.ls(url, detail=True)
        except Exception:
            entries = []

        base_url = url.rstrip("/") + "/"
        for entry_st in entries:
            name = Path(entry_st.info["name"].rstrip("/")).name
            if name in (".", ".."):
                continue
            child_url = base_url + name
            if stat.S_ISDIR(entry_st.st_mode):
                self._do_rmdir(child_url, client)
            else:
                if self.params.dry_run:
                    print(f"{child_url}\tSKIP")
                else:
                    try:
                        client.rm(child_url)
                        print(f"{child_url}\tDELETED")
                    except GfalFileNotFoundError:
                        self._set_error(errno.ENOENT)
                        print(f"{child_url}\tMISSING")

        if self.params.dry_run:
            print(f"{url}\tSKIP DIR")
        else:
            try:
                client.rmdir(url)
                print(f"{url}\tRMDIR")
            except GfalFileNotFoundError:
                self._set_error(errno.ENOENT)
                print(f"{url}\tMISSING")
            except Exception as e:
                self._set_error(1)
                print(f"{url}\tFAILED: {e}")

    def _set_error(self, code):
        if self.return_code == 0:
            self.return_code = code
