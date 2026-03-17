"""
gfal-rm implementation.
"""

import errno
import stat
import sys
from pathlib import Path

from gfal_cli import base, fs


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

        opts = fs.build_storage_options(self.params)
        for url in urls:
            self._do_rm(url, opts)

        return self.return_code

    def _do_rm(self, url, opts):
        fso, path = fs.url_to_fs(url, opts)

        if not self.params.just_delete:
            try:
                info = fso.info(path)
                st = fs.StatInfo(info)
            except FileNotFoundError:
                self._set_error(errno.ENOENT)
                print(f"{url}\tMISSING")
                return
            except Exception as e:
                self._set_error(1)
                print(f"{url}\tFAILED: {e}")
                raise

            if stat.S_ISDIR(st.st_mode):
                self._do_rmdir(url, fso, path, opts)
                return

        if self.params.dry_run:
            print(f"{url}\tSKIP")
            return

        try:
            fso.rm(path, recursive=False)
            print(f"{url}\tDELETED")
        except FileNotFoundError:
            self._set_error(errno.ENOENT)
            print(f"{url}\tMISSING")
        except Exception as e:
            self._set_error(1)
            print(f"{url}\tFAILED: {e}")
            raise

    def _do_rmdir(self, url, fso, path, opts):
        if not self.params.recursive:
            raise IsADirectoryError(f"Cannot remove '{url}': is a directory")

        # Remove contents first
        try:
            entries = fso.ls(path, detail=False)
        except Exception:
            entries = []

        base_url = url.rstrip("/") + "/"
        base_path = path.rstrip("/") + "/"
        for entry in entries:
            # entry is typically the full path
            name = Path(entry.rstrip("/")).name
            if name in (".", ".."):
                continue
            child_url = base_url + name
            child_path = base_path + name
            child_info = fso.info(child_path)
            child_st = fs.StatInfo(child_info)
            if stat.S_ISDIR(child_st.st_mode):
                self._do_rmdir(child_url, fso, child_path, opts)
            else:
                if self.params.dry_run:
                    print(f"{child_url}\tSKIP")
                else:
                    try:
                        fso.rm(child_path, recursive=False)
                        print(f"{child_url}\tDELETED")
                    except FileNotFoundError:
                        self._set_error(errno.ENOENT)
                        print(f"{child_url}\tMISSING")

        if self.params.dry_run:
            print(f"{url}\tSKIP DIR")
        else:
            try:
                fso.rmdir(path)
                print(f"{url}\tRMDIR")
            except FileNotFoundError:
                self._set_error(errno.ENOENT)
                print(f"{url}\tMISSING")
            except Exception as e:
                self._set_error(1)
                print(f"{url}\tFAILED: {e}")
                raise

    def _set_error(self, code):
        if self.return_code == 0:
            self.return_code = code
