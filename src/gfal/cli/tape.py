"""
Transitional tape/staging commands and the storage-token command.

The top-level router handles HTTP/WebDAV bringonline, archivepoll, and evict
through the external xrdfs client before this module is imported.  These
methods remain only as the fallback for local paths, unsupported protocols, or
options that have not migrated.  The HTTP token command is implemented here.
"""

import sys

from gfal.cli import base  # noqa: E402
from gfal.core.fs import build_storage_options
from gfal.core.token_defaults import DEFAULT_TOKEN_VALIDITY

_NOT_SUPPORTED_MSG = (
    "{prog}: tape operations are not supported for this operand; use a complete "
    "HTTP/WebDAV URL and a compatible xrdfs client.\n"
)


def retrieve_token(*args, **kwargs):
    """Import the HTTP token implementation only when ``gfal token`` runs."""
    from gfal.core.token import retrieve_token as _retrieve_token

    return _retrieve_token(*args, **kwargs)


class CommandTape(base.CommandBase):
    # ------------------------------------------------------------------
    # bringonline
    # ------------------------------------------------------------------

    @base.arg(
        "--pin-lifetime",
        type=int,
        default=None,
        metavar="SECONDS",
        help="desired pin lifetime in seconds",
    )
    @base.arg(
        "--desired-request-time",
        type=int,
        default=None,
        metavar="SECONDS",
        help="desired total request time in seconds",
    )
    @base.arg(
        "--staging-metadata",
        type=str,
        default=None,
        metavar="METADATA",
        help="metadata string for the bringonline operation",
    )
    @base.arg(
        "--polling-timeout",
        type=int,
        default=None,
        metavar="SECONDS",
        help="timeout for the polling operation",
    )
    @base.arg(
        "--from-file",
        type=str,
        default=None,
        metavar="FILE",
        help="read SURLs from a file, one per line",
    )
    @base.arg("surl", nargs="?", type=base.surl, help="Site URL")
    def execute_bringonline(self):
        """Report unsupported bring-online requests left on the fallback path."""
        sys.stderr.write(_NOT_SUPPORTED_MSG.format(prog=self.prog))
        return 1

    # ------------------------------------------------------------------
    # archivepoll
    # ------------------------------------------------------------------

    @base.arg(
        "--polling-timeout",
        type=int,
        default=None,
        metavar="SECONDS",
        help="timeout for the polling operation",
    )
    @base.arg(
        "--from-file",
        type=str,
        default=None,
        metavar="FILE",
        help="read SURLs from a file, one per line",
    )
    @base.arg("surl", nargs="?", type=base.surl, help="Site URL")
    def execute_archivepoll(self):
        """Report unsupported archive polls left on the fallback path."""
        sys.stderr.write(_NOT_SUPPORTED_MSG.format(prog=self.prog))
        return 1

    # ------------------------------------------------------------------
    # evict
    # ------------------------------------------------------------------

    @base.arg("file", type=base.surl, help="URI of the file to evict")
    @base.arg(
        "token",
        nargs="?",
        type=str,
        help="token from the bring-online request",
    )
    def execute_evict(self):
        """Report unsupported evictions left on the fallback path."""
        sys.stderr.write(_NOT_SUPPORTED_MSG.format(prog=self.prog))
        return 1

    # ------------------------------------------------------------------
    # token
    # ------------------------------------------------------------------

    @base.arg(
        "-w",
        "--write",
        action="store_true",
        help="request a write-access token",
    )
    @base.arg(
        "--validity",
        type=int,
        default=DEFAULT_TOKEN_VALIDITY,
        metavar="MINUTES",
        help="token validity in minutes",
    )
    @base.arg(
        "--issuer",
        type=str,
        default=None,
        metavar="URL",
        help="token issuer URL",
    )
    @base.arg("path", type=base.surl, help="URI to request token for")
    @base.arg(
        "activities",
        nargs="*",
        type=str,
        help="activities for macaroon request",
    )
    def execute_token(self):
        """Retrieve a storage-element issued token."""
        if self.params.validity < 0:
            sys.stderr.write("Validity must be a number >= 0\n")
            return 1

        activities = self.params.activities
        if self.params.verbose:
            if activities:
                print("Will use user-provided activities")
            else:
                access = "write" if self.params.write else "read"
                print(f"Will use default activities for {access} access")

        token = retrieve_token(
            self.params.path,
            issuer=self.params.issuer,
            validity=self.params.validity,
            write_access=self.params.write,
            activities=activities or None,
            storage_options=build_storage_options(self.params),
        )
        sys.stdout.write(token + "\n")
        return 0
