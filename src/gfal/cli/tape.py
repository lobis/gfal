"""
Tape / staging commands: bringonline, archivepoll, evict, token.

These commands require the native gfal2 C library (via python-gfal2) which is
not available in this fsspec-based reimplementation.  The tape/staging CLI
interfaces are preserved for backwards compatibility; those commands print a
clear "not supported" message and exit with code 1.  The HTTP token command is
implemented directly.
"""

import sys

from gfal.cli import base  # noqa: E402
from gfal.core.fs import build_storage_options
from gfal.core.token_defaults import DEFAULT_TOKEN_VALIDITY

_NOT_SUPPORTED_MSG = (
    "{prog}: this command requires the native gfal2 C library and is not "
    "supported in this fsspec-based implementation.\n"
    "Use the original gfal2-util package for tape/staging operations.\n"
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
        """Bring a file online from tape storage (not supported)."""
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
        """Poll the status of an archive (bring-online) request (not supported)."""
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
        """Evict a file from a disk buffer (not supported)."""
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
