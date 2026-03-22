"""
Tape / staging commands: bringonline, archivepoll, evict, token.

These commands require the native gfal2 C library (via python-gfal2) which is
not available in this fsspec-based reimplementation.  The CLI interface is
preserved for backwards compatibility; each command prints a clear
"not supported" message and exits with code 1.
"""

import sys

from gfal_cli import base  # noqa: E402

_NOT_SUPPORTED_MSG = (
    "{prog}: this command requires the native gfal2 C library and is not "
    "supported in this fsspec-based implementation.\n"
    "Use the original gfal2-util package for tape/staging operations.\n"
)


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

    @base.arg(
        "token",
        nargs="?",
        type=str,
        help="token from the bring-online request",
    )
    @base.arg("file", type=base.surl, help="URI of the file to evict")
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
        default=None,
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
    @base.arg(
        "activities",
        nargs="*",
        type=str,
        help="activities for macaroon request",
    )
    @base.arg("path", type=base.surl, help="URI to request token for")
    def execute_token(self):
        """Retrieve a storage-element issued token (not supported)."""
        sys.stderr.write(_NOT_SUPPORTED_MSG.format(prog=self.prog))
        return 1
