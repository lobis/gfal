"""
Tape / staging commands plus EOS token generation.

The tape/staging commands require the native gfal2 C library (via
python-gfal2), so they remain compatibility stubs.  The token command supports
the EOS Pilot token workflow used by this fsspec-based implementation.
"""

from __future__ import annotations

import subprocess
import sys
import time
from urllib.parse import urlparse

from gfal.cli import base  # noqa: E402

_NOT_SUPPORTED_MSG = (
    "{prog}: this command requires the native gfal2 C library and is not "
    "supported in this fsspec-based implementation.\n"
    "Use the original gfal2-util package for tape/staging operations.\n"
)
_DEFAULT_EOS_INSTANCE = "root://eospilot.cern.ch"
_DEFAULT_TOKEN_VALIDITY_MINUTES = 720


def _is_eos_pilot_host(hostname: str | None) -> bool:
    return (hostname or "").lower() == "eospilot.cern.ch"


def _normalise_eos_token_path(path: str) -> str:
    parsed = urlparse(path)
    if not parsed.scheme:
        eos_path = path
    elif parsed.scheme.lower() in {"root", "xroot", "http", "https"}:
        if not _is_eos_pilot_host(parsed.hostname):
            raise ValueError(
                f"EOS token generation only supports EOS Pilot URLs: {path}"
            )
        eos_path = parsed.path
        if eos_path.startswith("//"):
            eos_path = eos_path[1:]
    else:
        raise ValueError(f"unsupported EOS token path: {path}")

    if not eos_path.startswith("/eos/pilot/"):
        raise ValueError(
            "EOS token path must be under /eos/pilot/ or use an EOS Pilot URL"
        )
    return eos_path


def _derive_ssh_host(ssh_host: str | None, eos_instance: str) -> str:
    if ssh_host:
        return ssh_host
    parsed = urlparse(eos_instance)
    if _is_eos_pilot_host(parsed.hostname):
        return "eospilot"
    raise ValueError("--ssh-host is required when --eos-instance is not EOS Pilot")


def _extract_token(stdout: str) -> str:
    for line in stdout.splitlines():
        token = line.strip()
        if token:
            return token
    raise ValueError("EOS token command produced no token")


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
        default=_DEFAULT_TOKEN_VALIDITY_MINUTES,
        metavar="MINUTES",
        help="token validity in minutes",
    )
    @base.arg(
        "--ssh-host",
        type=str,
        default=None,
        metavar="HOST",
        help="SSH host used to run the EOS token command (default: eospilot for EOS Pilot).",
    )
    @base.arg(
        "--eos-instance",
        type=str,
        default=_DEFAULT_EOS_INSTANCE,
        metavar="ROOT_URL",
        help="EOS instance passed to the remote eos command.",
    )
    @base.arg(
        "--tree",
        action="store_true",
        default=False,
        help="request a token valid for the directory tree",
    )
    @base.arg(
        "--no-tree",
        action="store_true",
        default=False,
        help="request a token for only the exact path",
    )
    @base.arg(
        "--issuer",
        type=str,
        default=None,
        metavar="URL",
        help="token issuer URL",
    )
    @base.arg("path", type=str, help="EOS Pilot path or URI to request token for")
    @base.arg(
        "activities",
        nargs="*",
        type=str,
        help="activities for macaroon request",
    )
    def execute_token(self):
        """Retrieve a scoped EOS Pilot token via the EOS CLI over SSH."""
        try:
            eos_path = _normalise_eos_token_path(self.params.path)
            ssh_host = _derive_ssh_host(
                getattr(self.params, "ssh_host", None),
                self.params.eos_instance,
            )
            if self.params.tree and self.params.no_tree:
                raise ValueError("--tree and --no-tree are mutually exclusive")
            tree = (
                self.params.tree
                if self.params.tree or self.params.no_tree
                else eos_path.endswith("/")
            )
            permission = "rwx" if self.params.write else "rx"
            validity = self.params.validity or _DEFAULT_TOKEN_VALIDITY_MINUTES
            if validity <= 0:
                raise ValueError("--validity must be greater than zero")
            expires = int(time.time()) + int(validity) * 60
            argv = [
                "ssh",
                ssh_host,
                "eos",
                self.params.eos_instance,
                "token",
                "--path",
                eos_path,
                "--permission",
                permission,
                "--expires",
                str(expires),
            ]
            if tree:
                argv.append("--tree")

            proc = subprocess.run(
                argv,
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                check=False,
            )
            if proc.returncode != 0:
                sys.stderr.write(proc.stderr or f"{self.prog}: EOS token failed\n")
                return proc.returncode or 1

            token = _extract_token(proc.stdout)
            sys.stdout.write(token)
            sys.stdout.write("\n")
            return 0
        except Exception as exc:
            sys.stderr.write(f"{self.prog}: {exc}\n")
            return 1
