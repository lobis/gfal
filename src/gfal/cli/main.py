"""Top-level ``gfal`` command router.

Remote invocations of commands migrated to the external ``xrdfs`` backend are
selected before the previous CLI is imported. This keeps the migration
incremental and lets each remaining path move away from the Python protocol
stack independently.
"""

from __future__ import annotations

import os
import sys
from collections.abc import Sequence
from typing import Optional

from gfal.cli.xrdfs import dispatch, should_use_xrdfs

_XRDFS_COMMANDS = frozenset(("cat", "ls", "stat", "sum", "xattr"))
_LEGACY_ONLY_OPTIONS = frozenset((
    "-q",
    "--quiet",
    "--authz-token",
    "--verify",
    "--no-verify",
    "-r",
    "--reverse",
    "-S",
    "-U",
    "--sort",
))
_LEGACY_ONLY_PREFIXES = ("--authz-token=", "--sort=")
_VALUE_SHORT_OPTIONS = frozenset(("D", "t", "E", "C"))


def _requires_legacy_backend(command: str, arguments: Sequence[str]) -> bool:
    if os.environ.get("EOSAUTHZ") or os.environ.get("GFAL_AUTHZ_TOKEN"):
        return True

    legacy_short = {"q"}
    if command == "ls":
        legacy_short.update(("r", "S", "U"))

    for value in arguments:
        if value == "--":
            break
        if value in _LEGACY_ONLY_OPTIONS or any(
            value.startswith(prefix) for prefix in _LEGACY_ONLY_PREFIXES
        ):
            return True
        if not value.startswith("-") or value.startswith("--") or value == "-":
            continue
        for character in value[1:]:
            if character in _VALUE_SHORT_OPTIONS:
                break
            if character in legacy_short:
                return True
    return False


def main(argv: Optional[Sequence[str]] = None) -> int:
    """Route migrated remote commands to ``xrdfs`` and retain local fallback."""
    arguments = list(sys.argv if argv is None else argv)
    if not arguments:
        arguments = ["gfal"]

    if (
        len(arguments) > 1
        and arguments[1] in _XRDFS_COMMANDS
        and not _requires_legacy_backend(arguments[1], arguments[2:])
        and should_use_xrdfs(arguments[1], arguments[2:])
    ):
        command = arguments[1]
        return dispatch(command, arguments[2:], prog=f"gfal {command}")

    # Importing the old command implementation loads its optional protocol
    # stack, so keep this fallback lazy while the remaining commands migrate.
    from gfal.cli.shell import main as legacy_main

    return legacy_main(arguments)
