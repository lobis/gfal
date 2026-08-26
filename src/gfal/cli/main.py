"""Top-level ``gfal`` command router.

Remote invocations of commands migrated to the external ``xrdfs`` backend are
selected before the previous CLI is imported. This keeps the migration
incremental and lets each remaining path move away from the Python protocol
stack independently.
"""

from __future__ import annotations

import sys
from collections.abc import Sequence
from typing import Optional

from gfal.cli.xrdfs import (
    XRDFS_COMMANDS,
    dispatch,
    requires_legacy_backend,
    should_use_xrdfs,
)


def main(argv: Optional[Sequence[str]] = None) -> int:
    """Route migrated remote commands to ``xrdfs`` and retain local fallback."""
    arguments = list(sys.argv if argv is None else argv)
    if not arguments:
        arguments = ["gfal"]

    if len(arguments) > 1 and arguments[1] == "rm":
        from gfal.cli.local import dispatch_local_rm, should_use_local_rm

        if should_use_local_rm(arguments[2:]):
            return dispatch_local_rm(arguments[2:])

    if (
        len(arguments) > 1
        and arguments[1] in XRDFS_COMMANDS
        and not requires_legacy_backend(arguments[1], arguments[2:])
        and should_use_xrdfs(arguments[1], arguments[2:])
    ):
        command = arguments[1]
        return dispatch(command, arguments[2:], prog=f"gfal {command}")

    # Importing the old command implementation loads its optional protocol
    # stack, so keep this fallback lazy while the remaining commands migrate.
    from gfal.cli.shell import main as legacy_main

    return legacy_main(arguments)
