"""Dependency-free top-level router for the aggregate :command:`gfal` CLI."""

from __future__ import annotations

import os
import sys
from collections.abc import Sequence
from typing import Optional

from gfal import __version__
from gfal.cli.completion import (
    bash_source,
    command_names,
    dispatch_completion,
    zsh_source,
)
from gfal.cli.xrdfs import (
    XRDFS_COMMANDS,
    dispatch,
    requires_legacy_backend,
    routing_arguments,
    should_use_xrdfs,
)
from gfal.xrdfs import GFAL_ENOTSUP, error_description


def _print_help(to=None) -> None:
    if to is None:
        to = sys.stdout
    to.write(
        f"gfal {__version__} - GFAL-compatible CLI backed by xrdfs and xrdcp\n\n"
        "Usage: gfal COMMAND [OPTIONS]\n\n"
        "Commands:\n"
    )
    descriptions = {
        name: definition.description for name, definition in XRDFS_COMMANDS.items()
    }
    descriptions.update({
        "completion": "Generate shell completion source.",
        "cp": "Copy files using the XRootD client.",
        "save": "Read stdin and write it to a file.",
    })
    for name in command_names():
        to.write(f"  {name:<14} {descriptions[name]}\n")
    to.write("\nRun 'gfal COMMAND --help' for command-specific options.\n")


def _unsupported(command: str) -> int:
    sys.stderr.write(
        f"gfal {command} error: {GFAL_ENOTSUP} "
        f"({error_description(GFAL_ENOTSUP)}) - this invocation requires an "
        "option or protocol that has not been migrated to xrdfs\n"
    )
    return GFAL_ENOTSUP


def _dispatch_command(command: str, arguments: Sequence[str]) -> int:
    if command == "cp":
        from gfal.cli.cp import dispatch_cp

        return dispatch_cp(arguments)
    if command == "save":
        from gfal.cli.save import dispatch_save

        return dispatch_save(arguments)
    if command == "completion":
        return dispatch_completion(arguments)
    if command == "rm":
        from gfal.cli.local import dispatch_local_rm, should_use_local_rm

        if should_use_local_rm(arguments):
            return dispatch_local_rm(arguments)
    if command in XRDFS_COMMANDS:
        if requires_legacy_backend(command, arguments):
            return _unsupported(command)
        if should_use_xrdfs(command, arguments):
            return dispatch(command, arguments, prog=f"gfal {command}")
        parsed, information_requested = routing_arguments(command, arguments)
        if parsed is None and not information_requested:
            return dispatch(command, arguments, prog=f"gfal {command}")
        return _unsupported(command)
    sys.stderr.write(f"gfal: unknown command: {command}\n")
    return 2


def main(argv: Optional[Sequence[str]] = None) -> int:
    """Run the aggregate CLI without importing optional protocol libraries."""
    arguments = list(sys.argv if argv is None else argv)
    if not arguments:
        arguments = ["gfal"]

    completion_mode = os.environ.get("_GFAL_COMPLETE")
    if completion_mode == "bash_source":
        sys.stdout.write(bash_source())
        return 0
    if completion_mode == "zsh_source":
        sys.stdout.write(zsh_source())
        return 0

    if len(arguments) == 1 or arguments[1] in ("-h", "--help", "help"):
        if len(arguments) > 2 and arguments[1] == "help":
            return _dispatch_command(arguments[2], ["--help"])
        _print_help()
        return 0
    if arguments[1] in ("-V", "--version", "version"):
        print(f"gfal {__version__}")
        return 0
    if arguments[1].startswith("-"):
        sys.stderr.write(f"gfal: unknown option: {arguments[1]}\n")
        return 2
    return _dispatch_command(arguments[1], arguments[2:])


if __name__ == "__main__":
    raise SystemExit(main())
