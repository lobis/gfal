"""
Entry point: maps gfal-<cmd> executable names to execute_<cmd> methods.
"""

import sys
from pathlib import Path

from gfal_cli import (
    base,
    commands,  # noqa: F401  – registers GfalCommands subclass
    copy,  # noqa: F401  – registers CommandCopy subclass
    ls,  # noqa: F401  – registers CommandLs subclass
    rm,  # noqa: F401  – registers CommandRm subclass
)

# ---------------------------------------------------------------------------
# Command name → (class, method) resolution
# ---------------------------------------------------------------------------

# Aliases: executable suffix → execute_* method name
_ALIASES = {
    "cp": "copy",
}


def _find_command(cmd):
    method_name = "execute_" + cmd
    for cls in base.CommandBase.__subclasses__():
        method = getattr(cls, method_name, None)
        if method is not None:
            return cls, method
    raise ValueError(f"Unknown command: {cmd!r}")


def _command_from_argv0(argv0):
    """Extract the command token from the executable name.

    gfal-ls   → ls
    gfal-copy → copy
    gfal-cp   → copy  (alias)
    """
    name = Path(argv0).name
    # strip leading 'gfal-' prefix
    token = name.rsplit("-", 1)[1].lower() if "-" in name else name.lower()
    return _ALIASES.get(token, token)


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def main(argv=None):
    if argv is None:
        argv = sys.argv

    try:
        cmd = _command_from_argv0(argv[0])
        cls, func = _find_command(cmd)
    except ValueError as e:
        sys.stderr.write(f"{e}\n")
        sys.exit(1)

    inst = cls()
    inst.parse(func, argv)
    sys.exit(inst.execute(func))
