"""
Entry point: maps gfal-<cmd> executable names to execute_<cmd> methods.
"""

import os
import sys
from pathlib import Path

from gfal_cli import (
    base,
    commands,  # noqa: F401  – registers GfalCommands subclass
    copy,  # noqa: F401  – registers CommandCopy subclass
    ls,  # noqa: F401  – registers CommandLs subclass
    rm,  # noqa: F401  – registers CommandRm subclass
)


def _ensure_xrootd_dylib_path():
    """macOS-only: ensure the pyxrootd plugin directory is in DYLD_LIBRARY_PATH.

    The pip-packaged xrootd .dylib files embed $ORIGIN-style RPATHs (a Linux
    convention) which macOS dyld does not expand.  As a result the XRootD
    security plugins (GSI, kerberos, …) fail to load unless the containing
    directory is on DYLD_LIBRARY_PATH.

    dyld processes DYLD_LIBRARY_PATH only at process startup, so we must
    re-exec the current process with the updated environment before any XRootD
    code is loaded.  The re-exec is skipped when DYLD_LIBRARY_PATH already
    contains the plugin directory (i.e. on the second invocation).
    """
    if sys.platform != "darwin":
        return
    try:
        import pyxrootd as _px
    except ImportError:
        return  # xrootd not installed — nothing to fix

    plugin_dir = str(Path(_px.__file__).parent)
    current = os.environ.get("DYLD_LIBRARY_PATH", "")
    if plugin_dir in current.split(":"):
        return  # already set — no re-exec needed

    # Only re-exec when invoked as a real executable on disk.
    # When imported via `python3 -c "..."` or as a module, sys.argv[0] is
    # either '-c', '-m', or a bare name that isn't a file — re-exec in those
    # cases would either lose the inline script or try to run a non-existent
    # file as a Python script.
    if not Path(sys.argv[0]).is_file():
        return

    new_env = os.environ.copy()
    new_env["DYLD_LIBRARY_PATH"] = f"{plugin_dir}:{current}" if current else plugin_dir
    os.execve(sys.executable, [sys.executable] + sys.argv, new_env)


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
    name = Path(argv0).stem  # .stem strips .exe on Windows
    # strip leading 'gfal-' prefix
    token = name.rsplit("-", 1)[1].lower() if "-" in name else name.lower()
    return _ALIASES.get(token, token)


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def main(argv=None):
    _ensure_xrootd_dylib_path()

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
