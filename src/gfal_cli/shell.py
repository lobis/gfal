"""
Entry point: maps gfal-<cmd> and `gfal <cmd>` to execute_<cmd> methods.
"""

import contextlib
import os
import sys
from pathlib import Path

from gfal_cli import (
    base,
    commands,  # noqa: F401  – registers GfalCommands subclass
    copy,  # noqa: F401  – registers CommandCopy subclass
    ls,  # noqa: F401  – registers CommandLs subclass
    rm,  # noqa: F401  – registers CommandRm subclass
    tape,  # noqa: F401  – registers CommandTape subclass (bringonline/archivepoll/evict/token)
)

with contextlib.suppress(ImportError):
    from gfal_cli import tui  # noqa: F401  – registers CommandTui subclass
    # textual not installed → gfal-tui unavailable but all other commands work fine


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

_ALIASES = {}


def _find_command(cmd):
    method_name = "execute_" + cmd
    for cls in base.CommandBase.__subclasses__():
        method = getattr(cls, method_name, None)
        if method is not None:
            return cls, method
    raise ValueError(f"Unknown command: {cmd!r}")


def _command_from_argv0(argv0):
    """Extract the command token from the executable name.

    gfal-ls → ls
    gfal-cp → cp
    """
    name = Path(argv0).stem  # .stem strips .exe on Windows
    # strip leading 'gfal-' prefix
    token = name.rsplit("-", 1)[1].lower() if "-" in name else name.lower()
    return _ALIASES.get(token, token)


# ---------------------------------------------------------------------------
# `gfal` top-level help / version
# ---------------------------------------------------------------------------

_BUILTIN_SUBCMDS = {"version", "help"}


def _all_commands():
    """Return sorted list of (cmd_name, doc_line) for every registered command."""
    cmds = {}
    for cls in base.CommandBase.__subclasses__():
        for attr_name in dir(cls):
            if not attr_name.startswith("execute_"):
                continue
            cmd = attr_name[len("execute_") :]
            method = getattr(cls, attr_name)
            doc = (method.__doc__ or "").strip().split("\n")[0].rstrip(".")
            cmds[cmd] = doc
    return sorted(cmds.items())


def _print_gfal_help(to=sys.stdout):
    import rich_click as click

    # Build a RichGroup populated with one sub-command stub per registered command
    # so that rich-click renders the full panel-style help page.
    epilog = "Run [bold]gfal <command> --help[/bold] for more information on a command."
    grp = click.RichGroup(
        name="gfal",
        help=(
            f"[bold]gfal-cli {base.VERSION}[/bold] — "
            "GFAL2-compatible CLI tools based on fsspec (HTTP/HTTPS and XRootD)."
        ),
        epilog=epilog,
    )
    for cmd_name, doc in _all_commands():
        grp.add_command(
            click.RichCommand(name=cmd_name, help=doc, callback=lambda: None),
        )
    grp.add_command(
        click.RichCommand(
            name="version",
            help="Show the version and exit.",
            callback=lambda: None,
        ),
    )

    with contextlib.suppress(SystemExit):
        grp(["--help"], standalone_mode=True, prog_name="gfal")
    to.write("\n")


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def main(argv=None):
    _ensure_xrootd_dylib_path()

    # Native SSL support (macOS/Windows) via truststore
    try:
        import truststore

        truststore.inject_into_urllib3()
    except (ImportError, AttributeError):
        pass

    if argv is None:
        argv = sys.argv

    prog_stem = Path(argv[0]).stem  # e.g. "gfal", "gfal-ls", "gfal-cp"

    # -----------------------------------------------------------------------
    # `gfal` (no hyphen) — top-level dispatcher
    # -----------------------------------------------------------------------
    if prog_stem == "gfal":
        # No subcommand given -> show help
        if len(argv) < 2:
            _print_gfal_help()
            sys.exit(0)

        subcmd = argv[1]

        # gfal --help / gfal -h
        if subcmd in ("-h", "--help"):
            _print_gfal_help()
            sys.exit(0)

        # gfal --version / gfal -V
        if subcmd in ("-V", "--version"):
            sys.stdout.write(f"gfal-cli {base.VERSION}\n")
            sys.exit(0)

        # gfal version  (subcommand form)
        if subcmd == "version":
            sys.stdout.write(f"gfal-cli {base.VERSION}\n")
            sys.exit(0)

        # gfal help  (print help)
        if subcmd == "help":
            _print_gfal_help()
            sys.exit(0)

        # Unknown flag at top level (e.g. `gfal --foo`) — show help
        if subcmd.startswith("-"):
            sys.stderr.write(f"gfal: unknown option: {subcmd}\n\n")
            _print_gfal_help(to=sys.stderr)
            sys.exit(1)

        # Rewrite argv so the rest of main() sees `gfal-<subcmd> <rest...>`
        # This way `gfal ls -l /tmp` behaves exactly like `gfal-ls -l /tmp`.
        argv = [f"gfal-{subcmd}"] + argv[2:]

    # -----------------------------------------------------------------------
    # Standard hyphenated dispatch: gfal-ls, gfal-cp, …
    # -----------------------------------------------------------------------
    try:
        cmd = _command_from_argv0(argv[0])
        cls, func = _find_command(cmd)
    except ValueError as e:
        sys.stderr.write(f"{e}\n")
        sys.exit(1)

    inst = cls()
    inst.parse(func, argv)
    sys.exit(inst.execute(func))
