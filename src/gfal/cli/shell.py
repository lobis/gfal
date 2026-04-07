"""
Entry point for the supported ``gfal <command>`` CLI.
"""

import contextlib
import os
import sys
from pathlib import Path

from gfal.cli import (
    base,
    commands,  # noqa: F401  – registers GfalCommands subclass
    copy,  # noqa: F401  – registers CommandCopy subclass
    ls,  # noqa: F401  – registers CommandLs subclass
    rm,  # noqa: F401  – registers CommandRm subclass
    tape,  # noqa: F401  – registers CommandTape subclass (bringonline/archivepoll/evict/token)
)

try:
    import gfal.tui  # noqa: F401  – registers CommandTui subclass
except ImportError:
    # textual not installed — register a stub that prints a friendly error
    class _CommandTuiStub(base.CommandBase):
        @base.arg("src", nargs="?", help="source path")
        @base.arg("dst", nargs="?", help="destination path")
        def execute_tui(self):
            """Launch the Text User Interface (requires gfal[tui])."""
            sys.stderr.write(
                "error: the TUI requires optional dependencies.\n"
                "Install them with:  pip install 'gfal[tui]'\n"
            )
            sys.exit(1)


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


def _find_command(cmd):
    method_name = "execute_" + cmd
    for cls in base.CommandBase.__subclasses__():
        method = getattr(cls, method_name, None)
        if method is not None:
            return cls, method
    raise ValueError(f"Unknown command: {cmd!r}")


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
    try:
        import rich_click as _click

        GroupClass = _click.RichGroup
        CommandClass = _click.RichCommand
    except ImportError:
        import click as _click  # type: ignore[no-redef]

        GroupClass = _click.Group
        CommandClass = _click.Command

    epilog = "Run 'gfal <command> --help' for more information on a command."
    grp = GroupClass(
        name="gfal",
        help=f"gfal {base.VERSION} — GFAL2-compatible CLI tools based on fsspec (HTTP/HTTPS and XRootD).",
        epilog=epilog,
    )
    for cmd_name, doc in _all_commands():
        grp.add_command(CommandClass(name=cmd_name, help=doc, callback=lambda: None))
    grp.add_command(
        CommandClass(
            name="version", help="Show the version and exit.", callback=lambda: None
        )
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

    prog_stem = Path(argv[0]).stem

    if prog_stem == "gfal":
        if len(argv) < 2:
            _print_gfal_help()
            sys.exit(0)

        subcmd = argv[1]

        if subcmd in ("-h", "--help"):
            _print_gfal_help()
            sys.exit(0)

        if subcmd in ("-V", "--version"):
            sys.stdout.write(f"gfal {base.VERSION}\n")
            sys.exit(0)

        if subcmd == "version":
            sys.stdout.write(f"gfal {base.VERSION}\n")
            sys.exit(0)

        if subcmd == "help":
            _print_gfal_help()
            sys.exit(0)

        if subcmd.startswith("-"):
            sys.stderr.write(f"gfal: unknown option: {subcmd}\n\n")
            _print_gfal_help(to=sys.stderr)
            sys.exit(1)
        try:
            cls, func = _find_command(subcmd)
        except ValueError as e:
            sys.stderr.write(f"{e}\n")
            sys.exit(1)

        inst = cls()
        inst.parse(func, [f"gfal {subcmd}"] + argv[2:])
        sys.exit(inst.execute(func))

    sys.stderr.write(
        "This package exposes the CLI as 'gfal <command>', not as hyphenated "
        f"executables like '{Path(argv[0]).name}'. Use a shell alias if you need "
        "that form.\n"
    )
    sys.exit(1)
