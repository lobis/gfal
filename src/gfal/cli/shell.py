"""
Entry point for the supported ``gfal <command>`` CLI.
"""

import contextlib
import io
import os
import sys
from pathlib import Path

from gfal.cli import (
    base,
    commands,  # noqa: F401  – registers GfalCommands subclass
    copy,  # noqa: F401  – registers CommandCopy subclass
    ls,  # noqa: F401  – registers CommandLs subclass
    mount,  # noqa: F401  – registers CommandMount subclass
    rm,  # noqa: F401  – registers CommandRm subclass
    tape,  # noqa: F401  – registers CommandTape subclass (bringonline/archivepoll/evict)
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


def _find_command(cmd):
    method_name = "execute_" + cmd
    for cls in base.CommandBase.__subclasses__():
        method = getattr(cls, method_name, None)
        if method is not None:
            return cls, method
    raise ValueError(f"Unknown command: {cmd!r}")


# ---------------------------------------------------------------------------
# `gfal` top-level help / version / completion
# ---------------------------------------------------------------------------

_BUILTIN_SUBCMDS = {"version", "help", "completion"}


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
        help=f"gfal {base.VERSION} — GFAL2-compatible CLI tools based on fsspec (HTTP/HTTPS by default, XRootD when bindings are available).",
        epilog=epilog,
    )
    for cmd_name, doc in _all_commands():
        grp.add_command(CommandClass(name=cmd_name, help=doc, callback=lambda: None))
    grp.add_command(
        CommandClass(
            name="version", help="Show the version and exit.", callback=lambda: None
        )
    )

    grp.add_command(
        CommandClass(
            name="completion",
            help="Generate shell completion script.",
            callback=lambda: None,
        )
    )

    with contextlib.suppress(SystemExit):
        grp(["--help"], standalone_mode=True, prog_name="gfal")
    to.write("\n")


def _build_completion_group():
    """Build a Click Group with all commands attached for shell completion.

    This is used when Click's ``_GFAL_COMPLETE`` env var is set.  Each
    subcommand is built with its real Click params so that Click's completion
    machinery can suggest flags and option values.
    """
    try:
        import rich_click as _click

        GroupClass = _click.RichGroup
    except ImportError:
        import click as _click  # type: ignore[no-redef]

        GroupClass = _click.Group

    grp = GroupClass(name="gfal")

    seen: set = set()
    for cls in base.CommandBase.__subclasses__():
        for attr_name in dir(cls):
            if not attr_name.startswith("execute_"):
                continue
            cmd_name = attr_name[len("execute_") :]
            if cmd_name in seen:
                continue
            seen.add(cmd_name)

            method = getattr(cls, attr_name)
            help_text = (method.__doc__ or "").strip().split("\n")[0]
            prog_name = f"gfal {cmd_name}"

            click_cmd, _, _ = base._build_click_command(method, prog_name, help_text)
            # Override the name so the Group registers it as e.g. "ls", not "gfal ls"
            click_cmd.name = cmd_name
            grp.add_command(click_cmd)

    return grp


def _emit_bash_completion_source() -> None:
    """Emit a bash completion wrapper with support for ``gfal<TAB>``.

    Click returns no candidates when bash invokes completion on the command
    word itself (``COMP_CWORD=0`` for ``gfal<TAB>`` without a trailing space).
    Normalizing that exact case to the first-argument position avoids bash's
    fallback to filename completion.
    """
    sys.stdout.write(
        """_gfal_completion() {
    local IFS=$'\\n'
    local response
    local cmd="${1:-gfal}"
    local cword=$COMP_CWORD
    local words=("${COMP_WORDS[@]}")

    if [[ $cword -eq 0 && ${#words[@]} -eq 1 && ${words[0]} == "$cmd" ]]; then
        words=("$cmd" "")
        cword=1
    fi

    response=$(env COMP_WORDS="${words[*]}" COMP_CWORD=$cword _GFAL_COMPLETE=bash_complete "$cmd")

    for completion in $response; do
        IFS=',' read type value <<< "$completion"

        if [[ $type == 'dir' ]]; then
            COMPREPLY=()
            compopt -o dirnames
        elif [[ $type == 'file' ]]; then
            COMPREPLY=()
            compopt -o default
        elif [[ $type == 'plain' ]]; then
            COMPREPLY+=($value)
        fi
    done

    return 0
}

_gfal_completion_setup() {
    complete -o nosort -F _gfal_completion gfal
}

_gfal_completion_setup;
"""
    )


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def main(argv=None):
    _ensure_xrootd_dylib_path()

    if argv is None:
        argv = sys.argv

    prog_stem = Path(argv[0]).stem

    if prog_stem == "gfal":
        # Shell completion: delegate to Click's built-in completion machinery
        # when the _GFAL_COMPLETE env var is set by the shell's completion hook.
        complete_mode = os.environ.get("_GFAL_COMPLETE")
        if complete_mode:
            if complete_mode == "bash_source":
                _emit_bash_completion_source()
            else:
                grp = _build_completion_group()
                if complete_mode == "zsh_source":
                    # Zsh: prepend compinit bootstrap so completion works in fresh
                    # shells where compdef is not yet loaded (e.g. zsh -f).
                    script_out = io.StringIO()
                    with (
                        contextlib.redirect_stdout(script_out),
                        contextlib.suppress(SystemExit),
                    ):
                        grp.main(list(argv[1:]), prog_name="gfal", standalone_mode=True)
                    sys.stdout.write(
                        "autoload -Uz compinit\n"
                        "if ! whence compdef >/dev/null 2>&1; then\n"
                        "    compinit\n"
                        "fi\n"
                    )
                    sys.stdout.write(script_out.getvalue())
                else:
                    grp.main(list(argv[1:]), prog_name="gfal", standalone_mode=True)
            return

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
