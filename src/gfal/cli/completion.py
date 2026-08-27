"""Static shell completion for the dependency-free aggregate CLI."""

from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence


def command_names() -> tuple[str, ...]:
    """Return the public aggregate command names."""
    return (
        "archivepoll",
        "bringonline",
        "cat",
        "chmod",
        "completion",
        "cp",
        "evict",
        "ls",
        "mkdir",
        "rename",
        "rm",
        "save",
        "stat",
        "sum",
        "token",
        "xattr",
    )


def bash_source() -> str:
    commands = " ".join(command_names())
    return f"""_gfal_completion() {{
    local current="${{COMP_WORDS[COMP_CWORD]}}"
    if [[ $COMP_CWORD -eq 1 ]]; then
        COMPREPLY=( $(compgen -W '{commands}' -- "$current") )
    fi
}}
complete -F _gfal_completion gfal
"""


def zsh_source() -> str:
    commands = " ".join(command_names())
    return f"""#compdef gfal
_gfal() {{
    local -a commands
    commands=({commands})
    _describe 'command' commands
}}
compdef _gfal gfal
"""


def dispatch_completion(argv: Sequence[str], *, prog: str = "gfal completion") -> int:
    parser = argparse.ArgumentParser(
        prog=prog,
        description="Generate shell completion source.",
        allow_abbrev=False,
    )
    parser.add_argument("shell", choices=("bash", "zsh"))
    params = parser.parse_args(list(argv))
    sys.stdout.write(bash_source() if params.shell == "bash" else zsh_source())
    return 0
