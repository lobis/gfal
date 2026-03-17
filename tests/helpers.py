"""Shared test helpers for gfal-cli tests."""

import os
import subprocess
import sys

# Force UTF-8 I/O in every gfal-cli subprocess so Windows cp1252 consoles
# never cause UnicodeEncodeError when the help text or error messages contain
# non-ASCII characters.
_UTF8_ENV = {**os.environ, "PYTHONUTF8": "1"}


def run_gfal(cmd, *args, input=None):
    """
    Run ``gfal-<cmd>`` in a subprocess via the current Python interpreter.

    Returns ``(returncode, stdout, stderr)`` as strings.

    Args are passed as separate argv elements so paths with spaces are safe.
    ``input`` may be a str piped to stdin (useful for gfal-save).
    """
    script = (
        f"import sys; sys.argv=['gfal-{cmd}']+sys.argv[1:];"
        "from gfal_cli.shell import main; main()"
    )
    proc = subprocess.run(
        [sys.executable, "-c", script, *[str(a) for a in args]],
        capture_output=True,
        text=True,
        encoding="utf-8",
        input=input,
        env=_UTF8_ENV,
    )
    return proc.returncode, proc.stdout, proc.stderr


def run_gfal_binary(cmd, *args, input_bytes=None):
    """
    Like run_gfal but captures stdout as raw bytes (for cat/save binary tests).
    """
    script = (
        f"import sys; sys.argv=['gfal-{cmd}']+sys.argv[1:];"
        "from gfal_cli.shell import main; main()"
    )
    proc = subprocess.run(
        [sys.executable, "-c", script, *[str(a) for a in args]],
        capture_output=True,
        input=input_bytes,
        env=_UTF8_ENV,
    )
    return proc.returncode, proc.stdout, proc.stderr
