"""Shared test helpers for gfal-cli tests."""

import os
import subprocess
import sys
from pathlib import Path


def _subprocess_env():
    """Build the environment dict for gfal-cli subprocesses.

    Called at invocation time (not module import time) so that any env vars
    set by pytest fixtures — in particular SSL_CERT_FILE / REQUESTS_CA_BUNDLE
    added by the CERN CA fixture in conftest.py — are picked up correctly.

    On macOS the pip-packaged xrootd embeds $ORIGIN RPATHs that dyld does not
    expand, causing the XRootD security plugins to fail to load unless the
    pyxrootd directory is on DYLD_LIBRARY_PATH.  shell.main() handles this via
    a re-exec for real binaries, but test subprocesses are invoked as
    ``python -c "..."`` which is not a real file on disk, so the re-exec guard
    fires and the env var is never set.  We set it here instead.
    """
    env = {**os.environ, "PYTHONUTF8": "1"}
    env.setdefault("GFAL_CLI_GFAL2", "1")

    if sys.platform == "darwin":
        try:
            import pyxrootd as _px  # noqa: PLC0415

            plugin_dir = str(Path(_px.__file__).parent)
            current = env.get("DYLD_LIBRARY_PATH", "")
            if plugin_dir not in current.split(":"):
                env["DYLD_LIBRARY_PATH"] = (
                    f"{plugin_dir}:{current}" if current else plugin_dir
                )
        except ImportError:
            pass  # xrootd not installed — nothing to do

    return env


def run_gfal(cmd, *args, input=None, env=None):
    """
    Run ``gfal-<cmd>`` in a subprocess via the current Python interpreter.

    Returns ``(returncode, stdout, stderr)`` as strings.

    Args are passed as separate argv elements so paths with spaces are safe.
    ``input`` may be a str piped to stdin (useful for gfal-save).
    ``env`` may be a dict of extra environment variables to set (or override)
    on top of the base subprocess environment.
    """
    script = (
        f"import sys; sys.argv=['gfal-{cmd}']+sys.argv[1:];"
        "from gfal_cli.shell import main; main()"
    )
    subprocess_env = _subprocess_env()
    if env is not None:
        subprocess_env = {**subprocess_env, **env}
    proc = subprocess.run(
        [sys.executable, "-c", script, *[str(a) for a in args]],
        capture_output=True,
        text=True,
        encoding="utf-8",
        input=input,
        env=subprocess_env,
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
        env=_subprocess_env(),
    )
    return proc.returncode, proc.stdout, proc.stderr
