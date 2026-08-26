"""Process boundary for the external :command:`xrdcp` client."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Optional

from gfal.xrdfs import (
    XrdfsCapability,
    XrdfsResult,
    error_message,
    find_executable,
    run_command,
)

_REQUIRED_HELP_MARKERS = (
    "using a dash (-) for <src> uses stdin",
    "--force",
    "--nopbar",
    "--silent",
)


def find_xrdcp(environ: Optional[Mapping[str, str]] = None) -> Optional[str]:
    """Return the configured ``xrdcp`` path, or ``None`` when unavailable."""
    return find_executable("xrdcp", "GFAL_XRDCP", environ)


def run_xrdcp(
    executable: str,
    arguments: Sequence[str],
    *,
    environ: Mapping[str, str],
    timeout: Optional[float],
) -> XrdfsResult:
    """Run one ``xrdcp`` command with inherited stdin."""
    return run_command(
        executable,
        arguments,
        environ=environ,
        timeout=timeout,
    )


def check_xrdcp_capability(
    executable: str,
    *,
    environ: Mapping[str, str],
    timeout: Optional[float] = 5.0,
) -> XrdfsCapability:
    """Check that ``xrdcp`` supports byte-preserving stdin copies."""
    result = run_xrdcp(
        executable,
        ("--help",),
        environ=environ,
        timeout=timeout,
    )
    if result.timed_out:
        return XrdfsCapability("xrdcp --help timed out", timed_out=True)
    if result.returncode != 0:
        return XrdfsCapability(f"xrdcp --help failed: {error_message(result.stderr)}")

    help_text = (result.stdout + result.stderr).decode("utf-8", errors="replace")
    if any(marker not in help_text for marker in _REQUIRED_HELP_MARKERS):
        return XrdfsCapability("xrdcp lacks the required stdin copy interface")
    return XrdfsCapability()
