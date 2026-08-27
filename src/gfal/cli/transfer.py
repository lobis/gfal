"""Shared helpers for dependency-free data-transfer commands."""

from __future__ import annotations

import errno
import sys
import time
from typing import Optional

from gfal.xrdcp import check_xrdcp_capability, find_xrdcp
from gfal.xrdfs import (
    GFAL_ETIMEDOUT,
    error_description,
    error_exit_code,
    error_message,
    redact_authz,
)


def remaining(deadline: Optional[float]) -> Optional[float]:
    """Return the non-negative time remaining before *deadline*."""
    if deadline is None:
        return None
    return max(0.0, deadline - time.monotonic())


def prepare_xrdcp(
    prog: str,
    environment: dict[str, str],
    deadline: Optional[float],
    configured_timeout: int,
) -> tuple[Optional[str], int]:
    """Locate and validate the external xrdcp client."""
    executable = find_xrdcp(environment)
    if executable is None:
        configured = environment.get("GFAL_XRDCP")
        detail = f" configured by GFAL_XRDCP={configured!r}" if configured else ""
        sys.stderr.write(
            f"{prog}: xrdcp{detail} was not found; install xrootd-client\n"
        )
        return None, 127

    probe_timeout = 5.0
    if deadline is not None:
        left = remaining(deadline)
        assert left is not None
        probe_timeout = min(probe_timeout, left)
    capability = check_xrdcp_capability(
        executable,
        environ=environment,
        timeout=probe_timeout,
    )
    if capability.error:
        if capability.timed_out and deadline is not None:
            sys.stderr.write(f"Command timed out after {configured_timeout} seconds!\n")
            return None, GFAL_ETIMEDOUT
        sys.stderr.write(f"{prog}: incompatible xrdcp: {capability.error}\n")
        return None, 69
    return executable, 0


def report_xrdcp_result(prog: str, result, *, configured_timeout: int) -> int:
    """Translate an xrdcp result to the errno-style gfal CLI contract."""
    if result.returncode == 0:
        stderr = redact_authz(result.stderr.decode("utf-8", errors="replace"))
        sys.stderr.write(stderr)
        return 0
    code = error_exit_code(result)
    if code == errno.EINTR:
        sys.stderr.write("Caught keyboard interrupt. Canceling...\n")
        return code
    if result.timed_out:
        sys.stderr.write(f"Command timed out after {configured_timeout} seconds!\n")
        return code
    sys.stderr.write(
        f"{prog} error: {code} ({error_description(code)}) - "
        f"{error_message(result.stderr)}\n"
    )
    return code
