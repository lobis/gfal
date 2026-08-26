"""Dependency-free implementation of :command:`gfal save`."""

from __future__ import annotations

import argparse
import contextlib
import errno
import io
import sys
import time
from collections.abc import Sequence
from typing import Optional

from gfal.cli.local import local_path
from gfal.cli.xrdfs import (
    add_common_arguments,
    child_environment,
    supports_url,
    validate_common,
)
from gfal.xrdcp import check_xrdcp_capability, find_xrdcp, run_xrdcp
from gfal.xrdfs import (
    GFAL_EPROTONOSUPPORT,
    GFAL_ETIMEDOUT,
    error_description,
    error_exit_code,
    error_message,
    redact_authz,
)

_CHUNK_SIZE = 1024 * 1024


def _build_parser(prog: str) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog=prog,
        description=(
            "Gfal util SAVE command. Reads from stdin and writes to a file. "
            "If the file exists, it will be overwritten."
        ),
        allow_abbrev=False,
    )
    add_common_arguments(parser)
    parser.add_argument("file", help="URI of the file to be written")
    return parser


def should_use_native_save(argv: Sequence[str]) -> bool:
    """Return whether *argv* is understood by the dependency-free parser."""
    parser = _build_parser("gfal save")
    try:
        with (
            contextlib.redirect_stdout(io.StringIO()),
            contextlib.redirect_stderr(io.StringIO()),
        ):
            parser.parse_args(list(argv))
    except SystemExit as exc:
        return exc.code == 0
    return True


def _copy_stdin_to_local(path) -> None:
    source = getattr(sys.stdin, "buffer", sys.stdin)
    with path.open("wb") as destination:
        while True:
            chunk = source.read(_CHUNK_SIZE)
            if not chunk:
                break
            destination.write(chunk)


def _report_local_error(prog: str, exc: OSError) -> int:
    code = exc.errno or 1
    detail = exc.strerror or str(exc)
    sys.stderr.write(
        f"{prog} error: {code} ({error_description(code)}) - "
        f"errno reported by local system call {detail}\n"
    )
    return code


def _remaining(deadline: Optional[float]) -> Optional[float]:
    if deadline is None:
        return None
    return max(0.0, deadline - time.monotonic())


def _prepare_xrdcp(
    prog: str,
    environment: dict[str, str],
    deadline: Optional[float],
    configured_timeout: int,
) -> tuple[Optional[str], int]:
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
        remaining = _remaining(deadline)
        assert remaining is not None
        probe_timeout = min(probe_timeout, remaining)
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


def _report_xrdcp_result(prog: str, result, *, configured_timeout: int) -> int:
    if result.returncode == 0:
        stderr = redact_authz(result.stderr.decode("utf-8", errors="replace"))
        sys.stderr.write(stderr)
        return 0
    code = error_exit_code(result)
    if code == errno.EINTR:
        sys.stderr.write("Caught keyboard interrupt. Canceling...")
        return code
    if result.timed_out:
        sys.stderr.write(f"Command timed out after {configured_timeout} seconds!\n")
        return code
    sys.stderr.write(
        f"{prog} error: {code} ({error_description(code)}) - "
        f"{error_message(result.stderr)}\n"
    )
    return code


def dispatch_save(argv: Sequence[str], *, prog: str = "gfal save") -> int:
    """Write stdin to a local or remote destination without the old backend."""
    parser = _build_parser(prog)
    params = parser.parse_args(list(argv))
    validate_common(parser, params)

    path = local_path(params.file)
    if path is not None:
        try:
            _copy_stdin_to_local(path)
        except OSError as exc:
            return _report_local_error(prog, exc)
        return 0

    if not supports_url(params.file):
        sys.stderr.write(
            f"{prog} error: {GFAL_EPROTONOSUPPORT} "
            f"({error_description(GFAL_EPROTONOSUPPORT)}) - Protocol not "
            f"supported or path/url invalid: {redact_authz(params.file)}\n"
        )
        return GFAL_EPROTONOSUPPORT

    environment = child_environment(params)
    deadline = None if params.timeout <= 0 else time.monotonic() + params.timeout
    executable, status = _prepare_xrdcp(
        prog,
        environment,
        deadline,
        params.timeout,
    )
    if status:
        return status
    assert executable is not None
    result = run_xrdcp(
        executable,
        ("--force", "--nopbar", "--silent", "-", params.file),
        environ=environment,
        timeout=_remaining(deadline),
    )
    return _report_xrdcp_result(prog, result, configured_timeout=params.timeout)
