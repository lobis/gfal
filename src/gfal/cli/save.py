"""Dependency-free implementation of :command:`gfal save`."""

from __future__ import annotations

import argparse
import contextlib
import io
import sys
import time
from collections.abc import Sequence

from gfal.cli.local import local_path
from gfal.cli.transfer import prepare_xrdcp, remaining, report_xrdcp_result
from gfal.cli.xrdfs import (
    add_common_arguments,
    child_environment,
    supports_url,
    validate_common,
)
from gfal.xrdcp import run_xrdcp
from gfal.xrdfs import (
    GFAL_EPROTONOSUPPORT,
    error_description,
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
    executable, status = prepare_xrdcp(
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
        timeout=remaining(deadline),
    )
    return report_xrdcp_result(prog, result, configured_timeout=params.timeout)
