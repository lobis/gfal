"""Dependency-free implementation of :command:`gfal cp` using xrdcp."""

from __future__ import annotations

import argparse
import posixpath
import sys
import time
from collections.abc import Sequence
from pathlib import Path
from typing import Optional
from urllib.parse import unquote, urlsplit, urlunsplit

from gfal.cli.local import local_path
from gfal.cli.transfer import prepare_xrdcp, remaining, report_xrdcp_result
from gfal.cli.xrdfs import (
    REMOTE_SCHEMES,
    add_common_arguments,
    child_environment,
    read_nonempty_lines,
    supports_url,
    validate_common,
)
from gfal.xrdcp import run_xrdcp
from gfal.xrdfs import (
    GFAL_ENOTSUP,
    error_description,
    error_exit_code,
    error_message,
    find_xrdfs,
    redact_authz,
    run_xrdfs,
)


def _build_parser(prog: str) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog=prog,
        description="Gfal util CP command. Copy files using the XRootD client.",
        allow_abbrev=False,
    )
    add_common_arguments(parser)
    parser.add_argument("-f", "--force", action="store_true", help="overwrite files")
    parser.add_argument(
        "-p", "--parent", action="store_true", help="create destination parents"
    )
    parser.add_argument(
        "-r", "--recursive", action="store_true", help="copy directories recursively"
    )
    parser.add_argument("-q", "--quiet", action="store_true", help="suppress output")
    parser.add_argument("--from-file", metavar="FILE", help="read sources from FILE")
    parser.add_argument("--dry-run", action="store_true", help="show the copy plan")
    parser.add_argument("--abort-on-failure", action="store_true")
    parser.add_argument("--just-copy", action="store_true")
    parser.add_argument("--disable-cleanup", action="store_true")
    parser.add_argument("--no-delegation", action="store_true")
    parser.add_argument(
        "-K",
        "--checksum",
        metavar="ALGORITHM[:VALUE]",
        help="verify the transfer checksum",
    )
    parser.add_argument(
        "--checksum-mode",
        choices=("source", "target", "both"),
        default="both",
    )
    parser.add_argument(
        "--copy-mode",
        choices=("pull", "push", "streamed"),
    )
    parser.add_argument("--tpc", action="store_true")
    parser.add_argument("--tpc-only", action="store_true")
    parser.add_argument("--tpc-mode", choices=("pull", "push"), default="pull")
    parser.add_argument("-n", "--nbstreams", type=int, default=0)
    parser.add_argument("-T", "--transfer-timeout", type=int, default=0)
    parser.add_argument("--compare", choices=("size", "size_mtime", "checksum", "none"))
    parser.add_argument("--limit", type=int)
    parser.add_argument("--scitag", type=int)
    parser.add_argument("-s", "--src-spacetoken")
    parser.add_argument("-S", "--dst-spacetoken")
    parser.add_argument("--tcp-buffersize", type=int)
    parser.add_argument("--evict", action="store_true")
    parser.add_argument("--no-verify", action="store_true")
    parser.add_argument("operands", nargs="+", metavar="SOURCE|DESTINATION")
    return parser


def _unsupported_options(params: argparse.Namespace) -> list[str]:
    unsupported = []
    if params.copy_mode == "push" or params.tpc_mode == "push":
        unsupported.append("push-mode TPC")
    if params.compare:
        unsupported.append("--compare")
    if params.just_copy:
        unsupported.append("--just-copy")
    if params.no_delegation:
        unsupported.append("--no-delegation")
    if params.checksum_mode != "both":
        unsupported.append("--checksum-mode")
    if params.scitag is not None:
        unsupported.append("--scitag")
    if params.dst_spacetoken or params.src_spacetoken:
        unsupported.append("space tokens")
    if params.no_verify:
        unsupported.append("--no-verify")
    if params.tcp_buffersize is not None:
        unsupported.append("--tcp-buffersize")
    if params.evict:
        unsupported.append("--evict")
    return unsupported


def _parse_operands(
    parser: argparse.ArgumentParser, params: argparse.Namespace
) -> tuple[list[str], list[str], bool]:
    if params.from_file:
        if len(params.operands) != 1:
            parser.error("--from-file requires exactly one destination")
        try:
            sources = read_nonempty_lines(params.from_file)
        except OSError as exc:
            parser.error(str(exc))
        if not sources:
            parser.error("--from-file contains no sources")
        return sources, [params.operands[0]], True
    if len(params.operands) < 2:
        parser.error("a source and at least one destination are required")
    return [params.operands[0]], params.operands[1:], False


def _basename(value: str) -> str:
    parsed = urlsplit(value)
    path = unquote(parsed.path) if parsed.scheme else value
    return posixpath.basename(path.rstrip("/"))


def _join_destination(directory: str, source: str) -> str:
    name = _basename(source)
    parsed = urlsplit(directory)
    if parsed.scheme:
        path = parsed.path.rstrip("/") + "/" + name
        return urlunsplit((parsed.scheme, parsed.netloc, path, parsed.query, ""))
    return str(Path(directory) / name)


def _copy_pairs(
    sources: list[str], destinations: list[str], *, source_list: bool
) -> list[tuple[str, str]]:
    if source_list:
        destination = destinations[0]
        return [(source, _join_destination(destination, source)) for source in sources]
    source = sources[0]
    pairs = []
    for destination in destinations:
        pairs.append((source, destination))
        source = destination
    return pairs


def _validate_value(parser: argparse.ArgumentParser, value: str) -> None:
    if value == "-" or local_path(value) is not None:
        return
    if supports_url(value, REMOTE_SCHEMES):
        return
    parser.error(f"unsupported path or URL: {redact_authz(value)}")


def _remote_parent(value: str) -> Optional[str]:
    if not supports_url(value, REMOTE_SCHEMES):
        return None
    parsed = urlsplit(value)
    parent = posixpath.dirname(parsed.path.rstrip("/")) or "/"
    return urlunsplit((parsed.scheme, parsed.netloc, parent, "", ""))


def _create_parent(
    prog: str,
    destination: str,
    environment: dict[str, str],
    deadline: Optional[float],
) -> int:
    local = local_path(destination)
    if local is not None:
        try:
            local.parent.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            code = exc.errno or 1
            sys.stderr.write(
                f"{prog} error: {code} ({error_description(code)}) - {exc}\n"
            )
            return code
        return 0

    parent = _remote_parent(destination)
    if parent is None:
        return 0
    executable = find_xrdfs(environment)
    if executable is None:
        sys.stderr.write(f"{prog}: xrdfs was not found; install xrootd-client\n")
        return 127
    result = run_xrdfs(
        executable,
        ("mkdir", "-p", parent),
        environ=environment,
        timeout=remaining(deadline),
    )
    if result.returncode == 0:
        return 0
    code = error_exit_code(result)
    sys.stderr.write(
        f"{prog} error: {code} ({error_description(code)}) - "
        f"{error_message(result.stderr)}\n"
    )
    return code


def _xrdcp_arguments(
    params: argparse.Namespace, source: str, destination: str
) -> list[str]:
    arguments = ["--nopbar"]
    if params.quiet:
        arguments.append("--silent")
    if params.force:
        arguments.append("--force")
    if params.recursive:
        arguments.append("--recursive")
    if params.nbstreams > 0:
        arguments.extend(("--streams", str(params.nbstreams)))
    if params.checksum:
        checksum = params.checksum.removeprefix("=").lower()
        arguments.extend(("--cksum", checksum))
        if ":" in checksum and not params.disable_cleanup:
            arguments.append("--rm-bad-cksum")
    if params.tpc_only:
        arguments.extend(("--tpc", "only"))
    elif params.tpc or params.copy_mode == "pull":
        arguments.extend(("--tpc", "first"))
    arguments.extend((source, destination))
    return arguments


def dispatch_cp(argv: Sequence[str], *, prog: str = "gfal cp") -> int:
    """Copy files through xrdcp without importing the transitional backend."""
    parser = _build_parser(prog)
    params = parser.parse_args(list(argv))
    validate_common(parser, params)
    if (
        params.nbstreams < 0
        or params.transfer_timeout < 0
        or (params.limit is not None and params.limit < 1)
        or (params.tcp_buffersize is not None and params.tcp_buffersize < 1)
    ):
        parser.error("numeric copy options must be positive or zero where allowed")
    unsupported = _unsupported_options(params)
    if unsupported:
        detail = ", ".join(unsupported)
        sys.stderr.write(
            f"{prog} error: {GFAL_ENOTSUP} ({error_description(GFAL_ENOTSUP)}) - "
            f"not implemented by the xrdfs package: {detail}\n"
        )
        return GFAL_ENOTSUP

    sources, destinations, source_list = _parse_operands(parser, params)
    pairs = _copy_pairs(sources, destinations, source_list=source_list)
    if params.limit is not None:
        pairs = pairs[: params.limit]
    for source, destination in pairs:
        _validate_value(parser, source)
        _validate_value(parser, destination)

    if params.dry_run:
        for source, destination in pairs:
            print(f"Copy {redact_authz(source)} => {redact_authz(destination)}")
        return 0

    environment = child_environment(params)
    configured_timeout = params.transfer_timeout or params.timeout
    deadline = (
        None if configured_timeout <= 0 else time.monotonic() + configured_timeout
    )
    executable, status = prepare_xrdcp(
        prog,
        environment,
        deadline,
        configured_timeout,
    )
    if status:
        return status
    assert executable is not None

    first_failure = 0
    for source, destination in pairs:
        if params.parent:
            status = _create_parent(prog, destination, environment, deadline)
            if status:
                first_failure = first_failure or status
                if params.abort_on_failure:
                    break
                continue
        result = run_xrdcp(
            executable,
            _xrdcp_arguments(params, source, destination),
            environ=environment,
            timeout=remaining(deadline),
            passthrough_stdout=destination == "-",
        )
        status = report_xrdcp_result(
            prog,
            result,
            configured_timeout=configured_timeout,
        )
        if status:
            first_failure = first_failure or status
            if params.abort_on_failure:
                break
    return first_failure
