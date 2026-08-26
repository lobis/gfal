"""Dependency-free adapters for local filesystem commands."""

from __future__ import annotations

import argparse
import errno
import os
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Optional
from urllib.parse import unquote, urlsplit

from gfal.cli.xrdfs import parse_arguments, read_nonempty_lines, routing_arguments
from gfal.xrdfs import error_description, redact_authz


def _rm_urls(params: argparse.Namespace) -> Optional[list[str]]:
    if params.from_file and params.file:
        return []
    if not params.from_file:
        return list(params.file)
    try:
        return read_nonempty_lines(params.from_file)
    except OSError:
        return None


def _local_path(value: str) -> Optional[Path]:
    try:
        parsed = urlsplit(value)
    except ValueError:
        return None
    if not parsed.scheme:
        return Path(value)
    if parsed.scheme.lower() != "file" or parsed.netloc not in ("", "localhost"):
        return None
    if parsed.query or parsed.fragment:
        return None
    path = unquote(parsed.path)
    if "\0" in path:
        return None
    return Path(path)


def should_use_local_rm(argv: Sequence[str]) -> bool:
    """Return whether an rm invocation contains only local operands."""
    params, information_requested = routing_arguments("rm", argv)
    if information_requested or params is None:
        return False
    urls = _rm_urls(params)
    if urls is None or not urls:
        return True
    return all(_local_path(value) is not None for value in urls)


def _display_path(path: Path, *, as_uri: bool) -> str:
    if as_uri:
        return path.absolute().as_uri()
    return str(path)


def _print_removal_plan(path: Path, *, as_uri: bool) -> None:
    display = _display_path(path, as_uri=as_uri)
    if path.is_symlink() or not path.is_dir():
        print(f"{display}\tSKIP")
        return
    for child in path.iterdir():
        _print_removal_plan(child, as_uri=as_uri)
    print(f"{display}\tSKIP DIR")


def _remove_tree(path: Path, *, as_uri: bool) -> None:
    for child in path.iterdir():
        if child.is_dir() and not child.is_symlink():
            _remove_tree(child, as_uri=as_uri)
        else:
            child.unlink()
            print(f"{_display_path(child, as_uri=as_uri)}\tDELETED")
    path.rmdir()
    print(f"{_display_path(path, as_uri=as_uri)}\tRMDIR")


def _report_error(prog: str, value: str, exc: OSError) -> int:
    code = exc.errno or 1
    detail = exc.strerror or str(exc)
    sys.stderr.write(
        f"{prog} error: {code} ({error_description(code)}) - "
        f"{redact_authz(value)}: {detail}\n"
    )
    return code


def dispatch_local_rm(argv: Sequence[str], *, prog: str = "gfal rm") -> int:
    """Execute a local rm invocation using only the standard library."""
    _parser, params = parse_arguments("rm", argv, prog=prog)
    if params.from_file and params.file:
        sys.stderr.write(
            f"{prog}: --from-file and positional arguments cannot be combined\n"
        )
        return errno.EINVAL

    try:
        urls = (
            read_nonempty_lines(params.from_file)
            if params.from_file
            else list(params.file)
        )
    except OSError as exc:
        return _report_error(prog, params.from_file, exc)
    if not urls:
        sys.stderr.write(f"{prog}: No URI specified\n")
        return errno.EINVAL

    first_failure = 0
    for value in urls:
        path = _local_path(value)
        if path is None:
            sys.stderr.write(f"{prog}: expected a local path: {redact_authz(value)}\n")
            if not first_failure:
                first_failure = errno.EINVAL
            continue
        try:
            as_uri = urlsplit(value).scheme.lower() == "file"
            if params.just_delete:
                if params.dry_run:
                    print(f"{redact_authz(value)}\tSKIP")
                else:
                    path.unlink()
                    print(f"{redact_authz(value)}\tDELETED")
                continue
            path.lstat()
            is_directory = path.is_dir() and not path.is_symlink()
            if is_directory and not params.recursive:
                raise IsADirectoryError(
                    errno.EISDIR,
                    os.strerror(errno.EISDIR),
                    str(path),
                )
            if params.dry_run:
                _print_removal_plan(path, as_uri=as_uri)
            elif is_directory:
                _remove_tree(path, as_uri=as_uri)
            else:
                path.unlink()
                print(f"{redact_authz(value)}\tDELETED")
        except FileNotFoundError as exc:
            print(f"{redact_authz(value)}\tMISSING")
            if not first_failure:
                first_failure = exc.errno or errno.ENOENT
        except OSError as exc:
            code = _report_error(prog, value, exc)
            if code != errno.EISDIR:
                print(f"{redact_authz(value)}\tFAILED")
            if not first_failure:
                first_failure = code
    return first_failure
