"""The ``gfal`` command-line interface backed by the external ``xrdfs`` command."""

from __future__ import annotations

import argparse
import contextlib
import errno
import io
import json
import math
import os
import posixpath
import stat
import sys
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from types import MappingProxyType
from typing import Any, Optional
from urllib.parse import urlsplit

from gfal import __version__
from gfal.xrdfs import (
    GFAL_ETIMEDOUT,
    XrdfsResult,
    check_capability,
    error_description,
    error_exit_code,
    error_message,
    find_xrdfs,
    redact_authz,
    run_xrdfs,
)

_WEBDAV_SCHEMES = frozenset({
    "dav",
    "davs",
    "http",
    "https",
})
_XROOTD_SCHEMES = frozenset({
    "root",
    "roots",
    "xroot",
    "xroots",
})
_REMOTE_SCHEMES = _WEBDAV_SCHEMES | _XROOTD_SCHEMES

_COMMON_CAPABILITY_MARKERS = (
    "command-first batch",
    "--json print one JSON object per entry",
)
_COMMON_LEGACY_OPTIONS = frozenset((
    "-q",
    "--quiet",
    "--authz-token",
    "--verify",
    "--no-verify",
))
_COMMON_LEGACY_PREFIXES = ("--authz-token=",)
_COMMON_LEGACY_SHORT_OPTIONS = frozenset(("q",))
_VALUE_SHORT_OPTIONS = frozenset(("D", "t", "E", "C"))


@dataclass(frozen=True)
class XrdfsCommand:
    """Declarative routing information for one migrated command."""

    description: str
    add_arguments: Callable[[argparse.ArgumentParser], None]
    execute: Callable[
        [str, str, argparse.Namespace, Mapping[str, str], Optional[float]], int
    ]
    validate: Optional[
        Callable[[argparse.ArgumentParser, argparse.Namespace], None]
    ] = None
    route_when: Optional[Callable[[argparse.Namespace, Sequence[str]], bool]] = None
    capability_markers: tuple[str, ...] = ()
    legacy_options: frozenset[str] = frozenset()
    legacy_prefixes: tuple[str, ...] = ()
    legacy_short_options: frozenset[str] = frozenset()
    url_parameters: tuple[str, ...] = ("file",)
    url_schemes: frozenset[str] = _REMOTE_SCHEMES


# Populated after the command-specific parser and executor functions are defined.
XRDFS_COMMANDS: Mapping[str, XrdfsCommand]


def capability_markers() -> tuple[str, ...]:
    """Return the complete external ``xrdfs`` interface contract."""
    return _COMMON_CAPABILITY_MARKERS + tuple(
        marker
        for command in XRDFS_COMMANDS.values()
        for marker in command.capability_markers
    )


def requires_legacy_backend(command: str, arguments: Sequence[str]) -> bool:
    """Return whether transitional options require the previous backend."""
    definition = XRDFS_COMMANDS[command]
    if os.environ.get("EOSAUTHZ") or os.environ.get("GFAL_AUTHZ_TOKEN"):
        return True

    legacy_options = _COMMON_LEGACY_OPTIONS | definition.legacy_options
    legacy_prefixes = _COMMON_LEGACY_PREFIXES + definition.legacy_prefixes
    legacy_short = _COMMON_LEGACY_SHORT_OPTIONS | definition.legacy_short_options

    for value in arguments:
        if value == "--":
            break
        if value in legacy_options or any(
            value.startswith(prefix) for prefix in legacy_prefixes
        ):
            return True
        if not value.startswith("-") or value.startswith("--") or value == "-":
            continue
        for character in value[1:]:
            if character in _VALUE_SHORT_OPTIONS:
                break
            if character in legacy_short:
                return True
    return False


def supports_url(value: str, schemes: frozenset[str] = _REMOTE_SCHEMES) -> bool:
    """Return whether *value* is a complete URL supported by this backend."""
    try:
        parsed = urlsplit(value)
    except ValueError:
        return False
    return parsed.scheme.lower() in schemes and bool(parsed.netloc)


def _command_urls(definition: XrdfsCommand, params: argparse.Namespace) -> list[str]:
    values = []
    for parameter in definition.url_parameters:
        value = getattr(params, parameter)
        values.extend(value if isinstance(value, list) else [value])
    return values


def _option_requires_schemes(
    option: str,
    schemes: frozenset[str],
) -> Callable[[argparse.Namespace, Sequence[str]], bool]:
    """Build a routing rule for an option with protocol-specific support."""

    def route_when(params: argparse.Namespace, urls: Sequence[str]) -> bool:
        return not getattr(params, option) or all(
            supports_url(value, schemes) for value in urls
        )

    return route_when


def _uses_legacy_webdav_defaults(value: str, record: Mapping[str, Any]) -> bool:
    """Return whether metadata lacks the fields supplied by WebDAV stat."""
    try:
        scheme = urlsplit(value).scheme.lower()
    except ValueError:
        return False
    return scheme in _WEBDAV_SCHEMES and record.get("extended") is False


def _add_common_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "-V",
        "--version",
        action="version",
        version=f"gfal {__version__} (xrdfs backend)",
        help="output version information and exit",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="count",
        default=0,
        help="enable verbose mode; repeat for more detail",
    )
    parser.add_argument(
        "-D",
        "--definition",
        action="append",
        default=[],
        metavar="DEFINITION",
        help="accept a gfal parameter override (compatibility no-op)",
    )
    parser.add_argument(
        "-t",
        "--timeout",
        type=int,
        default=1800,
        help="maximum time for the operation to terminate (default: 1800 seconds)",
    )
    parser.add_argument("-E", "--cert", help="user certificate")
    parser.add_argument("--key", help="user private key")
    parser.add_argument(
        "-4",
        "--ipv4",
        dest="ipv4",
        action="store_true",
        help="force IPv4 addresses",
    )
    parser.add_argument(
        "-6",
        "--ipv6",
        dest="ipv6",
        action="store_true",
        help="force IPv6 addresses",
    )
    parser.add_argument(
        "-C",
        "--client-info",
        action="append",
        default=[],
        help="provide custom client-side information",
    )
    parser.add_argument(
        "--log-file",
        help="write XRootD client logs to the given file",
    )


def _add_ls_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("-a", "--all", action="store_true", help="display hidden files")
    parser.add_argument("-l", "--long", action="store_true", help="long listing format")
    parser.add_argument(
        "-d",
        "--directory",
        action="store_true",
        help="list directory entries instead of contents",
    )
    parser.add_argument(
        "-H",
        "--human-readable",
        action="store_true",
        help="with -l, print sizes in human-readable form",
    )
    parser.add_argument(
        "--xattr",
        action="append",
        default=[],
        help="query an additional attribute; may be repeated and is shown with -l",
    )
    parser.add_argument(
        "--time-style",
        choices=("full-iso", "long-iso", "iso", "locale"),
        default="locale",
        help="time style",
    )
    parser.add_argument(
        "--full-time",
        action="store_true",
        help="same as --time-style=full-iso",
    )
    parser.add_argument(
        "--color",
        choices=("always", "never", "auto"),
        default="auto",
        help="print colored entries with -l",
    )
    parser.add_argument("file", nargs="+", help="file's URI")


def _add_cat_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "-b",
        "--bytes",
        action="store_true",
        help="handle file contents as bytes (output is always byte-preserving)",
    )
    parser.add_argument("file", nargs="+", help="URI of the file to display")


def _add_stat_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("file", nargs="+", help="URI of the file to stat")


def _add_sum_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("file", help="file URI to use for checksum calculation")
    parser.add_argument(
        "checksum_type",
        help="checksum algorithm to use, for example ADLER32, CRC32, or MD5",
    )


def _add_xattr_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("file", help="file URI")
    parser.add_argument(
        "attribute",
        nargs="?",
        help="attribute to retrieve or set; use key=value to set",
    )


def _add_archivepoll_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--polling-timeout",
        type=int,
        default=0,
        metavar="SECONDS",
        help="timeout for the polling operation",
    )
    parser.add_argument(
        "--from-file",
        metavar="FILE",
        help="read SURLs from a file, one per line",
    )
    parser.add_argument("surl", nargs="?", help="Site URL")


def _add_bringonline_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--pin-lifetime",
        type=int,
        default=0,
        metavar="SECONDS",
        help="desired pin lifetime in seconds",
    )
    parser.add_argument(
        "--desired-request-time",
        type=int,
        default=None,
        metavar="SECONDS",
        help="desired total request time in seconds",
    )
    parser.add_argument(
        "--staging-metadata",
        default="",
        metavar="METADATA",
        help="metadata string for the bringonline operation",
    )
    _add_archivepoll_arguments(parser)


def _add_evict_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("file", help="URI of the file to evict")
    parser.add_argument(
        "token",
        nargs="?",
        default="",
        help="token from the bring-online request",
    )


def _validate_tape_source(
    parser: argparse.ArgumentParser, params: argparse.Namespace
) -> None:
    if params.from_file and params.surl:
        parser.error("could not combine --from-file with a positional SURL")
    if not params.from_file and not params.surl:
        parser.error("missing SURL")
    if params.polling_timeout < 0:
        parser.error("--polling-timeout must be non-negative")
    pin_lifetime = getattr(params, "pin_lifetime", 0)
    if pin_lifetime < 0:
        parser.error("--pin-lifetime must be non-negative")
    desired_request_time = getattr(params, "desired_request_time", None)
    if desired_request_time is not None and desired_request_time < 0:
        parser.error("--desired-request-time must be non-negative")
    if params.surl:
        _validate_remote_url(parser, params.surl, _WEBDAV_SCHEMES)


def _route_tape_source(params: argparse.Namespace, _urls: Sequence[str]) -> bool:
    if params.from_file:
        return True
    return bool(params.surl and supports_url(params.surl, _WEBDAV_SCHEMES))


def _add_mkdir_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "-m",
        "--mode",
        type=int,
        default=755,
        metavar="MODE",
        help="file permissions (octal)",
    )
    parser.add_argument(
        "-p",
        "--parents",
        action="store_true",
        help="no error if existing, make parent directories as needed",
    )
    parser.add_argument("directory", nargs="+", help="Directory's URI")


def _validate_mkdir(
    parser: argparse.ArgumentParser, params: argparse.Namespace
) -> None:
    if params.mode < 0:
        parser.error("argument -m/--mode: expected one argument")


def _add_chmod_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("mode", help="new mode, in octal")
    parser.add_argument("file", nargs="+", help="URI of the file to change permissions")


def _validate_chmod(
    parser: argparse.ArgumentParser, params: argparse.Namespace
) -> None:
    try:
        mode = int(params.mode, 8)
    except ValueError:
        parser.error("mode must be an octal number")
    if mode < 0:
        parser.error("mode must be a non-negative octal number")
    params.mode = f"{mode & 0o777:04o}"


def _add_rename_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("source", help="original file name")
    parser.add_argument("destination", help="new file name")


def _build_parser(
    command: str,
    prog: str,
    parser_class: type[argparse.ArgumentParser] = argparse.ArgumentParser,
) -> argparse.ArgumentParser:
    definition = XRDFS_COMMANDS[command]
    parser = parser_class(
        prog=prog,
        description=definition.description,
        allow_abbrev=False,
    )
    _add_common_arguments(parser)
    definition.add_arguments(parser)
    return parser


class _RoutingError(Exception):
    pass


class _InformationRequested(_RoutingError):
    pass


class _RoutingParser(argparse.ArgumentParser):
    """Parse only to select a backend, without printing or exiting."""

    def error(self, message: str) -> None:
        raise _RoutingError(message)

    def exit(self, status: int = 0, message: Optional[str] = None) -> None:
        if status == 0:
            raise _InformationRequested(message or "")
        raise _RoutingError(message or "")


def should_use_xrdfs(command: str, argv: Sequence[str]) -> bool:
    """Return whether *argv* is fully understood and has only remote operands."""
    definition = XRDFS_COMMANDS[command]
    parser = _build_parser(command, f"gfal {command}", _RoutingParser)
    try:
        with (
            contextlib.redirect_stdout(io.StringIO()),
            contextlib.redirect_stderr(io.StringIO()),
        ):
            params = parser.parse_args(list(argv))
    except _InformationRequested:
        return True
    except _RoutingError:
        return False

    values = _command_urls(definition, params)
    if definition.url_parameters:
        if not values or not all(
            supports_url(value, definition.url_schemes) for value in values
        ):
            return False
    elif definition.route_when is None:
        return False
    return definition.route_when is None or definition.route_when(params, values)


def _validate_common(
    parser: argparse.ArgumentParser, params: argparse.Namespace
) -> None:
    if params.ipv4 and params.ipv6:
        parser.error("-4 and -6 are mutually exclusive")


def _validate_remote_url(
    parser: argparse.ArgumentParser,
    value: str,
    schemes: frozenset[str],
) -> None:
    if not supports_url(value, schemes):
        parser.error(
            "the xrdfs backend requires a complete remote URL "
            f"(got {redact_authz(value)!r})"
        )


def _child_environment(params: argparse.Namespace) -> dict[str, str]:
    environment = os.environ.copy()

    if params.timeout > 0:
        environment["XRD_REQUESTTIMEOUT"] = str(params.timeout)
    else:
        environment.pop("XRD_REQUESTTIMEOUT", None)

    if params.cert:
        key = params.key or params.cert
        environment["X509_USER_CERT"] = params.cert
        environment["X509_USER_KEY"] = key
        environment.pop("X509_USER_PROXY", None)
        environment["XRD_HTTPCLIENTCERTFILE"] = params.cert
        environment["XRD_HTTPCLIENTKEYFILE"] = key
    elif params.key:
        environment["X509_USER_KEY"] = params.key
        environment["XRD_HTTPCLIENTKEYFILE"] = params.key

    if params.ipv4:
        environment["XRD_NETWORKSTACK"] = "IPv4"
    elif params.ipv6:
        environment["XRD_NETWORKSTACK"] = "IPv6"

    if params.client_info:
        environment["XRD_APPNAME"] = ";".join(params.client_info)

    if params.verbose:
        levels = ("Warning", "Info", "Debug")
        environment["XRD_LOGLEVEL"] = levels[min(params.verbose, 3) - 1]

    if params.log_file:
        environment["XRD_LOGFILE"] = params.log_file

    return environment


def _prepare_xrdfs(
    prog: str,
    deadline: Optional[float],
    configured_timeout: int,
) -> tuple[Optional[str], int]:
    executable = find_xrdfs()
    if executable is None:
        configured = os.environ.get("GFAL_XRDFS")
        detail = f" configured by GFAL_XRDFS={configured!r}" if configured else ""
        sys.stderr.write(
            f"{prog}: xrdfs{detail} was not found; install xrootd-client\n"
        )
        return None, 127

    probe_timeout = 5.0
    if deadline is not None:
        remaining = _remaining(deadline)
        assert remaining is not None
        probe_timeout = min(probe_timeout, remaining)
    capability = check_capability(
        executable,
        environ=os.environ,
        required_markers=capability_markers(),
        timeout=probe_timeout,
    )
    if capability.error:
        if capability.interrupted:
            sys.stderr.write("Caught keyboard interrupt. Canceling...")
            return None, errno.EINTR
        if capability.timed_out and deadline is not None:
            sys.stderr.write(f"Command timed out after {configured_timeout} seconds!\n")
            return None, GFAL_ETIMEDOUT
        sys.stderr.write(
            f"{prog}: incompatible xrdfs: {capability.error}; use an XRootD build "
            "containing the command-first compatibility changes\n"
        )
        return None, 69
    return executable, 0


def _remaining(deadline: Optional[float]) -> Optional[float]:
    if deadline is None:
        return None
    return max(0.0, deadline - time.monotonic())


def _write_bytes(stream: Any, data: bytes) -> bool:
    if not data:
        return True
    try:
        binary = getattr(stream, "buffer", None)
        if binary is not None:
            binary.write(data)
            binary.flush()
        else:
            stream.write(data.decode("utf-8", errors="replace"))
            stream.flush()
    except BrokenPipeError:
        _neutralize_broken_pipe(stream)
        return False
    return True


def _neutralize_broken_pipe(stream: Any) -> None:
    """Prevent CPython from retrying a failed buffered flush at shutdown."""
    try:
        descriptor = stream.fileno()
        devnull = os.open(os.devnull, os.O_WRONLY)
        try:
            os.dup2(devnull, descriptor)
        finally:
            os.close(devnull)
    except (AttributeError, OSError, ValueError):
        pass


def _report_result(
    prog: str,
    result: XrdfsResult,
    *,
    configured_timeout: int,
) -> int:
    if result.returncode == 0:
        safe_stderr = redact_authz(
            result.stderr.decode("utf-8", errors="replace")
        ).encode("utf-8")
        _write_bytes(sys.stderr, safe_stderr)
        return 0

    code = error_exit_code(result)
    if code == 255:
        return code
    if code == errno.EINTR:
        sys.stderr.write("Caught keyboard interrupt. Canceling...")
        return code
    if result.timed_out:
        sys.stderr.write(f"Command timed out after {configured_timeout} seconds!\n")
        return code

    message = error_message(result.stderr)
    description = error_description(code)
    sys.stderr.write(f"{prog} error: {code} ({description}) - {message}\n")
    return code


def _json_records(prog: str, result: XrdfsResult) -> tuple[list[dict[str, Any]], int]:
    records = []
    try:
        for line in result.stdout.decode("utf-8").splitlines():
            if not line.strip():
                continue
            record = json.loads(line)
            if not isinstance(record, dict):
                raise ValueError("JSON line is not an object")
            _validate_json_record(record)
            records.append(record)
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        code = getattr(errno, "EPROTO", 1)
        sys.stderr.write(
            f"{prog} error: {code} ({os.strerror(code)}) - "
            f"invalid JSON from xrdfs: {exc}\n"
        )
        return [], code
    return records, 0


def _json_document(prog: str, result: XrdfsResult) -> tuple[Any, int]:
    try:
        return json.loads(result.stdout.decode("utf-8")), 0
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        code = getattr(errno, "EPROTO", 1)
        sys.stderr.write(
            f"{prog} error: {code} ({os.strerror(code)}) - "
            f"invalid JSON from xrdfs: {exc}\n"
        )
        return None, code


def _tape_surls(prog: str, params: argparse.Namespace) -> tuple[list[str], int]:
    if params.surl:
        return [params.surl], 0

    try:
        values = [
            line.strip()
            for line in Path(params.from_file).read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
    except OSError as exc:
        code = exc.errno or 1
        sys.stderr.write(f"{prog} error: {code} ({error_description(code)}) - {exc}\n")
        return [], code

    if not values:
        sys.stderr.write(f"{prog}: no SURLs found in {params.from_file}\n")
        return [], 1
    for value in values:
        if not supports_url(value, _WEBDAV_SCHEMES):
            sys.stderr.write(
                f"{prog}: the xrdfs Tape backend requires complete HTTP/WebDAV "
                f"URLs (got {redact_authz(value)!r})\n"
            )
            return [], 2
    return values, 0


def _validate_json_record(record: Mapping[str, Any]) -> None:
    """Validate the stable metadata schema before formatting its values."""
    required = {
        "path",
        "type",
        "size",
        "mtime",
        "atime",
        "ctime",
        "flags",
        "flag_names",
        "extended",
        "mode",
        "permissions",
        "owner",
        "group",
        "checksum",
        "xattrs",
    }
    missing = sorted(required.difference(record))
    if missing:
        raise ValueError(f"metadata object is missing {', '.join(missing)}")
    if not isinstance(record["path"], str):
        raise ValueError("metadata path is not a string")
    if record["type"] not in ("file", "directory", "other"):
        raise ValueError("metadata type is invalid")
    for name in ("size", "mtime", "atime", "ctime", "flags"):
        if isinstance(record[name], bool) or not isinstance(record[name], int):
            raise ValueError(f"metadata {name} is not an integer")
    if not isinstance(record["extended"], bool):
        raise ValueError("metadata extended flag is not a boolean")
    for name in ("mode", "permissions", "owner", "group", "checksum"):
        if record[name] is not None and not isinstance(record[name], str):
            raise ValueError(f"metadata {name} is not a string or null")
    if not isinstance(record["flag_names"], list) or not all(
        isinstance(name, str) for name in record["flag_names"]
    ):
        raise ValueError("metadata flag_names is not a string array")
    if not isinstance(record["xattrs"], list) or not all(
        isinstance(item, dict)
        and isinstance(item.get("name"), str)
        and isinstance(item.get("value"), str)
        for item in record["xattrs"]
    ):
        raise ValueError("metadata xattrs is not a name/value array")


def _run_metadata(
    prog: str,
    executable: str,
    arguments: Sequence[str],
    params: argparse.Namespace,
    environment: Mapping[str, str],
    deadline: Optional[float],
) -> tuple[list[dict[str, Any]], int]:
    result = run_xrdfs(
        executable,
        arguments,
        environ=environment,
        timeout=_remaining(deadline),
    )
    status = _report_result(prog, result, configured_timeout=params.timeout)
    if status:
        return [], status
    return _json_records(prog, result)


def _record_type(record: Mapping[str, Any]) -> str:
    value = str(record.get("type", "other")).lower()
    return value if value in ("file", "directory", "other") else "other"


def _permissions_from_text(value: Any) -> Optional[int]:
    text = str(value or "")
    if len(text) == 10:
        text = text[1:]
    if len(text) != 9:
        return None

    permissions = 0
    bits = (
        stat.S_IRUSR,
        stat.S_IWUSR,
        stat.S_IXUSR,
        stat.S_IRGRP,
        stat.S_IWGRP,
        stat.S_IXGRP,
        stat.S_IROTH,
        stat.S_IWOTH,
        stat.S_IXOTH,
    )
    for character, bit in zip(text, bits):
        if character != "-":
            permissions |= bit
    return permissions


def _record_type_mode(record: Mapping[str, Any]) -> int:
    return {
        "file": stat.S_IFREG,
        "directory": stat.S_IFDIR,
        "other": 0,
    }[_record_type(record)]


def _flag_permissions(
    record: Mapping[str, Any],
    *,
    all_classes: bool,
    traverse_readable_directory: bool = False,
) -> int:
    names = {str(name) for name in record.get("flag_names", [])}
    readable = 0o444 if all_classes else stat.S_IRUSR
    writable = 0o222 if all_classes else stat.S_IWUSR
    executable = 0o111 if all_classes else stat.S_IXUSR
    permissions = 0
    for name, bits in (
        ("IsReadable", readable),
        ("IsWritable", writable),
        ("XBitSet", executable),
    ):
        if name in names:
            permissions |= bits
    if (
        traverse_readable_directory
        and _record_type(record) == "directory"
        and "IsReadable" in names
    ):
        permissions |= executable
    return permissions


def _record_mode(
    record: Mapping[str, Any], *, legacy_webdav_defaults: bool = False
) -> int:
    type_mode = _record_type_mode(record)

    raw_mode = record.get("mode")
    if raw_mode not in (None, ""):
        try:
            return type_mode | (int(str(raw_mode), 8) & 0o7777)
        except ValueError:
            pass

    parsed = _permissions_from_text(record.get("permissions"))
    if parsed is not None:
        return type_mode | parsed

    if legacy_webdav_defaults:
        return type_mode | 0o777

    return type_mode | _flag_permissions(
        record, all_classes=True, traverse_readable_directory=True
    )


def _legacy_root_mode(record: Mapping[str, Any], *, directory_entry: bool) -> int:
    return _record_type_mode(record) | _flag_permissions(
        record, all_classes=directory_entry
    )


def _numeric_identity(value: Any) -> int:
    try:
        return int(str(value))
    except (TypeError, ValueError):
        return 0


def _record_nlink(
    record: Mapping[str, Any], *, legacy_webdav_defaults: bool = False
) -> int:
    value = record.get("nlink")
    if value not in (None, ""):
        return _numeric_identity(value)
    return 0 if legacy_webdav_defaults else 1


def _legacy_metadata(
    value: str,
    record: Mapping[str, Any],
    *,
    directory_entry: bool,
) -> tuple[int, int, int, int]:
    try:
        scheme = urlsplit(value).scheme.lower()
    except ValueError:
        scheme = ""

    if scheme in _XROOTD_SCHEMES:
        mode = _legacy_root_mode(record, directory_entry=directory_entry)
        if directory_entry:
            return mode, 0, 0, 0
        return mode, 1, os.getuid(), os.getgid()

    legacy_webdav_defaults = _uses_legacy_webdav_defaults(value, record)
    return (
        _record_mode(record, legacy_webdav_defaults=legacy_webdav_defaults),
        _record_nlink(record, legacy_webdav_defaults=legacy_webdav_defaults),
        _numeric_identity(record.get("owner")),
        _numeric_identity(record.get("group")),
    )


def _epoch(
    record: Mapping[str, Any], name: str, *, fallback_to_mtime: bool = True
) -> float:
    value = record.get(name)
    if value in (None, 0, "0", "") and name != "mtime" and fallback_to_mtime:
        value = record.get("mtime", 0)
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _file_type_label(record_type: str) -> str:
    return {
        "file": "regular file",
        "directory": "directory",
        "other": "unknown",
    }[record_type]


def _execute_stat(
    prog: str,
    executable: str,
    params: argparse.Namespace,
    environment: Mapping[str, str],
    deadline: Optional[float],
) -> int:
    for index, value in enumerate(params.file):
        records, status = _run_metadata(
            prog,
            executable,
            ("stat", "--json", value),
            params,
            environment,
            deadline,
        )
        if status:
            return status
        if len(records) != 1:
            sys.stderr.write(f"{prog}: xrdfs stat returned {len(records)} records\n")
            return getattr(errno, "EPROTO", 1)

        if index:
            sys.stdout.write("\n")
        record = records[0]
        legacy_webdav_defaults = _uses_legacy_webdav_defaults(value, record)
        mode, _, uid, gid = _legacy_metadata(value, record, directory_entry=False)
        size = int(record["size"])

        sys.stdout.write(f"  File: '{redact_authz(value)}'\n")
        sys.stdout.write(f"  Size: {size}\t{_file_type_label(_record_type(record))}\n")
        sys.stdout.write(
            f"Access: ({stat.S_IMODE(mode):04o}/{stat.filemode(mode)})\t"
            f"Uid: {uid}\tGid: {gid}\t\n"
        )
        for label, field in (
            ("Access", "atime"),
            ("Modify", "mtime"),
            ("Change", "ctime"),
        ):
            fallback_to_mtime = not (legacy_webdav_defaults and field == "atime")
            formatted = datetime.fromtimestamp(
                _epoch(record, field, fallback_to_mtime=fallback_to_mtime)
            ).strftime("%Y-%m-%d %H:%M:%S.%f")
            sys.stdout.write(f"{label}: {formatted}\n")
    return 0


def _fmt_full_iso(timestamp: float) -> str:
    # gfal2-util formats local time but always appends this literal suffix.
    return datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M:%S.%f +0000")


def _fmt_long_iso(timestamp: float) -> str:
    return datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M")


def _fmt_iso(timestamp: float) -> str:
    value = datetime.fromtimestamp(timestamp)
    if (datetime.now() - value).days < 180:
        return value.strftime("%m-%d %H:%M")
    return value.strftime("%Y-%m-%d")


def _fmt_locale(timestamp: float) -> str:
    value = datetime.fromtimestamp(timestamp)
    day = value.strftime("%d").lstrip("0").rjust(2)
    if (datetime.now() - value).days < 180:
        return value.strftime(f"%b {day} %H:%M")
    return value.strftime(f"%b {day}  %Y")


_TIME_FORMATS = {
    "full-iso": _fmt_full_iso,
    "long-iso": _fmt_long_iso,
    "iso": _fmt_iso,
    "locale": _fmt_locale,
}


def _human_size(size: int) -> str:
    symbols = ("", "K", "M", "G", "T", "P")
    value = float(size)
    degree = 0
    while value >= 1024.0 and degree < len(symbols) - 1:
        value /= 1024.0
        degree += 1
    if value < 10.0:
        return f"{math.ceil(value * 10.0) / 10.0:0.1f}{symbols[degree]}"
    return f"{math.ceil(value):0.0f}{symbols[degree]}"


def _ls_colors() -> dict[str, str]:
    colors = {}
    for entry in os.environ.get("LS_COLORS", "").split(":"):
        if "=" in entry:
            kind, value = entry.split("=", 1)
            colors[kind] = value
    return colors


def _colored_name(name: str, mode: Optional[int], choice: str) -> str:
    enabled = choice == "always" or (choice == "auto" and sys.stdout.isatty())
    if not enabled:
        return name
    colors = _ls_colors()
    color = "037"
    if mode is None:
        color = colors.get("no", color)
    elif stat.S_ISDIR(mode):
        color = colors.get("di", color)
    elif stat.S_ISLNK(mode):
        color = colors.get("ln", color)
    elif mode & (stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH):
        color = colors.get("ex", color)
    return f"\033[{color}m{name}\033[0m"


def _normalized_url_path(value: str) -> str:
    path = urlsplit(value).path
    return posixpath.normpath("/" + path.lstrip("/"))


def _is_target_record(record: Mapping[str, Any], target: str) -> bool:
    path = str(record.get("path", "")).split("?", 1)[0]
    return posixpath.normpath("/" + path.lstrip("/")) == _normalized_url_path(target)


def _xattr_values(record: Mapping[str, Any]) -> list[str]:
    values = []
    for item in record.get("xattrs", []):
        if isinstance(item, dict):
            values.append(str(item.get("value", "")))
    return values


def _execute_ls(
    prog: str,
    executable: str,
    params: argparse.Namespace,
    environment: Mapping[str, str],
    deadline: Optional[float],
) -> int:
    # Despite its help text, current gfal2-util renders --full-time using the
    # long-iso layout. Preserve observed behavior for output compatibility.
    time_style = "long-iso" if params.full_time else params.time_style
    multiple = len(params.file) > 1
    for index, value in enumerate(params.file):
        arguments = ["ls", "--json"]
        if params.directory:
            arguments.append("--directory")
        if params.long:
            for attribute in params.xattr:
                arguments.extend(("--xattr", attribute))
        arguments.append(value)

        records, status = _run_metadata(
            prog,
            executable,
            arguments,
            params,
            environment,
            deadline,
        )
        if status:
            return status

        if multiple:
            if index:
                sys.stdout.write("\n")
            sys.stdout.write(f"{redact_authz(value)}:\n")

        for record in records:
            path = str(record["path"])
            target_record = params.directory or _is_target_record(record, value)
            try:
                entry_path = urlsplit(path).path
            except ValueError:
                entry_path = path.split("?", 1)[0]
            name = (
                redact_authz(value)
                if target_record
                else posixpath.basename(entry_path.rstrip("/"))
            )
            if not target_record and not params.all and name.startswith("."):
                continue

            mode, nlink, uid, gid = _legacy_metadata(
                value, record, directory_entry=not target_record
            )
            color_mode = mode if params.long else None
            display_name = _colored_name(name, color_mode, params.color)
            if not params.long:
                sys.stdout.write(f"{display_name}\n")
                continue

            size = int(record["size"])
            size_text = _human_size(size) if params.human_readable else str(size)
            size_width = 4 if params.human_readable else 9
            date = _TIME_FORMATS[time_style](_epoch(record, "mtime"))
            extras = "\t".join(_xattr_values(record))
            sys.stdout.write(
                f"{stat.filemode(mode)} {nlink:>3} {uid!s:<5} {gid!s:<5} "
                f"{size_text:>{size_width}} {date:<11} {display_name}\t{extras}\n"
            )
    return 0


def _execute_cat(
    prog: str,
    executable: str,
    params: argparse.Namespace,
    environment: Mapping[str, str],
    deadline: Optional[float],
) -> int:
    for value in params.file:
        arguments = ["cat"]
        if params.bytes:
            arguments.append("--bytes")
        arguments.append(value)
        result = run_xrdfs(
            executable,
            arguments,
            environ=environment,
            timeout=_remaining(deadline),
            passthrough_stdout=True,
        )
        status = _report_result(prog, result, configured_timeout=params.timeout)
        if status:
            return status
    return 0


def _execute_sum(
    prog: str,
    executable: str,
    params: argparse.Namespace,
    environment: Mapping[str, str],
    deadline: Optional[float],
) -> int:
    result = run_xrdfs(
        executable,
        ("sum", params.file, params.checksum_type),
        environ=environment,
        timeout=_remaining(deadline),
    )
    status = _report_result(prog, result, configured_timeout=params.timeout)
    if status:
        return status

    fields = result.stdout.decode("utf-8", errors="replace").split()
    if len(fields) != 2:
        sys.stderr.write(f"{prog}: invalid checksum response from xrdfs\n")
        return getattr(errno, "EPROTO", 1)
    display_url = redact_authz(params.file)
    if not _write_bytes(sys.stdout, f"{display_url} {fields[1]}\n".encode()):
        return 255
    return 0


def _execute_xattr(
    prog: str,
    executable: str,
    params: argparse.Namespace,
    environment: Mapping[str, str],
    deadline: Optional[float],
) -> int:
    attribute = params.attribute
    if attribute is None:
        arguments = ("xattr", params.file)
    elif "=" in attribute:
        key, value = attribute.split("=", 1)
        if not key or not value:
            return 0
        arguments = ("xattr", params.file, "set", attribute)
    else:
        arguments = ("xattr", params.file, "--", attribute)

    result = run_xrdfs(
        executable,
        arguments,
        environ=environment,
        timeout=_remaining(deadline),
    )
    status = _report_result(prog, result, configured_timeout=params.timeout)
    if status:
        return status
    if not _write_bytes(sys.stdout, result.stdout):
        return 255
    return 0


def _archivepoll_once(
    prog: str,
    executable: str,
    surls: Sequence[str],
    params: argparse.Namespace,
    environment: Mapping[str, str],
    deadline: Optional[float],
) -> tuple[int, int]:
    result = run_xrdfs(
        executable,
        ("query", "tape", "--json", "archiveinfo", *surls),
        environ=environment,
        timeout=_remaining(deadline),
    )
    status = _report_result(prog, result, configured_timeout=params.timeout)
    if status:
        return 0, status

    document, status = _json_document(prog, result)
    if status:
        return 0, status
    if not isinstance(document, list) or len(document) != len(surls):
        code = getattr(errno, "EPROTO", 1)
        sys.stderr.write(
            f"{prog} error: {code} ({os.strerror(code)}) - "
            "archiveinfo JSON does not match the requested SURLs\n"
        )
        return 0, code

    terminal = 0
    for surl, record in zip(surls, document):
        if not isinstance(record, dict):
            code = getattr(errno, "EPROTO", 1)
            sys.stderr.write(
                f"{prog} error: {code} ({os.strerror(code)}) - "
                "archiveinfo entry is not an object\n"
            )
            return 0, code

        error = record.get("error")
        locality = record.get("locality")
        display_surl = redact_authz(surl)
        if error:
            print(f"{display_surl} => FAILED: {redact_authz(str(error))}")
            terminal += 1
        elif locality in ("TAPE", "DISK_AND_TAPE"):
            print(f"{display_surl} READY")
            terminal += 1
        else:
            print(f"{display_surl} QUEUED")
    return terminal, 0


def _execute_archivepoll(
    prog: str,
    executable: str,
    params: argparse.Namespace,
    environment: Mapping[str, str],
    deadline: Optional[float],
) -> int:
    surls, status = _tape_surls(prog, params)
    if status:
        return status

    terminal, status = _archivepoll_once(
        prog, executable, surls, params, environment, deadline
    )
    if status:
        return status

    remaining = params.polling_timeout
    sleep_seconds = 1
    while terminal != len(surls) and remaining > 0:
        print(f"Archiving ongoing, sleep {sleep_seconds} seconds...")
        remaining -= sleep_seconds
        time.sleep(sleep_seconds)
        terminal, status = _archivepoll_once(
            prog, executable, surls, params, environment, deadline
        )
        if status:
            return status
        sleep_seconds = min(sleep_seconds * 2, 300)
    return 0


def _storage_endpoint(surls: Sequence[str]) -> Optional[str]:
    endpoints = {
        f"{parsed.scheme}://{parsed.netloc}"
        for parsed in (urlsplit(value) for value in surls)
    }
    if len(endpoints) != 1:
        return None
    return endpoints.pop()


def _stage_token(prog: str, result: XrdfsResult) -> tuple[str, int]:
    value = result.stdout.decode("utf-8", errors="replace").strip()
    if value.startswith("{"):
        try:
            document = json.loads(value)
            value = document.get("requestId", "")
        except (AttributeError, json.JSONDecodeError):
            value = ""
    if value and "\n" not in value:
        return value, 0

    code = getattr(errno, "EPROTO", 1)
    sys.stderr.write(
        f"{prog} error: {code} ({os.strerror(code)}) - "
        "stage response does not contain a request token\n"
    )
    return "", code


def _stage_status_by_path(
    prog: str, document: Any
) -> tuple[dict[str, Mapping[str, Any]], int]:
    if not isinstance(document, dict) or not isinstance(document.get("files"), list):
        code = getattr(errno, "EPROTO", 1)
        sys.stderr.write(
            f"{prog} error: {code} ({os.strerror(code)}) - "
            "stage status JSON does not contain a files array\n"
        )
        return {}, code

    records = {}
    for record in document["files"]:
        if not isinstance(record, dict) or not isinstance(record.get("path"), str):
            code = getattr(errno, "EPROTO", 1)
            sys.stderr.write(
                f"{prog} error: {code} ({os.strerror(code)}) - "
                "stage status contains an invalid file entry\n"
            )
            return {}, code
        records[record["path"]] = record
    return records, 0


def _bringonline_poll_once(
    prog: str,
    executable: str,
    endpoint: str,
    token: str,
    surls: Sequence[str],
    params: argparse.Namespace,
    environment: Mapping[str, str],
    deadline: Optional[float],
) -> tuple[int, int]:
    result = run_xrdfs(
        executable,
        (endpoint, "query", "prepare", token),
        environ=environment,
        timeout=_remaining(deadline),
    )
    status = _report_result(prog, result, configured_timeout=params.timeout)
    if status:
        return 0, status
    document, status = _json_document(prog, result)
    if status:
        return 0, status
    records, status = _stage_status_by_path(prog, document)
    if status:
        return 0, status

    terminal = 0
    for surl in surls:
        path = urlsplit(surl).path
        record = records.get(path)
        display_surl = redact_authz(surl)
        if record is None:
            print(f"{display_surl} => FAILED: missing status for requested file")
            terminal += 1
            continue

        state = str(record.get("state", "")).upper()
        error = record.get("error")
        if error or state in ("FAILED", "CANCELLED"):
            detail = str(error or state)
            print(f"{display_surl} => FAILED: {redact_authz(detail)}")
            terminal += 1
        elif record.get("onDisk") is True or state == "COMPLETED":
            print(f"{display_surl} READY")
            terminal += 1
        else:
            print(f"{display_surl} QUEUED")
    return terminal, 0


def _execute_bringonline(
    prog: str,
    executable: str,
    params: argparse.Namespace,
    environment: Mapping[str, str],
    deadline: Optional[float],
) -> int:
    surls, status = _tape_surls(prog, params)
    if status:
        return status
    endpoint = _storage_endpoint(surls)
    if endpoint is None:
        sys.stderr.write(
            f"{prog}: all SURLs must belong to the same storage endpoint\n"
        )
        return errno.EXDEV
    if params.desired_request_time is not None:
        sys.stderr.write(
            f"{prog}: warning: --desired-request-time has no WLCG Tape REST "
            "equivalent and is ignored\n"
        )

    arguments = ["prepare", "--stage"]
    if params.pin_lifetime:
        arguments.extend(("--pin-lifetime", str(params.pin_lifetime)))
    if params.staging_metadata:
        arguments.extend(("--metadata", params.staging_metadata))
    arguments.extend(surls)
    result = run_xrdfs(
        executable,
        arguments,
        environ=environment,
        timeout=_remaining(deadline),
    )
    status = _report_result(prog, result, configured_timeout=params.timeout)
    if status:
        return status
    token, status = _stage_token(prog, result)
    if status:
        return status

    print(f"Bringonline token: {token}")
    for surl in surls:
        print(f"{redact_authz(surl)} QUEUED")

    remaining = params.polling_timeout
    sleep_seconds = 1
    terminal = 0
    while terminal != len(surls) and remaining > 0:
        print(f"Request queued, sleep {sleep_seconds} seconds...")
        remaining -= sleep_seconds
        time.sleep(sleep_seconds)
        terminal, status = _bringonline_poll_once(
            prog,
            executable,
            endpoint,
            token,
            surls,
            params,
            environment,
            deadline,
        )
        if status:
            return status
        sleep_seconds = min(sleep_seconds * 2, 300)
    return 0


def _execute_evict(
    prog: str,
    executable: str,
    params: argparse.Namespace,
    environment: Mapping[str, str],
    deadline: Optional[float],
) -> int:
    arguments = ["prepare", "--evict"]
    if params.token:
        arguments.append(params.token)
    arguments.append(params.file)
    result = run_xrdfs(
        executable,
        arguments,
        environ=environment,
        timeout=_remaining(deadline),
    )
    return _report_result(prog, result, configured_timeout=params.timeout)


def _mkdir_mode(value: int) -> str:
    try:
        mode = int(str(value), 8)
    except ValueError:
        mode = 0o755
    return f"{mode & 0o777:04o}"


def _execute_mkdir(
    prog: str,
    executable: str,
    params: argparse.Namespace,
    environment: Mapping[str, str],
    deadline: Optional[float],
) -> int:
    options = ["--parents"] if params.parents else []
    options.append(f"--mode={_mkdir_mode(params.mode)}")
    for value in params.directory:
        result = run_xrdfs(
            executable,
            ("mkdir", *options, value),
            environ=environment,
            timeout=_remaining(deadline),
        )
        status = _report_result(prog, result, configured_timeout=params.timeout)
        if status:
            return status
    return 0


def _execute_chmod(
    prog: str,
    executable: str,
    params: argparse.Namespace,
    environment: Mapping[str, str],
    deadline: Optional[float],
) -> int:
    last_failure = 0
    for value in params.file:
        result = run_xrdfs(
            executable,
            ("chmod", params.mode, value),
            environ=environment,
            timeout=_remaining(deadline),
        )
        status = _report_result(prog, result, configured_timeout=params.timeout)
        if status in (errno.EINTR, GFAL_ETIMEDOUT):
            return status
        if status:
            last_failure = status
    return last_failure


def _execute_rename(
    prog: str,
    executable: str,
    params: argparse.Namespace,
    environment: Mapping[str, str],
    deadline: Optional[float],
) -> int:
    result = run_xrdfs(
        executable,
        ("mv", params.source, params.destination),
        environ=environment,
        timeout=_remaining(deadline),
    )
    return _report_result(prog, result, configured_timeout=params.timeout)


XRDFS_COMMANDS = MappingProxyType({
    "ls": XrdfsCommand(
        description="Gfal util LS command. List directory's contents.",
        add_arguments=_add_ls_arguments,
        execute=_execute_ls,
        legacy_options=frozenset(("--reverse", "--sort")),
        legacy_prefixes=("--sort=",),
        legacy_short_options=frozenset(("r", "S", "U")),
    ),
    "cat": XrdfsCommand(
        description="Gfal util CAT command. Sends to stdout the contents of files.",
        add_arguments=_add_cat_arguments,
        execute=_execute_cat,
    ),
    "stat": XrdfsCommand(
        description="Gfal util STAT command. Stats a file.",
        add_arguments=_add_stat_arguments,
        execute=_execute_stat,
        capability_markers=("stat [--json]",),
    ),
    "sum": XrdfsCommand(
        description="Gfal util SUM command. Calculates the checksum of a file.",
        add_arguments=_add_sum_arguments,
        execute=_execute_sum,
    ),
    "xattr": XrdfsCommand(
        description=(
            "Gfal util XATTR command. Gets or sets the extended attributes "
            "of files and directories."
        ),
        add_arguments=_add_xattr_arguments,
        execute=_execute_xattr,
        capability_markers=("xattr <path> [attribute]",),
    ),
    "archivepoll": XrdfsCommand(
        description="Gfal util ARCHIVEPOLL command. Execute archive polling.",
        add_arguments=_add_archivepoll_arguments,
        execute=_execute_archivepoll,
        validate=_validate_tape_source,
        capability_markers=("archiveinfo    <paths...>",),
        route_when=_route_tape_source,
        url_parameters=(),
        url_schemes=_WEBDAV_SCHEMES,
    ),
    "bringonline": XrdfsCommand(
        description="Gfal util BRINGONLINE command. Execute bring online.",
        add_arguments=_add_bringonline_arguments,
        execute=_execute_bringonline,
        validate=_validate_tape_source,
        capability_markers=("--pin-lifetime duration",),
        route_when=_route_tape_source,
        url_parameters=(),
        url_schemes=_WEBDAV_SCHEMES,
    ),
    "evict": XrdfsCommand(
        description="Gfal util EVICT command. Evict a file from a disk buffer.",
        add_arguments=_add_evict_arguments,
        execute=_execute_evict,
        url_schemes=_WEBDAV_SCHEMES,
    ),
    "mkdir": XrdfsCommand(
        description=(
            "Gfal util MKDIR command. Makes directories. By default, it sets "
            "file mode 0755."
        ),
        add_arguments=_add_mkdir_arguments,
        execute=_execute_mkdir,
        validate=_validate_mkdir,
        capability_markers=("mkdir [-p|--parents] [-m mode|--mode mode] <dirname>...",),
        route_when=_option_requires_schemes("parents", _XROOTD_SCHEMES),
        url_parameters=("directory",),
    ),
    "chmod": XrdfsCommand(
        description="Gfal util CHMOD command. Change the permissions of a file.",
        add_arguments=_add_chmod_arguments,
        execute=_execute_chmod,
        validate=_validate_chmod,
        capability_markers=("chmod <octal-mode> <path>",),
        url_schemes=_XROOTD_SCHEMES,
    ),
    "rename": XrdfsCommand(
        description="Gfal util RENAME command. Renames files or directories.",
        add_arguments=_add_rename_arguments,
        execute=_execute_rename,
        capability_markers=("mv <path1> <path2>",),
        url_parameters=("source", "destination"),
        url_schemes=_XROOTD_SCHEMES,
    ),
})


def dispatch(command: str, argv: Sequence[str], *, prog: Optional[str] = None) -> int:
    """Parse and execute one of the registered xrdfs-backed commands."""
    definition = XRDFS_COMMANDS[command]
    program = prog or f"gfal {command}"
    parser = _build_parser(command, program)
    params = parser.parse_args(list(argv))
    _validate_common(parser, params)
    if definition.validate is not None:
        definition.validate(parser, params)

    for value in _command_urls(definition, params):
        _validate_remote_url(parser, value, definition.url_schemes)

    deadline = None if params.timeout <= 0 else time.monotonic() + params.timeout
    try:
        executable, status = _prepare_xrdfs(
            program,
            deadline,
            params.timeout,
        )
        if status:
            return status
        assert executable is not None

        environment = _child_environment(params)
        status = definition.execute(
            program,
            executable,
            params,
            environment,
            deadline,
        )
        if status == 0:
            sys.stdout.flush()
        return status
    except BrokenPipeError:
        _neutralize_broken_pipe(sys.stdout)
        return 255
