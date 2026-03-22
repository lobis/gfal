"""
Base class and shared infrastructure for all gfal-cli commands.
"""

import argparse
import errno
import logging
import os
import signal
import sys
from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as _pkg_version
from pathlib import Path
from threading import Thread
from urllib.parse import urlparse

from rich.console import Console

try:
    VERSION = _pkg_version("gfal-cli")
except PackageNotFoundError:
    VERSION = "0.0.0+unknown"


# ---------------------------------------------------------------------------
# @arg and @interactive decorators (mirrors gfal2-util API)
# ---------------------------------------------------------------------------


def arg(*args, **kwargs):
    """Decorator that attaches argparse argument specs to an execute_* method."""

    def _decorator(func):
        if not hasattr(func, "arguments"):
            func.arguments = []
        if (args, kwargs) not in func.arguments:
            func.arguments.insert(0, (args, kwargs))
        return func

    return _decorator


def interactive(func):
    """Decorator that marks an execute_* method as interactive (must run in main thread)."""
    func.is_interactive = True
    return func


# ---------------------------------------------------------------------------
# URL normalisation helper (used as argparse type=)
# ---------------------------------------------------------------------------


def surl(value):
    """
    Argparse type converter: turns bare paths into file:// URLs.
    Passes '-' (stdin/stdout sentinel) through unchanged.
    """
    if value == "-":
        return value
    parsed = urlparse(value)
    # A single-char scheme is a Windows drive letter (e.g. "C:"), not a real URL scheme
    if not parsed.scheme or len(parsed.scheme) == 1:
        p = Path(value)
        if not p.is_absolute():
            p = Path.cwd() / p
        return p.as_uri()
    return value


def is_gfal2_compat():
    """
    Check if we should be in strict gfal2 compatibility mode.
    This disables modern output/colors and reverts to legacy formatting.
    """
    return os.getenv("GFAL_CLI_GFAL2") in ("1", "true", "TRUE", "yes")


def get_console(stderr=False):
    """
    Returns a rich Console, but with formatting/color disabled
    if the environment requests gfal2 compatibility.
    """
    if is_gfal2_compat():
        # Disable markup, emoji, and colors
        return Console(
            force_terminal=False,
            color_system=None,
            highlight=False,
            markup=False,
            emoji=False,
            stderr=stderr,
        )
    return Console(stderr=stderr)


# ---------------------------------------------------------------------------
# CommandBase
# ---------------------------------------------------------------------------


class CommandBase:
    def __init__(self):
        self.return_code = -1
        self.progress_bar = None
        self.console = get_console()
        self.err_console = get_console(stderr=True)

    @staticmethod
    def get_subclasses():
        return CommandBase.__subclasses__()

    # ------------------------------------------------------------------
    # Logging setup
    # ------------------------------------------------------------------

    @staticmethod
    def _setup_logger(level, log_file):
        level = max(0, min(3, level))
        log_level = logging.ERROR - level * 10  # 0→ERROR, 1→WARN, 2→INFO, 3→DEBUG

        root = logging.getLogger()
        root.setLevel(log_level)
        handler = (
            logging.FileHandler(log_file, mode="w")
            if log_file
            else logging.StreamHandler(sys.stderr)
        )
        handler.setLevel(log_level)
        fmt = logging.Formatter("%(levelname)s %(name)s: %(message)s")
        handler.setFormatter(fmt)
        root.addHandler(handler)

    # ------------------------------------------------------------------
    # Argument parsing
    # ------------------------------------------------------------------

    def parse(self, func, argv):
        command = func.__name__[len("execute_") :]
        doc = (func.__doc__ or "").strip().split("\n")[0]
        description = f"gfal-cli {command.upper()} command. {doc}"
        if description[-1] != ".":
            description += "."

        self.parser = argparse.ArgumentParser(
            prog=Path(argv[0]).name,
            description=description,
        )
        self.parser.add_argument(
            "-V",
            "--version",
            action="version",
            version=f"gfal-cli {VERSION}",
        )
        self.parser.add_argument(
            "-v",
            "--verbose",
            action="count",
            default=0,
            help="enable verbose mode (-v warnings, -vv info, -vvv debug)",
        )
        self.parser.add_argument(
            "-t",
            "--timeout",
            type=int,
            default=1800,
            help="maximum seconds for the operation (default: 1800)",
        )
        self.parser.add_argument(
            "-E",
            "--cert",
            type=str,
            default=None,
            help="user certificate (X.509 PEM or proxy)",
        )
        self.parser.add_argument(
            "--key",
            type=str,
            default=None,
            help="user private key (defaults to --cert if omitted)",
        )
        self.parser.add_argument(
            "--log-file",
            type=str,
            default=None,
            help="write log output to this file instead of stderr",
        )
        self.parser.add_argument(
            "--no-verify",
            dest="ssl_verify",
            action="store_false",
            default=True,
            help="skip SSL certificate verification (insecure; for self-signed certs)",
        )
        # Flags accepted for backwards compatibility with gfal2-util but not used
        # in this fsspec-based implementation.
        self.parser.add_argument(
            "-D",
            "--definition",
            type=str,
            action="append",
            default=None,
            metavar="DEFINITION",
            dest="definition",
            help="override a gfal2 parameter (accepted for compatibility; ignored)",
        )
        self.parser.add_argument(
            "-C",
            "--client-info",
            type=str,
            default=None,
            metavar="CLIENT_INFO",
            dest="client_info",
            help="provide custom client-side information (accepted for compatibility; ignored)",
        )
        self.parser.add_argument(
            "-4",
            "--ipv4",
            action="store_true",
            default=False,
            dest="ipv4_only",
            help="force IPv4 addresses only",
        )
        self.parser.add_argument(
            "-6",
            "--ipv6",
            action="store_true",
            default=False,
            dest="ipv6_only",
            help="force IPv6 addresses only",
        )

        for args, kwargs in getattr(func, "arguments", []):
            self.parser.add_argument(*args, **kwargs)

        self.params = self.parser.parse_args(argv[1:])
        self.prog = Path(argv[0]).name

    # ------------------------------------------------------------------
    # Execution
    # ------------------------------------------------------------------

    @staticmethod
    def _format_error(e):
        """Return a user-friendly error string for an exception.

        fsspec raises FileNotFoundError (and friends) with only the URL as
        the message and no errno/strerror.  In that case Python's default
        str(e) looks identical to a normal program output line, so we append
        the POSIX description ourselves.
        """
        msg = str(e)
        if not msg:
            msg = e.__class__.__name__

        # Identify the relevant path/URL if possible.
        path = getattr(e, "filename", None)
        if not path and e.args:
            # Handle standard OSError(errno, strerror, filename)
            if len(e.args) >= 3 and e.args[2] and isinstance(e.args[2], (str, bytes)):
                path = e.args[2]
            # Handle fsspec FileNotFoundError("root://url")
            elif isinstance(e.args[0], (str, bytes)) and (
                "://" in str(e.args[0]) or "/" in str(e.args[0])
            ):
                path = e.args[0]

        if isinstance(path, bytes):
            path = path.decode("utf-8", "replace")

        # 1. Handle Windows-specific error codes
        winerror = getattr(e, "winerror", None)
        _win_map = {
            2: "No such file or directory",  # ERROR_FILE_NOT_FOUND
            3: "No such file or directory",  # ERROR_PATH_NOT_FOUND
            5: "Permission denied",  # ERROR_ACCESS_DENIED
            17: "File exists",  # ERROR_ALREADY_EXISTS
            183: "File exists",  # ERROR_ALREADY_EXISTS (alt)
        }
        desc = None
        if winerror is not None:
            desc = _win_map.get(winerror)
        elif "[WinError 2]" in msg:
            desc = _win_map[2]
        elif "[WinError 3]" in msg:
            desc = _win_map[3]
        elif "[WinError 5]" in msg:
            desc = _win_map[5]
        elif "[WinError 17]" in msg or "[WinError 183]" in msg:
            desc = _win_map[17]

        if desc:
            return f"{path}: {desc}" if path else desc

        # 2. Map common exception types to POSIX strings for consistency.
        _descriptions = {
            FileNotFoundError: "No such file or directory",
            PermissionError: "Permission denied",
            IsADirectoryError: "Is a directory",
            NotADirectoryError: "Not a directory",
            FileExistsError: "File exists",
            TimeoutError: "Operation timed out",
        }
        for exc_type, description in _descriptions.items():
            if isinstance(e, exc_type):
                return f"{path}: {description}" if path else description

        strerror = getattr(e, "strerror", None)
        if strerror:
            # Avoid doubling if path is already in the string.
            if strerror not in msg:
                return f"{msg}: {strerror}"
            return msg
        # SSL / connection errors from requests: give a clear hint.
        try:
            import requests as _requests

            if isinstance(e, _requests.exceptions.SSLError):
                cause = str(e)
                if "WRONG_VERSION_NUMBER" in cause or "UNKNOWN_PROTOCOL" in cause:
                    return f"{msg}: server does not speak HTTPS on this port (try http:// instead)"
                return f"{msg}: SSL certificate error (use --no-verify to skip, or install the server CA. See: https://lobis.github.io/gfal-cli/installation/#cern-ca-certificates)"
            if isinstance(e, _requests.exceptions.ConnectionError):
                cause = str(e.__cause__ or e).lower()
                if "ssl" in cause or "certificate" in cause:
                    return f"{msg}: SSL certificate error (use --no-verify to skip, or install the server CA. See: https://lobis.github.io/gfal-cli/installation/#cern-ca-certificates)"
                return msg
        except ImportError:
            pass

        # SSL / connection errors from aiohttp (used by fsspec)
        try:
            import aiohttp as _aiohttp

            if isinstance(e, _aiohttp.ClientConnectorSSLError):
                return f"{msg}: SSL certificate error (use --no-verify to skip, or install the server CA. See: https://lobis.github.io/gfal-cli/installation/#cern-ca-certificates)"
            if isinstance(e, _aiohttp.ClientConnectorError):
                cause = str(e.__cause__ or e).lower()
                if "ssl" in cause or "certificate" in cause:
                    return f"{msg}: SSL certificate error (use --no-verify to skip, or install the server CA. See: https://lobis.github.io/gfal-cli/installation/#cern-ca-certificates)"
        except ImportError:
            pass
        # HTTP errors from aiohttp: status attribute carries the HTTP code
        status = getattr(e, "status", None)
        if status is not None:
            _http_descriptions = {
                400: "Bad Request",
                401: "Unauthorized",
                403: "Permission denied",
                404: "No such file or directory",
                405: "Method not allowed",
                408: "Request timeout",
                409: "Conflict",
                410: "Gone",
                500: "Internal server error",
                503: "Service unavailable",
            }
            url = getattr(e, "request_info", None)
            url_str = str(url.url) if url is not None else ""
            description = _http_descriptions.get(status, f"HTTP {status}")
            if url_str:
                return f"{url_str}: {description}"
            return f"{description}"
        # CERN hint
        if "cern.ch" in msg.lower() and any(
            term in msg.lower()
            for term in ("timeout", "connection", "unreachable", "refused", "gai")
        ):
            return f"{msg}: Could not connect. Are you on the CERN VPN?"

        # Last resort: str(e) was empty (e.g. NotImplementedError()).  Show the
        # exception type so the user sees something actionable instead of a
        # blank "ERROR:" line.
        if not msg:
            return f"({type(e).__name__})"
        return msg

    def _print_error(self, e):
        """Prints a formatted error message to stderr, respecting compatibility mode."""
        msg = self._format_error(e)
        if is_gfal2_compat():
            sys.stderr.write(f"{self.prog}: {msg}\n")
        else:
            self.err_console.print(f"[bold red]{self.prog}[/]: {msg}")

    def _executor(self, func):
        """Runs func(self) inside the worker thread, captures exceptions."""
        try:
            self.return_code = func(self)
            if self.return_code is None:
                self.return_code = 0
        except Exception as e:
            # Broken pipe (e.g. piped to `head`) is not an error.
            if isinstance(e, OSError) and e.errno == errno.EPIPE:
                self.return_code = 0
                return
            ecode = getattr(e, "errno", None)
            self._print_error(e)
            if ecode and 0 < ecode <= 255:
                self.return_code = ecode
            else:
                self.return_code = 1

    def execute(self, func):
        # Forced IP family (IPv4/v6)
        # This affects the `requests` layer globally for the duration of the command.
        ipv4_only = getattr(self.params, "ipv4_only", False)
        ipv6_only = getattr(self.params, "ipv6_only", False)
        if ipv4_only or ipv6_only:
            import socket

            import urllib3.util.connection as nsock

            family = socket.AF_INET if ipv4_only else socket.AF_INET6
            nsock.allowed_gai_family = lambda: family

        # Apply cert/key to environment (XRootD reads X509_* env vars)
        if self.params.cert:
            key = self.params.key or self.params.cert
            os.environ["X509_USER_CERT"] = self.params.cert
            os.environ["X509_USER_KEY"] = key
            os.environ.pop("X509_USER_PROXY", None)
        elif not os.environ.get("X509_USER_PROXY") and hasattr(os, "getuid"):
            # Auto-detect proxy at the standard location used by voms-proxy-init
            # (Unix only — os.getuid() is not available on Windows)
            default_proxy = Path(f"/tmp/x509up_u{os.getuid()}")
            if default_proxy.exists():
                os.environ["X509_USER_PROXY"] = str(default_proxy)

        self._setup_logger(self.params.verbose, self.params.log_file)

        # Interactive commands must run in the main thread (e.g. TUI signal handling).
        # This bypasses the worker thread and timeout logic.
        if getattr(func, "is_interactive", False):
            self._executor(func)
            return self.return_code

        t = Thread(target=self._executor, args=[func], daemon=True)
        t.start()

        try:
            timeout = self.params.timeout if self.params.timeout > 0 else None
            # join in a loop so KeyboardInterrupt is catchable
            deadline = timeout  # seconds remaining
            while t.is_alive():
                t.join(min(3600, deadline) if deadline is not None else 3600)
                if deadline is not None:
                    deadline -= 3600
                    if deadline <= 0:
                        break

            if t.is_alive():
                if self.progress_bar is not None:
                    self.progress_bar.stop(False)
                sys.stderr.write(
                    f"Command timed out after {self.params.timeout} seconds\n"
                )
                return errno.ETIMEDOUT

            return self.return_code

        except KeyboardInterrupt:
            sys.stderr.write("\nInterrupted\n")
            signal.signal(signal.SIGINT, signal.SIG_IGN)
            return errno.EINTR
