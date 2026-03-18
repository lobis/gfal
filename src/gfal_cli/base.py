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

try:
    VERSION = _pkg_version("gfal-cli")
except PackageNotFoundError:
    VERSION = "0.0.0+unknown"


# ---------------------------------------------------------------------------
# @arg decorator (mirrors gfal2-util API)
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
        return Path(value).resolve().as_uri()
    return value


# ---------------------------------------------------------------------------
# CommandBase
# ---------------------------------------------------------------------------


class CommandBase:
    def __init__(self):
        self.return_code = -1
        self.progress_bar = None

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
            action="store_true",
            default=False,
            dest="ipv4_only",
            help="force IPv4 addresses only (GridFTP only; accepted for compatibility; ignored)",
        )
        self.parser.add_argument(
            "-6",
            action="store_true",
            default=False,
            dest="ipv6_only",
            help="force IPv6 addresses only (GridFTP only; accepted for compatibility; ignored)",
        )

        for args, kwargs in getattr(func, "arguments", []):
            self.parser.add_argument(*args, **kwargs)

        self.params = self.parser.parse_args(argv[1:])
        self.progr = Path(argv[0]).name

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
        # On Windows, FileNotFoundError etc. carry winerror codes and use
        # Windows-specific strerror text.  Normalise to POSIX descriptions so
        # error messages are consistent across platforms.
        winerror = getattr(e, "winerror", None)
        if winerror is not None:
            _win_map = {
                2: "No such file or directory",  # ERROR_FILE_NOT_FOUND
                3: "No such file or directory",  # ERROR_PATH_NOT_FOUND
                5: "Permission denied",  # ERROR_ACCESS_DENIED
                17: "File exists",  # ERROR_ALREADY_EXISTS
                183: "File exists",  # ERROR_ALREADY_EXISTS (alt)
            }
            desc = _win_map.get(winerror)
            if desc:
                filename = getattr(e, "filename", None)
                return f"{filename}: {desc}" if filename else desc
        strerror = getattr(e, "strerror", None)
        if strerror:
            # Real OS errors already include strerror in str(e); avoid doubling.
            if strerror not in msg:
                return f"{msg}: {strerror}"
            return msg
        # fsspec-style: no strerror, but the type carries the semantics.
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
                return f"{msg}: {description}"
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
        return msg

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
            sys.stderr.write(f"{self.progr}: {self._format_error(e)}\n")
            if ecode and 0 < ecode <= 255:
                self.return_code = ecode
            else:
                self.return_code = 1

    def execute(self, func):
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
