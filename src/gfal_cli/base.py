"""
Base class and shared infrastructure for all gfal-cli commands.
"""

import argparse
import errno
import logging
import os
import signal
import sys
from pathlib import Path
from threading import Thread
from urllib.parse import urlparse, urlunparse

VERSION = "0.1.0"


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
    if not parsed.scheme:
        return urlunparse(("file", "", str(Path(value).resolve()), "", "", ""))
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

        for args, kwargs in getattr(func, "arguments", []):
            self.parser.add_argument(*args, **kwargs)

        self.params = self.parser.parse_args(argv[1:])
        self.progr = Path(argv[0]).name

    # ------------------------------------------------------------------
    # Execution
    # ------------------------------------------------------------------

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
            sys.stderr.write(f"{self.progr}: {e}\n")
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
