"""Process boundary for the external :command:`xrdfs` client.

This module intentionally uses only the Python standard library.  It is the
small compatibility seam between the Python entry points and the XRootD client
package supplied by the operating system.
"""

from __future__ import annotations

import contextlib
import errno
import os
import re
import shutil
import signal
import subprocess
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

_CAPABILITY_MARKERS = (
    "command-first batch",
    "--json print one JSON object per entry",
    "stat [--json]",
    "xattr <path> [attribute]",
)

# gfal2-util is distributed on Linux and returns Linux errno values even when
# this pure-Python frontend is developed on a platform whose errno table uses a
# different number (macOS uses 60 for ETIMEDOUT, for example).
GFAL_ETIMEDOUT = 110


@dataclass(frozen=True)
class XrdfsResult:
    """Result of one ``xrdfs`` child process."""

    returncode: int
    stdout: bytes
    stderr: bytes
    timed_out: bool = False


@dataclass(frozen=True)
class XrdfsCapability:
    """Result of checking the local wrapper contract."""

    error: Optional[str] = None
    timed_out: bool = False
    interrupted: bool = False


def find_xrdfs(environ: Optional[Mapping[str, str]] = None) -> Optional[str]:
    """Return the configured executable path, or ``None`` when unavailable."""
    environment = os.environ if environ is None else environ
    override = environment.get("GFAL_XRDFS")
    search_path = environment.get("PATH")

    if override:
        candidate = Path(override).expanduser()
        if candidate.parent != Path() or candidate.is_absolute():
            if candidate.is_file() and os.access(candidate, os.X_OK):
                return str(candidate)
            return None
        return shutil.which(override, path=search_path)

    return shutil.which("xrdfs", path=search_path)


def run_xrdfs(
    executable: str,
    arguments: Sequence[str],
    *,
    environ: Mapping[str, str],
    timeout: Optional[float],
    passthrough_stdout: bool = False,
) -> XrdfsResult:
    """Run one ``xrdfs`` command without invoking a shell.

    When ``passthrough_stdout`` is true, the child inherits stdout.  This is
    used by ``gfal cat`` so arbitrary bytes never pass through a text decoder.
    Stderr is always captured so failures can be translated to errno-style
    exit statuses.
    """
    if timeout is not None and timeout <= 0:
        return XrdfsResult(
            returncode=GFAL_ETIMEDOUT,
            stdout=b"",
            stderr=b"",
            timed_out=True,
        )
    try:
        process = subprocess.Popen(
            [executable, *arguments],
            env=dict(environ),
            stdout=None if passthrough_stdout else subprocess.PIPE,
            stderr=subprocess.PIPE,
            start_new_session=os.name == "posix",
        )
    except OSError as exc:
        return XrdfsResult(
            returncode=errno.ENOENT if isinstance(exc, FileNotFoundError) else 1,
            stdout=b"",
            stderr=str(exc).encode("utf-8", errors="replace"),
        )

    try:
        stdout, stderr = process.communicate(timeout=timeout)
    except subprocess.TimeoutExpired:
        stdout, stderr = _stop_process(process)
        return XrdfsResult(
            returncode=GFAL_ETIMEDOUT,
            stdout=stdout or b"",
            stderr=stderr or b"",
            timed_out=True,
        )
    except KeyboardInterrupt:
        stdout, stderr = _stop_process(process)
        return XrdfsResult(
            returncode=errno.EINTR,
            stdout=stdout or b"",
            stderr=stderr or b"",
        )

    return XrdfsResult(
        returncode=process.returncode,
        stdout=stdout or b"",
        stderr=stderr or b"",
    )


def _signal_process(process: subprocess.Popen, sig: signal.Signals) -> None:
    """Signal *process* and its descendants when the platform supports it."""
    try:
        if os.name == "posix":
            os.killpg(process.pid, sig)
        elif sig == signal.SIGTERM:
            process.terminate()
        else:
            process.kill()
    except OSError:
        pass


def _stop_process(process: subprocess.Popen) -> tuple[bytes, bytes]:
    """Stop a timed-out process tree without an unbounded final wait."""
    _signal_process(process, signal.SIGTERM)
    try:
        stdout, stderr = process.communicate(timeout=2)
    except subprocess.TimeoutExpired as terminated:
        _signal_process(process, getattr(signal, "SIGKILL", signal.SIGTERM))
        try:
            stdout, stderr = process.communicate(timeout=2)
        except subprocess.TimeoutExpired as killed:
            # A deliberately detached descendant may still hold an inherited
            # pipe open. Close our readers rather than waiting forever for it.
            for stream in (process.stdout, process.stderr):
                if stream is not None:
                    stream.close()
            with contextlib.suppress(subprocess.TimeoutExpired):
                process.wait(timeout=1)
            stdout = killed.output or terminated.output or b""
            stderr = killed.stderr or terminated.stderr or b""
    return stdout or b"", stderr or b""


def check_capability(
    executable: str,
    *,
    environ: Mapping[str, str],
    timeout: Optional[float] = 5.0,
) -> XrdfsCapability:
    """Check whether *executable* provides the wrapper-facing interface."""
    result = run_xrdfs(
        executable,
        ("--help",),
        environ=environ,
        timeout=timeout,
    )
    if result.timed_out:
        return XrdfsCapability("xrdfs --help timed out", timed_out=True)
    if result.returncode == errno.EINTR:
        return XrdfsCapability("xrdfs --help interrupted", interrupted=True)
    if result.returncode != 0:
        detail = error_message(result.stderr)
        return XrdfsCapability(f"xrdfs --help failed: {detail}")

    help_text = (result.stdout + result.stderr).decode("utf-8", errors="replace")
    missing = [marker for marker in _CAPABILITY_MARKERS if marker not in help_text]
    if missing:
        return XrdfsCapability(
            "xrdfs lacks the required command-first JSON compatibility interface"
        )
    return XrdfsCapability()


_ERROR_CODES = (
    (
        errno.ENOENT,
        (
            "no such file or directory",
            "file or object not found",
            "resource not found",
        ),
    ),
    (errno.EACCES, ("permission denied", "not authorized", "forbidden")),
    (errno.EEXIST, ("already exists", "target exists", "file exists")),
    (errno.ENOTDIR, ("not a directory", "target is not a directory")),
    (errno.EISDIR, ("is a directory", "target is a directory")),
    (GFAL_ETIMEDOUT, ("timed out", "timeout", "operation expired")),
    (errno.ECONNREFUSED, ("connection refused",)),
    (errno.EHOSTUNREACH, ("host is down", "host unreachable", "no route to host")),
    (errno.ENOSPC, ("no space left", "insufficient storage")),
    (errno.ENODATA, ("attribute not found", "no data available")),
    (errno.EINVAL, ("invalid argument", "invalid arguments")),
)


def error_exit_code(result: XrdfsResult) -> int:
    """Translate a child failure into the closest gfal2/POSIX exit code."""
    if result.timed_out:
        return GFAL_ETIMEDOUT
    if result.returncode == 0:
        return 0
    if result.returncode == errno.EINTR:
        return errno.EINTR
    sigpipe = getattr(signal, "SIGPIPE", None)
    if sigpipe is not None and result.returncode in (-sigpipe, 128 + sigpipe):
        return 255

    message = result.stderr.decode("utf-8", errors="replace").lower()
    if "broken pipe" in message or "unable to write to stdout" in message:
        return 255
    for code, fragments in _ERROR_CODES:
        if any(fragment in message for fragment in fragments):
            return code
    return 1


def error_message(stderr: bytes) -> str:
    """Extract a concise diagnostic from native ``xrdfs`` stderr."""
    lines = [
        line.strip() for line in stderr.decode("utf-8", errors="replace").splitlines()
    ]
    lines = [line for line in lines if line]
    if not lines:
        return "xrdfs operation failed"
    message = lines[-1]
    return redact_authz(re.sub(r"^\[(?:ERROR|FATAL)\]\s*", "", message))


def redact_authz(message: object) -> str:
    """Return *message* with EOS ``authz`` query values redacted."""
    return re.sub(
        r"(?i)(?<![A-Za-z0-9_.-])(authz=)[^\s'\"),&#]*",
        r"\1<redacted>",
        str(message),
    )
