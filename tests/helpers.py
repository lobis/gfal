"""Shared test helpers for gfal-cli tests."""

import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Optional

_DEFAULT_SUBPROCESS_TIMEOUT = int(os.environ.get("GFAL_TEST_SUBPROCESS_TIMEOUT", "30"))


def _decode_timeout_stream(value, *, binary: bool):
    """Normalize TimeoutExpired stdout/stderr payloads."""
    if value is None:
        return b"" if binary else ""
    if binary:
        return value if isinstance(value, bytes) else str(value).encode("utf-8")
    return (
        value.decode("utf-8", errors="replace") if isinstance(value, bytes) else value
    )


def _timed_out_result(exc: subprocess.TimeoutExpired, *, binary: bool = False):
    """Return a synthetic subprocess result for hung test commands."""
    stdout = _decode_timeout_stream(getattr(exc, "stdout", None), binary=binary)
    stderr = _decode_timeout_stream(getattr(exc, "stderr", None), binary=binary)
    timeout_msg = (
        f"Test helper timed out after {exc.timeout} seconds: {' '.join(exc.cmd)}\n"
    )
    if binary:
        return 124, stdout, stderr + timeout_msg.encode("utf-8")
    return 124, stdout, stderr + timeout_msg


def _subprocess_env():
    """Build the environment dict for gfal-cli subprocesses.

    Called at invocation time (not module import time) so that any env vars
    set by pytest fixtures — in particular SSL_CERT_FILE / REQUESTS_CA_BUNDLE
    added by the CERN CA fixture in conftest.py — are picked up correctly.

    On macOS the pip-packaged xrootd embeds $ORIGIN RPATHs that dyld does not
    expand, causing the XRootD security plugins to fail to load unless the
    pyxrootd directory is on DYLD_LIBRARY_PATH.  shell.main() handles this via
    a re-exec for real binaries, but test subprocesses are invoked as
    ``python -c "..."`` which is not a real file on disk, so the re-exec guard
    fires and the env var is never set.  We set it here instead.
    """
    env = {**os.environ, "PYTHONUTF8": "1"}
    env.setdefault("GFAL_CLI_GFAL2", "1")

    if sys.platform == "darwin":
        try:
            import pyxrootd as _px  # noqa: PLC0415

            plugin_dir = str(Path(_px.__file__).parent)
            current = env.get("DYLD_LIBRARY_PATH", "")
            if plugin_dir not in current.split(":"):
                env["DYLD_LIBRARY_PATH"] = (
                    f"{plugin_dir}:{current}" if current else plugin_dir
                )
        except ImportError:
            pass  # xrootd not installed — nothing to do

    return env


def run_gfal(
    cmd, *args, input=None, env=None, timeout: int = _DEFAULT_SUBPROCESS_TIMEOUT
):
    """
    Run ``gfal <cmd>`` in a subprocess via the current Python interpreter.

    Returns ``(returncode, stdout, stderr)`` as strings.

    Args are passed as separate argv elements so paths with spaces are safe.
    ``input`` may be a str piped to stdin (useful for gfal save).
    ``env`` may be a dict of extra environment variables to set (or override)
    on top of the base subprocess environment.
    """
    script = (
        f"import sys; sys.argv=['gfal', '{cmd}']+sys.argv[1:];"
        "from gfal.cli.shell import main; main()"
    )
    subprocess_env = _subprocess_env()
    if env is not None:
        subprocess_env = {**subprocess_env, **env}
    try:
        proc = subprocess.run(
            [sys.executable, "-c", script, *[str(a) for a in args]],
            capture_output=True,
            text=True,
            encoding="utf-8",
            input=input,
            env=subprocess_env,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired as exc:
        return _timed_out_result(exc)
    return proc.returncode, proc.stdout, proc.stderr


def _find_docker() -> Optional[str]:
    """Return the path to the Docker binary, or None if not found."""
    for candidate in (
        "docker",
        "/usr/local/bin/docker",
        "/Applications/Docker.app/Contents/Resources/bin/docker",
    ):
        path = shutil.which(candidate) or (
            candidate if Path(candidate).is_file() else None
        )
        if path:
            return path
    return None


# Docker image pre-built with CERN CAs, XRootD client, python3-xrootd, and
# fsspec-xrootd installed.  Used for XRootD integration tests that require
# proper GSI authentication (not available on macOS without /etc/grid-security).
_DOCKER_IMAGE = "xrootd-cern-test"

# Repo root — mounted read-only into the container so gfal is importable.
_REPO_ROOT = str(Path(__file__).parent.parent)


def docker_available() -> bool:
    """Return True if Docker is installed and the test image exists."""
    docker = _find_docker()
    if not docker:
        return False
    try:
        result = subprocess.run(
            [docker, "image", "inspect", _DOCKER_IMAGE],
            capture_output=True,
            timeout=10,
        )
        return result.returncode == 0
    except (OSError, subprocess.TimeoutExpired):
        return False


def _docker_run_command(
    shell_script: str,
    *,
    proxy_cert: Optional[str] = None,
    input: Optional[str] = None,
    timeout: int = 120,
):
    """Run a shell script inside the Docker test image."""
    docker = _find_docker()
    if not docker:
        raise RuntimeError("Docker not found")

    proxy = proxy_cert or os.environ.get("X509_USER_PROXY", "")

    volume_args = ["-v", f"{_REPO_ROOT}:/repo:ro"]
    env_args = []

    if proxy and Path(proxy).is_file():
        volume_args += ["-v", f"{proxy}:/tmp/x509proxy:ro"]
        env_args += ["-e", "X509_USER_PROXY=/tmp/x509proxy"]

    proc = subprocess.run(
        [
            docker,
            "run",
            "--rm",
            *volume_args,
            *env_args,
            _DOCKER_IMAGE,
            "sh",
            "-c",
            shell_script,
        ],
        capture_output=True,
        text=True,
        encoding="utf-8",
        input=input,
        timeout=timeout,
    )
    return proc.returncode, proc.stdout, proc.stderr


def run_gfal_docker(
    cmd, *args, proxy_cert: Optional[str] = None, input: Optional[str] = None
):
    """Run ``gfal <cmd>`` inside the Docker xrootd-cern-test container.

    Mounts the repo source read-only and installs it before running the
    command.  The CERN CAs and XRootD client are already baked into the image.

    Returns ``(returncode, stdout, stderr)`` as strings.
    """
    # fsspec-xrootd (gfal3-updates branch) is pre-installed in the image.
    # Copy /repo to a writable tmp dir first — hatch-vcs needs to write _version.py,
    # but /repo is mounted read-only.
    script = (
        "cp -r /repo /tmp/gfal-src && "
        "python3.12 -m pip install -q --no-deps /tmp/gfal-src > /dev/null 2>&1 && "
        f"python3.12 -c \"import sys; sys.argv=['gfal', '{cmd}']+sys.argv[1:]; "
        'from gfal.cli.shell import main; main()"'
    )

    cmd_args = [str(a) for a in args]
    escaped = " ".join(f"'{a}'" for a in cmd_args)
    return _docker_run_command(
        f"{script} {escaped}", proxy_cert=proxy_cert, input=input
    )


def run_gfal2_docker(
    cmd, *args, proxy_cert: Optional[str] = None, input: Optional[str] = None
):
    """Run legacy ``gfal-<cmd>`` inside the Docker xrootd-cern-test container.

    Alma's distro ``python3-gfal2`` bindings are built for Python 3.9 and crash
    under the image's default Python 3.12. Force the legacy launcher scripts to
    use Python 3.9 via ``GFAL_PYTHONBIN`` while keeping the new gfal CLI on
    Python 3.12.
    """
    cmd_args = [str(a) for a in args]
    escaped = " ".join(f"'{a}'" for a in cmd_args)
    return _docker_run_command(
        f"GFAL_PYTHONBIN=/usr/bin/python3.9 gfal-{cmd} {escaped}",
        proxy_cert=proxy_cert,
        input=input,
    )


def run_gfal_binary(
    cmd, *args, input_bytes=None, timeout: int = _DEFAULT_SUBPROCESS_TIMEOUT
):
    """
    Like run_gfal but captures stdout as raw bytes (for cat/save binary tests).
    """
    script = (
        f"import sys; sys.argv=['gfal', '{cmd}']+sys.argv[1:];"
        "from gfal.cli.shell import main; main()"
    )
    try:
        proc = subprocess.run(
            [sys.executable, "-c", script, *[str(a) for a in args]],
            capture_output=True,
            input=input_bytes,
            env=_subprocess_env(),
            timeout=timeout,
        )
    except subprocess.TimeoutExpired as exc:
        return _timed_out_result(exc, binary=True)
    return proc.returncode, proc.stdout, proc.stderr
