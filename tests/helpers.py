"""Shared test helpers for gfal-cli tests."""

import contextlib
import os
import shutil
import subprocess
import sys
import time
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


def _find_fusermount() -> Optional[str]:
    """Return the fusermount helper path, if available."""
    for candidate in (
        "fusermount3",
        "fusermount",
        "/bin/fusermount3",
        "/bin/fusermount",
    ):
        path = shutil.which(candidate) or (
            candidate if Path(candidate).is_file() else None
        )
        if path:
            return path
    return None


def _find_umount() -> Optional[str]:
    """Return the platform unmount helper path, if available."""
    for candidate in ("umount", "/sbin/umount", "/usr/sbin/umount"):
        path = shutil.which(candidate) or (
            candidate if Path(candidate).is_file() else None
        )
        if path:
            return path
    return None


def _find_diskutil() -> Optional[str]:
    """Return the macOS ``diskutil`` path, if available."""
    for candidate in ("diskutil", "/usr/sbin/diskutil"):
        path = shutil.which(candidate) or (
            candidate if Path(candidate).is_file() else None
        )
        if path:
            return path
    return None


def _find_mountpoint() -> Optional[str]:
    """Return the ``mountpoint`` helper path, if available."""
    for candidate in ("mountpoint", "/bin/mountpoint", "/usr/bin/mountpoint"):
        path = shutil.which(candidate) or (
            candidate if Path(candidate).is_file() else None
        )
        if path:
            return path
    return None


def _find_mount_cmd() -> Optional[str]:
    """Return the platform ``mount`` helper path, if available."""
    for candidate in ("mount", "/sbin/mount", "/usr/sbin/mount"):
        path = shutil.which(candidate) or (
            candidate if Path(candidate).is_file() else None
        )
        if path:
            return path
    return None


def fuse_available() -> bool:
    """Return True when the host appears capable of running FUSE mount tests."""
    if sys.platform == "linux":
        if _find_fusermount() is None:
            return False
        dev_fuse = Path("/dev/fuse")
        return dev_fuse.exists() and os.access(dev_fuse, os.R_OK | os.W_OK)

    if sys.platform == "darwin":
        return Path(
            "/Library/Filesystems/macfuse.fs/Contents/Resources/mount_macfuse"
        ).is_file()

    return False


def _mount_contains_path(mountpoint: Path) -> bool:
    """Return True when the OS mount table contains *mountpoint*."""
    mount_cmd = _find_mount_cmd()
    if mount_cmd is None:
        return False

    result = subprocess.run(
        [mount_cmd],
        capture_output=True,
        text=True,
        encoding="utf-8",
        timeout=5,
    )
    if result.returncode != 0:
        return False

    mountpoint_resolved = str(mountpoint.resolve())
    for line in result.stdout.splitlines():
        if (
            f" on {mountpoint_resolved} (" in line
            or f" on {mountpoint_resolved} type " in line
        ):
            return True
    return False


def _mount_ready(mountpoint: Path, initial_dev: Optional[int]) -> bool:
    """Return True when the mountpoint looks mounted."""
    mountpoint_cmd = _find_mountpoint()
    if mountpoint_cmd is not None:
        result = subprocess.run(
            [mountpoint_cmd, "-q", str(mountpoint)],
            capture_output=True,
            timeout=5,
        )
        if result.returncode == 0:
            return True

    if initial_dev is not None:
        with contextlib.suppress(OSError):
            if mountpoint.stat().st_dev != initial_dev:
                return True

    if sys.platform == "linux":
        mountinfo = Path("/proc/self/mountinfo")
        mountpoint_resolved = str(mountpoint.resolve())
        if mountinfo.exists():
            return any(
                len(parts) > 4 and parts[4] == mountpoint_resolved
                for parts in (
                    line.split()
                    for line in mountinfo.read_text(encoding="utf-8").splitlines()
                )
            )

    return _mount_contains_path(mountpoint)


def _unmount_gfal(mountpoint: Path) -> None:
    """Best-effort unmount helper for Linux and macOS test cleanup."""
    commands = []
    fusermount = _find_fusermount()
    if fusermount is not None:
        commands.append([fusermount, "-u", str(mountpoint)])
    umount = _find_umount()
    if umount is not None:
        commands.append([umount, str(mountpoint)])
    diskutil = _find_diskutil()
    if diskutil is not None:
        commands.append([diskutil, "unmount", "force", str(mountpoint)])

    for command in commands:
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            encoding="utf-8",
            timeout=10,
        )
        if result.returncode == 0:
            break


# Docker image pre-built with CERN CAs, XRootD client, python3-xrootd, and the
# gfal runtime dependencies plus the temporary fsspec-xrootd fork used for
# integration coverage. Used for XRootD integration tests that require proper
# GSI authentication (not available on macOS without /etc/grid-security).
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

    volume_args = [
        "-v",
        f"{_REPO_ROOT}:/repo:ro",
        # Mount the host /tmp so that tests using local temp files (e.g.
        # recursive copy with file:// URIs pointing to pytest tmp_path)
        # can access them inside the container.
        "-v",
        "/tmp:/tmp",
    ]
    env_args = []

    if proxy and Path(proxy).is_file():
        proxy_path = Path(proxy).resolve()
        if proxy_path.is_relative_to("/tmp"):
            # /tmp is already bind-mounted — the proxy is visible at its
            # original host path inside the container.
            env_args += ["-e", f"X509_USER_PROXY={proxy_path}"]
        else:
            # Proxy lives outside /tmp: add an explicit bind mount.
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
    cmd,
    *args,
    proxy_cert: Optional[str] = None,
    input: Optional[str] = None,
    timeout: int = 120,
):
    """Run ``gfal <cmd>`` inside the Docker xrootd-cern-test container.

    Mounts the repo source read-only and installs it before running the
    command.  The CERN CAs and XRootD client are already baked into the image.

    Returns ``(returncode, stdout, stderr)`` as strings.
    """
    # XRootD runtime dependencies are pre-installed in the image from pyproject.toml.
    # Copy /repo to a writable dir first — hatch-vcs needs to write _version.py,
    # but /repo is mounted read-only.  Use /var/tmp (not /tmp) because /tmp is
    # bind-mounted from the host and may be shared with pytest tmp_path dirs.
    script = (
        "cp -r /repo /var/tmp/gfal-src && "
        "python3 -m pip install -q --no-deps /var/tmp/gfal-src > /dev/null 2>&1 && "
        f"python3 -c \"import sys; sys.argv=['gfal', '{cmd}']+sys.argv[1:]; "
        'from gfal.cli.shell import main; main()"'
    )

    cmd_args = [str(a) for a in args]
    escaped = " ".join(f"'{a}'" for a in cmd_args)
    return _docker_run_command(
        f"{script} {escaped}", proxy_cert=proxy_cert, input=input, timeout=timeout
    )


def run_gfal2_docker(
    cmd, *args, proxy_cert: Optional[str] = None, input: Optional[str] = None
):
    """Run legacy ``gfal-<cmd>`` inside the Docker xrootd-cern-test container.

    Alma's distro ``python3-gfal2`` bindings are built for the image's system
    Python and crash under the image's default Python 3 runtime. Force the
    legacy launcher scripts to use ``/usr/bin/python3`` via ``GFAL_PYTHONBIN``
    while keeping the new gfal CLI on the image's default Python 3.
    """
    cmd_args = [str(a) for a in args]
    escaped = " ".join(f"'{a}'" for a in cmd_args)
    return _docker_run_command(
        f"GFAL_PYTHONBIN=/usr/bin/python3 gfal-{cmd} {escaped}",
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


@contextlib.contextmanager
def mounted_gfal(
    source,
    mountpoint,
    *,
    timeout: int = _DEFAULT_SUBPROCESS_TIMEOUT,
    env=None,
):
    """Run ``gfal mount`` in the background and unmount it on exit."""
    script = (
        "import sys; "
        "sys.argv=['gfal', 'mount'] + sys.argv[1:]; "
        "from gfal.cli.shell import main; "
        "main()"
    )
    subprocess_env = _subprocess_env()
    if env is not None:
        subprocess_env = {**subprocess_env, **env}

    mountpoint_path = Path(mountpoint)
    proc = subprocess.Popen(
        [sys.executable, "-c", script, str(source), str(mountpoint)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding="utf-8",
        env=subprocess_env,
    )

    deadline = time.monotonic() + timeout
    ready = False
    initial_dev = None
    with contextlib.suppress(OSError):
        initial_dev = mountpoint_path.stat().st_dev

    while time.monotonic() < deadline:
        if proc.poll() is not None:
            break
        try:
            ready = _mount_ready(mountpoint_path, initial_dev)
            if ready:
                break
        except (OSError, subprocess.TimeoutExpired):
            time.sleep(0.1)
            continue

    try:
        if not ready:
            stdout = ""
            stderr = ""
            try:
                stdout, stderr = proc.communicate(timeout=5)
            except subprocess.TimeoutExpired:
                proc.terminate()
                with contextlib.suppress(subprocess.TimeoutExpired):
                    stdout, stderr = proc.communicate(timeout=5)
                if proc.poll() is None:
                    proc.kill()
                    stdout, stderr = proc.communicate()
            raise RuntimeError(
                f"gfal mount did not become ready\nstdout:\n{stdout}\nstderr:\n{stderr}"
            )
        yield proc
    finally:
        _unmount_gfal(mountpoint_path.resolve())
        with contextlib.suppress(subprocess.TimeoutExpired):
            proc.wait(timeout=5)
        if proc.poll() is None:
            proc.terminate()
            with contextlib.suppress(subprocess.TimeoutExpired):
                proc.wait(timeout=5)
        if proc.poll() is None:
            proc.kill()
            with contextlib.suppress(subprocess.TimeoutExpired):
                proc.wait(timeout=5)
