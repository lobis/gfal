"""
Integration tests against eospilot.cern.ch (writable CERN EOS pilot instance).

These tests exercise real HTTP(S) endpoints with write access.  They are
marked ``integration`` and are **not** run by plain ``pytest tests/``; pass
``-m integration`` to include them.

Requirements
------------
- Network access to eospilot.cern.ch:443
- A valid X.509 proxy certificate (``X509_USER_PROXY`` env var, or auto-
  detected at ``/tmp/x509up_u<uid>``).
- ``--no-verify`` is used throughout because eospilot uses the CERN Root CA
  which is not trusted by default on most CI systems.

Paths
-----
- ``/eos/pilot/opstest/dteam/python3-gfal/tmp/``
    Writable by the service account (robot cert).  Files older than 24 h are
    cleaned up automatically; tests also clean up after themselves.
- ``/eos/pilot/opstest/dteam/python3-gfal/dteam-has-no-permissions-here/``
    Explicitly denied for the service account.  Used to test error handling.

Known stable public source file
---------------------------------
  https://eospublic.cern.ch//eos/opendata/phenix/emcal-finding-pi0s-and-photons/single_cluster_r5.C
  size    : 2184 bytes
  MD5     : 93f402e24c6f870470e1c5fcc5400e25
  ADLER32 : 335e754f
"""

import hashlib
import os
import socket
import sys
import uuid
from pathlib import Path

import pytest

from helpers import _docker_run_command, docker_available, run_gfal, run_gfal_docker

CI = os.environ.get("CI", "").lower() in {"1", "true", "yes"}

pytestmark = [pytest.mark.integration, pytest.mark.network]

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_PILOT_BASE = "https://eospilot.cern.ch//eos/pilot/opstest/dteam/python3-gfal/tmp"
_PILOT_NO_ACCESS = "https://eospilot.cern.ch//eos/pilot/opstest/dteam/python3-gfal/dteam-has-no-permissions-here"
_PUBSRC = (
    "https://eospublic.cern.ch//eos/opendata/phenix/"
    "emcal-finding-pi0s-and-photons/single_cluster_r5.C"
)
_PUBSRC_SIZE = 2184
_PUBSRC_MD5 = "93f402e24c6f870470e1c5fcc5400e25"
_PUBSRC_ADLER32 = "335e754f"

# ---------------------------------------------------------------------------
# Proxy detection
# ---------------------------------------------------------------------------


def _find_proxy():
    """Return path to X.509 proxy cert, or None if not found."""
    proxy = os.environ.get("X509_USER_PROXY", "")
    if proxy and Path(proxy).is_file():
        return proxy
    # Auto-detect the standard voms-proxy-init location
    try:
        uid = os.getuid()
    except AttributeError:
        # Windows — no getuid
        return None
    default = Path(f"/tmp/x509up_u{uid}")
    if default.is_file():
        return str(default)
    return None


# ---------------------------------------------------------------------------
# Skip markers
# ---------------------------------------------------------------------------


def _eospilot_reachable():
    try:
        with socket.create_connection(("eospilot.cern.ch", 443), timeout=5):
            return True
    except OSError:
        return False


requires_eospilot = pytest.mark.skipif(
    not _eospilot_reachable(),
    reason="eospilot.cern.ch:443 not reachable",
)

requires_proxy = pytest.mark.skipif(
    _find_proxy() is None,
    reason="No X.509 proxy found (set X509_USER_PROXY or run voms-proxy-init)",
)

requires_docker = pytest.mark.skipif(
    not docker_available(), reason="Docker image xrootd-cern-test not available"
)

requires_non_ci_for_flaky_pilot_writes = pytest.mark.skipif(
    CI,
    reason=(
        "Skipped in CI: eospilot write/identity paths are intermittently hanging; "
        "covered manually/outside CI until stabilized"
    ),
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def proxy_cert():
    """Return the path to the X.509 proxy certificate."""
    path = _find_proxy()
    if path is None:
        pytest.skip("No X.509 proxy found")
    return path


@pytest.fixture
def pilot_dir(proxy_cert):
    """Create a unique scratch directory on eospilot, yield URL, then clean up."""
    name = f"pytest-{uuid.uuid4().hex[:8]}"
    url = f"{_PILOT_BASE}/{name}"
    rc, out, err = run_gfal("mkdir", "-E", proxy_cert, "--no-verify", url)
    if rc != 0:
        pytest.skip(f"Could not create pilot_dir {url}: {err.strip()}")
    yield url
    # Cleanup — ignore errors
    run_gfal("rm", "-r", "-E", proxy_cert, "--no-verify", url)


# ---------------------------------------------------------------------------
# Convenience helper
# ---------------------------------------------------------------------------


def _run(cmd, proxy_cert, *args):
    """Call run_gfal with the proxy cert and --no-verify flags pre-filled."""
    return run_gfal(cmd, "-E", proxy_cert, "--no-verify", *args)


def _run_repo_gfal_docker_script(shell_script, proxy_cert):
    """Run a shell script in the Docker image after installing the current repo copy."""
    setup = (
        "cp -r /repo /tmp/gfal-src && "
        "python3.12 -m pip install -q --no-deps /tmp/gfal-src > /dev/null 2>&1 && "
    )
    return _docker_run_command(f"{setup}{shell_script}", proxy_cert=proxy_cert)


# ---------------------------------------------------------------------------
# TestEosPilotStreamingCopy
# ---------------------------------------------------------------------------


@requires_eospilot
@requires_proxy
class TestEosPilotStreamingCopy:
    def test_copy_small_file(self, proxy_cert, pilot_dir, tmp_path):
        """Download from eospublic and re-upload to pilot dir."""
        local = tmp_path / "src.C"
        rc, out, err = _run("cp", proxy_cert, "--no-verify", _PUBSRC, local.as_uri())
        assert rc == 0, err
        assert local.stat().st_size == _PUBSRC_SIZE

        dst = f"{pilot_dir}/copy_small.C"
        rc, out, err = _run("cp", proxy_cert, local.as_uri(), dst)
        assert rc == 0, err

    def test_copy_preserves_content(self, proxy_cert, pilot_dir, tmp_path):
        """Upload then download and verify bytes match."""
        data = b"eospilot content verify " * 50
        src = tmp_path / "verify_src.bin"
        src.write_bytes(data)

        dst = f"{pilot_dir}/verify.bin"
        rc, out, err = _run("cp", proxy_cert, src.as_uri(), dst)
        assert rc == 0, err

        downloaded = tmp_path / "verify_dst.bin"
        rc, out, err = _run("cp", proxy_cert, dst, downloaded.as_uri())
        assert rc == 0, err
        assert (
            hashlib.md5(downloaded.read_bytes()).hexdigest()
            == hashlib.md5(data).hexdigest()
        )

    def test_copy_with_checksum_md5(self, proxy_cert, pilot_dir, tmp_path):
        """Copy with -K MD5 checksum verification."""
        data = b"checksum test eospilot md5"
        src = tmp_path / "chksum_md5.bin"
        src.write_bytes(data)

        dst = f"{pilot_dir}/chksum_md5.bin"
        rc, out, err = _run("cp", proxy_cert, "-K", "MD5", src.as_uri(), dst)
        assert rc == 0, err

    def test_copy_with_checksum_adler32(self, proxy_cert, pilot_dir, tmp_path):
        """Copy with -K ADLER32 checksum verification."""
        data = b"checksum test eospilot adler32"
        src = tmp_path / "chksum_adler32.bin"
        src.write_bytes(data)

        dst = f"{pilot_dir}/chksum_adler32.bin"
        rc, out, err = _run("cp", proxy_cert, "-K", "ADLER32", src.as_uri(), dst)
        assert rc == 0, err

    def test_copy_empty_file(self, proxy_cert, pilot_dir, tmp_path):
        """Upload and download an empty file."""
        src = tmp_path / "empty.bin"
        src.write_bytes(b"")

        dst = f"{pilot_dir}/empty.bin"
        rc, out, err = _run("cp", proxy_cert, src.as_uri(), dst)
        assert rc == 0, err

        downloaded = tmp_path / "empty_dl.bin"
        rc, out, err = _run("cp", proxy_cert, dst, downloaded.as_uri())
        assert rc == 0, err
        assert downloaded.read_bytes() == b""

    def test_copy_large_file(self, proxy_cert, pilot_dir, tmp_path):
        """Upload a file larger than 4 MiB (crosses chunk boundary)."""
        data = b"X" * (5 * 1024 * 1024)
        src = tmp_path / "large.bin"
        src.write_bytes(data)

        dst = f"{pilot_dir}/large.bin"
        rc, out, err = _run("cp", proxy_cert, src.as_uri(), dst)
        assert rc == 0, err

        rc, out, err = _run("stat", proxy_cert, dst)
        assert rc == 0, err
        assert str(len(data)) in out

    def test_no_overwrite_without_force(self, proxy_cert, pilot_dir, tmp_path):
        """Copying to an existing destination without -f should fail."""
        src = tmp_path / "overwrite_src.bin"
        src.write_bytes(b"original")
        dst = f"{pilot_dir}/overwrite_test.bin"

        rc, out, err = _run("cp", proxy_cert, src.as_uri(), dst)
        assert rc == 0, err

        # Second copy to same destination without -f — should fail
        rc, out, err = _run("cp", proxy_cert, src.as_uri(), dst)
        assert rc != 0

    def test_force_overwrite(self, proxy_cert, pilot_dir, tmp_path):
        """Copying to an existing destination with -f should succeed."""
        src = tmp_path / "force_src.bin"
        src.write_bytes(b"version1")
        dst = f"{pilot_dir}/force_test.bin"

        rc, out, err = _run("cp", proxy_cert, src.as_uri(), dst)
        assert rc == 0, err

        src.write_bytes(b"version2")
        rc, out, err = _run("cp", proxy_cert, "-f", src.as_uri(), dst)
        assert rc == 0, err

        # Download and verify v2 is there
        dl = tmp_path / "force_dl.bin"
        rc, out, err = _run("cp", proxy_cert, dst, dl.as_uri())
        assert rc == 0, err
        assert dl.read_bytes() == b"version2"

    def test_copy_missing_source_fails(self, proxy_cert, pilot_dir):
        """Copying a non-existent source should fail with non-zero exit."""
        missing_src = f"{_PILOT_BASE}/this_does_not_exist_gfal_test_src.bin"
        dst = f"{pilot_dir}/should_not_exist.bin"
        rc, out, err = _run("cp", proxy_cert, missing_src, dst)
        assert rc != 0

    def test_copy_permission_denied(self, proxy_cert, tmp_path):
        """Copying to a directory without permissions should fail (403)."""
        src = tmp_path / "denied.bin"
        src.write_bytes(b"denied")
        dst = f"{_PILOT_NO_ACCESS}/denied.bin"
        rc, out, err = _run("cp", proxy_cert, src.as_uri(), dst)
        assert rc != 0
        assert "Permission denied" in err or "403" in err

    def test_copy_multiple_files(self, proxy_cert, pilot_dir, tmp_path):
        """Upload several files; all should succeed independently."""
        for i in range(3):
            src = tmp_path / f"multi_{i}.bin"
            src.write_bytes(f"file {i} content".encode())
            dst = f"{pilot_dir}/multi_{i}.bin"
            rc, out, err = _run("cp", proxy_cert, src.as_uri(), dst)
            assert rc == 0, f"file {i} failed: {err}"

    def test_copy_preserves_binary_content(self, proxy_cert, pilot_dir, tmp_path):
        """Binary data including null bytes should survive the round-trip."""
        data = bytes(range(256)) * 100
        src = tmp_path / "binary.bin"
        src.write_bytes(data)

        dst = f"{pilot_dir}/binary.bin"
        rc, out, err = _run("cp", proxy_cert, src.as_uri(), dst)
        assert rc == 0, err

        dl = tmp_path / "binary_dl.bin"
        rc, out, err = _run("cp", proxy_cert, dst, dl.as_uri())
        assert rc == 0, err
        assert dl.read_bytes() == data


@requires_eospilot
@requires_proxy
@requires_docker
class TestEosPilotStreamingCopyDocker:
    def test_preserve_times_warns_and_does_not_preserve_remote_https_mtime(
        self, proxy_cert
    ):
        remote = f"{_PILOT_BASE}/pytest-preserve-times-{uuid.uuid4().hex[:8]}.txt"
        script = f"""
set -e
printf 'preserve times\\n' >/tmp/src.txt
touch -t 200001010101 /tmp/src.txt
gfal cp -t 20 --preserve-times file:///tmp/src.txt '{remote}' >/tmp/cp.out 2>/tmp/cp.err
gfal stat -t 20 '{remote}' >/tmp/stat.out 2>/tmp/stat.err
gfal rm -t 20 '{remote}' >/tmp/rm.out 2>/tmp/rm.err || true
cat /tmp/cp.out
cat /tmp/stat.out
cat /tmp/cp.err >&2
cat /tmp/stat.err >&2
cat /tmp/rm.err >&2
"""
        rc, out, err = _run_repo_gfal_docker_script(script, proxy_cert)

        assert rc == 0, err
        assert "--preserve-times is only supported for local destinations" in err
        assert "2000-01-01" not in out

    def test_skip_if_same_skips_matching_remote_and_fails_on_mismatch(self, proxy_cert):
        remote = f"{_PILOT_BASE}/pytest-skip-if-same-{uuid.uuid4().hex[:8]}.txt"
        script = f"""
set -e
printf 'same-content\\n' >/tmp/src.txt
gfal cp -t 20 file:///tmp/src.txt '{remote}'
gfal cp -t 20 --skip-if-same file:///tmp/src.txt '{remote}' >/tmp/match.out 2>/tmp/match.err
printf 'different-content\\n' >/tmp/src2.txt
set +e
gfal cp -t 20 --skip-if-same file:///tmp/src2.txt '{remote}' >/tmp/mismatch.out 2>/tmp/mismatch.err
mismatch_rc=$?
set -e
gfal cat '{remote}' >/tmp/final.out 2>/tmp/final.err
gfal rm -t 20 '{remote}' >/tmp/rm.out 2>/tmp/rm.err || true
echo "MISMATCH_RC=$mismatch_rc"
cat /tmp/match.out
cat /tmp/mismatch.out
cat /tmp/final.out
cat /tmp/match.err >&2
cat /tmp/mismatch.err >&2
cat /tmp/final.err >&2
cat /tmp/rm.err >&2
"""
        rc, out, err = _run_repo_gfal_docker_script(script, proxy_cert)

        assert rc == 0, err
        assert "matching ADLER32 checksum" in out
        assert "MISMATCH_RC=1" in out
        assert "exists and overwrite is not set" in err
        assert "same-content" in out


# ---------------------------------------------------------------------------
# TestEosPilotStat
# ---------------------------------------------------------------------------


@requires_eospilot
@requires_proxy
class TestEosPilotStat:
    def test_stat_file(self, proxy_cert, pilot_dir, tmp_path):
        """stat on an uploaded file should show size and 'regular file'."""
        data = b"stat me " * 10
        src = tmp_path / "stat_me.bin"
        src.write_bytes(data)

        dst = f"{pilot_dir}/stat_me.bin"
        rc, out, err = _run("cp", proxy_cert, src.as_uri(), dst)
        assert rc == 0, err

        rc, out, err = _run("stat", proxy_cert, dst)
        assert rc == 0, err
        assert str(len(data)) in out
        assert "regular file" in out

    def test_stat_directory(self, proxy_cert, pilot_dir):
        """stat on the scratch directory should succeed and show 'directory'."""
        rc, out, err = _run("stat", proxy_cert, pilot_dir)
        assert rc == 0, err
        assert "File:" in out
        assert "directory" in out

    def test_stat_nonexistent_fails(self, proxy_cert, pilot_dir):
        """stat on a missing path should exit non-zero."""
        missing = f"{pilot_dir}/no_such_file_gfal_test.bin"
        rc, out, err = _run("stat", proxy_cert, missing)
        assert rc != 0

    def test_stat_shows_mtime(self, proxy_cert, pilot_dir, tmp_path):
        """stat output should contain a modification time."""
        src = tmp_path / "mtime.bin"
        src.write_bytes(b"mtime test")
        dst = f"{pilot_dir}/mtime.bin"
        rc, out, err = _run("cp", proxy_cert, src.as_uri(), dst)
        assert rc == 0, err

        rc, out, err = _run("stat", proxy_cert, dst)
        assert rc == 0, err
        # Stat output contains "Modify:" line
        assert "Modify" in out or "Modification" in out or "2026" in out

    def test_stat_no_access_dir(self, proxy_cert):
        """stat on the no-access directory should not crash (visible but unwritable)."""
        rc, out, err = _run("stat", proxy_cert, _PILOT_NO_ACCESS)
        # The directory exists; EOS may allow stat even if writes are denied
        # What matters is we get a clean exit or a clean error, not a traceback
        assert "Traceback" not in err


# ---------------------------------------------------------------------------
# TestEosPilotLs
# ---------------------------------------------------------------------------


@requires_eospilot
@requires_proxy
class TestEosPilotLs:
    def test_ls_empty_directory(self, proxy_cert, pilot_dir):
        """ls on a freshly-created empty directory should return no output."""
        rc, out, err = _run("ls", proxy_cert, pilot_dir)
        assert rc == 0, err
        assert out.strip() == ""

    def test_ls_shows_uploaded_file(self, proxy_cert, pilot_dir, tmp_path):
        """A file uploaded to the pilot dir should appear in ls output."""
        src = tmp_path / "ls_test.bin"
        src.write_bytes(b"list me")
        dst = f"{pilot_dir}/ls_test.bin"
        rc, out, err = _run("cp", proxy_cert, src.as_uri(), dst)
        assert rc == 0, err

        rc, out, err = _run("ls", proxy_cert, pilot_dir)
        assert rc == 0, err
        assert "ls_test.bin" in out

    def test_ls_shows_multiple_files(self, proxy_cert, pilot_dir, tmp_path):
        """All uploaded files should appear in ls output."""
        names = ["alpha.bin", "beta.bin", "gamma.bin"]
        for name in names:
            src = tmp_path / name
            src.write_bytes(b"data")
            _run("cp", proxy_cert, src.as_uri(), f"{pilot_dir}/{name}")

        rc, out, err = _run("ls", proxy_cert, pilot_dir)
        assert rc == 0, err
        for name in names:
            assert name in out

    def test_ls_long_format(self, proxy_cert, pilot_dir, tmp_path):
        """ls -l should succeed and show the filename and size."""
        data = b"long format test"
        src = tmp_path / "ls_long.bin"
        src.write_bytes(data)
        dst = f"{pilot_dir}/ls_long.bin"
        rc, out, err = _run("cp", proxy_cert, src.as_uri(), dst)
        assert rc == 0, err

        rc, out, err = _run("ls", proxy_cert, "-l", pilot_dir)
        assert rc == 0, err
        assert "ls_long.bin" in out
        assert str(len(data)) in out

    def test_ls_directory_flag(self, proxy_cert, pilot_dir):
        """ls -d should show the directory itself as a single entry."""
        rc, out, err = _run("ls", proxy_cert, "-d", pilot_dir)
        assert rc == 0, err
        lines = [ln for ln in out.splitlines() if ln.strip()]
        assert len(lines) >= 1

    def test_ls_shows_subdirectory(self, proxy_cert, pilot_dir):
        """A subdirectory should appear in ls output."""
        subdir = f"{pilot_dir}/subdir_ls_test"
        rc, out, err = _run("mkdir", proxy_cert, subdir)
        assert rc == 0, err

        rc, out, err = _run("ls", proxy_cert, pilot_dir)
        assert rc == 0, err
        assert "subdir_ls_test" in out

    def test_ls_no_access_dir_is_visible(self, proxy_cert):
        """The no-access directory itself is stat-able but has no listable children."""
        rc, out, err = _run("ls", proxy_cert, _PILOT_NO_ACCESS)
        # EOS allows PROPFIND Depth:0 (the directory is visible) but returns no
        # children for a directory the service account cannot read.  Either an
        # empty listing (rc=0, out empty) or just the directory name is valid;
        # what must NOT happen is a successful listing that exposes file contents.
        if rc == 0:
            lines = [ln for ln in out.splitlines() if ln.strip()]
            # At most one entry and it must be the directory itself, not a child
            assert len(lines) <= 1

    def test_ls_nonexistent_fails(self, proxy_cert, pilot_dir):
        """ls on a non-existent path should exit non-zero."""
        missing = f"{pilot_dir}/does_not_exist_ls/"
        rc, out, err = _run("ls", proxy_cert, missing)
        assert rc != 0


# ---------------------------------------------------------------------------
# TestEosPilotMkdirRm
# ---------------------------------------------------------------------------


@requires_eospilot
@requires_proxy
@requires_non_ci_for_flaky_pilot_writes
class TestEosPilotMkdirRm:
    def test_mkdir(self, proxy_cert, pilot_dir):
        """Create a subdirectory and verify it exists with stat."""
        subdir = f"{pilot_dir}/subdir_mkdir_test"
        rc, out, err = _run("mkdir", proxy_cert, subdir)
        assert rc == 0, err

        rc, out, err = _run("stat", proxy_cert, subdir)
        assert rc == 0, err

    def test_mkdir_parents(self, proxy_cert, pilot_dir):
        """mkdir -p should create nested directories."""
        deep = f"{pilot_dir}/deep/nested"
        rc, out, err = _run("mkdir", proxy_cert, "-p", deep)
        assert rc == 0, err

        rc, out, err = _run("stat", proxy_cert, deep)
        assert rc == 0, err

    def test_mkdir_existing_fails_without_p(self, proxy_cert, pilot_dir):
        """mkdir on an already-existing directory without -p should fail."""
        subdir = f"{pilot_dir}/dup_mkdir"
        rc, out, err = _run("mkdir", proxy_cert, subdir)
        assert rc == 0, err

        rc, out, err = _run("mkdir", proxy_cert, subdir)
        assert rc != 0

    def test_mkdir_existing_ok_with_p(self, proxy_cert, pilot_dir):
        """mkdir -p on an already-existing directory should succeed silently."""
        subdir = f"{pilot_dir}/dup_mkdir_p"
        rc, out, err = _run("mkdir", proxy_cert, subdir)
        assert rc == 0, err

        rc, out, err = _run("mkdir", proxy_cert, "-p", subdir)
        assert rc == 0, err

    def test_rm_file(self, proxy_cert, pilot_dir, tmp_path):
        """Upload a file, delete it, verify it is gone."""
        src = tmp_path / "rm_me.bin"
        src.write_bytes(b"delete me")
        dst = f"{pilot_dir}/rm_me.bin"
        rc, out, err = _run("cp", proxy_cert, src.as_uri(), dst)
        assert rc == 0, err

        rc, out, err = _run("rm", proxy_cert, dst)
        assert rc == 0, err

        rc, out, err = _run("stat", proxy_cert, dst)
        assert rc != 0

    def test_rm_directory(self, proxy_cert, pilot_dir):
        """Create a directory, rm it, verify it is gone."""
        subdir = f"{pilot_dir}/rm_dir_test"
        rc, out, err = _run("mkdir", proxy_cert, subdir)
        assert rc == 0, err

        rc, out, err = _run("rm", proxy_cert, "-r", subdir)
        assert rc == 0, err

        rc, out, err = _run("stat", proxy_cert, subdir)
        assert rc != 0

    def test_rm_recursive(self, proxy_cert, pilot_dir, tmp_path):
        """rm -r on a directory with files should remove everything."""
        subdir = f"{pilot_dir}/rm_recursive"
        rc, out, err = _run("mkdir", proxy_cert, subdir)
        assert rc == 0, err

        for i in range(3):
            src = tmp_path / f"f{i}.bin"
            src.write_bytes(b"data")
            _run("cp", proxy_cert, src.as_uri(), f"{subdir}/f{i}.bin")

        rc, out, err = _run("rm", proxy_cert, "-r", subdir)
        assert rc == 0, err

        rc, out, err = _run("stat", proxy_cert, subdir)
        assert rc != 0

    def test_rm_nonexistent_fails(self, proxy_cert, pilot_dir):
        """Deleting a non-existent file should exit non-zero."""
        missing = f"{pilot_dir}/no_such_file_rm_test.bin"
        rc, out, err = _run("rm", proxy_cert, missing)
        assert rc != 0

    def test_mkdir_permission_denied(self, proxy_cert):
        """Creating a directory in a no-access location should fail."""
        subdir = f"{_PILOT_NO_ACCESS}/subdir_denied"
        rc, out, err = _run("mkdir", proxy_cert, subdir)
        assert rc != 0
        assert "Permission denied" in err or "403" in err


# ---------------------------------------------------------------------------
# TestEosPilotRename
# ---------------------------------------------------------------------------


@requires_eospilot
@requires_proxy
class TestEosPilotRename:
    def test_rename_file(self, proxy_cert, pilot_dir, tmp_path):
        """Upload a file, rename it, verify src is gone and dst exists."""
        src = tmp_path / "rename_src.bin"
        src.write_bytes(b"rename me")
        src_url = f"{pilot_dir}/rename_src.bin"
        dst_url = f"{pilot_dir}/rename_dst.bin"

        rc, out, err = _run("cp", proxy_cert, src.as_uri(), src_url)
        assert rc == 0, err

        rc, out, err = _run("rename", proxy_cert, src_url, dst_url)
        assert rc == 0, err

        # Source should be gone
        rc, out, err = _run("stat", proxy_cert, src_url)
        assert rc != 0

        # Destination should exist
        rc, out, err = _run("stat", proxy_cert, dst_url)
        assert rc == 0, err

    def test_rename_preserves_content(self, proxy_cert, pilot_dir, tmp_path):
        """Renamed file should have identical content to the original."""
        data = b"content to preserve through rename"
        src = tmp_path / "ren_content_src.bin"
        src.write_bytes(data)
        src_url = f"{pilot_dir}/ren_content_src.bin"
        dst_url = f"{pilot_dir}/ren_content_dst.bin"

        rc, out, err = _run("cp", proxy_cert, src.as_uri(), src_url)
        assert rc == 0, err

        rc, out, err = _run("rename", proxy_cert, src_url, dst_url)
        assert rc == 0, err

        dl = tmp_path / "ren_dl.bin"
        rc, out, err = _run("cp", proxy_cert, dst_url, dl.as_uri())
        assert rc == 0, err
        assert dl.read_bytes() == data

    def test_rename_directory(self, proxy_cert, pilot_dir):
        """Renaming a directory should work."""
        src_url = f"{pilot_dir}/ren_dir_src"
        dst_url = f"{pilot_dir}/ren_dir_dst"

        rc, out, err = _run("mkdir", proxy_cert, src_url)
        assert rc == 0, err

        rc, out, err = _run("rename", proxy_cert, src_url, dst_url)
        assert rc == 0, err

        rc, out, err = _run("stat", proxy_cert, src_url)
        assert rc != 0

        rc, out, err = _run("stat", proxy_cert, dst_url)
        assert rc == 0, err

    def test_rename_nonexistent_fails(self, proxy_cert, pilot_dir):
        """Renaming a non-existent source should fail."""
        src_url = f"{pilot_dir}/no_such_rename_src.bin"
        dst_url = f"{pilot_dir}/no_such_rename_dst.bin"
        rc, out, err = _run("rename", proxy_cert, src_url, dst_url)
        assert rc != 0


# ---------------------------------------------------------------------------
# TestEosPilotCat
# ---------------------------------------------------------------------------


@requires_eospilot
@requires_proxy
class TestEosPilotCat:
    def test_cat_text_file(self, proxy_cert, pilot_dir, tmp_path):
        """cat should print the file contents to stdout."""
        content = "hello from eospilot\nline two\n"
        src = tmp_path / "cat_test.txt"
        src.write_text(content)
        dst = f"{pilot_dir}/cat_test.txt"
        rc, out, err = _run("cp", proxy_cert, src.as_uri(), dst)
        assert rc == 0, err

        rc, out, err = _run("cat", proxy_cert, dst)
        assert rc == 0, err
        assert out == content

    def test_cat_binary_file(self, proxy_cert, pilot_dir, tmp_path):
        """cat on a binary file should produce the exact bytes."""
        from helpers import run_gfal_binary

        data = bytes(range(256))
        src = tmp_path / "cat_bin.bin"
        src.write_bytes(data)
        dst = f"{pilot_dir}/cat_bin.bin"
        rc, out, err = _run("cp", proxy_cert, src.as_uri(), dst)
        assert rc == 0, err

        rc, out_bytes, _ = run_gfal_binary("cat", "-E", proxy_cert, "--no-verify", dst)
        assert rc == 0
        assert out_bytes == data

    def test_cat_nonexistent_fails(self, proxy_cert, pilot_dir):
        """cat on a missing file should exit non-zero."""
        missing = f"{pilot_dir}/no_such_cat_file.txt"
        rc, out, err = _run("cat", proxy_cert, missing)
        assert rc != 0


# ---------------------------------------------------------------------------
# TestEosPilotSum
# ---------------------------------------------------------------------------


@requires_eospilot
@requires_proxy
class TestEosPilotSum:
    def test_sum_md5(self, proxy_cert, pilot_dir, tmp_path):
        """Upload a file with known content and verify its MD5 checksum."""
        data = b"sum test data for md5 verification"
        src = tmp_path / "sum_md5.bin"
        src.write_bytes(data)
        expected_md5 = hashlib.md5(data).hexdigest()

        dst = f"{pilot_dir}/sum_md5.bin"
        rc, out, err = _run("cp", proxy_cert, src.as_uri(), dst)
        assert rc == 0, err

        rc, out, err = _run("sum", proxy_cert, dst, "MD5")
        assert rc == 0, err
        assert expected_md5 in out

    def test_sum_adler32(self, proxy_cert, pilot_dir, tmp_path):
        """Upload a file and verify ADLER32 output format (8 hex chars)."""
        data = b"sum test data for adler32 verification"
        src = tmp_path / "sum_adler32.bin"
        src.write_bytes(data)

        dst = f"{pilot_dir}/sum_adler32.bin"
        rc, out, err = _run("cp", proxy_cert, src.as_uri(), dst)
        assert rc == 0, err

        rc, out, err = _run("sum", proxy_cert, dst, "ADLER32")
        assert rc == 0, err
        parts = out.strip().split()
        assert len(parts) == 2
        assert len(parts[1]) == 8

    def test_sum_nonexistent_fails(self, proxy_cert, pilot_dir):
        """sum on a missing file should exit non-zero."""
        missing = f"{pilot_dir}/no_such_sum_file.bin"
        rc, out, err = _run("sum", proxy_cert, missing, "MD5")
        assert rc != 0

    def test_sum_empty_file(self, proxy_cert, pilot_dir, tmp_path):
        """MD5 of an empty file is the well-known constant."""
        src = tmp_path / "empty_sum.bin"
        src.write_bytes(b"")
        dst = f"{pilot_dir}/empty_sum.bin"
        rc, out, err = _run("cp", proxy_cert, src.as_uri(), dst)
        assert rc == 0, err

        rc, out, err = _run("sum", proxy_cert, dst, "MD5")
        assert rc == 0, err
        # MD5 of empty string
        assert hashlib.md5(b"").hexdigest() in out


# ---------------------------------------------------------------------------
# TestEosPilotSave
# ---------------------------------------------------------------------------


@requires_eospilot
@requires_proxy
class TestEosPilotSave:
    def test_save_from_stdin(self, proxy_cert, pilot_dir):
        """gfal save should upload stdin content to the destination."""
        content = "saved from stdin on eospilot\n"
        dst = f"{pilot_dir}/saved_stdin.txt"

        rc, out, err = run_gfal(
            "save", "-E", proxy_cert, "--no-verify", dst, input=content
        )
        assert rc == 0, err

        rc, out, err = _run("cat", proxy_cert, dst)
        assert rc == 0, err
        assert out == content

    def test_save_and_cat_roundtrip(self, proxy_cert, pilot_dir):
        """save then cat should recover identical content."""
        data = "line1\nline2\nline3\n"
        dst = f"{pilot_dir}/save_roundtrip.txt"

        rc, out, err = run_gfal(
            "save", "-E", proxy_cert, "--no-verify", dst, input=data
        )
        assert rc == 0, err

        rc, out, err = _run("cat", proxy_cert, dst)
        assert rc == 0, err
        assert out == data


# ---------------------------------------------------------------------------
# TestEosPilotTpc
# ---------------------------------------------------------------------------


@requires_eospilot
@requires_proxy
class TestEosPilotTpc:
    def test_tpc_copy_from_public(self, proxy_cert, pilot_dir):
        """Server-side copy from eospublic to eospilot using --tpc."""
        dst = f"{pilot_dir}/tpc_from_public.C"
        rc, out, err = _run("cp", proxy_cert, "--tpc", _PUBSRC, dst)
        assert rc == 0, err

        rc, out, err = _run("stat", proxy_cert, dst)
        assert rc == 0, err
        assert str(_PUBSRC_SIZE) in out

    def test_tpc_preserves_size(self, proxy_cert, pilot_dir):
        """TPC copy size must match the known source size."""
        dst = f"{pilot_dir}/tpc_size_check.C"
        rc, out, err = _run("cp", proxy_cert, "--tpc", _PUBSRC, dst)
        assert rc == 0, err

        rc, out, err = _run("stat", proxy_cert, dst)
        assert rc == 0, err
        assert str(_PUBSRC_SIZE) in out

    def test_tpc_only_fails_for_local_src(self, proxy_cert, pilot_dir, tmp_path):
        """--tpc-only from a local source must fail (TPC not applicable)."""
        src = tmp_path / "local_src.bin"
        src.write_bytes(b"local data")
        dst = f"{pilot_dir}/tpc_only_local.bin"

        rc, out, err = _run("cp", proxy_cert, "--tpc-only", src.as_uri(), dst)
        assert rc != 0

    def test_tpc_with_checksum(self, proxy_cert, pilot_dir):
        """--tpc combined with -K ADLER32 should verify the transferred file."""
        dst = f"{pilot_dir}/tpc_checksum.C"
        rc, out, err = _run("cp", proxy_cert, "--tpc", "-K", "ADLER32", _PUBSRC, dst)
        assert rc == 0, err

    def test_tpc_force_overwrite(self, proxy_cert, pilot_dir):
        """TPC with -f should overwrite an existing destination."""
        dst = f"{pilot_dir}/tpc_force.C"
        rc, out, err = _run("cp", proxy_cert, "--tpc", _PUBSRC, dst)
        assert rc == 0, err

        rc, out, err = _run("cp", proxy_cert, "--tpc", "-f", _PUBSRC, dst)
        assert rc == 0, err


# ---------------------------------------------------------------------------
# TestEosPilotXrootd  (root:// protocol)
# ---------------------------------------------------------------------------


def _xrootd_gsi_native() -> bool:
    """True if XRootD GSI auth is expected to work natively.

    On Linux, GSI works when the CERN CA certificates are in
    /etc/grid-security/certificates (pointed to by XRD_CADIR).  On macOS the
    pip-installed xrootd lacks a proper cert dir and falls back to Kerberos,
    so we use Docker instead.
    """
    if sys.platform != "linux":
        return False
    cadir = os.environ.get("XRD_CADIR", "")
    if cadir and Path(cadir).is_dir():
        return True
    return Path("/etc/grid-security/certificates").is_dir()


requires_xrootd_env = pytest.mark.skipif(
    not docker_available() and not _xrootd_gsi_native(),
    reason=(
        "XRootD GSI tests require either the xrootd-cern-test Docker image "
        "or Linux with /etc/grid-security/certificates (XRD_CADIR) set up"
    ),
)


@requires_eospilot
@requires_proxy
@requires_xrootd_env
class TestEosPilotXrootd:
    """XRootD protocol tests.

    On macOS: run inside the xrootd-cern-test Docker container (GSI auth
    requires CERN CAs in /etc/grid-security which the container provides).
    On Linux (CI): run natively — the integration job sets up XRD_CADIR.

    GSI authentication requires /etc/grid-security/certificates with proper
    CERN CA setup, which is already baked into the xrootd-cern-test image.
    On macOS the pip-installed xrootd falls back to Kerberos without the CA
    directory, so we delegate to Docker instead.
    """

    _XROOTD_BASE = "root://eospilot.cern.ch//eos/pilot/opstest/dteam/python3-gfal/tmp"
    _XROOTD_NO_ACCESS = "root://eospilot.cern.ch//eos/pilot/opstest/dteam/python3-gfal/dteam-has-no-permissions-here"

    def _run(self, cmd, proxy_cert, *args):
        # Do NOT pass -E to gfal: base.py would then set X509_USER_CERT/KEY and
        # remove X509_USER_PROXY, causing XRootD to use cert auth instead of
        # proxy auth (which the server rejects as "unauthorized identity").
        # X509_USER_PROXY must be set in the environment instead.
        if _xrootd_gsi_native():
            # Linux CI: run natively; X509_USER_PROXY is already in the env.
            return run_gfal(cmd, *args)
        # macOS (and other non-Linux): use Docker where CAs are pre-installed.
        return run_gfal_docker(cmd, *args, proxy_cert=proxy_cert)

    @pytest.fixture
    def xrootd_pilot_dir(self, proxy_cert):
        """Scratch dir created and cleaned up via HTTPS; yielded as root:// URL."""
        name = f"pytest-xrd-{uuid.uuid4().hex[:8]}"
        https_url = f"{_PILOT_BASE}/{name}"
        xrd_url = f"{self._XROOTD_BASE}/{name}"
        rc, out, err = run_gfal("mkdir", "-E", proxy_cert, "--no-verify", https_url)
        if rc != 0:
            pytest.skip(f"Could not create xrootd_pilot_dir: {err.strip()}")
        yield xrd_url
        run_gfal("rm", "-r", "-E", proxy_cert, "--no-verify", https_url)

    def test_copy_local_to_xrootd(self, proxy_cert, xrootd_pilot_dir):
        """Upload /etc/hostname via root:// and stat it."""
        dst = f"{xrootd_pilot_dir}/xrd_up.bin"
        rc, out, err = self._run("cp", proxy_cert, "/etc/hostname", dst)
        assert rc == 0, err

        rc, out, err = self._run("stat", proxy_cert, dst)
        assert rc == 0, err
        assert "regular file" in out

    def test_copy_xrootd_to_xrootd(self, proxy_cert, xrootd_pilot_dir):
        """Copy one file to another within eospilot via root://."""
        src = f"{xrootd_pilot_dir}/src.bin"
        dst = f"{xrootd_pilot_dir}/dst.bin"

        rc, out, err = self._run("cp", proxy_cert, "/etc/hostname", src)
        assert rc == 0, err

        rc, out, err = self._run("cp", proxy_cert, src, dst)
        assert rc == 0, err

        rc, out, err = self._run("stat", proxy_cert, dst)
        assert rc == 0, err

    def test_ls_xrootd_directory(self, proxy_cert, xrootd_pilot_dir):
        """ls over root:// should list uploaded files."""
        rc, out, err = self._run(
            "cp", proxy_cert, "/etc/hostname", f"{xrootd_pilot_dir}/xrd_ls.bin"
        )
        assert rc == 0, err

        rc, out, err = self._run("ls", proxy_cert, xrootd_pilot_dir)
        assert rc == 0, err
        assert "xrd_ls.bin" in out

    def test_stat_xrootd_directory(self, proxy_cert, xrootd_pilot_dir):
        """stat over root:// on the directory should report 'directory'."""
        rc, out, err = self._run("stat", proxy_cert, xrootd_pilot_dir)
        assert rc == 0, err
        assert "directory" in out

    def test_xrootd_permission_denied(self, proxy_cert):
        """Writing to the no-access path over root:// should fail."""
        dst = f"{self._XROOTD_NO_ACCESS}/xrd_denied.bin"
        rc, out, err = self._run("cp", proxy_cert, "/etc/hostname", dst)
        assert rc != 0

    def test_mkdir_xrootd(self, proxy_cert, xrootd_pilot_dir):
        """mkdir over root:// should create a subdirectory."""
        subdir = f"{xrootd_pilot_dir}/xrd_mkdir_sub"
        rc, out, err = self._run("mkdir", proxy_cert, subdir)
        assert rc == 0, err

        rc, out, err = self._run("stat", proxy_cert, subdir)
        assert rc == 0, err

    def test_rm_xrootd(self, proxy_cert, xrootd_pilot_dir):
        """rm over root:// should delete a file."""
        dst = f"{xrootd_pilot_dir}/xrd_rm.bin"
        rc, out, err = self._run("cp", proxy_cert, "/etc/hostname", dst)
        assert rc == 0, err

        rc, out, err = self._run("rm", proxy_cert, dst)
        assert rc == 0, err

        rc, out, err = self._run("stat", proxy_cert, dst)
        assert rc != 0

    def test_https_to_xrootd_copy(self, proxy_cert, xrootd_pilot_dir):
        """Cross-protocol: upload via HTTPS, read back via XRootD."""
        https_dst = (
            xrootd_pilot_dir.replace(
                "root://eospilot.cern.ch/", "https://eospilot.cern.ch/", 1
            )
            + "/cross.bin"
        )
        rc, out, err = run_gfal(
            "cp", "-E", proxy_cert, "--no-verify", "/etc/hosts", https_dst
        )
        assert rc == 0, err

        xrd_src = f"{xrootd_pilot_dir}/cross.bin"
        rc, out, err = self._run("stat", proxy_cert, xrd_src)
        assert rc == 0, err
