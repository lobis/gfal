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

Known stable public source file
---------------------------------
  https://eospublic.cern.ch//eos/opendata/phenix/emcal-finding-pi0s-and-photons/single_cluster_r5.C
  size    : 2184 bytes
  MD5     : 93f402e24c6f870470e1c5fcc5400e25
  ADLER32 : 335e754f
"""

import os
import socket
import uuid
from pathlib import Path

import pytest

from helpers import run_gfal

pytestmark = [pytest.mark.integration, pytest.mark.network]

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_PILOT_BASE = "https://eospilot.cern.ch//eos/pilot/opstest/dteam/gfal-cli/tmp"
_PILOT_NO_ACCESS = "https://eospilot.cern.ch//eos/pilot/opstest/dteam/gfal-cli/dteam-has-no-permissions-here"
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


# ---------------------------------------------------------------------------
# TestEospilotStreamingCopy
# ---------------------------------------------------------------------------


@requires_eospilot
@requires_proxy
class TestEospilotStreamingCopy:
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
        import hashlib

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

    def test_copy_with_checksum(self, proxy_cert, pilot_dir, tmp_path):
        """Copy with -K MD5 checksum verification."""
        data = b"checksum test eospilot"
        src = tmp_path / "chksum.bin"
        src.write_bytes(data)

        dst = f"{pilot_dir}/chksum.bin"
        rc, out, err = _run("cp", proxy_cert, "-K", "MD5", src.as_uri(), dst)
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

    def test_no_overwrite_without_force(self, proxy_cert, pilot_dir, tmp_path):
        """Copying to an existing destination without -f should fail."""
        src = tmp_path / "overwrite_src.bin"
        src.write_bytes(b"original")
        dst = f"{pilot_dir}/overwrite_test.bin"

        # First copy — should succeed
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


# ---------------------------------------------------------------------------
# TestEospilotStat
# ---------------------------------------------------------------------------


@requires_eospilot
@requires_proxy
class TestEospilotStat:
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
        """stat on the scratch directory should succeed and mention 'File:'."""
        rc, out, err = _run("stat", proxy_cert, pilot_dir)
        assert rc == 0, err
        assert "File:" in out

    def test_stat_nonexistent_fails(self, proxy_cert, pilot_dir):
        """stat on a missing path should exit non-zero."""
        missing = f"{pilot_dir}/no_such_file_gfal_test.bin"
        rc, out, err = _run("stat", proxy_cert, missing)
        assert rc != 0


# ---------------------------------------------------------------------------
# TestEospilotLs
# ---------------------------------------------------------------------------


@requires_eospilot
@requires_proxy
class TestEospilotLs:
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

    def test_ls_long_format(self, proxy_cert, pilot_dir, tmp_path):
        """ls -l should succeed and show the filename."""
        src = tmp_path / "ls_long.bin"
        src.write_bytes(b"long format")
        dst = f"{pilot_dir}/ls_long.bin"
        rc, out, err = _run("cp", proxy_cert, src.as_uri(), dst)
        assert rc == 0, err

        rc, out, err = _run("ls", proxy_cert, "-l", pilot_dir)
        assert rc == 0, err
        assert "ls_long.bin" in out

    def test_ls_directory_flag(self, proxy_cert, pilot_dir):
        """ls -d should show the directory itself as a single entry."""
        rc, out, err = _run("ls", proxy_cert, "-d", pilot_dir)
        assert rc == 0, err
        lines = [ln for ln in out.splitlines() if ln.strip()]
        assert len(lines) >= 1

    def test_ls_permission_denied(self, proxy_cert):
        """Listing a directory without permissions should fail (403)."""
        rc, out, err = _run("ls", proxy_cert, _PILOT_NO_ACCESS)
        assert rc != 0
        assert "Permission denied" in err or "403" in err


# ---------------------------------------------------------------------------
# TestEospilotMkdirRm
# ---------------------------------------------------------------------------


@requires_eospilot
@requires_proxy
class TestEospilotMkdirRm:
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

    def test_rm_nonexistent_fails(self, proxy_cert, pilot_dir):
        """Deleting a non-existent file should exit non-zero."""
        missing = f"{pilot_dir}/no_such_file_rm_test.bin"
        rc, out, err = _run("rm", proxy_cert, missing)
        assert rc != 0

    def test_mkdir_permission_denied(self, proxy_cert):
        """Creating a directory in a no-access location should fail (403)."""
        subdir = f"{_PILOT_NO_ACCESS}/subdir_denied"
        rc, out, err = _run("mkdir", proxy_cert, subdir)
        assert rc != 0
        assert "Permission denied" in err or "403" in err


# ---------------------------------------------------------------------------
# TestEospilotRename
# ---------------------------------------------------------------------------


@requires_eospilot
@requires_proxy
class TestEospilotRename:
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


# ---------------------------------------------------------------------------
# TestEospilotSum
# ---------------------------------------------------------------------------


@requires_eospilot
@requires_proxy
class TestEospilotSum:
    def test_sum_md5(self, proxy_cert, pilot_dir, tmp_path):
        """Upload a file with known content and verify its MD5 checksum."""
        import hashlib

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
        """Upload a file with known content and verify its ADLER32 checksum."""
        data = b"sum test data for adler32 verification"
        src = tmp_path / "sum_adler32.bin"
        src.write_bytes(data)

        dst = f"{pilot_dir}/sum_adler32.bin"
        rc, out, err = _run("cp", proxy_cert, src.as_uri(), dst)
        assert rc == 0, err

        rc, out, err = _run("sum", proxy_cert, dst, "ADLER32")
        assert rc == 0, err
        # Verify the output has the expected checksum format (8 hex chars)
        parts = out.strip().split()
        assert len(parts) == 2
        assert len(parts[1]) == 8


# ---------------------------------------------------------------------------
# TestEospilotTpc
# ---------------------------------------------------------------------------


@requires_eospilot
@requires_proxy
class TestEospilotTpc:
    def test_tpc_copy_from_public(self, proxy_cert, pilot_dir):
        """Server-side copy from eospublic to eospilot using --tpc."""
        dst = f"{pilot_dir}/tpc_from_public.C"
        rc, out, err = _run("cp", proxy_cert, "--tpc", _PUBSRC, dst)
        assert rc == 0, err

        # Verify size with stat
        rc, out, err = _run("stat", proxy_cert, dst)
        assert rc == 0, err
        assert str(_PUBSRC_SIZE) in out

    def test_auto_tpc_http_to_http(self, proxy_cert, pilot_dir):
        """HTTP-to-HTTP copy without --tpc flag should attempt TPC automatically."""
        dst = f"{pilot_dir}/auto_tpc.C"
        # Without --tpc the copy mode is 'auto'; for HTTP->HTTP it tries TPC first
        rc, out, err = _run("cp", proxy_cert, _PUBSRC, dst)
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
