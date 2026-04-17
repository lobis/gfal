"""
Read-only integration tests against eospublic.cern.ch (CERN Open Data).

These tests exercise real HTTP and XRootD endpoints.  They are marked
``integration`` and are **not** run by plain ``pytest tests/``; pass
``-m integration`` to include them.

The CI workflow installs the CERN Root CA 2 certificate into the system
trust store so that both aiohttp and requests accept the server certificate.

Known stable test file
----------------------
  https://eospublic.cern.ch/eos/opendata/phenix/emcal-finding-pi0s-and-photons/single_cluster_r5.C
  size    : 2184 bytes
  MD5     : 93f402e24c6f870470e1c5fcc5400e25
  ADLER32 : 335e754f

Note on gfal-ls
---------------
Directory listing over HTTP uses WebDAV PROPFIND (implemented in
``webdav.py``).  EOS supports PROPFIND on publicly readable directories, so
``TestHttpLs`` exercises this code path against the real endpoint.  Plain
GET for directory browsing returns 403 ("Browsing is disabled") — that is a
different request from PROPFIND and is expected.
"""

import hashlib
import socket

import pytest

from conftest import CI, require_test_prereq
from helpers import run_gfal

# ---------------------------------------------------------------------------
# Connection markers
# ---------------------------------------------------------------------------

_HTTP_HOST = "eospublic.cern.ch"
_HTTP_PORT = 443
_XROOTD_PORT = 1094


def _tcp_reachable(host: str, port: int, timeout: float = 5.0) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


requires_http = pytest.mark.skipif(
    not _tcp_reachable(_HTTP_HOST, _HTTP_PORT) and not CI,
    reason=f"{_HTTP_HOST}:{_HTTP_PORT} not reachable",
)


def _xrootd_available() -> bool:
    try:
        import XRootD  # noqa: F401

        return True
    except ImportError:
        return False


requires_xrootd = pytest.mark.skipif(
    (not _tcp_reachable(_HTTP_HOST, _XROOTD_PORT) or not _xrootd_available())
    and not CI,
    reason=f"XRootD not available (port {_XROOTD_PORT} unreachable or xrootd package not installed)",
)


@pytest.fixture(scope="session", autouse=True)
def _require_ci_eospublic_prereqs():
    if not CI:
        return
    require_test_prereq(
        _tcp_reachable(_HTTP_HOST, _HTTP_PORT),
        f"{_HTTP_HOST}:{_HTTP_PORT} not reachable",
    )
    require_test_prereq(
        _tcp_reachable(_HTTP_HOST, _XROOTD_PORT),
        f"{_HTTP_HOST}:{_XROOTD_PORT} not reachable",
    )
    require_test_prereq(
        _xrootd_available(),
        "XRootD Python bindings are not installed for eospublic integration tests",
    )


pytestmark = [pytest.mark.integration, pytest.mark.network]

# ---------------------------------------------------------------------------
# Test constants
# ---------------------------------------------------------------------------

_BASE_HTTP = "https://eospublic.cern.ch/eos/opendata"
_BASE_ROOT = "root://eospublic.cern.ch//eos/opendata"

_SMALL_FILE_HTTP = (
    f"{_BASE_HTTP}/phenix/emcal-finding-pi0s-and-photons/single_cluster_r5.C"
)
_SMALL_FILE_ROOT = (
    f"{_BASE_ROOT}/phenix/emcal-finding-pi0s-and-photons/single_cluster_r5.C"
)

_SMALL_FILE_SIZE = 2184
_SMALL_FILE_MD5 = "93f402e24c6f870470e1c5fcc5400e25"
_SMALL_FILE_ADLER32 = "335e754f"

# A directory known to exist on eospublic
_DIR_HTTP = f"{_BASE_HTTP}/phenix/emcal-finding-pi0s-and-photons/"
_DIR_ROOT = f"{_BASE_ROOT}/phenix/emcal-finding-pi0s-and-photons/"

# ---------------------------------------------------------------------------
# HTTP tests
# ---------------------------------------------------------------------------


@requires_http
class TestHttpStat:
    def test_stat_file(self):
        rc, out, err = run_gfal("stat", _SMALL_FILE_HTTP)

        assert rc == 0
        assert str(_SMALL_FILE_SIZE) in out
        assert "File:" in out

    def test_stat_shows_regular_file(self):
        rc, out, err = run_gfal("stat", _SMALL_FILE_HTTP)

        assert rc == 0
        assert "regular file" in out

    def test_stat_opendata_dir(self):
        """Stat the opendata directory URL.

        Note: fsspec's HTTP filesystem does not detect directory type from a plain
        HTTP response — it reports 'regular file' for directory URLs.  We only
        verify the command succeeds and returns stat output.
        """
        rc, out, err = run_gfal("stat", f"{_BASE_HTTP}/")

        assert rc == 0
        assert "File:" in out


@requires_http
class TestHttpCat:
    def test_cat_file(self):
        rc, out, err = run_gfal("cat", _SMALL_FILE_HTTP)

        assert rc == 0
        assert len(out) == _SMALL_FILE_SIZE

    def test_cat_file_content_checksum(self):
        """Verify the actual content matches the known MD5."""
        from helpers import run_gfal_binary

        rc, stdout, stderr = run_gfal_binary("cat", _SMALL_FILE_HTTP)

        assert rc == 0
        assert hashlib.md5(stdout).hexdigest() == _SMALL_FILE_MD5


@requires_http
class TestHttpSum:
    def test_sum_md5(self):
        rc, out, err = run_gfal("sum", _SMALL_FILE_HTTP, "MD5")

        assert rc == 0
        assert _SMALL_FILE_MD5 in out

    def test_sum_adler32(self):
        rc, out, err = run_gfal("sum", _SMALL_FILE_HTTP, "ADLER32")

        assert rc == 0
        assert _SMALL_FILE_ADLER32 in out

    def test_sum_sha256(self):
        """SHA256 should also work (client-side computation)."""
        rc, out, err = run_gfal("sum", _SMALL_FILE_HTTP, "SHA256")

        assert rc == 0
        # Just verify it produces a 64-char hex string
        parts = out.strip().split()
        assert len(parts) == 2
        assert len(parts[1]) == 64


@requires_http
class TestHttpCopy:
    def test_copy_file(self, tmp_path):
        dst = tmp_path / "downloaded.C"

        rc, out, err = run_gfal("cp", _SMALL_FILE_HTTP, dst.as_uri())

        assert rc == 0
        assert dst.stat().st_size == _SMALL_FILE_SIZE

    def test_copy_file_checksum(self, tmp_path):
        dst = tmp_path / "downloaded.C"

        rc, out, err = run_gfal("cp", "-K", "ADLER32", _SMALL_FILE_HTTP, dst.as_uri())

        assert rc == 0
        assert dst.stat().st_size == _SMALL_FILE_SIZE

    def test_copy_file_md5_verify(self, tmp_path):
        dst = tmp_path / "downloaded.C"

        rc, out, err = run_gfal("cp", "-K", "MD5", _SMALL_FILE_HTTP, dst.as_uri())

        assert rc == 0
        assert hashlib.md5(dst.read_bytes()).hexdigest() == _SMALL_FILE_MD5


# ---------------------------------------------------------------------------
# XRootD tests
# ---------------------------------------------------------------------------


@requires_xrootd
class TestXrootdStat:
    def test_stat_file(self):
        rc, out, err = run_gfal("stat", _SMALL_FILE_ROOT)

        assert rc == 0
        assert str(_SMALL_FILE_SIZE) in out

    def test_stat_directory(self):
        rc, out, err = run_gfal("stat", f"{_BASE_ROOT}/phenix/")

        assert rc == 0
        assert "directory" in out


@requires_xrootd
class TestXrootdLs:
    def test_ls_directory(self):
        rc, out, err = run_gfal("ls", f"{_BASE_ROOT}/phenix/")

        assert rc == 0
        assert "emcal-finding-pi0s-and-photons" in out

    def test_ls_long_format(self):
        rc, out, err = run_gfal("ls", "-l", _DIR_ROOT)

        assert rc == 0
        assert "single_cluster_r5.C" in out

    def test_ls_directory_flag(self):
        rc, out, err = run_gfal("ls", "-d", _DIR_ROOT)

        assert rc == 0
        lines = [ln for ln in out.splitlines() if ln.strip()]
        assert len(lines) == 1


@requires_xrootd
class TestXrootdSum:
    def test_sum_adler32(self):
        rc, out, err = run_gfal("sum", _SMALL_FILE_ROOT, "ADLER32")

        assert rc == 0
        assert _SMALL_FILE_ADLER32 in out

    def test_sum_md5(self):
        rc, out, err = run_gfal("sum", _SMALL_FILE_ROOT, "MD5")

        assert rc == 0
        assert _SMALL_FILE_MD5 in out


@requires_xrootd
class TestXrootdCopy:
    def test_copy_file(self, tmp_path):
        dst = tmp_path / "downloaded.C"

        rc, out, err = run_gfal("cp", _SMALL_FILE_ROOT, dst.as_uri())

        assert rc == 0
        assert dst.stat().st_size == _SMALL_FILE_SIZE

    def test_copy_file_checksum(self, tmp_path):
        dst = tmp_path / "downloaded.C"

        rc, out, err = run_gfal("cp", "-K", "ADLER32", _SMALL_FILE_ROOT, dst.as_uri())

        assert rc == 0
        assert dst.stat().st_size == _SMALL_FILE_SIZE

    def test_copy_preserves_content(self, tmp_path):
        dst = tmp_path / "downloaded.C"

        rc, out, err = run_gfal("cp", _SMALL_FILE_ROOT, dst.as_uri())

        assert rc == 0
        assert hashlib.md5(dst.read_bytes()).hexdigest() == _SMALL_FILE_MD5


@requires_xrootd
class TestXrootdCat:
    def test_cat_file(self):
        from helpers import run_gfal_binary

        rc, stdout, stderr = run_gfal_binary("cat", _SMALL_FILE_ROOT)

        assert rc == 0
        assert len(stdout) == _SMALL_FILE_SIZE
        assert hashlib.md5(stdout).hexdigest() == _SMALL_FILE_MD5


# ---------------------------------------------------------------------------
# HTTP WebDAV ls tests
# ---------------------------------------------------------------------------


@requires_http
class TestHttpLs:
    """Directory listing via WebDAV PROPFIND over HTTPS."""

    def test_ls_directory(self):
        """PROPFIND ls should enumerate files inside the phenix directory."""
        rc, out, err = run_gfal("ls", _DIR_HTTP)

        assert rc == 0
        assert "single_cluster_r5.C" in out

    def test_ls_long_format(self):
        """ls -l should include the filename and the known file size."""
        rc, out, err = run_gfal("ls", "-l", _DIR_HTTP)

        assert rc == 0
        assert "single_cluster_r5.C" in out
        assert str(_SMALL_FILE_SIZE) in out

    def test_ls_directory_flag(self):
        """ls -d on a directory URL should list the directory itself (one line)."""
        rc, out, err = run_gfal("ls", "-d", _DIR_HTTP)

        assert rc == 0
        lines = [ln for ln in out.splitlines() if ln.strip()]
        assert len(lines) == 1

    def test_ls_single_file(self):
        """ls on a single file URL should succeed and name the file."""
        rc, out, err = run_gfal("ls", _SMALL_FILE_HTTP)

        assert rc == 0
        assert "single_cluster_r5.C" in out


# ---------------------------------------------------------------------------
# HTTP error-handling tests
# ---------------------------------------------------------------------------

_MISSING_HTTP = f"{_BASE_HTTP}/this_does_not_exist_gfal_integration_test.bin"


@requires_http
class TestHttpErrors:
    """Error handling for non-existent HTTP resources."""

    def test_stat_nonexistent_fails(self):
        """stat on a missing HTTP path should exit non-zero."""
        rc, out, err = run_gfal("stat", _MISSING_HTTP)

        assert rc != 0

    def test_stat_nonexistent_shows_error_message(self):
        """The error output should mention the missing resource."""
        rc, out, err = run_gfal("stat", _MISSING_HTTP)

        assert rc != 0
        combined = (out + err).lower()
        assert (
            "no such file" in combined or "404" in combined or "not found" in combined
        )

    def test_cat_nonexistent_fails(self):
        """cat on a missing HTTP path should exit non-zero."""
        rc, out, err = run_gfal("cat", _MISSING_HTTP)

        assert rc != 0

    def test_copy_nonexistent_source_fails(self, tmp_path):
        """Copying from a non-existent HTTP source should fail."""
        dst = tmp_path / "should_not_exist.bin"

        rc, out, err = run_gfal("cp", _MISSING_HTTP, dst.as_uri())

        assert rc != 0
        assert not dst.exists()

    def test_sum_nonexistent_fails(self):
        """sum on a missing HTTP path should exit non-zero."""
        rc, out, err = run_gfal("sum", _MISSING_HTTP, "MD5")

        assert rc != 0


# ---------------------------------------------------------------------------
# XRootD error-handling tests
# ---------------------------------------------------------------------------

_MISSING_ROOT = f"{_BASE_ROOT}/this_does_not_exist_gfal_integration_test.bin"


@requires_xrootd
class TestXrootdErrors:
    """Error handling for non-existent XRootD resources."""

    def test_stat_nonexistent_fails(self):
        """stat on a missing XRootD path should exit non-zero."""
        rc, out, err = run_gfal("stat", _MISSING_ROOT)

        assert rc != 0

    def test_cat_nonexistent_fails(self):
        """cat on a missing XRootD path should exit non-zero."""
        rc, out, err = run_gfal("cat", _MISSING_ROOT)

        assert rc != 0

    def test_copy_nonexistent_source_fails(self, tmp_path):
        """Copying from a non-existent XRootD source should fail."""
        dst = tmp_path / "should_not_exist.bin"

        rc, out, err = run_gfal("cp", _MISSING_ROOT, dst.as_uri())

        assert rc != 0
        assert not dst.exists()

    def test_sum_nonexistent_fails(self):
        """sum on a missing XRootD path should exit non-zero."""
        rc, out, err = run_gfal("sum", _MISSING_ROOT, "ADLER32")

        assert rc != 0
