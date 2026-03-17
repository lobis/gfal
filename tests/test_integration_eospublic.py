"""
Read-only integration tests against eospublic.cern.ch (CERN Open Data).

These tests exercise real HTTP and XRootD endpoints.  They are marked
``integration`` and are **not** run by plain ``pytest tests/``; pass
``-m integration`` to include them.

The CI workflow installs the CERN Root CA 2 certificate into the system
trust store and sets ``SSL_CERT_FILE`` / ``REQUESTS_CA_BUNDLE`` so that
both aiohttp and requests accept the server certificate without any
``--no-verify`` workaround.

Known stable test file
----------------------
  https://eospublic.cern.ch/eos/opendata/phenix/emcal-finding-pi0s-and-photons/single_cluster_r5.C
  size    : 2184 bytes
  MD5     : 93f402e24c6f870470e1c5fcc5400e25
  ADLER32 : 335e754f

Note on gfal-ls
---------------
The EOS HTTP endpoint does not serve HTML directory listings (GET returns
403 "Browsing is disabled").  Listing requires WebDAV PROPFIND, which is
not yet implemented in fsspec's HTTPFileSystem.  ls tests are therefore
limited to single-file stat calls; directory ls is covered by the XRootD
tests that use the ``root://`` scheme.
"""

import socket

import pytest

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
    not _tcp_reachable(_HTTP_HOST, _HTTP_PORT),
    reason=f"{_HTTP_HOST}:{_HTTP_PORT} not reachable",
)


def _xrootd_available() -> bool:
    try:
        import XRootD  # noqa: F401

        return True
    except ImportError:
        return False


requires_xrootd = pytest.mark.skipif(
    not _tcp_reachable(_HTTP_HOST, _XROOTD_PORT) or not _xrootd_available(),
    reason=f"XRootD not available (port {_XROOTD_PORT} unreachable or xrootd package not installed)",
)

pytestmark = pytest.mark.integration

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

# ---------------------------------------------------------------------------
# HTTP tests
# ---------------------------------------------------------------------------


@requires_http
def test_http_stat_file():
    rc, out, err = run_gfal("stat", _SMALL_FILE_HTTP)

    assert rc == 0
    assert str(_SMALL_FILE_SIZE) in out
    assert "File:" in out


@requires_http
def test_http_stat_shows_regular_file():
    rc, out, err = run_gfal("stat", _SMALL_FILE_HTTP)

    assert rc == 0
    assert "regular file" in out


@requires_http
def test_http_cat_file():
    rc, out, err = run_gfal("cat", _SMALL_FILE_HTTP)

    assert rc == 0
    assert len(out) == _SMALL_FILE_SIZE


@requires_http
def test_http_sum_md5(tmp_path):
    rc, out, err = run_gfal("sum", _SMALL_FILE_HTTP, "MD5")

    assert rc == 0
    assert _SMALL_FILE_MD5 in out


@requires_http
def test_http_sum_adler32(tmp_path):
    rc, out, err = run_gfal("sum", _SMALL_FILE_HTTP, "ADLER32")

    assert rc == 0
    assert _SMALL_FILE_ADLER32 in out


@requires_http
def test_http_copy_file(tmp_path):
    dst = tmp_path / "downloaded.C"

    rc, out, err = run_gfal("cp", _SMALL_FILE_HTTP, dst.as_uri())

    assert rc == 0
    assert dst.stat().st_size == _SMALL_FILE_SIZE


@requires_http
def test_http_copy_file_checksum(tmp_path):
    dst = tmp_path / "downloaded.C"

    rc, out, err = run_gfal("cp", "-K", "ADLER32", _SMALL_FILE_HTTP, dst.as_uri())

    assert rc == 0
    assert dst.stat().st_size == _SMALL_FILE_SIZE


@requires_http
def test_http_stat_opendata_dir():
    """Stat the opendata directory URL.

    Note: fsspec's HTTP filesystem does not detect directory type from a plain
    HTTP response — it reports 'regular file' for directory URLs.  We only
    verify the command succeeds and returns stat output.
    """
    rc, out, err = run_gfal("stat", f"{_BASE_HTTP}/")

    assert rc == 0
    assert "File:" in out


# ---------------------------------------------------------------------------
# XRootD tests
# ---------------------------------------------------------------------------


@requires_xrootd
def test_xrootd_stat_file():
    rc, out, err = run_gfal("stat", _SMALL_FILE_ROOT)

    assert rc == 0
    assert str(_SMALL_FILE_SIZE) in out


@requires_xrootd
def test_xrootd_stat_directory():
    rc, out, err = run_gfal("stat", f"{_BASE_ROOT}/phenix/")

    assert rc == 0
    assert "directory" in out


@requires_xrootd
def test_xrootd_ls_directory():
    rc, out, err = run_gfal("ls", f"{_BASE_ROOT}/phenix/")

    assert rc == 0
    assert "emcal-finding-pi0s-and-photons" in out


@requires_xrootd
def test_xrootd_sum_adler32():
    rc, out, err = run_gfal("sum", _SMALL_FILE_ROOT, "ADLER32")

    assert rc == 0
    assert _SMALL_FILE_ADLER32 in out


@requires_xrootd
def test_xrootd_copy_file(tmp_path):
    dst = tmp_path / "downloaded.C"

    rc, out, err = run_gfal("cp", _SMALL_FILE_ROOT, dst.as_uri())

    assert rc == 0
    assert dst.stat().st_size == _SMALL_FILE_SIZE
