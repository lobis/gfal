"""Shared pytest fixtures for gfal-cli tests."""

import os
import ssl
import urllib.request
from pathlib import Path

import pytest

# ---------------------------------------------------------------------------
# CERN Root CA 2 — required to reach eospublic.cern.ch over HTTPS
# ---------------------------------------------------------------------------

_CERN_CA_URL = (
    "https://cafiles.cern.ch/cafiles/certificates/"
    "CERN%20Root%20Certification%20Authority%202.crt"
)
# User-level cache: survives across test runs so we only download once.
_CACHE_DIR = Path.home() / ".cache" / "gfal-cli-tests"
_CERN_CA_DER = _CACHE_DIR / "cern-root-ca-2.der"
_CERN_CA_PEM = _CACHE_DIR / "cern-root-ca-2.pem"


def _download_cern_ca() -> Path:
    """Download (and cache) the CERN Root CA 2 certificate as PEM.

    cafiles.cern.ch is itself signed by the CERN Root CA, so we must skip
    SSL verification for this specific bootstrap download.  This is safe:
    we are fetching a *public* CA certificate whose fingerprint we could
    verify out-of-band, and the download is only used to set up local testing.

    Returns the path to the PEM file.
    """
    _CACHE_DIR.mkdir(parents=True, exist_ok=True)

    if not _CERN_CA_PEM.exists():
        if not _CERN_CA_DER.exists():
            # Skip verification — the CA cert itself is what we are downloading.
            no_verify_ctx = ssl.create_default_context()
            no_verify_ctx.check_hostname = False
            no_verify_ctx.verify_mode = ssl.CERT_NONE
            with urllib.request.urlopen(_CERN_CA_URL, context=no_verify_ctx) as resp:  # noqa: S310
                _CERN_CA_DER.write_bytes(resp.read())
        der_bytes = _CERN_CA_DER.read_bytes()
        pem_str = ssl.DER_cert_to_PEM_cert(der_bytes)
        _CERN_CA_PEM.write_text(pem_str)

    return _CERN_CA_PEM


@pytest.fixture(scope="session", autouse=True)
def _cern_ca_bundle(tmp_path_factory):
    """Ensure aiohttp / requests can verify eospublic.cern.ch's certificate.

    When the CI workflow already sets ``SSL_CERT_FILE`` (after installing the
    CERN Root CA into the system trust store) this fixture is a no-op.

    Otherwise, it:
    1. Downloads and caches the CERN Root CA 2 PEM certificate.
    2. Creates a combined bundle: certifi's default bundle + CERN Root CA 2.
    3. Sets ``SSL_CERT_FILE`` and ``REQUESTS_CA_BUNDLE`` in ``os.environ`` so
       both aiohttp and requests pick it up.  Because ``helpers._subprocess_env``
       captures ``os.environ`` at call time, all gfal-cli subprocesses spawned
       by the test suite inherit the updated env.
    """
    if os.environ.get("SSL_CERT_FILE"):
        return  # CI already configured the bundle — nothing to do

    try:
        import certifi

        cern_pem = _download_cern_ca()

        # Build a combined bundle: certifi's bundle + CERN Root CA 2
        combined = tmp_path_factory.mktemp("ca") / "bundle.pem"
        combined.write_bytes(
            Path(certifi.where()).read_bytes() + b"\n" + cern_pem.read_bytes()
        )

        os.environ["SSL_CERT_FILE"] = str(combined)
        os.environ["REQUESTS_CA_BUNDLE"] = str(combined)
    except Exception as exc:
        # If anything goes wrong (no network, certifi not installed, etc.),
        # don't abort the whole test session — integration tests will simply
        # fail with an SSL error and their skip markers still apply.
        import warnings

        warnings.warn(
            f"Could not set up CERN Root CA bundle: {exc}\n"
            "Integration tests against eospublic.cern.ch may fail with SSL errors.",
            stacklevel=1,
        )


# ---------------------------------------------------------------------------
# Basic file fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def data_file(tmp_path):
    """A 1025-byte binary test file (matches reference gfal2-util test size)."""
    f = tmp_path / "data.bin"
    f.write_bytes(os.urandom(1025))
    return f


@pytest.fixture
def text_file(tmp_path):
    """A small text file."""
    f = tmp_path / "hello.txt"
    f.write_text("hello world\n")
    return f


@pytest.fixture
def empty_file(tmp_path):
    """A zero-byte file."""
    f = tmp_path / "empty.bin"
    f.write_bytes(b"")
    return f


@pytest.fixture
def large_file(tmp_path):
    """A 5 MiB file (larger than CHUNK_SIZE = 4 MiB)."""
    f = tmp_path / "large.bin"
    f.write_bytes(b"X" * (5 * 1024 * 1024))
    return f


# ---------------------------------------------------------------------------
# Directory fixtures (mirrors gfal2-util's TestBase setUp)
# ---------------------------------------------------------------------------


@pytest.fixture
def populated_dir(tmp_path):
    """
    A directory containing two files and a subdirectory.

    Mirrors the reference gfal2-util test setup:
      dirname/
        f1.bin   (1025 bytes)
        f2.bin   (1025 bytes)
        subdir/
    """
    d = tmp_path / "testdir"
    d.mkdir()
    f1 = d / "f1.bin"
    f2 = d / "f2.bin"
    f1.write_bytes(os.urandom(1025))
    f2.write_bytes(os.urandom(1025))
    sub = d / "subdir"
    sub.mkdir()
    return d


@pytest.fixture
def nested_dir(tmp_path):
    """
    A deeper directory tree for recursive operations.

      tree/
        a.txt
        sub1/
          b.txt
          sub2/
            c.txt
    """
    root = tmp_path / "tree"
    root.mkdir()
    (root / "a.txt").write_text("a")
    sub1 = root / "sub1"
    sub1.mkdir()
    (sub1 / "b.txt").write_text("b")
    sub2 = sub1 / "sub2"
    sub2.mkdir()
    (sub2 / "c.txt").write_text("c")
    return root


@pytest.fixture
def hidden_dir(tmp_path):
    """A directory with hidden and visible files."""
    d = tmp_path / "hidden_test"
    d.mkdir()
    (d / ".hidden1").write_text("h1")
    (d / ".hidden2").write_text("h2")
    (d / "visible1").write_text("v1")
    (d / "visible2").write_text("v2")
    return d


@pytest.fixture
def permission_file(tmp_path):
    """A file with known permissions (644)."""
    f = tmp_path / "perm.txt"
    f.write_text("content")
    f.chmod(0o644)
    return f
