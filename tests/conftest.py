"""Shared pytest fixtures for gfal-cli tests."""

import os
import ssl
import urllib.request
from pathlib import Path

import pytest

CI = os.environ.get("CI", "").lower() in {"1", "true", "yes"}


def require_test_prereq(condition: bool, reason: str) -> None:
    """Skip locally when a test prereq is missing, but fail in CI."""
    if condition:
        return
    if CI:
        pytest.fail(reason)
    pytest.skip(reason)


# ---------------------------------------------------------------------------
# Retry hook: automatically rerun any test tagged @pytest.mark.network
# ---------------------------------------------------------------------------

_NETWORK_RERUNS = 5
_NETWORK_RERUNS_DELAY = 30  # seconds between retries


def pytest_collection_modifyitems(items):
    """Add rerun-failure markers to all tests tagged with ``network``."""
    for item in items:
        if item.get_closest_marker("network") and not item.get_closest_marker("flaky"):
            item.add_marker(
                pytest.mark.flaky(
                    reruns=_NETWORK_RERUNS, reruns_delay=_NETWORK_RERUNS_DELAY
                ),
                append=False,
            )
        if item.get_closest_marker("xrootd"):
            # Keep all local XRootD-backed tests on the same worker. Running several
            # independent XRootD HTTPS fixtures in parallel is the main source of the
            # late-suite xdist hangs we see locally.
            item.add_marker(pytest.mark.xdist_group(name="xrootd"), append=False)


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


# ---------------------------------------------------------------------------
# Local XRootD server fixtures
# ---------------------------------------------------------------------------


def _find_free_port():
    """Bind to port 0 and return the OS-assigned port number."""
    import socket

    with socket.socket() as s:
        s.bind(("", 0))
        return s.getsockname()[1]


def _wait_for_port(host, port, timeout=10.0):
    """Block until a TCP port accepts connections or timeout expires."""
    import socket
    import time

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with socket.create_connection((host, port), timeout=0.5):
                return True
        except (ConnectionRefusedError, OSError):
            time.sleep(0.1)
    return False


def _wait_for_https(url, timeout=10.0, method="HEAD"):
    """Block until an HTTPS endpoint responds or timeout expires."""
    import time
    import urllib.error

    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        req = urllib.request.Request(url, method=method)
        try:
            with urllib.request.urlopen(req, context=ctx, timeout=1):  # noqa: S310
                return True
        except urllib.error.HTTPError:
            # A real HTTP response (even 4xx/5xx) proves the HTTPS listener is up.
            return True
        except (urllib.error.URLError, OSError):
            time.sleep(0.1)
    return False


def _find_xrdhttp_lib():
    """Locate the XrdHttp shared library for XRootD HTTPS support.

    Returns the path string to pass in ``xrd.protocol http:PORT <lib>``,
    or None when the library cannot be found.
    """
    import ctypes.util
    import sys

    if sys.platform == "darwin":
        # Homebrew on Apple Silicon / Intel
        for candidate in (
            "/opt/homebrew/lib/libXrdHttp-5.so",
            "/usr/local/lib/libXrdHttp-5.so",
        ):
            if Path(candidate).exists():
                return candidate
    else:
        # Linux — try the linker search path first, then common locations.
        found = ctypes.util.find_library("XrdHttp-5")
        if found:
            return found
        for candidate in (
            "/lib/x86_64-linux-gnu/libXrdHttp-5.so",
            "/usr/lib64/libXrdHttp-5.so",
            "/usr/lib/libXrdHttp-5.so",
        ):
            if Path(candidate).exists():
                return candidate

    return None


@pytest.fixture(scope="session")
def xrootd_server(tmp_path_factory):
    """Start a local XRootD server (root:// + https://) for integration tests.

    Yields a dict with keys:
      ``data_dir``  — pathlib.Path to the directory being served
      ``root_url``  — base URL for the XRootD protocol (root://localhost:PORT/)
      ``https_url`` — base URL for the HTTPS interface (https://localhost:PORT/)
      ``cert_pem``  — path to the self-signed CA/server PEM (for TLS verification)
    """
    import shutil
    import subprocess

    xrootd_bin = shutil.which("xrootd")
    if xrootd_bin is None:
        require_test_prereq(False, "xrootd binary not found")

    try:
        import fsspec_xrootd  # noqa: F401
    except ImportError:
        require_test_prereq(False, "fsspec-xrootd not installed")

    base = tmp_path_factory.mktemp("xrootd")
    data_dir = base / "data"
    data_dir.mkdir()
    cfg_dir = base / "cfg"
    cfg_dir.mkdir()

    xroot_port = _find_free_port()
    http_port = _find_free_port()

    # Self-signed certificate for the HTTPS interface
    cert_pem = cfg_dir / "cert.pem"
    key_pem = cfg_dir / "key.pem"
    try:
        subprocess.run(
            [
                "openssl",
                "req",
                "-x509",
                "-newkey",
                "rsa:2048",
                "-keyout",
                str(key_pem),
                "-out",
                str(cert_pem),
                "-days",
                "1",
                "-nodes",
                "-subj",
                "/CN=localhost",
            ],
            capture_output=True,
            check=True,
        )
        has_tls = True
    except (subprocess.CalledProcessError, FileNotFoundError):
        has_tls = False

    cfg_lines = [
        f"xrd.port {xroot_port}",
        f"oss.localroot {data_dir}",
        "xrd.protocol xrootd *",
        "xrootd.export /",
        "sec.protbind * none",
    ]
    if has_tls:
        # Find the XrdHttp shared library.  On macOS (Homebrew) it lives under
        # /opt/homebrew/lib; on Linux it is in the standard system library path.
        xrdhttp_lib = _find_xrdhttp_lib()
        if xrdhttp_lib is None:
            has_tls = False
        else:
            cfg_lines += [
                f"xrd.protocol http:{http_port} {xrdhttp_lib}",
                f"http.cert {cert_pem}",
                f"http.key {key_pem}",
            ]

    cfg_file = cfg_dir / "xrootd.cfg"
    cfg_file.write_text("\n".join(cfg_lines) + "\n")
    log_file = cfg_dir / "xrootd.log"

    proc = subprocess.Popen(
        [xrootd_bin, "-c", str(cfg_file), "-l", str(log_file)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    if not _wait_for_port("localhost", xroot_port, timeout=10.0):
        proc.kill()
        log_content = log_file.read_text() if log_file.exists() else "(no log)"
        require_test_prereq(
            False,
            f"XRootD server did not start in time.\nLog:\n{log_content}",
        )

    https_url = None
    if has_tls:
        candidate_https_url = f"https://127.0.0.1:{http_port}/"
        health_name = ".gfal_https_healthcheck.txt"
        (data_dir / health_name).write_text("ok")
        if _wait_for_https(
            candidate_https_url + health_name, timeout=10.0, method="HEAD"
        ):
            https_url = candidate_https_url

    yield {
        "data_dir": data_dir,
        # XRootD requires double-slash for absolute paths: root://host//abs/path
        # Single-slash would be a relative path (disallowed by the server config).
        "root_url": f"root://localhost:{xroot_port}//",
        "https_url": https_url,
        "cert_pem": str(cert_pem) if has_tls else None,
    }

    proc.terminate()
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()
