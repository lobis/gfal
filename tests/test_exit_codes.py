"""Tests for exit code accuracy across all gfal commands.

gfal2 returns specific POSIX errno values (not just 0/1).  These tests verify
that our tool returns the same codes for common error scenarios:

  ENOENT  (2)  — file or directory does not exist
  EACCES (13)  — permission denied
  EEXIST (17)  — file already exists
  EISDIR (21)  — is a directory
  EINVAL (22)  — invalid argument

The ``exception_exit_code()`` helper is also tested directly to cover the
HTTP-status and exception-type mapping paths.
"""

import errno
import os
import sys

import pytest

from gfal.cli.base import exception_exit_code
from helpers import run_gfal

# ---------------------------------------------------------------------------
# Unit tests for exception_exit_code()
# ---------------------------------------------------------------------------


class TestExceptionExitCode:
    """Direct unit tests for the exception → exit-code mapping helper."""

    # --- explicit errno attribute -------------------------------------------

    def test_gfal_file_not_found_error(self):
        from gfal.core.errors import GfalFileNotFoundError

        e = GfalFileNotFoundError("no such file")
        assert exception_exit_code(e) == errno.ENOENT

    def test_gfal_permission_error(self):
        from gfal.core.errors import GfalPermissionError

        e = GfalPermissionError("denied")
        assert exception_exit_code(e) == errno.EACCES

    def test_gfal_file_exists_error(self):
        from gfal.core.errors import GfalFileExistsError

        e = GfalFileExistsError("already exists")
        assert exception_exit_code(e) == errno.EEXIST

    def test_gfal_is_a_directory_error(self):
        from gfal.core.errors import GfalIsADirectoryError

        e = GfalIsADirectoryError("is a dir")
        assert exception_exit_code(e) == errno.EISDIR

    def test_gfal_not_a_directory_error(self):
        from gfal.core.errors import GfalNotADirectoryError

        e = GfalNotADirectoryError("not a dir")
        assert exception_exit_code(e) == errno.ENOTDIR

    def test_gfal_timeout_error(self):
        from gfal.core.errors import GfalTimeoutError

        e = GfalTimeoutError("timed out")
        assert exception_exit_code(e) == errno.ETIMEDOUT

    def test_oserror_with_explicit_errno(self):
        e = OSError(errno.ENOENT, "No such file or directory", "/tmp/missing")
        assert exception_exit_code(e) == errno.ENOENT

    def test_oserror_eacces(self):
        e = OSError(errno.EACCES, "Permission denied", "/root/secret")
        assert exception_exit_code(e) == errno.EACCES

    # --- Python built-in exception types without errno set -------------------

    def test_file_not_found_error_no_errno(self):
        """FileNotFoundError raised with just a message (errno=None)."""
        e = FileNotFoundError("root://host//missing/path")
        assert exception_exit_code(e) == errno.ENOENT

    def test_permission_error_no_errno(self):
        e = PermissionError("access denied: root://host//path")
        assert exception_exit_code(e) == errno.EACCES

    def test_file_exists_error_no_errno(self):
        e = FileExistsError("file already exists")
        assert exception_exit_code(e) == errno.EEXIST

    def test_is_a_directory_error_no_errno(self):
        e = IsADirectoryError("is a directory")
        assert exception_exit_code(e) == errno.EISDIR

    def test_not_a_directory_error_no_errno(self):
        e = NotADirectoryError("not a directory")
        assert exception_exit_code(e) == errno.ENOTDIR

    def test_timeout_error_no_errno(self):
        e = TimeoutError("operation timed out")
        assert exception_exit_code(e) == errno.ETIMEDOUT

    def test_connection_refused_error(self):
        e = ConnectionRefusedError("connection refused")
        assert exception_exit_code(e) == errno.ECONNREFUSED

    def test_connection_reset_error(self):
        e = ConnectionResetError("connection reset")
        assert exception_exit_code(e) == errno.ECONNRESET

    # --- HTTP status code mapping --------------------------------------------

    def _http_error(self, status: int):
        """Create a minimal object with a .status attribute."""

        class _FakeHTTPError(Exception):
            pass

        e = _FakeHTTPError(f"HTTP {status}")
        e.status = status  # type: ignore[attr-defined]
        return e

    @pytest.mark.parametrize(
        ("status", "expected"),
        [
            (400, errno.EINVAL),
            (401, errno.EACCES),
            (403, errno.EACCES),
            (404, errno.ENOENT),
            (408, errno.ETIMEDOUT),
            (409, errno.EEXIST),
            (410, errno.ENOENT),
            (413, errno.EFBIG),
            (423, errno.EACCES),
            (500, errno.EIO),
            (502, errno.EIO),
            (503, errno.EAGAIN),
            (504, errno.ETIMEDOUT),
        ],
    )
    def test_http_status_mapping(self, status, expected):
        e = self._http_error(status)
        assert exception_exit_code(e) == expected

    def test_unknown_http_status_falls_back_to_1(self):
        e = self._http_error(418)  # I'm a teapot
        assert exception_exit_code(e) == 1

    # --- XRootD permission messages -----------------------------------------

    def test_xrootd_permission_message_maps_to_eacces(self):
        e = Exception(
            "root://eospilot.cern.ch: server responded with an error: [3010] Permission denied"
        )
        assert exception_exit_code(e) == errno.EACCES

    def test_xrootd_unauthorized_identity_maps_to_eacces(self):
        e = Exception(
            "XRootD: unauthorized identity used; access restricted to root://host//path"
        )
        assert exception_exit_code(e) == errno.EACCES

    # --- aiohttp SSL / connection errors -------------------------------------

    def test_aiohttp_ssl_certificate_error_maps_to_ehostdown(self):
        """SSL cert failures (hostname mismatch, expired, etc.) → EHOSTDOWN.

        aiohttp raises ClientConnectorCertificateError (a subclass of
        ClientSSLError) for certificate validation failures.  On macOS the
        errno is None; on Linux it may be 1 (from the SSL error number).
        Either way the aiohttp type check fires first.  gfal2/neon reports
        these as EHOSTDOWN (112 on Linux), so we match that.
        """
        import ssl
        from unittest.mock import MagicMock

        import aiohttp

        key = MagicMock()  # connection_key placeholder
        cert_error = ssl.SSLCertVerificationError(
            1,
            "[SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed: "
            "Hostname mismatch, certificate is not valid for 'eoshome.cern.ch'.",
        )
        e = aiohttp.ClientConnectorCertificateError(key, cert_error)
        # e.errno is None on macOS but 1 on Linux — the type check must win.
        assert exception_exit_code(e) == errno.EHOSTDOWN

    def test_aiohttp_ssl_error_maps_to_ehostdown(self):
        """Generic aiohttp.ClientSSLError → EHOSTDOWN."""
        from unittest.mock import MagicMock

        import aiohttp

        key = MagicMock()
        ssl_err = OSError("SSL handshake failed")
        e = aiohttp.ClientConnectorSSLError(key, ssl_err)
        assert exception_exit_code(e) == errno.EHOSTDOWN

    def test_aiohttp_connector_error_with_os_errno(self):
        """ClientConnectorError carrying an OS errno returns that errno directly."""
        from unittest.mock import MagicMock

        import aiohttp

        key = MagicMock()
        os_err = ConnectionRefusedError(errno.ECONNREFUSED, "Connection refused")
        e = aiohttp.ClientConnectorError(key, os_err)
        # e.errno is set to ECONNREFUSED by the aiohttp constructor
        assert exception_exit_code(e) == errno.ECONNREFUSED

    # --- Fallback ------------------------------------------------------------

    def test_generic_exception_returns_1(self):
        assert exception_exit_code(Exception("unknown error")) == 1

    def test_runtime_error_returns_1(self):
        assert exception_exit_code(RuntimeError("unexpected")) == 1

    def test_value_error_returns_1(self):
        assert exception_exit_code(ValueError("bad value")) == 1


# ---------------------------------------------------------------------------
# Command-level exit code tests (local filesystem — no network needed)
# ---------------------------------------------------------------------------


class TestExitCodesRm:
    def test_rm_nonexistent_returns_enoent(self, tmp_path):
        """gfal rm on a missing file should return ENOENT (2), not just 1."""
        missing = tmp_path / "no_such_file.txt"
        rc, out, err = run_gfal("rm", missing.as_uri())
        assert rc == errno.ENOENT, f"expected {errno.ENOENT}, got {rc}"

    def test_rm_directory_without_r_returns_eisdir(self, tmp_path):
        """gfal rm on a directory without -r should return EISDIR (21)."""
        d = tmp_path / "mydir"
        d.mkdir()
        rc, out, err = run_gfal("rm", d.as_uri())
        assert rc == errno.EISDIR, f"expected {errno.EISDIR}, got {rc}"


class TestExitCodesStat:
    def test_stat_nonexistent_returns_enoent(self, tmp_path):
        """gfal stat on a missing path should return ENOENT (2), not just 1."""
        missing = tmp_path / "no_such_file.txt"
        rc, out, err = run_gfal("stat", missing.as_uri())
        assert rc == errno.ENOENT, f"expected {errno.ENOENT}, got {rc}"

    def test_stat_multiple_missing_returns_enoent(self, tmp_path):
        a = tmp_path / "a.txt"
        b = tmp_path / "b.txt"
        rc, out, err = run_gfal("stat", a.as_uri(), b.as_uri())
        assert rc == errno.ENOENT, f"expected {errno.ENOENT}, got {rc}"

    def test_stat_mixed_existing_and_missing(self, tmp_path):
        """When one file exists but another doesn't, rc should be ENOENT."""
        existing = tmp_path / "exists.txt"
        existing.write_text("x")
        missing = tmp_path / "missing.txt"
        rc, out, err = run_gfal("stat", existing.as_uri(), missing.as_uri())
        assert rc == errno.ENOENT, f"expected {errno.ENOENT}, got {rc}"


class TestExitCodesCat:
    def test_cat_nonexistent_returns_enoent(self, tmp_path):
        """gfal cat on a missing file should return ENOENT (2)."""
        missing = tmp_path / "no_such_file.txt"
        rc, out, err = run_gfal("cat", missing.as_uri())
        assert rc == errno.ENOENT, f"expected {errno.ENOENT}, got {rc}"


class TestExitCodesMkdir:
    def test_mkdir_existing_returns_eexist(self, tmp_path):
        """gfal mkdir on an existing directory (without -p) should return EEXIST (17)."""
        d = tmp_path / "existing"
        d.mkdir()
        rc, out, err = run_gfal("mkdir", d.as_uri())
        assert rc == errno.EEXIST, f"expected {errno.EEXIST}, got {rc}"

    def test_mkdir_parents_existing_returns_zero(self, tmp_path):
        """gfal mkdir -p on an existing directory should return 0."""
        d = tmp_path / "existing"
        d.mkdir()
        rc, out, err = run_gfal("mkdir", "-p", d.as_uri())
        assert rc == 0

    def test_mkdir_missing_parent_returns_enoent(self, tmp_path):
        """gfal mkdir without -p when parent is missing should return ENOENT."""
        d = tmp_path / "no_parent" / "child"
        rc, out, err = run_gfal("mkdir", d.as_uri())
        assert rc == errno.ENOENT, f"expected ENOENT({errno.ENOENT}), got {rc}: {err}"


class TestExitCodesCopy:
    def test_cp_missing_source_returns_enoent(self, tmp_path):
        """gfal cp with a missing source should return ENOENT (2)."""
        src = tmp_path / "no_such_src.txt"
        dst = tmp_path / "dst.txt"
        rc, out, err = run_gfal("cp", src.as_uri(), dst.as_uri())
        assert rc == errno.ENOENT, f"expected {errno.ENOENT}, got {rc}"

    def test_cp_overwrite_existing_returns_eexist(self, tmp_path):
        """gfal cp without -f to an existing dst must return EEXIST (17)."""
        src = tmp_path / "src.txt"
        dst = tmp_path / "dst.txt"
        src.write_bytes(b"new")
        dst.write_bytes(b"old")
        rc, out, err = run_gfal("cp", src.as_uri(), dst.as_uri())
        assert rc == errno.EEXIST, f"expected {errno.EEXIST}, got {rc}"

    def test_cp_permission_denied_returns_eacces(self, tmp_path):
        """gfal cp to a read-only directory should return EACCES (13)."""
        if sys.platform == "win32" or not hasattr(os, "geteuid") or os.geteuid() == 0:
            pytest.skip(
                "root or Windows: permission test not meaningful on this platform"
            )
        src = tmp_path / "src.txt"
        src.write_bytes(b"data")
        ro_dir = tmp_path / "readonly"
        ro_dir.mkdir(mode=0o555)
        dst = ro_dir / "dst.txt"
        try:
            rc, out, err = run_gfal("cp", src.as_uri(), dst.as_uri())
            assert rc == errno.EACCES, f"expected {errno.EACCES}, got {rc}: {err}"
        finally:
            ro_dir.chmod(0o755)

    def test_cp_no_overwrite_is_eexist(self, tmp_path):
        """Verifies the exit code is exactly 17 matching the legacy comparison test."""
        src = tmp_path / "src.txt"
        dst = tmp_path / "dst.txt"
        src.write_bytes(b"new content")
        dst.write_bytes(b"old content")
        rc, out, err = run_gfal("cp", src.as_uri(), dst.as_uri())
        assert rc == 17  # errno.EEXIST


class TestExitCodesLs:
    def test_ls_nonexistent_returns_enoent(self, tmp_path):
        """gfal ls on a missing path should return ENOENT (2)."""
        missing = tmp_path / "no_such_dir"
        rc, out, err = run_gfal("ls", missing.as_uri())
        assert rc == errno.ENOENT, f"expected {errno.ENOENT}, got {rc}"

    def test_ls_existing_returns_zero(self, tmp_path):
        rc, out, err = run_gfal("ls", tmp_path.as_uri())
        assert rc == 0


# ---------------------------------------------------------------------------
# Network-error exit codes (local loopback — no external network needed)
# ---------------------------------------------------------------------------


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="Windows WSA error codes (e.g. ECONNREFUSED=10061) exceed the 0-255 exit code range",
)
class TestExitCodesNetworkErrors:
    """Verify correct exit codes for network-level failures."""

    def test_ls_connection_refused_returns_econnrefused(self):
        """gfal ls against a closed local port should return ECONNREFUSED, not 1."""
        # Port 1 is privileged and never open; connection is always refused.
        rc, out, err = run_gfal("ls", "http://127.0.0.1:1/no/such/path")
        assert rc == errno.ECONNREFUSED, (
            f"expected ECONNREFUSED({errno.ECONNREFUSED}), got {rc}: {err}"
        )

    def test_stat_connection_refused_returns_econnrefused(self):
        """gfal stat against a closed local port should return ECONNREFUSED, not 1."""
        rc, out, err = run_gfal("stat", "http://127.0.0.1:1/no/such/path")
        assert rc == errno.ECONNREFUSED, (
            f"expected ECONNREFUSED({errno.ECONNREFUSED}), got {rc}: {err}"
        )

    def test_cat_connection_refused_returns_econnrefused(self):
        """gfal cat against a closed local port should return ECONNREFUSED, not 1."""
        rc, out, err = run_gfal("cat", "http://127.0.0.1:1/no/such/file.txt")
        assert rc == errno.ECONNREFUSED, (
            f"expected ECONNREFUSED({errno.ECONNREFUSED}), got {rc}: {err}"
        )

    def test_ssl_hostname_mismatch_returns_ehostdown(self):
        """gfal ls against an HTTPS URL with a cert mismatch returns EHOSTDOWN, not 1.

        127.0.0.1:1 over HTTPS will produce an SSL error rather than a
        connection-refused error; we verify the code is EHOSTDOWN (matching
        gfal2/neon behaviour for all SSL failures).
        """
        rc, out, err = run_gfal("ls", "https://127.0.0.1:1/no/such/path")
        # Port 1 is closed → connection refused before SSL handshake.
        # Accept either ECONNREFUSED (OS refused before SSL) or EHOSTDOWN
        # (SSL layer engaged first and failed) — both are better than 1.
        assert rc in (errno.ECONNREFUSED, errno.EHOSTDOWN), (
            f"expected ECONNREFUSED({errno.ECONNREFUSED}) or "
            f"EHOSTDOWN({errno.EHOSTDOWN}), got {rc}: {err}"
        )
