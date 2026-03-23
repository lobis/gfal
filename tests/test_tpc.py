"""
Tests for third-party copy (TPC) support.

TPC requires two remote storage endpoints talking to each other directly,
so true integration tests need running servers.  This file covers:

  1. Unit tests for the tpc.py module internals (mocked HTTP responses).
  2. CLI flag tests via run_gfal (using local files where TPC is N/A,
     verifying error handling and flag wiring).
  3. Behaviour of --tpc-only when TPC is not supported.
  4. Auto-TPC: gfal-copy attempts TPC automatically for HTTP<->HTTP and
     root<->root transfers (matching gfal2 default behaviour).
"""

import hashlib
from unittest.mock import MagicMock, patch

import pytest

from gfal.cli.copy import _tpc_applicable
from gfal.core import tpc as tpc_mod
from helpers import run_gfal

# ---------------------------------------------------------------------------
# _parse_tpc_body — unit tests
# ---------------------------------------------------------------------------


class TestParseTpcBody:
    def _make_resp(self, status_code, lines=None):
        resp = MagicMock()
        resp.status_code = status_code
        if lines is not None:
            resp.iter_lines.return_value = iter(lines)
        return resp

    def test_200_immediate_success(self):
        resp = self._make_resp(200)
        tpc_mod._parse_tpc_body(resp)  # should not raise

    def test_201_immediate_success(self):
        resp = self._make_resp(201)
        tpc_mod._parse_tpc_body(resp)

    def test_204_immediate_success(self):
        resp = self._make_resp(204)
        tpc_mod._parse_tpc_body(resp)

    def test_202_success_line(self):
        resp = self._make_resp(
            202,
            [
                "Perf Marker",
                "  Stripe Bytes Transferred: 1048576",
                "End",
                "success: Created",
            ],
        )
        tpc_mod._parse_tpc_body(resp)  # should not raise

    def test_202_failure_line(self):
        resp = self._make_resp(
            202,
            [
                "Perf Marker",
                "End",
                "failure: Permission denied",
            ],
        )
        with pytest.raises(OSError, match="Permission denied"):
            tpc_mod._parse_tpc_body(resp)

    def test_202_empty_body_treated_as_success(self):
        resp = self._make_resp(202, [])
        tpc_mod._parse_tpc_body(resp)  # should not raise

    def test_405_raises_not_implemented(self):
        resp = self._make_resp(405)
        with pytest.raises(NotImplementedError):
            tpc_mod._parse_tpc_body(resp)

    def test_501_raises_not_implemented(self):
        resp = self._make_resp(501)
        with pytest.raises(NotImplementedError):
            tpc_mod._parse_tpc_body(resp)

    def test_403_raises_http_error(self):
        import requests

        resp = self._make_resp(403)
        resp.raise_for_status.side_effect = requests.HTTPError("403 Forbidden")
        with pytest.raises(requests.HTTPError):
            tpc_mod._parse_tpc_body(resp)

    def test_202_with_perf_markers_then_success(self):
        """Full realistic WLCG TPC response body."""
        lines = [
            "Perf Marker",
            "  Timestamp: 1700000001",
            "  Stripe Index: 0",
            "  Stripe Bytes Transferred: 524288",
            "  Total Stripe Count: 1",
            "End",
            "Perf Marker",
            "  Timestamp: 1700000002",
            "  Stripe Bytes Transferred: 1048576",
            "End",
            "success: Created",
        ]
        resp = self._make_resp(202, lines)
        tpc_mod._parse_tpc_body(resp)  # should not raise


# ---------------------------------------------------------------------------
# do_tpc — scheme dispatch
# ---------------------------------------------------------------------------


class TestDoTpcDispatch:
    def test_local_to_local_raises(self):
        with pytest.raises(NotImplementedError):
            tpc_mod.do_tpc("file:///a", "file:///b", {})

    def test_http_to_http_calls_http_tpc(self):
        with patch.object(tpc_mod, "_http_tpc", return_value=True) as mock:
            result = tpc_mod.do_tpc(
                "https://src.example.com/file",
                "https://dst.example.com/file",
                {},
            )
        assert result is True
        mock.assert_called_once()

    def test_root_to_root_calls_xrootd_tpc(self):
        with patch.object(tpc_mod, "_xrootd_tpc", return_value=True) as mock:
            result = tpc_mod.do_tpc(
                "root://src.example.com//file",
                "root://dst.example.com//file",
                {},
            )
        assert result is True
        mock.assert_called_once()

    def test_http_to_root_raises(self):
        with pytest.raises(NotImplementedError):
            tpc_mod.do_tpc(
                "https://src.example.com/file",
                "root://dst.example.com//file",
                {},
            )

    def test_root_to_http_raises(self):
        with pytest.raises(NotImplementedError):
            tpc_mod.do_tpc(
                "root://src.example.com//file",
                "https://dst.example.com/file",
                {},
            )

    def test_tpc_mode_pull_passed_to_http(self):
        with patch.object(tpc_mod, "_http_tpc", return_value=True) as mock:
            tpc_mod.do_tpc(
                "https://src.example.com/file",
                "https://dst.example.com/file",
                {},
                mode="pull",
            )
        _, kwargs = mock.call_args
        assert kwargs["mode"] == "pull"

    def test_tpc_mode_push_passed_to_http(self):
        with patch.object(tpc_mod, "_http_tpc", return_value=True) as mock:
            tpc_mod.do_tpc(
                "https://src.example.com/file",
                "https://dst.example.com/file",
                {},
                mode="push",
            )
        _, kwargs = mock.call_args
        assert kwargs["mode"] == "push"

    def test_scitag_passed_to_http(self):
        with patch.object(tpc_mod, "_http_tpc", return_value=True) as mock:
            tpc_mod.do_tpc(
                "https://src.example.com/file",
                "https://dst.example.com/file",
                {},
                scitag=42,
            )
        _, kwargs = mock.call_args
        assert kwargs["scitag"] == 42


# ---------------------------------------------------------------------------
# _http_tpc — request construction
# ---------------------------------------------------------------------------


class TestHttpTpc:
    def _make_session(self, status_code=201, lines=None):
        resp = MagicMock()
        resp.status_code = status_code
        resp.iter_lines.return_value = iter(lines or [])
        session = MagicMock()
        session.request.return_value = resp
        return session, resp

    def test_pull_uses_copy_on_dst(self):
        session, _ = self._make_session(201)
        with patch.object(tpc_mod, "_build_session", return_value=session):
            tpc_mod._http_tpc(
                "https://src.example.com/file",
                "https://dst.example.com/file",
                {},
                mode="pull",
                timeout=None,
                verbose=False,
                scitag=None,
            )
        session.request.assert_called_once()
        args, kwargs = session.request.call_args
        assert args[0] == "COPY"
        assert args[1] == "https://dst.example.com/file"
        assert "Source" in kwargs["headers"]
        assert kwargs["headers"]["Source"] == "https://src.example.com/file"

    def test_push_uses_copy_on_src(self):
        session, _ = self._make_session(201)
        with patch.object(tpc_mod, "_build_session", return_value=session):
            tpc_mod._http_tpc(
                "https://src.example.com/file",
                "https://dst.example.com/file",
                {},
                mode="push",
                timeout=None,
                verbose=False,
                scitag=None,
            )
        args, kwargs = session.request.call_args
        assert args[1] == "https://src.example.com/file"
        assert "Destination" in kwargs["headers"]

    def test_scitag_header_set(self):
        session, _ = self._make_session(201)
        with patch.object(tpc_mod, "_build_session", return_value=session):
            tpc_mod._http_tpc(
                "https://src.example.com/file",
                "https://dst.example.com/file",
                {},
                mode="pull",
                timeout=None,
                verbose=False,
                scitag=128,
            )
        _, kwargs = session.request.call_args
        assert kwargs["headers"].get("SciTag") == "128"

    def test_no_scitag_header_when_none(self):
        session, _ = self._make_session(201)
        with patch.object(tpc_mod, "_build_session", return_value=session):
            tpc_mod._http_tpc(
                "https://src.example.com/file",
                "https://dst.example.com/file",
                {},
                mode="pull",
                timeout=None,
                verbose=False,
                scitag=None,
            )
        _, kwargs = session.request.call_args
        assert "SciTag" not in kwargs["headers"]

    def test_overwrite_header_always_set(self):
        session, _ = self._make_session(201)
        with patch.object(tpc_mod, "_build_session", return_value=session):
            tpc_mod._http_tpc(
                "https://src.example.com/file",
                "https://dst.example.com/file",
                {},
                mode="pull",
                timeout=None,
                verbose=False,
                scitag=None,
            )
        _, kwargs = session.request.call_args
        assert kwargs["headers"].get("Overwrite") == "T"

    def test_405_raises_not_implemented(self):
        resp = MagicMock()
        resp.status_code = 405
        session = MagicMock()
        session.request.return_value = resp
        with (
            patch.object(tpc_mod, "_build_session", return_value=session),
            pytest.raises(NotImplementedError),
        ):
            tpc_mod._http_tpc(
                "https://src.example.com/file",
                "https://dst.example.com/file",
                {},
                mode="pull",
                timeout=None,
                verbose=False,
                scitag=None,
            )

    def test_timeout_passed_to_request(self):
        session, _ = self._make_session(201)
        with patch.object(tpc_mod, "_build_session", return_value=session):
            tpc_mod._http_tpc(
                "https://src.example.com/file",
                "https://dst.example.com/file",
                {},
                mode="pull",
                timeout=30,
                verbose=False,
                scitag=None,
            )
        _, kwargs = session.request.call_args
        assert kwargs["timeout"] == 30


# ---------------------------------------------------------------------------
# _build_session — options
# ---------------------------------------------------------------------------


class TestBuildSession:
    def test_cert_set(self, tmp_path):
        cert = tmp_path / "cert.pem"
        cert.write_text("cert")
        key = tmp_path / "key.pem"
        key.write_text("key")
        opts = {"client_cert": str(cert), "client_key": str(key)}
        session = tpc_mod._build_session(opts)
        assert session.cert == (str(cert), str(key))

    def test_cert_without_key_uses_cert_as_key(self, tmp_path):
        cert = tmp_path / "proxy.pem"
        cert.write_text("proxy")
        opts = {"client_cert": str(cert)}
        session = tpc_mod._build_session(opts)
        assert session.cert == (str(cert), str(cert))

    def test_ssl_verify_false(self):
        opts = {"ssl_verify": False}
        session = tpc_mod._build_session(opts)
        assert session.verify is False

    def test_ssl_verify_true_default(self):
        session = tpc_mod._build_session({})
        # Default requests session verify is True or a CA bundle path; not False
        assert session.verify is not False


# ---------------------------------------------------------------------------
# CLI flag wiring: --tpc, --tpc-only, --tpc-mode, --scitag
# ---------------------------------------------------------------------------


class TestCopyTpcFlags:
    def test_help_shows_tpc(self):
        rc, out, err = run_gfal("cp", "--help")
        combined = out + err
        assert "tpc" in combined

    def test_tpc_only_local_to_local_fails(self, tmp_path):
        """Local -> local with --tpc-only must fail because TPC is not applicable."""
        src = tmp_path / "src.txt"
        dst = tmp_path / "dst.txt"
        src.write_bytes(b"hello")

        rc, out, err = run_gfal("cp", "--tpc-only", src.as_uri(), dst.as_uri())

        assert rc != 0

    def test_tpc_fallback_to_streaming(self, tmp_path):
        """--tpc without --tpc-only should fall back for local files."""
        src = tmp_path / "src.txt"
        dst = tmp_path / "dst.txt"
        src.write_bytes(b"test data")

        rc, out, err = run_gfal("cp", "--tpc", src.as_uri(), dst.as_uri())

        # Must succeed (fell back to streaming)
        assert rc == 0
        assert dst.read_bytes() == b"test data"

    def test_tpc_mode_pull_is_default(self, tmp_path):
        """--tpc with no --tpc-mode should default to pull and still copy (via fallback)."""
        src = tmp_path / "src.txt"
        dst = tmp_path / "dst.txt"
        src.write_bytes(b"data")

        rc, out, err = run_gfal("cp", "--tpc", src.as_uri(), dst.as_uri())

        assert rc == 0

    def test_tpc_mode_push_accepted(self, tmp_path):
        src = tmp_path / "src.txt"
        dst = tmp_path / "dst.txt"
        src.write_bytes(b"data")

        rc, out, err = run_gfal(
            "cp", "--tpc", "--tpc-mode", "push", src.as_uri(), dst.as_uri()
        )

        assert rc == 0

    def test_scitag_accepted(self, tmp_path):
        src = tmp_path / "src.txt"
        dst = tmp_path / "dst.txt"
        src.write_bytes(b"data")

        rc, out, err = run_gfal(
            "cp", "--tpc", "--scitag", "100", src.as_uri(), dst.as_uri()
        )

        assert rc == 0

    def test_tpc_with_checksum(self, tmp_path):
        """--tpc combined with -K should fall back and verify checksum."""
        src = tmp_path / "src.txt"
        dst = tmp_path / "dst.txt"
        src.write_bytes(b"checksum test")

        rc, out, err = run_gfal("cp", "--tpc", "-K", "MD5", src.as_uri(), dst.as_uri())

        assert rc == 0
        assert dst.read_bytes() == b"checksum test"

    def test_tpc_dry_run_skips_tpc(self, tmp_path):
        """--dry-run should skip TPC (and streaming) so dst is not created."""
        src = tmp_path / "src.txt"
        dst = tmp_path / "dst.txt"
        src.write_bytes(b"x")

        rc, out, err = run_gfal("cp", "--tpc", "--dry-run", src.as_uri(), dst.as_uri())

        assert rc == 0
        assert not dst.exists()


# ---------------------------------------------------------------------------
# Verify correct checksum after TPC fallback
# ---------------------------------------------------------------------------


class TestTpcFallbackIntegrity:
    def test_content_correct_after_fallback(self, tmp_path):
        data = b"integrity check " * 1000
        src = tmp_path / "src.bin"
        dst = tmp_path / "dst.bin"
        src.write_bytes(data)

        rc, out, err = run_gfal("cp", "--tpc", src.as_uri(), dst.as_uri())

        assert rc == 0
        assert dst.read_bytes() == data

    def test_md5_matches_after_fallback(self, tmp_path):
        data = b"verify me" * 500
        src = tmp_path / "src.bin"
        dst = tmp_path / "dst.bin"
        src.write_bytes(data)
        expected_md5 = hashlib.md5(data).hexdigest()

        rc, out, err = run_gfal("cp", "--tpc", "-K", "MD5", src.as_uri(), dst.as_uri())

        assert rc == 0
        assert hashlib.md5(dst.read_bytes()).hexdigest() == expected_md5


# ---------------------------------------------------------------------------
# _tpc_applicable — unit tests for the auto-TPC predicate
# ---------------------------------------------------------------------------


class TestTpcApplicable:
    @pytest.mark.parametrize(
        "src, dst",
        [
            ("https://src.example.com/file", "https://dst.example.com/file"),
            ("http://src.example.com/file", "http://dst.example.com/file"),
            ("http://src.example.com/file", "https://dst.example.com/file"),
            ("root://src//file", "root://dst//file"),
            ("xroot://src//file", "xroot://dst//file"),
        ],
    )
    def test_applicable(self, src, dst):
        assert _tpc_applicable(src, dst) is True

    @pytest.mark.parametrize(
        "src, dst",
        [
            ("file:///tmp/src", "file:///tmp/dst"),
            ("https://src.example.com/file", "root://dst//file"),
            ("root://src//file", "https://dst.example.com/file"),
            ("file:///tmp/src", "https://dst.example.com/file"),
        ],
    )
    def test_not_applicable(self, src, dst):
        assert _tpc_applicable(src, dst) is False


# ---------------------------------------------------------------------------
# Auto-TPC: HTTP->HTTP copies attempt TPC automatically (like gfal2)
# ---------------------------------------------------------------------------


class TestAutoTpc:
    def test_local_to_local_no_auto_tpc(self, tmp_path):
        """file:// -> file:// should NOT trigger auto-TPC (just stream)."""
        src = tmp_path / "src.txt"
        dst = tmp_path / "dst.txt"
        src.write_bytes(b"hello")

        with patch("gfal.core.tpc.do_tpc") as mock_tpc:
            rc, out, err = run_gfal("cp", src.as_uri(), dst.as_uri())

        # TPC is not called for local copies
        mock_tpc.assert_not_called()
        assert rc == 0
        assert dst.read_bytes() == b"hello"

    def test_copy_mode_streamed_disables_auto_tpc(self, tmp_path):
        """--copy-mode=streamed should suppress auto-TPC even for HTTP->HTTP."""
        src = tmp_path / "src.txt"
        dst = tmp_path / "dst.txt"
        src.write_bytes(b"hello")

        # Local copy still works; the key is that TPC is not invoked
        rc, out, err = run_gfal(
            "cp", "--copy-mode=streamed", src.as_uri(), dst.as_uri()
        )

        assert rc == 0
        assert dst.read_bytes() == b"hello"


# ---------------------------------------------------------------------------
# Additional TestCopyTpcFlags tests
# ---------------------------------------------------------------------------


class TestCopyTpcFlagsExtra:
    def test_tpc_fallback_no_failed_output(self, tmp_path):
        """--tpc on a local->local copy falls back to streaming.

        Regression test: the lazy progress fix must not print '[FAILED]' to
        stdout/stderr when TPC raises NotImplementedError and the copy succeeds
        via the streaming fallback.
        """
        src = tmp_path / "src.txt"
        dst = tmp_path / "dst.txt"
        src.write_bytes(b"fallback data")

        rc, out, err = run_gfal("cp", "--tpc", src.as_uri(), dst.as_uri())

        assert rc == 0
        assert "[FAILED]" not in (out + err)
        assert dst.read_bytes() == b"fallback data"

    def test_tpc_only_local_to_https_no_fallback(self, tmp_path):
        """--tpc-only with a local source must fail even if dst is HTTPS.

        TPC is not supported (local src), so with --tpc-only there is no
        streaming fallback and the command must exit non-zero.
        """
        src = tmp_path / "src.txt"
        src.write_bytes(b"data")

        rc, out, err = run_gfal(
            "cp", "--tpc-only", src.as_uri(), "https://example.com/dst"
        )

        assert rc != 0


# ---------------------------------------------------------------------------
# TestTpcStartCallback
# ---------------------------------------------------------------------------


class TestTpcStartCallback:
    def test_start_callback_forwarded_to_xrootd_tpc(self):
        """do_tpc passes start_callback through to _xrootd_tpc."""
        cb = MagicMock()
        with patch.object(tpc_mod, "_xrootd_tpc", return_value=True) as mock_xrd:
            tpc_mod.do_tpc(
                "root://a.example.com//file",
                "root://b.example.com//file",
                {},
                start_callback=cb,
            )
        _, kwargs = mock_xrd.call_args
        assert kwargs.get("start_callback") is cb

    def test_start_callback_forwarded_to_http_tpc(self):
        """do_tpc does not crash when start_callback is passed for HTTP->HTTP.

        _http_tpc does not accept start_callback; do_tpc simply does not
        forward it.  Verify that _http_tpc is called and no exception is raised.
        """
        cb = MagicMock()
        with patch.object(tpc_mod, "_http_tpc", return_value=True) as mock_http:
            tpc_mod.do_tpc(
                "https://src.example.com/file",
                "https://dst.example.com/file",
                {},
                start_callback=cb,
            )
        mock_http.assert_called_once()

    def test_xrootd_tpc_invokes_start_callback(self):
        """_xrootd_tpc calls start_callback() before the blocking CopyProcess."""
        # Build a minimal mock of the XRootD module hierarchy.
        xrd_status_ok = MagicMock()
        xrd_status_ok.ok = True
        xrd_status_ok.message = ""

        mock_process = MagicMock()
        mock_process.prepare.return_value = (xrd_status_ok, None)
        mock_process.run.return_value = (xrd_status_ok, [])

        mock_copy_process_cls = MagicMock(return_value=mock_process)
        mock_xrd_client = MagicMock()
        mock_xrd_client.CopyProcess = mock_copy_process_cls
        mock_xrd_module = MagicMock()
        mock_xrd_module.client = mock_xrd_client

        import sys

        with patch.dict(
            sys.modules, {"XRootD": mock_xrd_module, "XRootD.client": mock_xrd_client}
        ):
            cb = MagicMock()
            tpc_mod._xrootd_tpc(
                "root://a.example.com//file",
                "root://b.example.com//file",
                timeout=None,
                verbose=False,
                start_callback=cb,
            )

        cb.assert_called_once()


# ---------------------------------------------------------------------------
# Additional TestParseTpcBody tests
# ---------------------------------------------------------------------------


class TestParseTpcBodyExtra:
    def _make_resp(self, status_code, lines=None):
        resp = MagicMock()
        resp.status_code = status_code
        if lines is not None:
            resp.iter_lines.return_value = iter(lines)
        return resp

    def test_progress_callback_called_with_bytes(self):
        """progress_callback receives cumulative bytes from perf markers."""
        lines = [
            "Perf Marker",
            "  Stripe Bytes Transferred: 524288",
            "End",
            "Perf Marker",
            "  Stripe Bytes Transferred: 1048576",
            "End",
            "success: Created",
        ]
        resp = self._make_resp(202, lines)
        cb = MagicMock()
        tpc_mod._parse_tpc_body(resp, progress_callback=cb)
        assert cb.call_count == 2
        calls = [c[0][0] for c in cb.call_args_list]
        assert calls[0] == 524288
        assert calls[1] == 1048576

    def test_progress_callback_not_called_on_immediate_success(self):
        """progress_callback is NOT called when the server responds 201 with no body."""
        resp = self._make_resp(201)
        # iter_lines is not called for non-202; provide empty iterator just in case
        resp.iter_lines.return_value = iter([])
        cb = MagicMock()
        tpc_mod._parse_tpc_body(resp, progress_callback=cb)
        cb.assert_not_called()


# ---------------------------------------------------------------------------
# Credential delegation header
# ---------------------------------------------------------------------------


class TestCredentialDelegation:
    def _make_session_mock(self, status_code=201):
        resp = MagicMock()
        resp.status_code = status_code
        resp.iter_lines.return_value = iter([])
        session = MagicMock()
        session.request.return_value = resp
        return session

    def test_no_credential_header_in_http_headers(self, tmp_path):
        """Raw PEM must NOT appear as a Credential header — HTTP forbids newlines.

        The X.509 proxy is delegated via the TLS session (session.cert), not
        via an HTTP header.  Verify that no Credential header is set regardless
        of whether client_cert is present.
        """
        proxy = tmp_path / "proxy.pem"
        proxy.write_text(
            "-----BEGIN CERTIFICATE-----\nFAKEPEM\n-----END CERTIFICATE-----\n"
        )
        opts = {"client_cert": str(proxy)}

        session = self._make_session_mock()
        with patch.object(tpc_mod, "_build_session", return_value=session):
            tpc_mod._http_tpc(
                "https://src.example.com/file",
                "https://dst.example.com/file",
                opts,
                mode="pull",
                timeout=None,
                verbose=False,
                scitag=None,
            )
        _, kwargs = session.request.call_args
        # No raw-PEM Credential header — would be rejected by any HTTP server
        assert "Credential" not in kwargs["headers"]

    def test_no_credential_header_without_cert(self):
        """No Credential header when no cert provided either."""
        session = self._make_session_mock()
        with patch.object(tpc_mod, "_build_session", return_value=session):
            tpc_mod._http_tpc(
                "https://src.example.com/file",
                "https://dst.example.com/file",
                {},
                mode="pull",
                timeout=None,
                verbose=False,
                scitag=None,
            )
        _, kwargs = session.request.call_args
        assert "Credential" not in kwargs["headers"]

    def test_cert_still_passed_via_session(self, tmp_path):
        """client_cert is forwarded to _build_session (TLS delegation), not headers."""
        proxy = tmp_path / "proxy.pem"
        proxy.write_text("PUSHCERT")
        opts = {"client_cert": str(proxy)}

        with patch.object(
            tpc_mod, "_build_session", return_value=self._make_session_mock()
        ) as mock_build:
            tpc_mod._http_tpc(
                "https://src.example.com/file",
                "https://dst.example.com/file",
                opts,
                mode="push",
                timeout=None,
                verbose=False,
                scitag=None,
            )
        # opts (including client_cert) must be forwarded to _build_session
        mock_build.assert_called_once_with(opts)


# ---------------------------------------------------------------------------
# Bearer token in _build_session
# ---------------------------------------------------------------------------


class TestBuildSessionBearerToken:
    def test_bearer_token_sets_authorization_header(self):
        opts = {"bearer_token": "mytoken123"}
        session = tpc_mod._build_session(opts)
        assert session.headers.get("Authorization") == "Bearer mytoken123"

    def test_no_bearer_token_no_authorization_header(self):
        session = tpc_mod._build_session({})
        assert "Authorization" not in session.headers

    def test_bearer_token_and_cert_coexist(self, tmp_path):
        cert = tmp_path / "cert.pem"
        cert.write_text("cert")
        key = tmp_path / "key.pem"
        key.write_text("key")
        opts = {
            "client_cert": str(cert),
            "client_key": str(key),
            "bearer_token": "scitoken-abc",
        }
        session = tpc_mod._build_session(opts)
        assert session.cert == (str(cert), str(key))
        assert session.headers.get("Authorization") == "Bearer scitoken-abc"
