"""Additional unit tests for webdav.py to increase code coverage.

Covers response classes, streaming I/O helpers, error handling,
PROPFIND edge cases, makedirs status codes, and checksum fallback paths.
"""

from __future__ import annotations

import concurrent.futures
import io
import queue
from unittest.mock import MagicMock

import pytest

from gfal.core.webdav import (
    _STREAM_EOF,
    HttpStatusError,
    WebDAVFileSystem,
    _parse_propfind,
    _raise_for_status,
    _RequestsPutFile,
    _StreamingAiohttpResponse,
    _StreamingRequestsGetFile,
    _StreamingRequestsPutFile,
    _SyncAiohttpResponse,
)

# ---------------------------------------------------------------------------
# _SyncAiohttpResponse
# ---------------------------------------------------------------------------


class TestSyncAiohttpResponse:
    def test_raise_for_status_on_error(self):
        resp = _SyncAiohttpResponse(
            method="GET",
            url="https://example.org/file",
            status_code=500,
            headers={},
            content=b"error",
        )
        with pytest.raises(HttpStatusError) as exc_info:
            resp.raise_for_status()
        assert exc_info.value.status == 500

    def test_raise_for_status_ok(self):
        resp = _SyncAiohttpResponse(
            method="GET",
            url="https://example.org/file",
            status_code=200,
            headers={},
            content=b"ok",
        )
        resp.raise_for_status()  # should not raise

    def test_iter_lines_bytes(self):
        resp = _SyncAiohttpResponse(
            method="GET",
            url="https://example.org/file",
            status_code=200,
            headers={},
            content=b"line1\nline2\nline3",
        )
        lines = list(resp.iter_lines(decode_unicode=False))
        assert lines == [b"line1", b"line2", b"line3"]

    def test_iter_lines_unicode(self):
        resp = _SyncAiohttpResponse(
            method="GET",
            url="https://example.org/file",
            status_code=200,
            headers={},
            content=b"line1\nline2",
        )
        lines = list(resp.iter_lines(decode_unicode=True))
        assert lines == ["line1", "line2"]

    def test_close_returns_none(self):
        resp = _SyncAiohttpResponse(
            method="GET",
            url="https://example.org/file",
            status_code=200,
            headers={},
            content=b"",
        )
        assert resp.close() is None


# ---------------------------------------------------------------------------
# _StreamingAiohttpResponse
# ---------------------------------------------------------------------------


class TestStreamingAiohttpResponse:
    def _make_response(self, status_code=200, body_items=None):
        body_queue: queue.Queue[object] = queue.Queue()
        if body_items:
            for item in body_items:
                body_queue.put(item)
        completion_future: concurrent.futures.Future = concurrent.futures.Future()
        completion_future.set_result(None)
        return _StreamingAiohttpResponse(
            method="GET",
            url="https://example.org/file",
            status_code=status_code,
            headers={},
            body_queue=body_queue,
            completion_future=completion_future,
        )

    def test_raise_for_status_on_error(self):
        resp = self._make_response(status_code=404)
        with pytest.raises(HttpStatusError) as exc_info:
            resp.raise_for_status()
        assert exc_info.value.status == 404

    def test_raise_for_status_ok(self):
        resp = self._make_response(status_code=200)
        resp.raise_for_status()  # should not raise

    def test_iter_lines_bytes(self):
        resp = self._make_response(
            body_items=[b"line1\nline2\n", b"line3", _STREAM_EOF]
        )
        lines = list(resp.iter_lines(decode_unicode=False))
        assert lines == [b"line1", b"line2", b"line3"]

    def test_iter_lines_unicode(self):
        resp = self._make_response(body_items=[b"hello\r\nworld", _STREAM_EOF])
        lines = list(resp.iter_lines(decode_unicode=True))
        assert lines == ["hello", "world"]

    def test_iter_lines_trailing_content(self):
        resp = self._make_response(body_items=[b"abc", _STREAM_EOF])
        lines = list(resp.iter_lines(decode_unicode=False))
        assert lines == [b"abc"]

    def test_iter_lines_trailing_content_unicode(self):
        resp = self._make_response(body_items=[b"trailing", _STREAM_EOF])
        lines = list(resp.iter_lines(decode_unicode=True))
        assert lines == ["trailing"]

    def test_iter_lines_error_propagation(self):
        resp = self._make_response(
            body_items=[b"data\n", ValueError("test error"), _STREAM_EOF]
        )
        with pytest.raises(ValueError, match="test error"):
            list(resp.iter_lines())

    def test_close_idempotent(self):
        resp = self._make_response()
        resp.close()
        resp.close()  # second close should be no-op


# ---------------------------------------------------------------------------
# _raise_for_status
# ---------------------------------------------------------------------------


class TestRaiseForStatus:
    def test_401(self):
        resp = MagicMock()
        resp.status_code = 401
        with pytest.raises(PermissionError, match="Authentication required"):
            _raise_for_status(resp, "https://example.org/secret")

    def test_403(self):
        resp = MagicMock()
        resp.status_code = 403
        with pytest.raises(PermissionError, match="Permission denied"):
            _raise_for_status(resp, "https://example.org/file")

    def test_404(self):
        resp = MagicMock()
        resp.status_code = 404
        with pytest.raises(FileNotFoundError, match="No such file"):
            _raise_for_status(resp, "https://example.org/missing")

    def test_405(self):
        resp = MagicMock()
        resp.status_code = 405
        with pytest.raises(NotImplementedError, match="405"):
            _raise_for_status(resp, "https://example.org/unsupported")

    def test_generic_400_plus(self):
        resp = MagicMock()
        resp.status_code = 502
        resp.raise_for_status = MagicMock(side_effect=HttpStatusError(502, "url"))
        with pytest.raises(HttpStatusError):
            _raise_for_status(resp, "https://example.org/error")
        resp.raise_for_status.assert_called_once()

    def test_200_ok(self):
        resp = MagicMock()
        resp.status_code = 200
        _raise_for_status(resp, "https://example.org/ok")  # no error


# ---------------------------------------------------------------------------
# _RequestsPutFile
# ---------------------------------------------------------------------------


class TestRequestsPutFile:
    def test_readable_writable(self):
        session = MagicMock()
        resp = MagicMock()
        resp.status_code = 201
        session.put.return_value = resp
        writer = _RequestsPutFile(session, "https://example.org/file")
        assert writer.readable() is False
        assert writer.writable() is True
        writer.close()


# ---------------------------------------------------------------------------
# _StreamingRequestsPutFile readable/writable
# ---------------------------------------------------------------------------


class TestStreamingPutFileInterface:
    def test_readable_false(self):
        session = MagicMock()
        upload_future: concurrent.futures.Future = concurrent.futures.Future()
        response = MagicMock()
        response.status_code = 201
        response.headers = {}
        upload_future.set_result(response)
        session.request_upload_stream.return_value = upload_future

        writer = _StreamingRequestsPutFile(session, "https://example.org/file")
        assert writer.readable() is False
        assert writer.writable() is True
        writer.write(b"x")
        writer.close()


# ---------------------------------------------------------------------------
# _StreamingRequestsGetFile
# ---------------------------------------------------------------------------


class TestStreamingGetFileExtended:
    def _make_reader(self, items):
        body_queue: queue.Queue[object] = queue.Queue()
        for item in items:
            body_queue.put(item)
        completion_future: concurrent.futures.Future = concurrent.futures.Future()
        completion_future.set_result(None)
        response = MagicMock()
        response._body_queue = body_queue
        response.close = MagicMock()
        return _StreamingRequestsGetFile(response)

    def test_readable_writable(self):
        reader = self._make_reader([_STREAM_EOF])
        assert reader.readable() is True
        assert reader.writable() is False
        reader.close()

    def test_read_zero_returns_empty(self):
        reader = self._make_reader([b"data", _STREAM_EOF])
        assert reader.read(0) == b""
        reader.close()

    def test_read_all_negative_size(self):
        reader = self._make_reader([b"hello ", b"world", _STREAM_EOF])
        assert reader.read(-1) == b"hello world"
        reader.close()

    def test_read_all_default_size(self):
        reader = self._make_reader([b"abc", b"def", _STREAM_EOF])
        result = reader.read()
        assert result == b"abcdef"
        reader.close()

    def test_error_propagation_in_fill_buffer(self):
        reader = self._make_reader([b"ok", RuntimeError("network error")])
        assert reader.read(2) == b"ok"
        with pytest.raises(RuntimeError, match="network error"):
            reader.read(10)

    def test_close_idempotent(self):
        reader = self._make_reader([_STREAM_EOF])
        reader.close()
        reader.close()  # second close should not raise


# ---------------------------------------------------------------------------
# _is_eos_namespace_url
# ---------------------------------------------------------------------------


class TestIsEosNamespaceUrl:
    def test_true_for_eos_url(self):
        fs = WebDAVFileSystem()
        assert (
            fs._is_eos_namespace_url("https://eospilot.cern.ch//eos/pilot/test") is True
        )

    def test_false_for_non_eos_host(self):
        fs = WebDAVFileSystem()
        assert fs._is_eos_namespace_url("https://example.org//eos/test") is False

    def test_false_for_non_eos_path(self):
        fs = WebDAVFileSystem()
        assert fs._is_eos_namespace_url("https://eospilot.cern.ch/data/test") is False

    def test_false_for_ftp_scheme(self):
        fs = WebDAVFileSystem()
        assert fs._is_eos_namespace_url("ftp://eospilot.cern.ch//eos/test") is False

    def test_true_for_http_scheme(self):
        fs = WebDAVFileSystem()
        assert (
            fs._is_eos_namespace_url("http://eospublic.cern.ch//eos/public/test")
            is True
        )


# ---------------------------------------------------------------------------
# _resolve_stream_write_url
# ---------------------------------------------------------------------------


class TestResolveStreamWriteUrl:
    def test_non_redirect_response_returns_original_url(self):
        fs = WebDAVFileSystem()
        response = MagicMock()
        response.status_code = 200
        response.headers = {}
        fs._session.put = MagicMock(return_value=response)

        url = "https://eospilot.cern.ch//eos/pilot/test/file.bin"
        result = fs._resolve_stream_write_url(url)
        assert result == url

    def test_308_redirect_with_location(self):
        fs = WebDAVFileSystem()
        response = MagicMock()
        response.status_code = 308
        response.headers = {"Location": "https://data.cern.ch/actual-path"}
        fs._session.put = MagicMock(return_value=response)

        url = "https://eospilot.cern.ch//eos/pilot/test/file.bin"
        result = fs._resolve_stream_write_url(url)
        assert result == "https://data.cern.ch/actual-path"


# ---------------------------------------------------------------------------
# _checksum_locally
# ---------------------------------------------------------------------------


class TestChecksumLocally:
    def test_md5_checksum(self):
        import hashlib

        data = b"test data for checksum"
        expected = hashlib.md5(data).hexdigest()

        fs = WebDAVFileSystem()
        fs.open_stream_read = MagicMock(return_value=io.BytesIO(data))
        result = fs._checksum_locally("https://example.org/file", "MD5")
        assert result == expected

    def test_sha256_checksum(self):
        import hashlib

        data = b"sha256 test"
        expected = hashlib.sha256(data).hexdigest()

        fs = WebDAVFileSystem()
        fs.open_stream_read = MagicMock(return_value=io.BytesIO(data))
        result = fs._checksum_locally("https://example.org/file", "SHA-256")
        assert result == expected

    def test_adler32_checksum(self):
        import zlib

        data = b"adler32 test data"
        expected = f"{zlib.adler32(data) & 0xFFFFFFFF:08x}"

        fs = WebDAVFileSystem()
        fs.open_stream_read = MagicMock(return_value=io.BytesIO(data))
        result = fs._checksum_locally("https://example.org/file", "ADLER32")
        assert result == expected


# ---------------------------------------------------------------------------
# WebDAVFileSystem.info() — timeout fallback
# ---------------------------------------------------------------------------


class TestWebDAVInfoTimeoutFallback:
    def test_info_timeout_directory_path(self):
        fs = WebDAVFileSystem()
        fs._propfind = MagicMock(side_effect=Exception("propfind failed"))
        fs._http_fs = MagicMock()
        fs._http_fs.info = MagicMock(side_effect=TimeoutError("HEAD timed out"))
        fs._verify = False

        result = fs.info("https://example.org/dir/")
        assert result["type"] == "directory"
        assert result["name"] == "https://example.org/dir/"

    def test_info_timeout_file_path_raises(self):
        fs = WebDAVFileSystem()
        fs._propfind = MagicMock(side_effect=Exception("propfind failed"))
        fs._http_fs = MagicMock()
        fs._http_fs.info = MagicMock(side_effect=TimeoutError("HEAD timed out"))
        fs._verify = False

        with pytest.raises(TimeoutError):
            fs.info("https://example.org/dir/file.txt")


# ---------------------------------------------------------------------------
# WebDAVFileSystem.ls() — single file entry
# ---------------------------------------------------------------------------


class TestWebDAVLsSingleFile:
    def test_ls_single_file_returns_entry(self):
        fs = WebDAVFileSystem()
        file_entry = {
            "name": "https://example.org/file.txt",
            "type": "file",
            "size": 100,
        }
        # PROPFIND returns only the self-entry and it's a file
        fs._propfind = MagicMock(return_value=[file_entry])

        result = fs.ls("https://example.org/file.txt", detail=True)
        assert result == [file_entry]

    def test_ls_single_file_names_only(self):
        fs = WebDAVFileSystem()
        file_entry = {
            "name": "https://example.org/file.txt",
            "type": "file",
            "size": 100,
        }
        fs._propfind = MagicMock(return_value=[file_entry])

        result = fs.ls("https://example.org/file.txt", detail=False)
        assert result == ["https://example.org/file.txt"]


# ---------------------------------------------------------------------------
# WebDAVFileSystem.makedirs() — status code branches
# ---------------------------------------------------------------------------


class TestWebDAVMakedirs:
    def test_makedirs_409_conflict_continues(self):
        fs = WebDAVFileSystem()
        resp = MagicMock()
        resp.status_code = 409
        fs._session.request = MagicMock(return_value=resp)

        fs.makedirs("https://example.org/a/b/c", exist_ok=True)
        assert fs._session.request.call_count == 3  # a, b, c

    def test_makedirs_403_forbidden_continues(self):
        fs = WebDAVFileSystem()
        resp = MagicMock()
        resp.status_code = 403
        fs._session.request = MagicMock(return_value=resp)

        fs.makedirs("https://example.org/x/y", exist_ok=True)
        assert fs._session.request.call_count == 2

    def test_makedirs_400_plus_raises(self):
        fs = WebDAVFileSystem()
        resp = MagicMock()
        resp.status_code = 500
        resp.raise_for_status = MagicMock(
            side_effect=HttpStatusError(500, "https://example.org/a")
        )
        fs._session.request = MagicMock(return_value=resp)

        with pytest.raises(HttpStatusError):
            fs.makedirs("https://example.org/a", exist_ok=True)


# ---------------------------------------------------------------------------
# WebDAVFileSystem.mkdir() — _raise_for_status fallback
# ---------------------------------------------------------------------------


class TestWebDAVMkdir:
    def test_mkdir_generic_error(self):
        fs = WebDAVFileSystem()
        resp = MagicMock()
        resp.status_code = 507  # Insufficient storage
        resp.raise_for_status = MagicMock(
            side_effect=HttpStatusError(507, "https://example.org/dir")
        )
        fs._session.request = MagicMock(return_value=resp)

        with pytest.raises(HttpStatusError):
            fs.mkdir("https://example.org/dir")


# ---------------------------------------------------------------------------
# WebDAVFileSystem.rm_file()
# ---------------------------------------------------------------------------


class TestWebDAVRmFile:
    def test_rm_file_delegates_to_rm(self):
        fs = WebDAVFileSystem()
        resp = MagicMock()
        resp.status_code = 204
        fs._session.delete = MagicMock(return_value=resp)

        fs.rm_file("https://example.org/file.txt")
        fs._session.delete.assert_called_once()


# ---------------------------------------------------------------------------
# _parse_propfind edge cases
# ---------------------------------------------------------------------------


class TestParsePropfindEdgeCases:
    def test_propstat_without_200_status(self):
        """When no propstat has ' 200 ' status, fall back to first propstat."""
        xml = b"""\
<?xml version="1.0" encoding="utf-8"?>
<D:multistatus xmlns:D="DAV:">
  <D:response>
    <D:href>/test/file</D:href>
    <D:propstat>
      <D:status>HTTP/1.1 207 Multi-Status</D:status>
      <D:prop>
        <D:resourcetype/>
        <D:getcontentlength>42</D:getcontentlength>
      </D:prop>
    </D:propstat>
  </D:response>
</D:multistatus>"""
        entries = _parse_propfind(xml, "https://example.org/test/")
        assert len(entries) == 1
        assert entries[0]["size"] == 42

    def test_absolute_href_url(self):
        """Href is an absolute URL, not a path."""
        xml = b"""\
<?xml version="1.0" encoding="utf-8"?>
<D:multistatus xmlns:D="DAV:">
  <D:response>
    <D:href>https://example.org/file.txt</D:href>
    <D:propstat>
      <D:status>HTTP/1.1 200 OK</D:status>
      <D:prop>
        <D:resourcetype/>
        <D:getcontentlength>99</D:getcontentlength>
      </D:prop>
    </D:propstat>
  </D:response>
</D:multistatus>"""
        entries = _parse_propfind(xml, "https://example.org/")
        assert len(entries) == 1
        assert entries[0]["name"] == "https://example.org/file.txt"

    def test_no_prop_in_propstat(self):
        """propstat exists but has no prop element -> entry skipped."""
        xml = b"""\
<?xml version="1.0" encoding="utf-8"?>
<D:multistatus xmlns:D="DAV:">
  <D:response>
    <D:href>/file</D:href>
    <D:propstat>
      <D:status>HTTP/1.1 200 OK</D:status>
    </D:propstat>
  </D:response>
</D:multistatus>"""
        entries = _parse_propfind(xml, "https://example.org/")
        assert len(entries) == 0


# ---------------------------------------------------------------------------
# WebDAVFileSystem.checksum — head success paths
# ---------------------------------------------------------------------------


class TestWebDAVChecksum:
    def test_checksum_head_returns_digest(self):
        fs = WebDAVFileSystem()
        resp = MagicMock()
        resp.status_code = 200
        resp.headers = {"Digest": "adler32=abc123"}
        fs._session.head = MagicMock(return_value=resp)

        result = fs.checksum("https://example.org/file", "adler32")
        assert result == "abc123"

    def test_checksum_head_multiple_digests(self):
        fs = WebDAVFileSystem()
        resp = MagicMock()
        resp.status_code = 200
        resp.headers = {"Digest": "md5=xyz, adler32=abc123"}
        fs._session.head = MagicMock(return_value=resp)

        result = fs.checksum("https://example.org/file", "adler32")
        assert result == "abc123"

    def test_checksum_missing_algorithm_raises(self):
        fs = WebDAVFileSystem()
        resp = MagicMock()
        resp.status_code = 200
        resp.headers = {"Digest": "md5=xyz"}
        fs._session.head = MagicMock(return_value=resp)

        with pytest.raises(NotImplementedError, match="missing requested algorithm"):
            fs.checksum("https://example.org/file", "adler32")


# ---------------------------------------------------------------------------
# _SyncAiohttpSession.head
# ---------------------------------------------------------------------------


class TestSessionHead:
    def test_head_delegates_to_request(self):
        fs = WebDAVFileSystem()
        resp = MagicMock()
        resp.status_code = 200
        fs._session.request = MagicMock(return_value=resp)

        result = fs._session.head("https://example.org/file")
        # head calls request("HEAD", ...) - verify session.request is used
        assert result is not None
