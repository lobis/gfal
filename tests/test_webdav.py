"""
Unit tests for the WebDAV filesystem layer (webdav.py).

A lightweight in-process HTTP server that handles WebDAV methods
(PROPFIND / MKCOL / DELETE / MOVE) is started once per test session.
No external network access is required.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import contextlib
import io
import posixpath
import queue
import socket
import ssl
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from unittest.mock import MagicMock

import aiohttp
import pytest

from gfal.core.webdav import (
    _STREAM_EOF,
    WebDAVFileSystem,
    _ensure_collection_url,
    _http_fs_opts,
    _norm_url,
    _parse_propfind,
    _should_suppress_loop_exception,
    _StreamingRequestsGetFile,
    _StreamingRequestsPutFile,
    _SyncAiohttpSession,
)

# ---------------------------------------------------------------------------
# Minimal mock WebDAV server
# ---------------------------------------------------------------------------

# Shared in-memory filesystem: set of paths that exist; entries whose name
# ends with '/' are directories.
_vfs: set[str] = set()
_vfs_lock = threading.Lock()

_PROPFIND_TMPL = """\
<?xml version="1.0" encoding="utf-8"?>
<D:multistatus xmlns:D="DAV:">
{responses}
</D:multistatus>"""

_RESPONSE_TMPL = """\
  <D:response>
    <D:href>{href}</D:href>
    <D:propstat>
      <D:prop>
        <D:resourcetype>{rtype}</D:resourcetype>
        <D:getcontentlength>{size}</D:getcontentlength>
        <D:getlastmodified>Mon, 18 Mar 2026 10:00:00 GMT</D:getlastmodified>
      </D:prop>
      <D:status>HTTP/1.1 200 OK</D:status>
    </D:propstat>
  </D:response>"""

_COLLECTION = "<D:collection/>"


def _make_propfind_response(path: str, depth: int) -> str:
    """Build a PROPFIND XML body for the in-memory VFS."""
    norm = path.rstrip("/")
    is_dir = (norm + "/") in _vfs or norm == ""

    # Depth:0 — just the resource itself
    self_type = _COLLECTION if is_dir else ""
    responses = [
        _RESPONSE_TMPL.format(
            href=path.rstrip("/") + ("/" if is_dir else ""),
            rtype=self_type,
            size=0 if is_dir else 42,
        )
    ]

    if depth == 1 and is_dir:
        for entry in sorted(_vfs):
            # Direct children only — use posixpath so it works on Windows too
            epath = entry.rstrip("/")
            parent = posixpath.dirname(epath)
            if parent.rstrip("/") != norm:
                continue
            child_is_dir = entry.endswith("/")
            responses.append(
                _RESPONSE_TMPL.format(
                    href=entry,
                    rtype=_COLLECTION if child_is_dir else "",
                    size=0 if child_is_dir else 42,
                )
            )

    return _PROPFIND_TMPL.format(responses="\n".join(responses))


class _ReuseAddrHTTPServer(ThreadingHTTPServer):
    """HTTPServer with SO_REUSEADDR to prevent Windows 'address already in use' errors."""

    allow_reuse_address = True


class _WebDAVHandler(BaseHTTPRequestHandler):
    # Enable HTTP/1.1 keep-alive to avoid connection teardown
    # races that cause spurious ConnectionAbortedError (WinError 10053)
    # on Windows when reusing heavily recycled TCP sockets.
    protocol_version = "HTTP/1.1"

    def log_message(self, *args):
        pass  # silence request logging during tests

    def do_PROPFIND(self):
        length = int(self.headers.get("Content-Length", "0"))
        if length > 0:
            self.rfile.read(length)
        with _vfs_lock:
            norm = self.path.rstrip("/")
            # 404 if not root and not in vfs at all
            if norm != "" and (norm + "/") not in _vfs and norm not in _vfs:
                self.send_response(404)
                self.send_header("Content-Length", "0")
                self.end_headers()
                return
            depth = int(self.headers.get("Depth", "0"))
            body = _make_propfind_response(self.path, depth).encode()
        self.send_response(207)
        self.send_header("Content-Type", "application/xml; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_MKCOL(self):
        path = self.path.rstrip("/") + "/"
        with _vfs_lock:
            if path in _vfs:
                self.send_response(405)
                self.send_header("Content-Length", "0")
                self.end_headers()
                return
            parent = posixpath.dirname(path.rstrip("/")).rstrip("/") + "/"
            # root is always "/"
            if parent != "/" and parent not in _vfs:
                self.send_response(409)
                self.send_header("Content-Length", "0")
                self.end_headers()
                return
            _vfs.add(path)
        self.send_response(201)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def do_DELETE(self):
        path = self.path
        norm = path.rstrip("/")
        with _vfs_lock:
            # Remove file or directory
            to_remove = {
                e for e in _vfs if e.rstrip("/") == norm or e.startswith(norm + "/")
            }
            if not to_remove:
                self.send_response(404)
                self.send_header("Content-Length", "0")
                self.end_headers()
                return
            _vfs.difference_update(to_remove)
        self.send_response(204)
        self.end_headers()

    def do_MOVE(self):
        dst = self.headers.get("Destination", "")
        if dst.startswith("http://") or dst.startswith("https://"):
            from urllib.parse import urlparse as _up

            dst = _up(dst).path
        src = self.path.rstrip("/")
        dst = dst.rstrip("/")
        with _vfs_lock:
            to_move = {
                e for e in _vfs if e.rstrip("/") == src or e.startswith(src + "/")
            }
            if not to_move:
                self.send_response(404)
                self.send_header("Content-Length", "0")
                self.end_headers()
                return
            for entry in list(to_move):
                new = dst + entry[len(src) :]
                _vfs.discard(entry)
                _vfs.add(new)
        self.send_response(201)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def do_GET(self):
        path = self.path.rstrip("/")
        with _vfs_lock:
            exists = path in _vfs or (path + "/") in _vfs
        if not exists:
            self.send_response(404)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        body = b"hello"
        self.send_response(200)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_PUT(self):
        if self.headers.get("Transfer-Encoding", "").lower() == "chunked":
            while True:
                size_line = self.rfile.readline()
                if not size_line:
                    break
                chunk_size = int(size_line.strip().split(b";", 1)[0], 16)
                if chunk_size == 0:
                    self.rfile.readline()
                    break
                self.rfile.read(chunk_size)
                self.rfile.read(2)
        else:
            length = int(self.headers.get("Content-Length", 0))
            self.rfile.read(length)
        with _vfs_lock:
            _vfs.add(self.path.rstrip("/"))
        self.send_response(201)
        self.send_header("Content-Length", "0")
        self.end_headers()


@pytest.fixture(scope="session")
def dav_server():
    """Start a mock WebDAV server and return its base URL."""
    server = _ReuseAddrHTTPServer(("127.0.0.1", 0), _WebDAVHandler)
    port = server.server_address[1]
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    # Wait until the server is actually accepting connections (important on Windows
    # where thread scheduling lag can cause the first request to arrive before
    # serve_forever() has entered its select loop).
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        with socket.socket() as s:
            if s.connect_ex(("127.0.0.1", port)) == 0:
                break
        time.sleep(0.02)
    yield f"http://127.0.0.1:{port}"
    server.shutdown()


@pytest.fixture(autouse=True)
def reset_vfs():
    """Reset the in-memory VFS before every test."""
    with _vfs_lock:
        _vfs.clear()
    yield


# ---------------------------------------------------------------------------
# _parse_propfind unit tests
# ---------------------------------------------------------------------------


class TestParsePropfind:
    def test_relative_href_does_not_inherit_authz_query(self):
        xml = b"""\
<?xml version="1.0"?>
<D:multistatus xmlns:D="DAV:">
  <D:response>
    <D:href>/eos/pilot/test/file.txt</D:href>
    <D:propstat>
      <D:prop>
        <D:resourcetype/>
        <D:getcontentlength>1234</D:getcontentlength>
      </D:prop>
      <D:status>HTTP/1.1 200 OK</D:status>
    </D:propstat>
  </D:response>
</D:multistatus>"""

        entries = _parse_propfind(
            xml, "https://eospilot.cern.ch//eos/pilot/test/?authz=zteos64%3Aabc"
        )

        assert entries[0]["name"] == "https://eospilot.cern.ch/eos/pilot/test/file.txt"

    def test_file_entry(self):
        xml = b"""\
<?xml version="1.0"?>
<D:multistatus xmlns:D="DAV:">
  <D:response>
    <D:href>/file.txt</D:href>
    <D:propstat>
      <D:prop>
        <D:resourcetype/>
        <D:getcontentlength>1234</D:getcontentlength>
        <D:getlastmodified>Mon, 18 Mar 2026 10:00:00 GMT</D:getlastmodified>
      </D:prop>
      <D:status>HTTP/1.1 200 OK</D:status>
    </D:propstat>
  </D:response>
</D:multistatus>"""
        entries = _parse_propfind(xml, "http://server/file.txt")
        assert len(entries) == 1
        e = entries[0]
        assert e["type"] == "file"
        assert e["size"] == 1234
        assert e["mtime"] > 0

    def test_directory_entry(self):
        xml = b"""\
<?xml version="1.0"?>
<D:multistatus xmlns:D="DAV:">
  <D:response>
    <D:href>/mydir/</D:href>
    <D:propstat>
      <D:prop>
        <D:resourcetype><D:collection/></D:resourcetype>
        <D:getcontentlength>0</D:getcontentlength>
      </D:prop>
      <D:status>HTTP/1.1 200 OK</D:status>
    </D:propstat>
  </D:response>
</D:multistatus>"""
        entries = _parse_propfind(xml, "http://server/mydir/")
        assert len(entries) == 1
        assert entries[0]["type"] == "directory"

    def test_malformed_xml_returns_empty(self):
        assert _parse_propfind(b"not xml at all", "http://server/") == []

    def test_missing_href_skipped(self):
        xml = b"""\
<?xml version="1.0"?>
<D:multistatus xmlns:D="DAV:">
  <D:response>
    <D:propstat><D:prop/></D:propstat>
  </D:response>
</D:multistatus>"""
        assert _parse_propfind(xml, "http://server/") == []


class TestWebDAVUrlHelpers:
    def test_ensure_collection_url_preserves_authz_query(self):
        url = "https://eospilot.cern.ch//eos/pilot/test?authz=zteos64%3Aabc"

        result = _ensure_collection_url(url)

        assert result == "https://eospilot.cern.ch//eos/pilot/test/?authz=zteos64%3Aabc"

    def test_norm_url_ignores_authz_query_for_identity(self):
        assert _norm_url(
            "https://eospilot.cern.ch//eos/pilot/test/?authz=zteos64%3Aabc"
        ) == _norm_url("https://eospilot.cern.ch/eos/pilot/test/")


class TestHttpFsOpts:
    def test_http_fs_opts_passes_ipv4_only(self):
        opts = _http_fs_opts({
            "ssl_verify": True,
            "client_cert": "/tmp/cert.pem",
            "client_key": "/tmp/key.pem",
            "timeout": 12,
            "ipv4_only": True,
        })

        get_client = opts["get_client"]
        assert get_client.keywords["client_cert"] == "/tmp/cert.pem"
        assert get_client.keywords["client_key"] == "/tmp/key.pem"
        assert get_client.keywords["timeout"] == 12
        assert get_client.keywords["ipv4_only"] is True
        assert get_client.keywords["ipv6_only"] is False

    def test_http_fs_opts_passes_ipv6_only_without_verify(self):
        opts = _http_fs_opts({"ssl_verify": False, "ipv6_only": True})

        get_client = opts["get_client"]
        assert get_client.func.__name__ == "_no_verify_get_client"
        assert get_client.keywords["ipv4_only"] is False
        assert get_client.keywords["ipv6_only"] is True

    def test_http_fs_opts_passes_bearer_header_to_http_client(self):
        opts = _http_fs_opts({
            "bearer_token": "token-123",
            "timeout": 9,
            "anon": True,
        })

        assert opts["anon"] is True
        assert "bearer_token" not in opts
        assert "timeout" not in opts
        get_client = opts["get_client"]
        assert get_client.keywords["headers"] == {"Authorization": "Bearer token-123"}
        assert get_client.keywords["timeout"] == 9


class TestSyncAiohttpSession:
    def test_loads_client_cert_chain_into_ssl_context(self, monkeypatch):
        fake_context = MagicMock()

        monkeypatch.setattr(
            "gfal.core.webdav._make_ssl_context", lambda verify: fake_context
        )

        session = _SyncAiohttpSession({
            "ssl_verify": False,
            "client_cert": "/tmp/usercert.pem",
            "client_key": "/tmp/userkey.pem",
        })

        fake_context.load_cert_chain.assert_called_once_with(
            "/tmp/usercert.pem", "/tmp/userkey.pem"
        )
        assert session.cert == ("/tmp/usercert.pem", "/tmp/userkey.pem")

    def test_suppresses_connection_lost_future_warning(self):
        err = ConnectionError("Connection lost")

        assert _should_suppress_loop_exception({
            "message": "Future exception was never retrieved",
            "exception": err,
        })

    def test_does_not_suppress_other_future_warnings(self):
        assert not _should_suppress_loop_exception({
            "message": "Future exception was never retrieved",
            "exception": RuntimeError("boom"),
        })

    def test_does_not_suppress_other_loop_messages(self):
        assert not _should_suppress_loop_exception({
            "message": "Unhandled exception in event loop",
            "exception": ConnectionError("Connection lost"),
        })

    def test_request_async_times_out_session_close(self, monkeypatch):
        class _FakeResponse:
            status = 201
            headers = {"Content-Length": "0"}
            url = "https://example.org/file"

            async def read(self):
                return b""

            def close(self):
                return None

        class _FakeSession:
            def __init__(self):
                self.closed = False
                self.close_cancelled = False

            async def request(
                self,
                method,
                url,
                headers=None,
                data=None,
                allow_redirects=True,
            ):
                del method, url, headers, data, allow_redirects
                return _FakeResponse()

            async def close(self):
                try:
                    await asyncio.sleep(10)
                except asyncio.CancelledError:
                    self.close_cancelled = True
                    raise
                finally:
                    self.closed = True

        fake_session = _FakeSession()
        monkeypatch.setattr(
            _SyncAiohttpSession,
            "_make_client_session",
            lambda self, timeout: fake_session,
        )

        session = _SyncAiohttpSession({"ssl_verify": False, "timeout": 3})
        started = time.monotonic()
        resp = asyncio.run(
            session._request_async("PUT", "https://example.org/file", data=b"payload")
        )
        elapsed = time.monotonic() - started

        assert resp.status_code == 201
        assert elapsed < 2.5
        assert fake_session.closed is True
        assert fake_session.close_cancelled is True

    def test_make_client_session_uses_fast_ssl_shutdown(self, monkeypatch):
        connector_calls = []
        session_calls = []
        connector_instance = None

        class _FakeConnector:
            pass

        class _FakeSession:
            pass

        def _fake_connector(**kwargs):
            nonlocal connector_instance
            connector_calls.append(kwargs)
            connector_instance = _FakeConnector()
            return connector_instance

        def _fake_session(**kwargs):
            session_calls.append(kwargs)
            return _FakeSession()

        monkeypatch.setattr("gfal.core.webdav.aiohttp.TCPConnector", _fake_connector)
        monkeypatch.setattr("gfal.core.webdav.aiohttp.ClientSession", _fake_session)

        session = _SyncAiohttpSession({"ssl_verify": False, "timeout": 3})
        client_timeout = aiohttp.ClientTimeout(total=3)
        created = session._make_client_session(timeout=client_timeout)

        assert isinstance(created, _FakeSession)
        assert connector_calls == [
            {
                "ssl": session._ssl_context,
                "enable_cleanup_closed": True,
                "family": 0,
                "ssl_shutdown_timeout": 0,
            }
        ]
        assert session_calls == [
            {
                "connector": connector_instance,
                "timeout": client_timeout,
                "ssl_shutdown_timeout": 0,
            }
        ]

    def test_make_client_session_skips_ssl_shutdown_for_older_aiohttp(
        self, monkeypatch
    ):
        connector_calls = []
        session_calls = []
        connector_instance = None

        class _FakeConnector:
            pass

        class _FakeSession:
            pass

        def _fake_connector(*, ssl, enable_cleanup_closed, family):
            nonlocal connector_instance
            connector_calls.append({
                "ssl": ssl,
                "enable_cleanup_closed": enable_cleanup_closed,
                "family": family,
            })
            connector_instance = _FakeConnector()
            return connector_instance

        def _fake_session(*, connector, timeout):
            session_calls.append({
                "connector": connector,
                "timeout": timeout,
            })
            return _FakeSession()

        monkeypatch.setattr("gfal.core.webdav.aiohttp.TCPConnector", _fake_connector)
        monkeypatch.setattr("gfal.core.webdav.aiohttp.ClientSession", _fake_session)

        session = _SyncAiohttpSession({"ssl_verify": False, "timeout": 3})
        client_timeout = aiohttp.ClientTimeout(total=3)
        created = session._make_client_session(timeout=client_timeout)

        assert isinstance(created, _FakeSession)
        assert connector_calls == [
            {
                "ssl": session._ssl_context,
                "enable_cleanup_closed": True,
                "family": 0,
            }
        ]
        assert session_calls == [
            {
                "connector": connector_instance,
                "timeout": client_timeout,
            }
        ]

    def test_make_connector_uses_ipv4_family(self, monkeypatch):
        connector_calls = []

        class _FakeConnector:
            pass

        def _fake_connector(**kwargs):
            connector_calls.append(kwargs)
            return _FakeConnector()

        monkeypatch.setattr("gfal.core.webdav.aiohttp.TCPConnector", _fake_connector)

        session = _SyncAiohttpSession({"ssl_verify": False, "ipv4_only": True})
        created = session._make_connector()

        assert isinstance(created, _FakeConnector)
        assert connector_calls == [
            {
                "ssl": session._ssl_context,
                "enable_cleanup_closed": True,
                "family": socket.AF_INET,
                "ssl_shutdown_timeout": 0,
            }
        ]

    def test_make_connector_uses_ipv6_family(self, monkeypatch):
        connector_calls = []

        class _FakeConnector:
            pass

        def _fake_connector(**kwargs):
            connector_calls.append(kwargs)
            return _FakeConnector()

        monkeypatch.setattr("gfal.core.webdav.aiohttp.TCPConnector", _fake_connector)

        session = _SyncAiohttpSession({"ssl_verify": False, "ipv6_only": True})
        created = session._make_connector()

        assert isinstance(created, _FakeConnector)
        assert connector_calls == [
            {
                "ssl": session._ssl_context,
                "enable_cleanup_closed": True,
                "family": socket.AF_INET6,
                "ssl_shutdown_timeout": 0,
            }
        ]

    def test_request_stream_async_retries_connection_errors(self, monkeypatch):
        class _FakeContent:
            async def iter_any(self):
                if False:
                    yield b""

        class _FakeResponse:
            status = 202
            headers = {"Content-Length": "0"}
            url = "https://example.org/file"
            content = _FakeContent()

            def close(self):
                return None

        class _FakeSession:
            def __init__(self, *, should_fail):
                self.should_fail = should_fail
                self.closed = False

            async def request(
                self,
                method,
                url,
                headers=None,
                data=None,
                allow_redirects=True,
            ):
                del method, url, headers, data, allow_redirects
                if self.should_fail:
                    raise aiohttp.ClientConnectionError("Connection lost")
                return _FakeResponse()

            async def close(self):
                self.closed = True

        sessions = []

        def _make_session(self, timeout):
            del self, timeout
            session = _FakeSession(should_fail=len(sessions) < 2)
            sessions.append(session)
            return session

        async def _fake_sleep(delay):
            del delay
            return None

        monkeypatch.setattr(
            _SyncAiohttpSession,
            "_make_client_session",
            _make_session,
        )
        monkeypatch.setattr("gfal.core.webdav.asyncio.sleep", _fake_sleep)

        session = _SyncAiohttpSession({"ssl_verify": False, "timeout": 3})
        response = session.request(
            "COPY",
            "https://example.org/file",
            stream=True,
        )

        assert list(response.iter_lines()) == []
        assert len(sessions) == 3
        assert sessions[0].closed is True
        assert sessions[1].closed is True

    def test_request_upload_stream_async_uses_fixed_length_payload(self, monkeypatch):
        seen = {}

        class _FakeWriter:
            def __init__(self):
                self.chunks = []

            async def write(self, chunk):
                self.chunks.append(chunk)

        class _FakeResponse:
            status = 201
            headers = {"Content-Length": "0"}
            url = "https://example.org/file"

            async def read(self):
                return b""

            def close(self):
                return None

        class _FakeSession:
            def __init__(self):
                self.closed = False

            async def request(self, method, url, headers=None, data=None):
                del method, url, headers
                seen["size"] = getattr(data, "size", None)
                writer = _FakeWriter()
                await data.write(writer)
                seen["body"] = b"".join(writer.chunks)
                return _FakeResponse()

            async def close(self):
                self.closed = True

        fake_session = _FakeSession()
        monkeypatch.setattr(
            _SyncAiohttpSession,
            "_make_client_session",
            lambda self, timeout: fake_session,
        )

        session = _SyncAiohttpSession({"ssl_verify": False, "timeout": 3})
        body_queue: queue.Queue[object] = queue.Queue()
        body_queue.put(b"payload")
        body_queue.put(_STREAM_EOF)

        resp = asyncio.run(
            session._request_upload_stream_async(
                "PUT",
                "https://example.org/file",
                body_queue=body_queue,
                content_length=7,
            )
        )

        assert resp.status_code == 201
        assert seen["size"] == 7
        assert seen["body"] == b"payload"
        assert fake_session.closed is True

    def test_close_stops_background_loop_thread(self):
        session = _SyncAiohttpSession({"ssl_verify": False, "timeout": 3})

        loop = session._ensure_loop()
        assert loop.is_running()
        assert session._thread is not None
        assert session._thread.is_alive()

        session.close()

        assert session._thread is None
        assert session._loop is None

    def test_ensure_loop_is_shared_across_threads(self):
        session = _SyncAiohttpSession({"ssl_verify": False, "timeout": 3})
        ready = threading.Barrier(8)
        loops = []

        def _worker():
            ready.wait()
            loops.append(session._ensure_loop())

        threads = [threading.Thread(target=_worker) for _ in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        assert len({id(loop) for loop in loops}) == 1
        session.close()


# ---------------------------------------------------------------------------
# WebDAVFileSystem integration tests against mock server
# ---------------------------------------------------------------------------


class TestWebDAVInfo:
    def test_info_root(self, dav_server):
        fs = WebDAVFileSystem()
        info = fs.info(dav_server + "/")
        assert info["type"] == "directory"

    def test_info_file(self, dav_server):
        with _vfs_lock:
            _vfs.add("/file.txt")
        fs = WebDAVFileSystem()
        info = fs.info(dav_server + "/file.txt")
        assert info["type"] == "file"
        assert info["size"] == 42

    def test_info_missing_raises(self, dav_server):
        fs = WebDAVFileSystem()
        with pytest.raises(FileNotFoundError):
            fs.info(dav_server + "/no_such_file.txt")


class TestWebDAVLs:
    def test_ls_empty_dir(self, dav_server):
        fs = WebDAVFileSystem()
        entries = fs.ls(dav_server + "/")
        assert entries == []

    def test_ls_shows_children(self, dav_server):
        with _vfs_lock:
            _vfs.add("/dir/")
            _vfs.add("/dir/a.txt")
            _vfs.add("/dir/b.txt")
        fs = WebDAVFileSystem()
        entries = fs.ls(dav_server + "/dir")
        names = [e["name"].rstrip("/").rsplit("/", 1)[-1] for e in entries]
        assert sorted(names) == ["a.txt", "b.txt"]

    def test_ls_detail_false(self, dav_server):
        with _vfs_lock:
            _vfs.add("/dir/")
            _vfs.add("/dir/f.txt")
        fs = WebDAVFileSystem()
        entries = fs.ls(dav_server + "/dir", detail=False)
        assert isinstance(entries[0], str)

    def test_ls_distinguishes_dirs(self, dav_server):
        with _vfs_lock:
            _vfs.add("/top/")
            _vfs.add("/top/sub/")
            _vfs.add("/top/file.txt")
        fs = WebDAVFileSystem()
        entries = fs.ls(dav_server + "/top")
        types = {e["name"].rstrip("/").rsplit("/", 1)[-1]: e["type"] for e in entries}
        assert types["sub"] == "directory"
        assert types["file.txt"] == "file"

    def test_ls_nonexistent_raises(self, dav_server):
        fs = WebDAVFileSystem()
        with pytest.raises((FileNotFoundError, Exception)):
            fs.ls(dav_server + "/no_such_dir")


class TestWebDAVMkdir:
    def test_mkdir_creates_directory(self, dav_server):
        fs = WebDAVFileSystem()
        fs.mkdir(dav_server + "/newdir")
        with _vfs_lock:
            assert "/newdir/" in _vfs

    def test_mkdir_existing_raises(self, dav_server):
        with _vfs_lock:
            _vfs.add("/existing/")
        fs = WebDAVFileSystem()
        with pytest.raises(FileExistsError):
            fs.mkdir(dav_server + "/existing")

    def test_mkdir_missing_parent_raises(self, dav_server):
        fs = WebDAVFileSystem()
        with pytest.raises((FileNotFoundError, Exception)):
            fs.mkdir(dav_server + "/no_parent/child")

    def test_makedirs_creates_nested(self, dav_server):
        fs = WebDAVFileSystem()
        fs.makedirs(dav_server + "/a/b/c")
        with _vfs_lock:
            assert "/a/" in _vfs
            assert "/a/b/" in _vfs
            assert "/a/b/c/" in _vfs

    def test_makedirs_exist_ok(self, dav_server):
        with _vfs_lock:
            _vfs.add("/exists/")
        fs = WebDAVFileSystem()
        # Should not raise
        fs.makedirs(dav_server + "/exists", exist_ok=True)

    def test_mkdir_with_create_parents(self, dav_server):
        fs = WebDAVFileSystem()
        fs.mkdir(dav_server + "/p/q", create_parents=True)
        with _vfs_lock:
            assert "/p/" in _vfs
            assert "/p/q/" in _vfs


class TestWebDAVRm:
    def test_rm_file(self, dav_server):
        with _vfs_lock:
            _vfs.add("/todelete.txt")
        fs = WebDAVFileSystem()
        fs.rm(dav_server + "/todelete.txt")
        with _vfs_lock:
            assert "/todelete.txt" not in _vfs

    def test_rm_directory(self, dav_server):
        with _vfs_lock:
            _vfs.add("/rmdir/")
        fs = WebDAVFileSystem()
        fs.rm(dav_server + "/rmdir")
        with _vfs_lock:
            assert "/rmdir/" not in _vfs

    def test_rm_missing_raises(self, dav_server):
        fs = WebDAVFileSystem()
        with pytest.raises(FileNotFoundError):
            fs.rm(dav_server + "/ghost.txt")

    def test_rmdir(self, dav_server):
        with _vfs_lock:
            _vfs.add("/emptydir/")
        fs = WebDAVFileSystem()
        fs.rmdir(dav_server + "/emptydir")
        with _vfs_lock:
            assert "/emptydir/" not in _vfs


class TestWebDAVMv:
    def test_mv_file(self, dav_server):
        with _vfs_lock:
            _vfs.add("/src.txt")
        fs = WebDAVFileSystem()
        fs.mv(dav_server + "/src.txt", dav_server + "/dst.txt")
        with _vfs_lock:
            assert "/src.txt" not in _vfs
            assert "/dst.txt" in _vfs

    def test_mv_directory(self, dav_server):
        with _vfs_lock:
            _vfs.add("/srcdir/")
            _vfs.add("/srcdir/f.txt")
        fs = WebDAVFileSystem()
        fs.mv(dav_server + "/srcdir", dav_server + "/dstdir")
        with _vfs_lock:
            assert "/srcdir/" not in _vfs
            assert "/dstdir/" in _vfs


class TestWebDAVIsdir:
    def test_isdir_true(self, dav_server):
        with _vfs_lock:
            _vfs.add("/a_dir/")
        fs = WebDAVFileSystem()
        assert fs.isdir(dav_server + "/a_dir") is True

    def test_isdir_false_for_file(self, dav_server):
        with _vfs_lock:
            _vfs.add("/a_file.txt")
        fs = WebDAVFileSystem()
        assert fs.isdir(dav_server + "/a_file.txt") is False

    def test_isdir_false_for_missing(self, dav_server):
        fs = WebDAVFileSystem()
        assert fs.isdir(dav_server + "/nonexistent") is False


# ---------------------------------------------------------------------------
# End-to-end via gfal CLI commands against the mock server
# ---------------------------------------------------------------------------


class TestWebDAVViaGfalCli:
    def test_gfal_ls_directory(self, dav_server):
        with _vfs_lock:
            _vfs.add("/cli_dir/")
            _vfs.add("/cli_dir/hello.txt")
        from helpers import run_gfal

        rc, out, err = run_gfal("ls", dav_server + "/cli_dir")
        assert rc == 0
        assert "hello.txt" in out

    def test_gfal_ls_missing_fails(self, dav_server):
        from helpers import run_gfal

        rc, out, err = run_gfal("ls", dav_server + "/no_such")
        assert rc != 0

    def test_gfal_mkdir(self, dav_server):
        from helpers import run_gfal

        rc, out, err = run_gfal("mkdir", dav_server + "/gfal_newdir")
        assert rc == 0
        with _vfs_lock:
            assert "/gfal_newdir/" in _vfs

    def test_gfal_mkdir_parents(self, dav_server):
        from helpers import run_gfal

        rc, out, err = run_gfal("mkdir", "-p", dav_server + "/gfal_p/q")
        assert rc == 0
        with _vfs_lock:
            assert "/gfal_p/" in _vfs
            assert "/gfal_p/q/" in _vfs

    def test_gfal_rm_file(self, dav_server):
        with _vfs_lock:
            _vfs.add("/gfal_rm_me.txt")
        from helpers import run_gfal

        rc, out, err = run_gfal("rm", dav_server + "/gfal_rm_me.txt")
        assert rc == 0
        with _vfs_lock:
            assert "/gfal_rm_me.txt" not in _vfs

    def test_gfal_stat_file(self, dav_server):
        with _vfs_lock:
            _vfs.add("/stat_me.txt")
        from helpers import run_gfal

        rc, out, err = run_gfal("stat", dav_server + "/stat_me.txt")
        assert rc == 0
        assert "File:" in out

    def test_gfal_cp_upload(self, dav_server, tmp_path):
        """gfal-cp from a local file to the mock WebDAV server."""
        from helpers import run_gfal

        local = tmp_path / "upload_src.txt"
        local.write_bytes(b"upload content")

        rc, out, err = run_gfal("cp", local.as_uri(), dav_server + "/uploaded.txt")
        assert rc == 0
        with _vfs_lock:
            assert "/uploaded.txt" in _vfs

    def test_gfal_cp_download(self, dav_server, tmp_path):
        """gfal-cp from the mock WebDAV server to a local file."""
        from helpers import run_gfal

        with _vfs_lock:
            _vfs.add("/download.txt")

        local_dst = tmp_path / "downloaded.txt"
        rc, out, err = run_gfal("cp", dav_server + "/download.txt", local_dst.as_uri())
        assert rc == 0
        assert local_dst.exists()
        # Mock server returns b"hello" for any GET
        assert local_dst.read_bytes() == b"hello"

    def test_gfal_rename(self, dav_server):
        """gfal-rename moves a file on the mock WebDAV server."""
        from helpers import run_gfal

        with _vfs_lock:
            _vfs.add("/rename_src.txt")

        rc, out, err = run_gfal(
            "rename",
            dav_server + "/rename_src.txt",
            dav_server + "/rename_dst.txt",
        )
        assert rc == 0
        with _vfs_lock:
            assert "/rename_dst.txt" in _vfs
            assert "/rename_src.txt" not in _vfs

    def test_gfal_cat_via_webdav(self, dav_server):
        """gfal-cat reads file content from the mock WebDAV server."""
        from helpers import run_gfal

        with _vfs_lock:
            _vfs.add("/cat_me.txt")

        rc, out, err = run_gfal("cat", dav_server + "/cat_me.txt")
        assert rc == 0
        assert "hello" in out


class TestWebDAVSslError:
    """SSL errors should not be silently mapped to 'No such file or directory'."""

    def test_ssl_error_propagates_from_info(self):
        """info() re-raises SSLError when ssl_verify=True (default)."""
        from unittest.mock import patch

        fs = WebDAVFileSystem()  # ssl_verify defaults to True
        ssl_exc = ssl.SSLError("SSL: CERTIFICATE_VERIFY_FAILED")

        with (
            patch.object(fs, "_propfind", side_effect=ssl_exc),
            pytest.raises(ssl.SSLError),
        ):
            fs.info("https://example.com/path")

    def test_ssl_error_falls_through_when_no_verify(self, dav_server):
        """info() falls through to _http_fs when ssl_verify=False (--no-verify)."""
        from unittest.mock import patch

        fs = WebDAVFileSystem({"ssl_verify": False})
        ssl_exc = ssl.SSLError("SSL: CERTIFICATE_VERIFY_FAILED")

        with _vfs_lock:
            _vfs.add("/nv_fallback.txt")

        # With ssl_verify=False, SSLError from PROPFIND is caught; falls through
        # to _http_fs.info() which contacts the (plain HTTP) mock server.
        with patch.object(fs, "_propfind", side_effect=ssl_exc):
            info = fs.info(dav_server + "/nv_fallback.txt")
        assert info["type"] == "file"

    def test_connection_error_propagates_from_info(self):
        """info() re-raises ConnectionError when ssl_verify=True (default)."""
        from unittest.mock import patch

        fs = WebDAVFileSystem()
        conn_exc = aiohttp.ClientConnectionError("connection refused")

        with (
            patch.object(fs, "_propfind", side_effect=conn_exc),
            pytest.raises(aiohttp.ClientConnectionError),
        ):
            fs.info("https://example.com/path")

    def test_405_still_falls_through_to_head(self, dav_server):
        """NotImplementedError (405) falls through to HEAD as before."""
        import contextlib
        from unittest.mock import patch

        with _vfs_lock:
            _vfs.add("/fallback.txt")
        fs = WebDAVFileSystem()

        original_propfind = fs._propfind

        def propfind_raise_on_depth0(url, depth=0):
            if depth == 0:
                raise NotImplementedError("405")
            return original_propfind(url, depth=depth)

        # Should not raise — falls through to _http_fs.info()
        with (
            patch.object(fs, "_propfind", side_effect=propfind_raise_on_depth0),
            contextlib.suppress(Exception),
        ):
            fs.info(dav_server + "/fallback.txt")


class TestWebDAVChmod:
    def test_chmod_is_noop(self, dav_server):
        """WebDAVFileSystem.chmod() is a documented no-op — must not raise."""
        with _vfs_lock:
            _vfs.add("/chmodfile.txt")
        fs = WebDAVFileSystem()
        # Should not raise
        fs.chmod(dav_server + "/chmodfile.txt", 0o644)

    def test_chmod_on_missing_path_noop(self, dav_server):
        """chmod on a non-existent path is also a no-op (HTTP has no permission model)."""
        fs = WebDAVFileSystem()
        fs.chmod(dav_server + "/does_not_exist.txt", 0o755)


class TestWebDAVOpenWrite:
    def test_open_write_creates_entry(self, dav_server):
        """open(url, 'wb') followed by write+close should PUT the file."""
        fs = WebDAVFileSystem()
        with fs.open(dav_server + "/written.txt", "wb") as f:
            f.write(b"hello webdav")
        with _vfs_lock:
            assert "/written.txt" in _vfs

    def test_open_write_uses_configured_timeout(self):
        from unittest.mock import MagicMock

        fs = WebDAVFileSystem({"timeout": 12})
        mock_resp = MagicMock()
        mock_resp.status_code = 201
        fs._session.put = MagicMock(return_value=mock_resp)

        with fs.open("https://server/upload.bin", "wb") as f:
            f.write(b"payload")

        fs._session.put.assert_called_once()
        assert fs._session.put.call_args.kwargs["timeout"] == 12

    def test_open_write_403_maps_to_permission_error(self):
        from unittest.mock import MagicMock

        fs = WebDAVFileSystem()
        mock_resp = MagicMock()
        mock_resp.status_code = 403
        fs._session.put = MagicMock(return_value=mock_resp)

        with (
            pytest.raises(PermissionError),
            fs.open("https://server/denied.bin", "wb") as f,
        ):
            f.write(b"payload")

    def test_open_write_timeout_maps_to_timeout_error(self):
        fs = WebDAVFileSystem({"timeout": 3})
        fs._session.put = MagicMock(side_effect=TimeoutError("slow"))

        with pytest.raises(TimeoutError), fs.open("https://server/slow.bin", "wb") as f:
            f.write(b"payload")

    def test_open_read_returns_hello(self, dav_server):
        """open(url, 'rb') should GET and return the mock content."""
        with _vfs_lock:
            _vfs.add("/readable.txt")
        fs = WebDAVFileSystem()
        with fs.open(dav_server + "/readable.txt", "rb") as f:
            data = f.read()
        assert data == b"hello"


class TestWebDAVPropfindExtra:
    def test_multiple_children(self, dav_server):
        with _vfs_lock:
            _vfs.add("/multi/")
            for i in range(5):
                _vfs.add(f"/multi/file{i}.txt")
        fs = WebDAVFileSystem()
        entries = fs.ls(dav_server + "/multi")
        assert len(entries) == 5

    def test_nested_dirs_not_shown_at_depth1(self, dav_server):
        """Depth:1 PROPFIND should not return grandchildren."""
        with _vfs_lock:
            _vfs.add("/parent/")
            _vfs.add("/parent/child/")
            _vfs.add("/parent/child/grandchild.txt")
        fs = WebDAVFileSystem()
        entries = fs.ls(dav_server + "/parent")
        names = [e["name"].rstrip("/").rsplit("/", 1)[-1] for e in entries]
        assert "child" in names
        assert "grandchild.txt" not in names


# ---------------------------------------------------------------------------
# Bearer token in _make_session
# ---------------------------------------------------------------------------


class TestMakeSessionBearerToken:
    def test_bearer_token_added_to_headers(self):
        from gfal.core.webdav import _make_session

        session = _make_session({"bearer_token": "my-macaroon"})
        assert session.headers.get("Authorization") == "Bearer my-macaroon"

    def test_no_bearer_token_no_auth_header(self):
        from gfal.core.webdav import _make_session

        session = _make_session({})
        assert "Authorization" not in session.headers


# ---------------------------------------------------------------------------
# HTTP directory detection (text/html mimetype heuristic)
# ---------------------------------------------------------------------------


class TestHttpDirectoryDetection:
    def test_info_trailing_slash_returns_directory(self, dav_server):
        """info() on a path ending with '/' should return type='directory'."""
        from unittest.mock import patch

        fs = WebDAVFileSystem()

        # Simulate a non-WebDAV server: PROPFIND raises NotImplementedError,
        # HEAD returns text/html (directory index).
        mock_head_info = {
            "name": dav_server + "/dir/",
            "size": 0,
            "type": "file",
            "mimetype": "text/html; charset=utf-8",
        }
        with (
            patch.object(fs, "_propfind", side_effect=NotImplementedError("405")),
            patch.object(fs._http_fs, "info", return_value=mock_head_info),
        ):
            info = fs.info(dav_server + "/dir/")
        assert info["type"] == "directory"

    def test_info_text_html_mimetype_returns_directory(self, dav_server):
        """info() with text/html mimetype from HEAD sets type='directory'."""
        from unittest.mock import patch

        fs = WebDAVFileSystem()
        mock_head_info = {
            "name": dav_server + "/index",
            "size": 1024,
            "type": "file",
            "mimetype": "text/html",
        }
        with (
            patch.object(fs, "_propfind", side_effect=NotImplementedError("405")),
            patch.object(fs._http_fs, "info", return_value=mock_head_info),
        ):
            info = fs.info(dav_server + "/index")
        assert info["type"] == "directory"

    def test_info_plain_file_stays_file(self, dav_server):
        """info() with application/octet-stream keeps type='file'."""
        from unittest.mock import patch

        fs = WebDAVFileSystem()
        mock_head_info = {
            "name": dav_server + "/data.bin",
            "size": 512,
            "type": "file",
            "mimetype": "application/octet-stream",
        }
        with (
            patch.object(fs, "_propfind", side_effect=NotImplementedError("405")),
            patch.object(fs._http_fs, "info", return_value=mock_head_info),
        ):
            info = fs.info(dav_server + "/data.bin")
        assert info["type"] == "file"


# ---------------------------------------------------------------------------
# ls() fallback for non-WebDAV servers (405 on PROPFIND)
# ---------------------------------------------------------------------------


class TestLsNonWebDAVFallback:
    def test_ls_falls_back_to_info_on_405(self, dav_server):
        """ls() on a 405-responding server returns single-entry list from info()."""
        from unittest.mock import patch

        with _vfs_lock:
            _vfs.add("/fallback_file.txt")
        fs = WebDAVFileSystem()

        # Make PROPFIND fail with 405 but info() succeed via HEAD
        with patch.object(fs, "_propfind", side_effect=NotImplementedError("405")):
            entries = fs.ls(dav_server + "/fallback_file.txt", detail=True)

        assert len(entries) == 1
        assert "fallback_file" in entries[0]["name"]

    def test_ls_fallback_names_only(self, dav_server):
        """ls() with detail=False still works on non-WebDAV fallback."""
        from unittest.mock import patch

        with _vfs_lock:
            _vfs.add("/fallback2.txt")
        fs = WebDAVFileSystem()

        with patch.object(fs, "_propfind", side_effect=NotImplementedError("405")):
            names = fs.ls(dav_server + "/fallback2.txt", detail=False)

        assert len(names) == 1
        assert isinstance(names[0], str)


# ---------------------------------------------------------------------------
# Checksum / Digest header parsing
# ---------------------------------------------------------------------------


class TestWebDAVChecksum:
    def test_checksum_parses_digest_header(self):
        from unittest.mock import MagicMock

        fs = WebDAVFileSystem()
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.headers = {"Digest": "md5=O123456789, adler32=335e754f"}
        fs._session.head = MagicMock(return_value=mock_resp)

        assert fs.checksum("https://server/file", "adler32") == "335e754f"
        assert fs.checksum("https://server/file", "MD5") == "O123456789"

        fs._session.head.assert_called_with(
            "https://server/file", headers={"Want-Digest": "md5"}, timeout=10
        )

    def test_checksum_no_digest_header_raises(self):
        from unittest.mock import MagicMock

        fs = WebDAVFileSystem()
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.headers = {}
        fs._session.head = MagicMock(return_value=mock_resp)

        with pytest.raises(NotImplementedError):
            fs.checksum("https://server/file", "adler32")

    def test_checksum_missing_algorithm_raises(self):
        from unittest.mock import MagicMock

        fs = WebDAVFileSystem()
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.headers = {"Digest": "sha256=abcdef"}
        fs._session.head = MagicMock(return_value=mock_resp)

        with pytest.raises(NotImplementedError):
            fs.checksum("https://server/file", "adler32")


class TestStreamingRequestsPutFile:
    def test_streaming_writer_keeps_bytes_without_copy(self):
        session = MagicMock()
        upload_future: concurrent.futures.Future = concurrent.futures.Future()
        response = MagicMock()
        response.status_code = 201
        response.headers = {}
        upload_future.set_result(response)
        session.request_upload_stream.return_value = upload_future

        payload = b"payload"
        writer = _StreamingRequestsPutFile(
            session,
            "https://example.org/file",
            content_length=len(payload),
        )
        assert writer.write(payload) == len(payload)
        writer.close()

        body_queue = session.request_upload_stream.call_args.kwargs["body_queue"]
        first_item = body_queue.get_nowait()
        assert first_item is payload
        assert session.request_upload_stream.call_args.kwargs["content_length"] == 7

    def test_write_starts_upload_before_close(self):
        first_chunk = threading.Event()
        upload_finished: concurrent.futures.Future = concurrent.futures.Future()

        class _Session:
            def request_upload_stream(
                self,
                method,
                url,
                *,
                body_queue,
                content_length,
                timeout,
                headers=None,
            ):
                del method, url, content_length, timeout, headers

                def _consume():
                    try:
                        chunk = body_queue.get(timeout=1)
                        assert chunk == b"payload"
                        first_chunk.set()
                        eof = body_queue.get(timeout=1)
                        assert eof is _STREAM_EOF
                        response = MagicMock()
                        response.status_code = 201
                        response.headers = {}
                        upload_finished.set_result(response)
                    except Exception as exc:
                        if not upload_finished.done():
                            upload_finished.set_exception(exc)

                threading.Thread(target=_consume, daemon=True).start()
                return upload_finished

        writer = _StreamingRequestsPutFile(_Session(), "https://example.org/file")
        assert writer.write(b"payload") == len(b"payload")

        assert first_chunk.wait(1), "upload did not start before close()"
        writer.close()

    def test_close_does_not_retry_put_after_connection_error(self):
        session = MagicMock()
        upload_future: concurrent.futures.Future = concurrent.futures.Future()
        session.request_upload_stream.return_value = upload_future
        writer = _StreamingRequestsPutFile(session, "https://example.org/file")
        writer.write(b"payload")
        upload_future.set_exception(aiohttp.ClientConnectionError("Connection lost"))

        with pytest.raises(aiohttp.ClientConnectionError):
            writer.close()

        session.request_upload_stream.assert_called_once()

    def test_write_fails_fast_on_http_error_response(self):
        session = MagicMock()
        upload_future: concurrent.futures.Future = concurrent.futures.Future()
        response = MagicMock()
        response.status_code = 403
        response.headers = {}
        upload_future.set_result(response)
        session.request_upload_stream.return_value = upload_future
        writer = _StreamingRequestsPutFile(session, "https://example.org/file")

        with pytest.raises(PermissionError):
            writer.write(b"payload")
        with contextlib.suppress(PermissionError):
            writer.close()

    def test_write_re_raises_stored_upload_exception(self):
        session = MagicMock()
        upload_future: concurrent.futures.Future = concurrent.futures.Future()
        response = MagicMock()
        response.status_code = 403
        response.headers = {}
        upload_future.set_result(response)
        session.request_upload_stream.return_value = upload_future
        writer = _StreamingRequestsPutFile(session, "https://example.org/file")

        with pytest.raises(PermissionError):
            writer.write(b"payload")
        with pytest.raises(PermissionError):
            writer.write(b"payload")
        with contextlib.suppress(PermissionError):
            writer.close()


class TestWebDAVStreamWriteResolution:
    def test_open_stream_write_resolves_eos_redirect(self):
        fs = WebDAVFileSystem()
        response = MagicMock()
        response.status_code = 307
        response.headers = {
            "Location": "http://data-node.cern.ch:8443/eos/test/file?token=abc"
        }
        fs._session.put = MagicMock(return_value=response)

        writer = fs.open_stream_write(
            "https://eospilot.cern.ch//eos/pilot/opstest/dteam/python3-gfal/tmp/file.bin",
            content_length=123,
        )
        upload_future: concurrent.futures.Future = concurrent.futures.Future()
        upload_response = MagicMock()
        upload_response.status_code = 201
        upload_response.headers = {}
        upload_future.set_result(upload_response)
        writer._upload_future = upload_future

        assert writer._url == "http://data-node.cern.ch:8443/eos/test/file?token=abc"
        fs._session.put.assert_called_once_with(
            "https://eospilot.cern.ch//eos/pilot/opstest/dteam/python3-gfal/tmp/file.bin",
            data=b"",
            timeout=fs._timeout,
            headers={"Content-Length": "0"},
            allow_redirects=False,
        )
        assert writer._content_length == 123
        writer.close()

    def test_open_stream_write_skips_preflight_for_non_eos_urls(self):
        fs = WebDAVFileSystem()
        fs._session.put = MagicMock()

        writer = fs.open_stream_write(
            "https://example.org/upload.bin",
            content_length=321,
        )
        upload_future: concurrent.futures.Future = concurrent.futures.Future()
        upload_response = MagicMock()
        upload_response.status_code = 201
        upload_response.headers = {}
        upload_future.set_result(upload_response)
        writer._upload_future = upload_future

        assert writer._url == "https://example.org/upload.bin"
        assert writer._content_length == 321
        fs._session.put.assert_not_called()
        writer.close()

    def test_open_stream_write_resolves_eos_redirect_with_query(self):
        fs = WebDAVFileSystem()
        response = MagicMock()
        response.status_code = 307
        response.headers = {
            "Location": "http://data-node.cern.ch:8443/eos/test/file?token=abc"
        }
        fs._session.put = MagicMock(return_value=response)

        writer = fs.open_stream_write(
            "https://eospilot.cern.ch//eos/pilot/opstest/dteam/python3-gfal/tmp/file.bin?eos.mtime=946684800",
            content_length=123,
        )
        upload_future: concurrent.futures.Future = concurrent.futures.Future()
        upload_response = MagicMock()
        upload_response.status_code = 201
        upload_response.headers = {}
        upload_future.set_result(upload_response)
        writer._upload_future = upload_future

        assert writer._url == "http://data-node.cern.ch:8443/eos/test/file?token=abc"
        fs._session.put.assert_called_once_with(
            "https://eospilot.cern.ch//eos/pilot/opstest/dteam/python3-gfal/tmp/file.bin?eos.mtime=946684800",
            data=b"",
            timeout=fs._timeout,
            headers={"Content-Length": "0"},
            allow_redirects=False,
        )
        writer.close()


class TestWebDAVStreamRead:
    def test_streaming_get_file_reads_from_response_queue(self):
        body_queue: queue.Queue[object] = queue.Queue()
        body_queue.put(b"abclo")
        body_queue.put(b"world")
        body_queue.put(_STREAM_EOF)
        completion_future: concurrent.futures.Future = concurrent.futures.Future()
        completion_future.set_result(None)
        response = MagicMock()
        response._body_queue = body_queue
        response.close = MagicMock()

        reader = _StreamingRequestsGetFile(response)

        assert reader.read(3) == b"abc"
        assert reader.read(4) == b"lowo"
        assert reader.read(10) == b"rld"
        assert reader.read(1) == b""
        reader.close()
        response.close.assert_called_once()

    def test_open_stream_read_uses_streaming_get_session(self):
        fs = WebDAVFileSystem()
        response = MagicMock()
        response.status_code = 200
        response.headers = {}
        response._body_queue = queue.Queue()
        completion_future: concurrent.futures.Future = concurrent.futures.Future()
        completion_future.set_result(None)
        response.close = MagicMock()
        fs._session.request = MagicMock(return_value=response)

        reader = fs.open_stream_read("https://example.org/data.bin")

        assert isinstance(reader, _StreamingRequestsGetFile)
        fs._session.request.assert_called_once_with(
            "GET",
            "https://example.org/data.bin",
            timeout=fs._timeout,
            stream=True,
        )

    def test_open_stream_read_closes_response_on_http_error(self):
        fs = WebDAVFileSystem()
        response = MagicMock()
        response.status_code = 403
        response.headers = {}
        response.close = MagicMock()
        fs._session.request = MagicMock(return_value=response)

        with pytest.raises(PermissionError):
            fs.open_stream_read("https://example.org/forbidden.bin")

        response.close.assert_called_once()


class TestWebDAVChecksumFallback:
    def test_checksum_falls_back_to_client_side_when_head_times_out(self):
        fs = WebDAVFileSystem()
        fs._session.head = MagicMock(side_effect=TimeoutError("timed out"))
        fs.open_stream_read = MagicMock(return_value=io.BytesIO(b"hello world"))

        assert fs.checksum("https://example.org/data.bin", "ADLER32") == "1a0b045d"
