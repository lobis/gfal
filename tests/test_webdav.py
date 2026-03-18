"""
Unit tests for the WebDAV filesystem layer (webdav.py).

A lightweight in-process HTTP server that handles WebDAV methods
(PROPFIND / MKCOL / DELETE / MOVE) is started once per test session.
No external network access is required.
"""

from __future__ import annotations

import posixpath
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest

from gfal_cli.webdav import WebDAVFileSystem, _parse_propfind

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


class _WebDAVHandler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass  # silence request logging during tests

    def do_PROPFIND(self):
        with _vfs_lock:
            norm = self.path.rstrip("/")
            # 404 if not root and not in vfs at all
            if norm != "" and (norm + "/") not in _vfs and norm not in _vfs:
                self.send_response(404)
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
                self.end_headers()
                return
            parent = posixpath.dirname(path.rstrip("/")).rstrip("/") + "/"
            # root is always "/"
            if parent != "/" and parent not in _vfs:
                self.send_response(409)
                self.end_headers()
                return
            _vfs.add(path)
        self.send_response(201)
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
                self.end_headers()
                return
            for entry in list(to_move):
                new = dst + entry[len(src) :]
                _vfs.discard(entry)
                _vfs.add(new)
        self.send_response(201)
        self.end_headers()

    def do_GET(self):
        path = self.path.rstrip("/")
        with _vfs_lock:
            exists = path in _vfs or (path + "/") in _vfs
        if not exists:
            self.send_response(404)
            self.end_headers()
            return
        body = b"hello"
        self.send_response(200)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_PUT(self):
        length = int(self.headers.get("Content-Length", 0))
        self.rfile.read(length)
        with _vfs_lock:
            _vfs.add(self.path.rstrip("/"))
        self.send_response(201)
        self.end_headers()


@pytest.fixture(scope="session")
def dav_server():
    """Start a mock WebDAV server and return its base URL."""
    server = HTTPServer(("127.0.0.1", 0), _WebDAVHandler)
    port = server.server_address[1]
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
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
