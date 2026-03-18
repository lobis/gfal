"""Unit tests for the fsspec integration layer (fs.py)."""

import stat as stat_module
from pathlib import Path

import pytest

from gfal_cli.fs import StatInfo, isdir, normalize_url, stat, url_to_fs

# ---------------------------------------------------------------------------
# normalize_url
# ---------------------------------------------------------------------------


class TestNormalizeUrl:
    def test_bare_path(self, tmp_path):
        f = tmp_path / "foo.txt"
        result = normalize_url(str(f))
        assert result.startswith("file://")
        assert "foo.txt" in result

    def test_bare_relative_path(self):
        result = normalize_url("relative/path.txt")
        assert result.startswith("file://")

    def test_file_url_unchanged(self):
        url = "file:///tmp/foo.txt"
        assert normalize_url(url) == url

    def test_dav_to_http(self):
        assert normalize_url("dav://host/path") == "http://host/path"

    def test_davs_to_https(self):
        assert normalize_url("davs://host/path") == "https://host/path"

    def test_http_unchanged(self):
        url = "http://example.com/file"
        assert normalize_url(url) == url

    def test_https_unchanged(self):
        url = "https://example.com/file"
        assert normalize_url(url) == url

    def test_root_unchanged(self):
        url = "root://eosuser.cern.ch//eos/user/x/xyz/file"
        assert normalize_url(url) == url

    def test_xroot_unchanged(self):
        url = "xroot://server.example.com//data/file"
        assert normalize_url(url) == url

    def test_sentinel_dash(self):
        assert normalize_url("-") == "-"

    def test_dav_preserves_path(self):
        result = normalize_url("dav://host:8080/some/path/file.txt")
        assert result == "http://host:8080/some/path/file.txt"

    def test_davs_preserves_path(self):
        result = normalize_url("davs://host:443/path/to/file")
        assert result == "https://host:443/path/to/file"


# ---------------------------------------------------------------------------
# StatInfo
# ---------------------------------------------------------------------------


class TestStatInfo:
    def test_file_with_explicit_mode(self):
        info = {"type": "file", "size": 1024, "mode": 0o100644}
        si = StatInfo(info)
        assert si.st_size == 1024
        assert stat_module.S_ISREG(si.st_mode)
        assert stat_module.S_IMODE(si.st_mode) == 0o644

    def test_directory_no_mode(self):
        info = {"type": "directory", "size": 0}
        si = StatInfo(info)
        assert stat_module.S_ISDIR(si.st_mode)
        assert stat_module.S_IMODE(si.st_mode) == 0o755

    def test_file_no_mode_synthesised(self):
        info = {"type": "file", "size": 512}
        si = StatInfo(info)
        assert stat_module.S_ISREG(si.st_mode)
        assert stat_module.S_IMODE(si.st_mode) == 0o644

    def test_default_size_zero(self):
        si = StatInfo({})
        assert si.st_size == 0

    def test_explicit_size(self):
        si = StatInfo({"size": 4096})
        assert si.st_size == 4096

    def test_size_from_none(self):
        """size=None should be treated as 0."""
        si = StatInfo({"size": None})
        assert si.st_size == 0

    def test_default_uid_gid(self):
        si = StatInfo({})
        assert si.st_uid == 0
        assert si.st_gid == 0

    def test_explicit_uid_gid(self):
        si = StatInfo({"uid": 1000, "gid": 500})
        assert si.st_uid == 1000
        assert si.st_gid == 500

    def test_default_nlink(self):
        si = StatInfo({})
        assert si.st_nlink == 1

    def test_explicit_nlink(self):
        si = StatInfo({"nlink": 3})
        assert si.st_nlink == 3

    def test_default_timestamps(self):
        si = StatInfo({})
        assert si.st_mtime == 0.0
        assert si.st_atime == 0.0
        assert si.st_ctime == 0.0

    def test_atime_falls_back_to_mtime(self):
        info = {"type": "file", "size": 0, "mtime": 1_700_000_000.0}
        si = StatInfo(info)
        assert si.st_mtime == 1_700_000_000.0
        assert si.st_atime == 1_700_000_000.0
        assert si.st_ctime == 1_700_000_000.0

    def test_explicit_timestamps(self):
        info = {
            "type": "file",
            "size": 0,
            "mtime": 1000.0,
            "atime": 2000.0,
            "ctime": 3000.0,
        }
        si = StatInfo(info)
        assert si.st_mtime == 1000.0
        assert si.st_atime == 2000.0
        assert si.st_ctime == 3000.0

    def test_slots_exist(self):
        """Verify __slots__ are properly defined."""
        si = StatInfo({})
        with pytest.raises(AttributeError):
            si.nonexistent_attr = 42

    def test_info_dict_stored(self):
        info = {"type": "file", "size": 100}
        si = StatInfo(info)
        assert si._info is info

    def test_string_size_coerced(self):
        """Backends that return size as string should be handled."""
        si = StatInfo({"size": "2048"})
        assert si.st_size == 2048

    def test_mode_with_setuid_setgid(self):
        """Full mode including setuid/setgid bits."""
        info = {"type": "file", "size": 0, "mode": 0o104755}
        si = StatInfo(info)
        assert si.st_mode == 0o104755


# ---------------------------------------------------------------------------
# url_to_fs
# ---------------------------------------------------------------------------


class TestUrlToFs:
    def test_file_uri(self, tmp_path):
        f = tmp_path / "test.txt"
        f.write_text("hello")
        fso, path = url_to_fs(f.as_uri())
        assert Path(path) == f

    def test_bare_path(self, tmp_path):
        f = tmp_path / "test.txt"
        f.write_text("hello")
        fso, path = url_to_fs(str(f))
        assert Path(path) == f

    def test_file_fs_can_read(self, tmp_path):
        f = tmp_path / "test.txt"
        f.write_bytes(b"data")
        fso, path = url_to_fs(f.as_uri())
        with fso.open(path, "rb") as fh:
            assert fh.read() == b"data"

    def test_http_returns_webdav_fs(self):
        from gfal_cli.webdav import WebDAVFileSystem

        fso, path = url_to_fs("http://example.com/file")
        assert isinstance(fso, WebDAVFileSystem)
        assert path == "http://example.com/file"

    def test_https_returns_webdav_fs(self):
        from gfal_cli.webdav import WebDAVFileSystem

        fso, path = url_to_fs("https://example.com/file")
        assert isinstance(fso, WebDAVFileSystem)

    def test_dav_normalized_to_http(self):
        from gfal_cli.webdav import WebDAVFileSystem

        fso, path = url_to_fs("dav://example.com/file")
        assert isinstance(fso, WebDAVFileSystem)
        assert path == "http://example.com/file"

    def test_davs_normalized_to_https(self):
        from gfal_cli.webdav import WebDAVFileSystem

        fso, path = url_to_fs("davs://example.com/file")
        assert isinstance(fso, WebDAVFileSystem)
        assert path == "https://example.com/file"

    def test_storage_options_forwarded(self, tmp_path):
        """storage_options shouldn't cause errors for local filesystem."""
        f = tmp_path / "test.txt"
        f.write_text("x")
        fso, path = url_to_fs(f.as_uri(), {"ssl_verify": True})
        assert Path(path) == f


# ---------------------------------------------------------------------------
# stat
# ---------------------------------------------------------------------------


class TestStat:
    def test_regular_file(self, tmp_path):
        f = tmp_path / "test.txt"
        f.write_bytes(b"hello world")
        si = stat(f.as_uri())
        assert si.st_size == 11
        assert stat_module.S_ISREG(si.st_mode)

    def test_directory(self, tmp_path):
        si = stat(tmp_path.as_uri())
        assert stat_module.S_ISDIR(si.st_mode)

    def test_nonexistent_raises(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            stat((tmp_path / "no_such").as_uri())

    def test_empty_file_size_zero(self, tmp_path):
        f = tmp_path / "empty"
        f.write_bytes(b"")
        si = stat(f.as_uri())
        assert si.st_size == 0

    def test_symlink(self, tmp_path):
        """Stat follows symlinks by default in fsspec."""
        target = tmp_path / "target.txt"
        target.write_text("data")
        link = tmp_path / "link.txt"
        link.symlink_to(target)
        si = stat(link.as_uri())
        assert si.st_size == 4


# ---------------------------------------------------------------------------
# isdir
# ---------------------------------------------------------------------------


class TestIsDir:
    def test_directory(self, tmp_path):
        assert isdir(tmp_path.as_uri())

    def test_file(self, tmp_path):
        f = tmp_path / "test.txt"
        f.write_text("x")
        assert not isdir(f.as_uri())

    def test_nonexistent_returns_false(self, tmp_path):
        assert not isdir((tmp_path / "no_such_dir").as_uri())


# ---------------------------------------------------------------------------
# build_storage_options
# ---------------------------------------------------------------------------


class TestBuildStorageOptions:
    def test_no_cert(self):
        from types import SimpleNamespace

        from gfal_cli.fs import build_storage_options

        params = SimpleNamespace(cert=None, key=None, ssl_verify=True)
        opts = build_storage_options(params)
        assert opts == {}

    def test_cert_and_key(self):
        from types import SimpleNamespace

        from gfal_cli.fs import build_storage_options

        params = SimpleNamespace(
            cert="/path/to/cert.pem", key="/path/to/key.pem", ssl_verify=True
        )
        opts = build_storage_options(params)
        assert opts["client_cert"] == "/path/to/cert.pem"
        assert opts["client_key"] == "/path/to/key.pem"

    def test_cert_without_key_uses_cert_as_key(self):
        from types import SimpleNamespace

        from gfal_cli.fs import build_storage_options

        params = SimpleNamespace(cert="/path/to/proxy.pem", key=None, ssl_verify=True)
        opts = build_storage_options(params)
        assert opts["client_cert"] == "/path/to/proxy.pem"
        assert opts["client_key"] == "/path/to/proxy.pem"

    def test_ssl_verify_false(self):
        from types import SimpleNamespace

        from gfal_cli.fs import build_storage_options

        params = SimpleNamespace(cert=None, key=None, ssl_verify=False)
        opts = build_storage_options(params)
        assert opts["ssl_verify"] is False

    def test_x509_proxy_from_env(self, monkeypatch, tmp_path):
        """X509_USER_PROXY env var is used as client cert when no --cert given."""
        from types import SimpleNamespace

        from gfal_cli.fs import build_storage_options

        proxy = tmp_path / "proxy.pem"
        proxy.write_text("fake proxy")
        monkeypatch.setenv("X509_USER_PROXY", str(proxy))

        params = SimpleNamespace(cert=None, key=None, ssl_verify=True)
        opts = build_storage_options(params)
        assert opts.get("client_cert") == str(proxy)
        assert opts.get("client_key") == str(proxy)

    def test_explicit_cert_overrides_x509_proxy(self, monkeypatch, tmp_path):
        """An explicit --cert flag takes precedence over X509_USER_PROXY."""
        from types import SimpleNamespace

        from gfal_cli.fs import build_storage_options

        proxy = tmp_path / "proxy.pem"
        proxy.write_text("fake proxy")
        explicit_cert = tmp_path / "user_cert.pem"
        explicit_cert.write_text("user cert")
        monkeypatch.setenv("X509_USER_PROXY", str(proxy))

        params = SimpleNamespace(cert=str(explicit_cert), key=None, ssl_verify=True)
        opts = build_storage_options(params)
        assert opts.get("client_cert") == str(explicit_cert)
        # The proxy path must NOT appear
        assert opts.get("client_cert") != str(proxy)

    def test_x509_proxy_nonexistent_path_ignored(self, monkeypatch):
        """A non-existent path in X509_USER_PROXY is silently ignored."""
        from types import SimpleNamespace

        from gfal_cli.fs import build_storage_options

        monkeypatch.setenv("X509_USER_PROXY", "/tmp/this_does_not_exist_gfal_test.pem")

        params = SimpleNamespace(cert=None, key=None, ssl_verify=True)
        opts = build_storage_options(params)
        # A non-existent proxy file must not be forwarded as client_cert
        assert "client_cert" not in opts

    def test_x509_proxy_empty_string_ignored(self, monkeypatch):
        """An empty X509_USER_PROXY is silently ignored."""
        from types import SimpleNamespace

        from gfal_cli.fs import build_storage_options

        monkeypatch.setenv("X509_USER_PROXY", "")

        params = SimpleNamespace(cert=None, key=None, ssl_verify=True)
        opts = build_storage_options(params)
        assert opts == {}
