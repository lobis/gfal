"""Unit tests for the fsspec integration layer (fs.py)."""

import asyncio
import datetime
import stat as stat_module
from pathlib import Path
from unittest.mock import patch

import pytest

from gfal.core.fs import (
    RootProtocolFallbackWarning,
    StatInfo,
    _clear_cached_webdav_filesystems,
    _generic_storage_opts,
    _root_url_to_https,
    _to_timestamp,
    _verify_get_client,
    isdir,
    normalize_url,
    stat,
    url_to_fs,
)

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

    def test_zero_nlink_is_preserved(self):
        si = StatInfo({"nlink": 0})
        assert si.st_nlink == 0

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
        from gfal.core.webdav import WebDAVFileSystem

        fso, path = url_to_fs("http://example.com/file")
        assert isinstance(fso, WebDAVFileSystem)
        assert path == "http://example.com/file"

    def test_https_returns_webdav_fs(self):
        from gfal.core.webdav import WebDAVFileSystem

        fso, path = url_to_fs("https://example.com/file")
        assert isinstance(fso, WebDAVFileSystem)

    def test_dav_normalized_to_http(self):
        from gfal.core.webdav import WebDAVFileSystem

        fso, path = url_to_fs("dav://example.com/file")
        assert isinstance(fso, WebDAVFileSystem)
        assert path == "http://example.com/file"

    def test_davs_normalized_to_https(self):
        from gfal.core.webdav import WebDAVFileSystem

        fso, path = url_to_fs("davs://example.com/file")
        assert isinstance(fso, WebDAVFileSystem)
        assert path == "https://example.com/file"

    def test_http_reuses_cached_webdav_fs_for_same_storage_options(self):
        _clear_cached_webdav_filesystems()
        try:
            first, _ = url_to_fs("https://example.com/one", {"ssl_verify": False})
            second, _ = url_to_fs("https://example.com/two", {"ssl_verify": False})
            third, _ = url_to_fs("https://example.com/three", {"ssl_verify": True})
        finally:
            _clear_cached_webdav_filesystems()

        assert first is second
        assert first is not third

    def test_storage_options_forwarded(self, tmp_path):
        """storage_options shouldn't cause errors for local filesystem."""
        f = tmp_path / "test.txt"
        f.write_text("x")
        fso, path = url_to_fs(f.as_uri(), {"ssl_verify": True})
        assert Path(path) == f

    def test_root_url_to_https_preserves_absolute_path(self):
        assert (
            _root_url_to_https("root://eospublic.cern.ch//eos/opendata/file.root")
            == "https://eospublic.cern.ch/eos/opendata/file.root"
        )

    def test_root_falls_back_to_https_when_xrootd_deps_missing(self):
        with (
            patch(
                "gfal.core.fs.fsspec.url_to_fs",
                side_effect=ModuleNotFoundError("No module named 'fsspec_xrootd'"),
            ),
            pytest.warns(
                RootProtocolFallbackWarning,
                match=(
                    "retrying root://eospublic.cern.ch//eos/opendata/file.root "
                    "via HTTPS as https://eospublic.cern.ch/eos/opendata/file.root"
                ),
            ),
        ):
            fso, path = url_to_fs("root://eospublic.cern.ch//eos/opendata/file.root")

        from gfal.core.webdav import WebDAVFileSystem

        assert isinstance(fso, WebDAVFileSystem)
        assert path == "https://eospublic.cern.ch/eos/opendata/file.root"


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

        from gfal.core.fs import build_storage_options

        params = SimpleNamespace(cert=None, key=None, ssl_verify=True)
        opts = build_storage_options(params)
        assert opts == {}

    def test_cert_and_key(self):
        from types import SimpleNamespace

        from gfal.core.fs import build_storage_options

        params = SimpleNamespace(
            cert="/path/to/cert.pem", key="/path/to/key.pem", ssl_verify=True
        )
        opts = build_storage_options(params)
        assert opts["client_cert"] == "/path/to/cert.pem"
        assert opts["client_key"] == "/path/to/key.pem"

    def test_cert_without_key_uses_cert_as_key(self):
        from types import SimpleNamespace

        from gfal.core.fs import build_storage_options

        params = SimpleNamespace(cert="/path/to/proxy.pem", key=None, ssl_verify=True)
        opts = build_storage_options(params)
        assert opts["client_cert"] == "/path/to/proxy.pem"
        assert opts["client_key"] == "/path/to/proxy.pem"

    def test_ssl_verify_false(self):
        from types import SimpleNamespace

        from gfal.core.fs import build_storage_options

        params = SimpleNamespace(cert=None, key=None, ssl_verify=False)
        opts = build_storage_options(params)
        assert opts["ssl_verify"] is False

    def test_timeout_forwarded(self):
        from types import SimpleNamespace

        from gfal.core.fs import build_storage_options

        params = SimpleNamespace(cert=None, key=None, ssl_verify=True, timeout=45)
        opts = build_storage_options(params)
        assert opts["timeout"] == 45

    def test_timeout_zero_not_forwarded(self):
        from types import SimpleNamespace

        from gfal.core.fs import build_storage_options

        params = SimpleNamespace(cert=None, key=None, ssl_verify=True, timeout=0)
        opts = build_storage_options(params)
        assert "timeout" not in opts

    def test_x509_proxy_from_env(self, monkeypatch, tmp_path):
        """X509_USER_PROXY env var is used as client cert when no --cert given."""
        from types import SimpleNamespace

        from gfal.core.fs import build_storage_options

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

        from gfal.core.fs import build_storage_options

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

        from gfal.core.fs import build_storage_options

        monkeypatch.setenv("X509_USER_PROXY", "/tmp/this_does_not_exist_gfal_test.pem")

        params = SimpleNamespace(cert=None, key=None, ssl_verify=True)
        opts = build_storage_options(params)
        # A non-existent proxy file must not be forwarded as client_cert
        assert "client_cert" not in opts

    def test_x509_proxy_empty_string_ignored(self, monkeypatch):
        """An empty X509_USER_PROXY is silently ignored."""
        from types import SimpleNamespace

        from gfal.core.fs import build_storage_options

        monkeypatch.setenv("X509_USER_PROXY", "")

        params = SimpleNamespace(cert=None, key=None, ssl_verify=True)
        opts = build_storage_options(params)
        assert opts == {}


# ---------------------------------------------------------------------------
# Bearer token support in build_storage_options
# ---------------------------------------------------------------------------


class TestBuildStorageOptionsBearerToken:
    def test_bearer_token_env_var(self, monkeypatch):
        from types import SimpleNamespace

        from gfal.core.fs import build_storage_options

        monkeypatch.setenv("BEARER_TOKEN", "token-from-env")
        monkeypatch.delenv("BEARER_TOKEN_FILE", raising=False)

        params = SimpleNamespace(cert=None, key=None, ssl_verify=True)
        opts = build_storage_options(params)
        assert opts.get("bearer_token") == "token-from-env"

    def test_bearer_token_file_env_var(self, monkeypatch, tmp_path):
        from types import SimpleNamespace

        from gfal.core.fs import build_storage_options

        token_file = tmp_path / "token"
        token_file.write_text("file-token\n")
        monkeypatch.delenv("BEARER_TOKEN", raising=False)
        monkeypatch.setenv("BEARER_TOKEN_FILE", str(token_file))

        params = SimpleNamespace(cert=None, key=None, ssl_verify=True)
        opts = build_storage_options(params)
        assert opts.get("bearer_token") == "file-token"

    def test_bearer_token_env_takes_priority_over_file(self, monkeypatch, tmp_path):
        from types import SimpleNamespace

        from gfal.core.fs import build_storage_options

        token_file = tmp_path / "token"
        token_file.write_text("file-token\n")
        monkeypatch.setenv("BEARER_TOKEN", "env-token")
        monkeypatch.setenv("BEARER_TOKEN_FILE", str(token_file))

        params = SimpleNamespace(cert=None, key=None, ssl_verify=True)
        opts = build_storage_options(params)
        assert opts.get("bearer_token") == "env-token"

    def test_no_bearer_token_env_no_key(self, monkeypatch):
        from types import SimpleNamespace

        from gfal.core.fs import build_storage_options

        monkeypatch.delenv("BEARER_TOKEN", raising=False)
        monkeypatch.delenv("BEARER_TOKEN_FILE", raising=False)

        params = SimpleNamespace(cert=None, key=None, ssl_verify=True)
        opts = build_storage_options(params)
        assert "bearer_token" not in opts


# ---------------------------------------------------------------------------
# EOS authz token support
# ---------------------------------------------------------------------------


class TestEosAuthzToken:
    def test_authz_token_param_added_to_storage_options(self):
        from types import SimpleNamespace

        from gfal.core.fs import build_storage_options

        params = SimpleNamespace(
            cert=None,
            key=None,
            ssl_verify=True,
            authz_token="zteos64:abc",
        )
        opts = build_storage_options(params)

        assert opts["authz_token"] == "zteos64:abc"
        assert "bearer_token" not in opts

    def test_eosauthz_env_added_to_storage_options(self, monkeypatch):
        from types import SimpleNamespace

        from gfal.core.fs import build_storage_options

        monkeypatch.setenv("EOSAUTHZ", "zteos64:env")
        monkeypatch.delenv("GFAL_AUTHZ_TOKEN", raising=False)

        params = SimpleNamespace(
            cert=None,
            key=None,
            ssl_verify=True,
            authz_token=None,
        )
        opts = build_storage_options(params)

        assert opts["authz_token"] == "zteos64:env"

    def test_authz_cli_param_takes_priority_over_env(self, monkeypatch):
        from types import SimpleNamespace

        from gfal.core.fs import build_storage_options

        monkeypatch.setenv("EOSAUTHZ", "zteos64:env")

        params = SimpleNamespace(
            cert=None,
            key=None,
            ssl_verify=True,
            authz_token="zteos64:cli",
        )
        opts = build_storage_options(params)

        assert opts["authz_token"] == "zteos64:cli"

    def test_gfal_authz_token_env_fallback(self, monkeypatch):
        from types import SimpleNamespace

        from gfal.core.fs import build_storage_options

        monkeypatch.delenv("EOSAUTHZ", raising=False)
        monkeypatch.setenv("GFAL_AUTHZ_TOKEN", "zteos64:gfal")

        params = SimpleNamespace(cert=None, key=None, ssl_verify=True)
        opts = build_storage_options(params)

        assert opts["authz_token"] == "zteos64:gfal"

    def test_eos_authz_url_for_root_preserves_query(self):
        from gfal.core.fs import eos_authz_url

        url = eos_authz_url(
            "root://eospilot.cern.ch//eos/pilot/test/file.txt?eos.app=gfal",
            "zteos64:abc",
        )

        assert url is not None
        assert url.startswith("root://eospilot.cern.ch//eos/pilot/test/file.txt?")
        assert "eos.app=gfal" in url
        assert "authz=zteos64%3Aabc" in url

    def test_eos_authz_url_for_https(self):
        from gfal.core.fs import eos_authz_url

        url = eos_authz_url(
            "https://eospilot.cern.ch//eos/pilot/test/file.txt",
            "zteos64:abc",
        )

        assert url == (
            "https://eospilot.cern.ch//eos/pilot/test/file.txt?authz=zteos64%3Aabc"
        )

    def test_eos_authz_url_does_not_override_existing_authz(self):
        from gfal.core.fs import eos_authz_url

        url = eos_authz_url(
            "root://eospilot.cern.ch//eos/pilot/test/file.txt?authz=old",
            "zteos64:new",
        )

        assert url is not None
        assert "authz=old" in url
        assert "new" not in url

    def test_eos_authz_url_ignores_non_eos_and_local_paths(self):
        from gfal.core.fs import eos_authz_url

        assert eos_authz_url("root://example.org//some/file", "tok") is None
        assert eos_authz_url("https://myeos.example.org//file", "tok") is None
        assert eos_authz_url("/tmp/local-file", "tok") is None

    def test_eos_authz_url_accepts_non_cern_eos_hosts(self):
        from gfal.core.fs import eos_authz_url

        url = eos_authz_url("root://eos.example.org//eos/foo/file", "tok")
        assert url is not None
        assert "authz=tok" in url

    def test_bearer_token_file_missing_ignored(self, monkeypatch):
        from types import SimpleNamespace

        from gfal.core.fs import build_storage_options

        monkeypatch.delenv("BEARER_TOKEN", raising=False)
        monkeypatch.setenv("BEARER_TOKEN_FILE", "/tmp/this_does_not_exist_bearer_token")

        params = SimpleNamespace(cert=None, key=None, ssl_verify=True)
        opts = build_storage_options(params)
        assert "bearer_token" not in opts


# ---------------------------------------------------------------------------
# fsspec HTTP client factory
# ---------------------------------------------------------------------------


class TestVerifyGetClient:
    def test_timeout_and_headers_are_forwarded_to_aiohttp(self, monkeypatch):
        connector_calls = []
        session_calls = []

        class _FakeConnector:
            pass

        class _FakeSession:
            pass

        def _fake_connector(**kwargs):
            connector_calls.append(kwargs)
            return _FakeConnector()

        def _fake_session(**kwargs):
            session_calls.append(kwargs)
            return _FakeSession()

        monkeypatch.setattr("aiohttp.TCPConnector", _fake_connector)
        monkeypatch.setattr("aiohttp.ClientSession", _fake_session)

        session = asyncio.run(
            _verify_get_client(
                verify=True,
                timeout=42,
                headers={"Authorization": "Bearer abc"},
                ipv4_only=True,
            )
        )

        assert isinstance(session, _FakeSession)
        assert connector_calls[0]["family"] != 0
        assert session_calls[0]["headers"] == {"Authorization": "Bearer abc"}
        assert session_calls[0]["timeout"].total == 42


# ---------------------------------------------------------------------------
# _to_timestamp
# ---------------------------------------------------------------------------


class TestToTimestamp:
    def test_none_returns_zero(self):
        assert _to_timestamp(None) == 0.0

    def test_float_unchanged(self):
        assert _to_timestamp(1_700_000_000.0) == 1_700_000_000.0

    def test_int_unchanged(self):
        assert _to_timestamp(1_700_000_000) == 1_700_000_000.0

    def test_zero_preserved(self):
        """Unix epoch (0) must not be treated as missing."""
        assert _to_timestamp(0) == 0.0
        assert _to_timestamp(0.0) == 0.0

    def test_naive_datetime(self):
        """Naive datetime is converted via .timestamp()."""
        dt = datetime.datetime(2024, 1, 15, 12, 0, 0)
        assert _to_timestamp(dt) == dt.timestamp()

    def test_aware_datetime(self):
        """Timezone-aware datetime is handled correctly."""
        dt = datetime.datetime(2024, 1, 15, 12, 0, 0, tzinfo=datetime.timezone.utc)
        assert _to_timestamp(dt) == dt.timestamp()


# ---------------------------------------------------------------------------
# StatInfo — alternate mtime keys (SFTP and S3)
# ---------------------------------------------------------------------------


class TestStatInfoAlternateMtimeKeys:
    def test_sftp_time_key(self):
        """SFTP info dicts use 'time' for mtime instead of 'mtime'."""
        ts = 1_700_000_000.0
        si = StatInfo({"type": "file", "size": 0, "time": ts})
        assert si.st_mtime == ts
        assert si.st_atime == ts
        assert si.st_ctime == ts

    def test_sftp_time_key_with_datetime(self):
        """SFTP may return a datetime object under the 'time' key."""
        dt = datetime.datetime(2024, 3, 10, 8, 0, 0, tzinfo=datetime.timezone.utc)
        si = StatInfo({"type": "file", "size": 0, "time": dt})
        assert si.st_mtime == pytest.approx(dt.timestamp(), abs=1e-3)

    def test_s3_last_modified_key(self):
        """S3 info dicts use 'LastModified' (a datetime) for mtime."""
        dt = datetime.datetime(2024, 6, 1, 0, 0, 0, tzinfo=datetime.timezone.utc)
        si = StatInfo({"type": "file", "size": 100, "LastModified": dt})
        assert si.st_mtime == pytest.approx(dt.timestamp(), abs=1e-3)
        assert si.st_atime == si.st_mtime
        assert si.st_ctime == si.st_mtime

    def test_mtime_key_takes_precedence_over_time(self):
        """'mtime' wins over 'time' when both are present."""
        si = StatInfo({"mtime": 1000.0, "time": 2000.0})
        assert si.st_mtime == 1000.0

    def test_no_mtime_keys_returns_zero(self):
        """When no mtime keys are present, mtime defaults to 0."""
        si = StatInfo({"type": "file", "size": 0})
        assert si.st_mtime == 0.0


# ---------------------------------------------------------------------------
# _generic_storage_opts
# ---------------------------------------------------------------------------


class TestGenericStorageOpts:
    def test_strips_http_specific_keys(self):
        opts = {
            "client_cert": "/tmp/cert.pem",
            "client_key": "/tmp/key.pem",
            "ssl_verify": False,
            "bearer_token": "tok",
            "ipv4_only": True,
            "ipv6_only": False,
            "timeout": 30,
        }
        result = _generic_storage_opts(opts)
        assert result == {}

    def test_preserves_non_http_keys(self):
        opts = {
            "key": "AKID",
            "secret": "SECRET",
            "endpoint_url": "http://localhost:5000",
        }
        result = _generic_storage_opts(opts)
        assert result == opts

    def test_mixed_opts(self):
        opts = {
            "client_cert": "/tmp/cert.pem",
            "anon": True,
            "region_name": "us-east-1",
        }
        result = _generic_storage_opts(opts)
        assert "client_cert" not in result
        assert result["anon"] is True
        assert result["region_name"] == "us-east-1"

    def test_empty_opts(self):
        assert _generic_storage_opts({}) == {}
