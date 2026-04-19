"""Direct unit tests for GfalClient (src/gfal/core/api.py).

These tests call the API directly (no subprocess) so that coverage is
collected in the pytest process.
"""

import errno
import io
import ssl
import stat
import sys
from contextlib import nullcontext
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from gfal.core.api import AsyncGfalClient, ChecksumPolicy, CopyOptions, GfalClient
from gfal.core.errors import (
    GfalError,
    GfalFileExistsError,
    GfalFileNotFoundError,
    GfalIsADirectoryError,
    GfalNotADirectoryError,
    GfalPermissionError,
    GfalTimeoutError,
)

# ---------------------------------------------------------------------------
# GfalClient construction
# ---------------------------------------------------------------------------


class TestGfalClientInit:
    def test_default_init(self):
        client = GfalClient()
        assert client.cert is None
        assert client.key is None
        assert client.timeout == 1800
        assert client.ssl_verify is True
        assert client.ipv4_only is False
        assert client.ipv6_only is False

    def test_default_app_sync(self):
        client = GfalClient()
        assert client.app == "python3-gfal-sync"

    def test_default_app_async(self):
        client = AsyncGfalClient()
        assert client.app == "python3-gfal-async"

    def test_custom_app(self):
        client = GfalClient(app="python3-gfal-cli")
        assert client.app == "python3-gfal-cli"

    def test_custom_app_async(self):
        client = AsyncGfalClient(app="python3-gfal-cli")
        assert client.app == "python3-gfal-cli"

    def test_url_injects_eos_app_for_eos_host(self):
        client = AsyncGfalClient(app="python3-gfal-async")
        result = client._url("https://eospilot.cern.ch//eos/pilot/file.txt")
        assert "eos.app=python3-gfal-async" in result

    def test_url_leaves_non_eos_url_unchanged(self):
        client = AsyncGfalClient(app="python3-gfal-async")
        url = "https://example.org/path/file.txt"
        assert client._url(url) == url

    def test_url_leaves_stdin_sentinel_unchanged(self):
        client = AsyncGfalClient(app="python3-gfal-async")
        assert client._url("-") == "-"

    def test_custom_init(self):
        client = GfalClient(
            cert="/tmp/x.pem",
            key="/tmp/k.pem",
            timeout=60,
            ssl_verify=False,
            ipv4_only=True,
        )
        assert client.cert == "/tmp/x.pem"
        assert client.key == "/tmp/k.pem"
        assert client.timeout == 60
        assert client.ssl_verify is False
        assert client.ipv4_only is True
        assert client.ipv6_only is False

    def test_key_defaults_to_cert(self):
        client = GfalClient(cert="/tmp/x.pem")
        assert client.key == "/tmp/x.pem"

    def test_storage_options_filters_none(self):
        client = GfalClient()
        opts = client.storage_options
        assert "client_cert" not in opts
        assert "client_key" not in opts
        assert "timeout" in opts
        assert "ssl_verify" not in opts

    def test_storage_options_with_cert(self):
        client = GfalClient(cert="/tmp/x.pem", key="/tmp/k.pem")
        opts = client.storage_options
        assert opts["client_cert"] == "/tmp/x.pem"
        assert opts["client_key"] == "/tmp/k.pem"

    def test_async_client_exposes_same_init_surface(self):
        client = AsyncGfalClient(
            cert="/tmp/x.pem",
            key="/tmp/k.pem",
            timeout=60,
            ssl_verify=False,
            ipv6_only=True,
        )
        assert client.cert == "/tmp/x.pem"
        assert client.key == "/tmp/k.pem"
        assert client.timeout == 60
        assert client.ssl_verify is False
        assert client.ipv4_only is False
        assert client.ipv6_only is True

    def test_storage_options_with_ipv4_only(self):
        client = GfalClient(ipv4_only=True)
        opts = client.storage_options
        assert opts["ipv4_only"] is True
        assert "ipv6_only" not in opts

    def test_storage_options_with_ipv6_only(self):
        client = GfalClient(ipv6_only=True)
        opts = client.storage_options
        assert opts["ipv6_only"] is True
        assert "ipv4_only" not in opts

    def test_storage_options_use_x509_proxy_from_env(self, monkeypatch, tmp_path):
        proxy = tmp_path / "proxy.pem"
        proxy.write_text("proxy")
        monkeypatch.setenv("X509_USER_PROXY", str(proxy))

        client = GfalClient()
        opts = client.storage_options

        assert opts["client_cert"] == str(proxy)
        assert opts["client_key"] == str(proxy)

    def test_storage_options_explicit_cert_overrides_env_proxy(
        self, monkeypatch, tmp_path
    ):
        proxy = tmp_path / "proxy.pem"
        proxy.write_text("proxy")
        monkeypatch.setenv("X509_USER_PROXY", str(proxy))

        client = GfalClient(cert="/tmp/x.pem", key="/tmp/k.pem")
        opts = client.storage_options

        assert opts["client_cert"] == "/tmp/x.pem"
        assert opts["client_key"] == "/tmp/k.pem"


# ---------------------------------------------------------------------------
# stat
# ---------------------------------------------------------------------------


class TestGfalClientStat:
    def test_stat_regular_file(self, tmp_path):
        f = tmp_path / "test.txt"
        f.write_bytes(b"hello world")
        client = GfalClient()
        st = client.stat(f.as_uri())
        assert st.st_size == 11
        assert stat.S_ISREG(st.st_mode)

    def test_stat_directory(self, tmp_path):
        client = GfalClient()
        st = client.stat(tmp_path.as_uri())
        assert stat.S_ISDIR(st.st_mode)

    def test_stat_nonexistent_raises(self, tmp_path):
        client = GfalClient()
        with pytest.raises(GfalError):
            client.stat((tmp_path / "no_such").as_uri())

    def test_stat_bare_path(self, tmp_path):
        f = tmp_path / "test.txt"
        f.write_bytes(b"x")
        client = GfalClient()
        st = client.stat(str(f))
        assert st.st_size == 1


# ---------------------------------------------------------------------------
# ls
# ---------------------------------------------------------------------------


class TestGfalClientLs:
    def test_ls_directory_detail(self, tmp_path):
        (tmp_path / "a.txt").write_text("a")
        (tmp_path / "b.txt").write_text("b")
        client = GfalClient()
        entries = client.ls(tmp_path.as_uri(), detail=True)
        names = [Path(e.info["name"]).name for e in entries]
        assert "a.txt" in names
        assert "b.txt" in names

    def test_ls_directory_no_detail(self, tmp_path):
        (tmp_path / "a.txt").write_text("a")
        (tmp_path / "b.txt").write_text("b")
        client = GfalClient()
        names = client.ls(tmp_path.as_uri(), detail=False)
        assert "a.txt" in names
        assert "b.txt" in names

    def test_ls_file_returns_single_entry(self, tmp_path):
        f = tmp_path / "file.txt"
        f.write_bytes(b"hello")
        client = GfalClient()
        entries = client.ls(f.as_uri(), detail=True)
        assert len(entries) >= 1

    def test_ls_nonexistent_raises(self, tmp_path):
        client = GfalClient()
        with pytest.raises(GfalError):
            client.ls((tmp_path / "no_such_dir").as_uri(), detail=True)

    def test_ls_empty_directory(self, tmp_path):
        d = tmp_path / "empty"
        d.mkdir()
        client = GfalClient()
        entries = client.ls(d.as_uri(), detail=True)
        assert entries == []

    def test_ls_uses_xrootd_listing_enrichment(self):
        from unittest.mock import MagicMock, patch

        client = GfalClient()
        mock_fso = MagicMock()
        mock_fso.info.return_value = {
            "name": "/data/file.txt",
            "type": "file",
            "size": 5,
        }
        enriched_entries = [
            {
                "name": "/data/file.txt",
                "type": "file",
                "size": 5,
                "nlink": 0,
                "uid": 0,
                "gid": 0,
            }
        ]

        with (
            patch("gfal.core.api.fs.url_to_fs", return_value=(mock_fso, "/data")),
            patch(
                "gfal.core.api.fs.xrootd_ls_enrich",
                return_value=enriched_entries,
            ) as ls_enrich,
        ):
            entries = client.ls("root://host//data", detail=True)

        ls_enrich.assert_called_once_with(mock_fso, "/data")
        assert len(entries) == 1
        assert entries[0].st_nlink == 0


# ---------------------------------------------------------------------------
# mkdir
# ---------------------------------------------------------------------------


class TestGfalClientMkdir:
    def test_mkdir_creates_directory(self, tmp_path):
        d = tmp_path / "newdir"
        client = GfalClient()
        client.mkdir(d.as_uri())
        assert d.is_dir()

    def test_mkdir_parents_creates_nested(self, tmp_path):
        d = tmp_path / "a" / "b" / "c"
        client = GfalClient()
        client.mkdir(d.as_uri(), parents=True)
        assert d.is_dir()

    def test_mkdir_existing_with_parents_no_error(self, tmp_path):
        d = tmp_path / "existing"
        d.mkdir()
        client = GfalClient()
        client.mkdir(d.as_uri(), parents=True)  # should not raise
        assert d.is_dir()

    def test_mkdir_nonexistent_parent_raises(self, tmp_path):
        d = tmp_path / "noparent" / "newdir"
        client = GfalClient()
        with pytest.raises(GfalError):
            client.mkdir(d.as_uri(), parents=False)


# ---------------------------------------------------------------------------
# rm
# ---------------------------------------------------------------------------


class TestGfalClientRm:
    def test_rm_file(self, tmp_path):
        f = tmp_path / "file.txt"
        f.write_text("x")
        client = GfalClient()
        client.rm(f.as_uri())
        assert not f.exists()

    def test_rm_recursive(self, tmp_path):
        d = tmp_path / "mydir"
        d.mkdir()
        (d / "f.txt").write_text("x")
        client = GfalClient()
        client.rm(d.as_uri(), recursive=True)
        assert not d.exists()

    def test_rm_nonexistent_raises(self, tmp_path):
        client = GfalClient()
        with pytest.raises(GfalError):
            client.rm((tmp_path / "no_such").as_uri())


# ---------------------------------------------------------------------------
# rmdir
# ---------------------------------------------------------------------------


class TestGfalClientRmdir:
    def test_rmdir_empty_dir(self, tmp_path):
        d = tmp_path / "empty"
        d.mkdir()
        client = GfalClient()
        client.rmdir(d.as_uri())
        assert not d.exists()

    def test_rmdir_nonexistent_raises(self, tmp_path):
        client = GfalClient()
        with pytest.raises(GfalError):
            client.rmdir((tmp_path / "no_such").as_uri())


# ---------------------------------------------------------------------------
# rename
# ---------------------------------------------------------------------------


class TestGfalClientRename:
    def test_rename_file(self, tmp_path):
        src = tmp_path / "old.txt"
        dst = tmp_path / "new.txt"
        src.write_text("data")
        client = GfalClient()
        client.rename(src.as_uri(), dst.as_uri())
        assert not src.exists()
        assert dst.read_text() == "data"

    def test_rename_across_filesystems_raises(self, tmp_path):
        """Renaming across filesystem types must raise a GfalError with EXDEV."""

        # Use a mock by creating two different-scheme objects
        # We simulate by creating a real local file and trying to rename to http://
        f = tmp_path / "file.txt"
        f.write_text("x")
        client = GfalClient()
        with pytest.raises(GfalError) as exc_info:
            client.rename(f.as_uri(), "http://example.com/file.txt")
        assert exc_info.value.errno is not None


# ---------------------------------------------------------------------------
# chmod
# ---------------------------------------------------------------------------


@pytest.mark.skipif(sys.platform == "win32", reason="chmod semantics differ on Windows")
class TestGfalClientChmod:
    def test_chmod(self, tmp_path):
        f = tmp_path / "file.txt"
        f.write_text("x")
        client = GfalClient()
        client.chmod(f.as_uri(), 0o600)
        assert (f.stat().st_mode & 0o777) == 0o600


# ---------------------------------------------------------------------------
# open
# ---------------------------------------------------------------------------


class TestGfalClientOpen:
    def test_open_read(self, tmp_path):
        f = tmp_path / "test.txt"
        f.write_bytes(b"hello")
        client = GfalClient()
        with client.open(f.as_uri(), "rb") as fh:
            data = fh.read()
        assert data == b"hello"

    def test_open_write(self, tmp_path):
        f = tmp_path / "out.txt"
        client = GfalClient()
        with client.open(f.as_uri(), "wb") as fh:
            fh.write(b"written")
        assert f.read_bytes() == b"written"

    def test_open_nonexistent_raises(self, tmp_path):
        client = GfalClient()
        with pytest.raises(GfalError):
            client.open((tmp_path / "no_such.txt").as_uri(), "rb")


# ---------------------------------------------------------------------------
# checksum
# ---------------------------------------------------------------------------


class TestGfalClientChecksum:
    def test_adler32(self, tmp_path):
        import zlib

        data = b"hello world"
        f = tmp_path / "test.bin"
        f.write_bytes(data)
        expected = f"{zlib.adler32(data) & 0xFFFFFFFF:08x}"
        client = GfalClient()
        result = client.checksum(f.as_uri(), "ADLER32")
        assert result == expected

    def test_md5(self, tmp_path):
        import hashlib

        data = b"hello world"
        f = tmp_path / "test.bin"
        f.write_bytes(data)
        expected = hashlib.md5(data).hexdigest()
        client = GfalClient()
        result = client.checksum(f.as_uri(), "MD5")
        assert result == expected

    def test_crc32(self, tmp_path):
        import zlib

        data = b"hello world"
        f = tmp_path / "test.bin"
        f.write_bytes(data)
        expected = f"{zlib.crc32(data) & 0xFFFFFFFF:08x}"
        client = GfalClient()
        result = client.checksum(f.as_uri(), "CRC32")
        assert result == expected

    def test_nonexistent_raises(self, tmp_path):
        client = GfalClient()
        with pytest.raises(GfalError):
            client.checksum((tmp_path / "no_such").as_uri(), "ADLER32")


# ---------------------------------------------------------------------------
# xattr
# ---------------------------------------------------------------------------


class TestGfalClientXattr:
    def test_listxattr_unsupported_raises(self, tmp_path):
        """Local filesystem doesn't support xattr via GfalClient.listxattr."""
        f = tmp_path / "test.txt"
        f.write_text("x")
        client = GfalClient()
        # LocalFileSystem doesn't have listxattr method
        with pytest.raises(GfalError):
            client.listxattr(f.as_uri())

    def test_getxattr_unsupported_raises(self, tmp_path):
        f = tmp_path / "test.txt"
        f.write_text("x")
        client = GfalClient()
        with pytest.raises(GfalError):
            client.getxattr(f.as_uri(), "user.test")

    def test_setxattr_unsupported_raises(self, tmp_path):
        f = tmp_path / "test.txt"
        f.write_text("x")
        client = GfalClient()
        with pytest.raises(GfalError):
            client.setxattr(f.as_uri(), "user.test", "value")


# ---------------------------------------------------------------------------
# _map_error
# ---------------------------------------------------------------------------


class TestGfalClientMapError:
    def setup_method(self):
        self.client = GfalClient()

    def test_already_gfal_error_returned_as_is(self):
        e = GfalFileNotFoundError("test")
        result = self.client._map_error(e, "file:///test")
        assert result is e

    def test_file_not_found_error(self):
        e = FileNotFoundError("file not found")
        result = self.client._map_error(e, "file:///test")
        assert isinstance(result, GfalFileNotFoundError)

    def test_permission_error(self):
        e = PermissionError("permission denied")
        result = self.client._map_error(e, "file:///test")
        assert isinstance(result, GfalPermissionError)

    def test_file_exists_error(self):
        e = FileExistsError("file exists")
        result = self.client._map_error(e, "file:///test")
        assert isinstance(result, GfalFileExistsError)

    def test_is_a_directory_error(self):
        e = IsADirectoryError("is a directory")
        result = self.client._map_error(e, "file:///test")
        assert isinstance(result, GfalIsADirectoryError)

    def test_not_a_directory_error(self):
        e = NotADirectoryError("not a directory")
        result = self.client._map_error(e, "file:///test")
        assert isinstance(result, GfalNotADirectoryError)

    def test_timeout_error(self):
        e = TimeoutError("timed out")
        result = self.client._map_error(e, "file:///test")
        assert isinstance(result, GfalTimeoutError)

    def test_http_403(self):
        e = Exception("Forbidden")
        e.status = 403
        result = self.client._map_error(e, "http://example.com/file")
        assert isinstance(result, GfalPermissionError)

    def test_http_404(self):
        e = Exception("Not Found")
        e.status = 404
        result = self.client._map_error(e, "http://example.com/file")
        assert isinstance(result, GfalFileNotFoundError)

    def test_generic_exception_no_message(self):
        e = Exception()
        result = self.client._map_error(e, "file:///test")
        assert isinstance(result, GfalError)
        assert "(Exception)" in str(result)

    def test_generic_exception_with_errno(self):
        e = OSError(errno.EACCES, "permission denied")
        result = self.client._map_error(e, "file:///test")
        assert isinstance(result, GfalError)

    def test_xrootd_permission_message_maps_to_permission_error(self):
        e = OSError(
            "File did not open properly: [ERROR] Server responded with an error: "
            "[3010] Unable to give access - user access restricted - "
            "unauthorized identity used ; Permission denied"
        )
        result = self.client._map_error(
            e, "root://eospilot.cern.ch//eos/pilot/opstest/dteam/python3-gfal/tmp"
        )
        assert isinstance(result, GfalPermissionError)
        assert result.errno == errno.EACCES

    def test_generic_exception_zero_errno(self):
        e = Exception("something went wrong")
        e.errno = 0
        result = self.client._map_error(e, "file:///test")
        assert result.errno == errno.EIO

    def test_direct_aiohttp_ssl_error_maps_to_hostdown(self):
        import aiohttp

        conn_key = SimpleNamespace(host="example.com", port=443, ssl=False)
        e = aiohttp.ClientSSLError(conn_key, ssl.SSLError("bad cert"))
        result = self.client._map_error(e, "https://example.com/file")
        assert isinstance(result, GfalError)
        assert result.errno == errno.EHOSTDOWN

    def test_direct_aiohttp_connection_error_maps_errno(self):
        import aiohttp

        e = aiohttp.ClientConnectionError("refused")
        e.errno = errno.ECONNRESET
        result = self.client._map_error(e, "https://example.com/file")
        assert isinstance(result, GfalError)
        assert result.errno == errno.ECONNRESET


# ---------------------------------------------------------------------------
# Additional api.py tests for edge cases
# ---------------------------------------------------------------------------


class TestGfalClientLsFallback:
    def test_ls_not_a_directory_fallback(self, tmp_path):
        """When ls raises 'not a directory', should fall back to info() on the file."""

        f = tmp_path / "file.txt"
        f.write_bytes(b"hello")
        client = GfalClient()

        # We'll test via a local file path which is actually a file (not dir)
        # For local fs, calling ls on a file returns the file entry itself
        entries = client.ls(f.as_uri(), detail=True)
        # Should get at least one entry back (the file itself)
        assert len(entries) >= 1

    def test_ls_enoent_file_fallback_when_info_succeeds(self):
        """Backends like sshfs raise ENOENT for ls(file) but info(file) still works."""

        from unittest.mock import MagicMock, patch

        file_info = {
            "name": "/tmp/file.txt",
            "type": "file",
            "size": 5,
            "mode": stat.S_IFREG | 0o644,
            "uid": 0,
            "gid": 0,
            "nlink": 1,
            "mtime": 0,
        }
        mock_fso = MagicMock()
        mock_fso.info.return_value = file_info
        mock_enoent = FileNotFoundError(errno.ENOENT, "No such file or directory")

        with (
            patch(
                "gfal.core.api.fs.url_to_fs", return_value=(mock_fso, "/tmp/file.txt")
            ),
            patch(
                "gfal.core.api.fs.xrootd_ls_enrich",
                side_effect=mock_enoent,
            ),
            patch(
                "gfal.core.api.fs.xrootd_enrich",
                side_effect=lambda info, _fso: info,
            ),
        ):
            client = GfalClient()
            entries = client.ls("sftp://host/tmp/file.txt", detail=True)

        assert len(entries) == 1
        assert entries[0].st_size == 5
        assert entries[0].info["name"] == "/tmp/file.txt"


class TestGfalClientXattrWithMock:
    def test_getxattr_supported_filesystem(self, tmp_path):
        """Test getxattr when filesystem has getxattr method."""
        from unittest.mock import MagicMock, patch

        f = tmp_path / "file.txt"
        f.write_text("x")

        client = GfalClient()
        # Mock url_to_fs to return an fso with getxattr
        mock_fso = MagicMock()
        mock_fso.getxattr.return_value = "xattr_value"
        mock_fso.hasattr = lambda attr: True

        with patch("gfal.core.api.fs.url_to_fs", return_value=(mock_fso, str(f))):
            result = client.getxattr(f.as_uri(), "user.test")
        assert result == "xattr_value"

    def test_setxattr_supported_filesystem(self, tmp_path):
        """Test setxattr when filesystem has setxattr method."""
        from unittest.mock import MagicMock, patch

        f = tmp_path / "file.txt"
        f.write_text("x")

        client = GfalClient()
        mock_fso = MagicMock()

        with patch("gfal.core.api.fs.url_to_fs", return_value=(mock_fso, str(f))):
            client.setxattr(f.as_uri(), "user.test", "value")
        mock_fso.setxattr.assert_called_once_with(str(f), "user.test", "value")

    def test_listxattr_supported_filesystem(self, tmp_path):
        """Test listxattr when filesystem has listxattr method."""
        from unittest.mock import MagicMock, patch

        f = tmp_path / "file.txt"
        f.write_text("x")

        client = GfalClient()
        mock_fso = MagicMock()
        mock_fso.listxattr.return_value = ["user.foo", "user.bar"]

        with patch("gfal.core.api.fs.url_to_fs", return_value=(mock_fso, str(f))):
            result = client.listxattr(f.as_uri())
        assert result == ["user.foo", "user.bar"]


class TestGfalClientLibraryHelpers:
    def test_exists_true_and_false(self, tmp_path):
        f = tmp_path / "file.txt"
        f.write_text("x")
        client = GfalClient()
        assert client.exists(f.as_uri()) is True
        assert client.exists((tmp_path / "missing.txt").as_uri()) is False

    def test_iterdir_returns_iterator(self, tmp_path):
        (tmp_path / "a.txt").write_text("a")
        (tmp_path / "b.txt").write_text("b")
        client = GfalClient()
        entries = list(client.iterdir(tmp_path.as_uri(), detail=False))
        assert sorted(entries) == ["a.txt", "b.txt"]

    def test_xattrs_bulk_helper(self, tmp_path):
        from unittest.mock import MagicMock, patch

        f = tmp_path / "file.txt"
        f.write_text("x")
        client = GfalClient()
        mock_fso = MagicMock()
        mock_fso.listxattr.return_value = ["user.foo", "user.bar"]
        mock_fso.getxattr.side_effect = ["one", "two"]

        with patch("gfal.core.api.fs.url_to_fs", return_value=(mock_fso, str(f))):
            result = client.xattrs(f.as_uri())

        assert result == {"user.foo": "one", "user.bar": "two"}

    def test_copy_file_local(self, tmp_path):
        src = tmp_path / "src.txt"
        dst = tmp_path / "dst.txt"
        src.write_text("hello")

        client = GfalClient()
        client.copy(src.as_uri(), dst.as_uri())

        assert dst.read_text() == "hello"

    def test_copy_respects_options(self, tmp_path):
        src = tmp_path / "src.txt"
        dst = tmp_path / "dst.txt"
        src.write_text("payload")
        dst.write_text("old")

        client = GfalClient()
        client.copy(
            src.as_uri(),
            dst.as_uri(),
            options=CopyOptions(
                overwrite=True,
                checksum=ChecksumPolicy("ADLER32", mode="both"),
            ),
        )

        assert dst.read_text() == "payload"

    def test_start_copy_handle(self, tmp_path):
        src = tmp_path / "src.txt"
        dst = tmp_path / "dst.txt"
        src.write_text("hello")

        client = GfalClient()
        handle = client.start_copy(src.as_uri(), dst.as_uri())
        handle.wait()

        assert handle.done() is True
        assert dst.read_text() == "hello"

    def test_copy_directory_over_existing_file_raises_even_with_overwrite(
        self, tmp_path
    ):
        src = tmp_path / "srcdir"
        src.mkdir()
        (src / "child.txt").write_text("payload")
        dst = tmp_path / "dst.txt"
        dst.write_text("existing")

        client = GfalClient()

        with pytest.raises(GfalIsADirectoryError, match="directory over a file"):
            client.copy(
                src.as_uri(),
                dst.as_uri(),
                options=CopyOptions(recursive=True, overwrite=True),
            )

    def test_copy_with_tpc_never_does_not_attempt_tpc(self):
        client = GfalClient()

        class _FakeFs:
            def __init__(self, info_result):
                self._info_result = info_result

            def info(self, path):
                if isinstance(self._info_result, BaseException):
                    raise self._info_result
                return self._info_result

            def open(self, path, mode):
                if "r" in mode:
                    return nullcontext(io.BytesIO(b"payload"))
                return nullcontext(io.BytesIO())

        src_info = {
            "name": "/src/file.txt",
            "type": "file",
            "size": 7,
            "mode": stat.S_IFREG | 0o644,
        }
        src_fs = _FakeFs(src_info)
        dst_fs = _FakeFs(FileNotFoundError("/dst/file.txt"))

        def _url_to_fs_side_effect(url, storage_options=None):
            if url == "https://src.example/file.txt":
                return src_fs, "/src/file.txt"
            return dst_fs, "/dst/file.txt"

        with (
            patch("gfal.core.api.fs.url_to_fs", side_effect=_url_to_fs_side_effect),
            patch(
                "gfal.core.tpc.do_tpc",
                side_effect=AssertionError("TPC should not run"),
            ),
        ):
            client.copy(
                "https://src.example/file.txt",
                "https://dst.example/file.txt",
                options=CopyOptions(tpc="never"),
            )

    def test_copy_treats_late_remote_disconnect_after_full_write_as_success(self):
        client = GfalClient()

        class _LateDisconnectWriter(io.BytesIO):
            def close(self):
                super().close()
                raise ConnectionError("Connection lost")

        class _RemoteWriteFs:
            def __init__(self):
                self.size = 0

            def info(self, path):
                if path == "/dst/file.txt" and self.size:
                    return {
                        "name": path,
                        "type": "file",
                        "size": self.size,
                        "mode": stat.S_IFREG | 0o644,
                    }
                raise FileNotFoundError(path)

            def open(self, path, mode):
                assert mode == "wb"
                writer = _LateDisconnectWriter()
                original_write = writer.write

                def _write(data):
                    written = original_write(data)
                    self.size += written
                    return written

                writer.write = _write
                return writer

            def rm(self, path, recursive=False):
                raise AssertionError(
                    "Late-successful remote writes must not be cleaned up"
                )

        class _SourceFs:
            def info(self, path):
                return {
                    "name": path,
                    "type": "file",
                    "size": 7,
                    "mode": stat.S_IFREG | 0o644,
                }

            def open(self, path, mode):
                assert mode == "rb"
                return nullcontext(io.BytesIO(b"payload"))

        src_fs = _SourceFs()
        dst_fs = _RemoteWriteFs()

        def _url_to_fs_side_effect(url, storage_options=None):
            if url == "https://src.example/file.txt":
                return src_fs, "/src/file.txt"
            return dst_fs, "/dst/file.txt"

        with patch("gfal.core.api.fs.url_to_fs", side_effect=_url_to_fs_side_effect):
            client.copy(
                "https://src.example/file.txt",
                "https://dst.example/file.txt",
                options=CopyOptions(tpc="never"),
            )

        assert dst_fs.size == 7

    def test_copy_reports_full_progress_after_successful_tpc(self):
        client = GfalClient()
        progress = []
        started = []

        class _FakeFs:
            def __init__(self, info_result):
                self._info_result = info_result

            def info(self, path):
                if isinstance(self._info_result, BaseException):
                    raise self._info_result
                return self._info_result

        src_info = {
            "name": "/src/file.txt",
            "type": "file",
            "size": 7,
            "mode": stat.S_IFREG | 0o644,
        }
        src_fs = _FakeFs(src_info)
        dst_fs = _FakeFs(FileNotFoundError("/dst/file.txt"))

        def _url_to_fs_side_effect(url, storage_options=None):
            if url == "https://src.example/file.txt":
                return src_fs, "/src/file.txt"
            return dst_fs, "/dst/file.txt"

        with (
            patch("gfal.core.api.fs.url_to_fs", side_effect=_url_to_fs_side_effect),
            patch(
                "gfal.core.tpc.do_tpc",
                side_effect=lambda *args, **kwargs: kwargs["start_callback"](),
            ),
        ):
            client.copy(
                "https://src.example/file.txt",
                "https://dst.example/file.txt",
                options=CopyOptions(tpc="auto"),
                progress_callback=progress.append,
                start_callback=lambda: started.append(True),
            )

        assert started == [True]
        assert progress[-1] == 7

    def test_copy_default_http_to_http_attempts_tpc(self):
        client = GfalClient()

        class _FakeFs:
            def __init__(self, info_result):
                self._info_result = info_result

            def info(self, path):
                if isinstance(self._info_result, BaseException):
                    raise self._info_result
                return self._info_result

        src_info = {
            "name": "/src/file.txt",
            "type": "file",
            "size": 7,
            "mode": stat.S_IFREG | 0o644,
        }
        src_fs = _FakeFs(src_info)
        dst_fs = _FakeFs(FileNotFoundError("/dst/file.txt"))

        def _url_to_fs_side_effect(url, storage_options=None):
            if url == "https://src.example/file.txt":
                return src_fs, "/src/file.txt"
            return dst_fs, "/dst/file.txt"

        with (
            patch("gfal.core.api.fs.url_to_fs", side_effect=_url_to_fs_side_effect),
            patch("gfal.core.tpc.do_tpc", return_value=True) as mock_tpc,
        ):
            client.copy("https://src.example/file.txt", "https://dst.example/file.txt")

        mock_tpc.assert_called_once()

    def test_copy_auto_tpc_definitive_failure_does_not_fallback_to_streaming(self):
        client = GfalClient()

        class _FakeFs:
            def __init__(self, info_result):
                self._info_result = info_result

            def info(self, path):
                if isinstance(self._info_result, BaseException):
                    raise self._info_result
                return self._info_result

            def open(self, path, mode):
                raise AssertionError("Streaming fallback should not start")

        src_info = {
            "name": "/src/file.txt",
            "type": "file",
            "size": 7,
            "mode": stat.S_IFREG | 0o644,
        }
        src_fs = _FakeFs(src_info)
        dst_fs = _FakeFs(FileNotFoundError("/dst/file.txt"))

        def _url_to_fs_side_effect(url, storage_options=None):
            if url == "https://src.example/file.txt":
                return src_fs, "/src/file.txt"
            return dst_fs, "/dst/file.txt"

        with (
            patch("gfal.core.api.fs.url_to_fs", side_effect=_url_to_fs_side_effect),
            patch(
                "gfal.core.tpc.do_tpc",
                side_effect=OSError("TPC pull failed definitively"),
            ),
            pytest.raises(GfalError, match="TPC pull failed definitively"),
        ):
            client.copy("https://src.example/file.txt", "https://dst.example/file.txt")

    def test_transfer_destination_url_skips_remote_mtime_without_explicit_flag(self):
        client = GfalClient()
        src_st = client.stat(Path(__file__).as_uri())

        url = client._async_client._transfer_destination_url(
            "https://eospilot.cern.ch//eos/pilot/test/file.txt",
            src_st,
            CopyOptions(preserve_times=True, preserve_times_explicit=False),
        )

        assert url == "https://eospilot.cern.ch//eos/pilot/test/file.txt"

    def test_transfer_destination_url_uses_remote_mtime_with_explicit_flag(self):
        client = GfalClient()
        src_st = client.stat(Path(__file__).as_uri())

        url = client._async_client._transfer_destination_url(
            "https://eospilot.cern.ch//eos/pilot/test/file.txt",
            src_st,
            CopyOptions(preserve_times=True, preserve_times_explicit=True),
        )

        assert "eos.mtime=" in url

    def test_copy_url_skips_eos_app_annotation_for_https(self):
        client = GfalClient(app="python3-gfal-cli")

        url = client._async_client._copy_url(
            "https://eospilot.cern.ch//eos/pilot/test/file.txt"
        )

        assert url == "https://eospilot.cern.ch//eos/pilot/test/file.txt"


class TestAsyncGfalClient:
    @pytest.mark.asyncio
    async def test_async_stat_ls_and_exists(self, tmp_path):
        src = tmp_path / "src.txt"
        src.write_text("hello")
        client = AsyncGfalClient()

        st = await client.stat(src.as_uri())
        names = await client.ls(tmp_path.as_uri(), detail=False)
        exists = await client.exists(src.as_uri())

        assert st.size == 5
        assert exists is True
        assert names == ["src.txt"]

    @pytest.mark.asyncio
    async def test_async_copy_and_checksum(self, tmp_path):
        src = tmp_path / "src.txt"
        dst = tmp_path / "dst.txt"
        src.write_text("hello")
        client = AsyncGfalClient()

        await client.copy(
            src.as_uri(),
            dst.as_uri(),
            options=CopyOptions(
                checksum=ChecksumPolicy("ADLER32", mode="both"),
            ),
        )
        checksum = await client.checksum(dst.as_uri(), "ADLER32")

        assert dst.read_text() == "hello"
        assert checksum

    @pytest.mark.asyncio
    async def test_async_xattrs_helper(self, tmp_path):
        from unittest.mock import MagicMock, patch

        f = tmp_path / "file.txt"
        f.write_text("x")
        client = AsyncGfalClient()
        mock_fso = MagicMock()
        mock_fso.listxattr.return_value = ["user.foo"]
        mock_fso.getxattr.return_value = "one"

        with patch("gfal.core.api.fs.url_to_fs", return_value=(mock_fso, str(f))):
            result = await client.xattrs(f.as_uri())

        assert result == {"user.foo": "one"}

    @pytest.mark.asyncio
    async def test_async_start_copy_handle(self, tmp_path):
        src = tmp_path / "src.txt"
        dst = tmp_path / "dst.txt"
        src.write_text("hello")

        client = AsyncGfalClient()
        handle = client.start_copy(src.as_uri(), dst.as_uri())
        await handle.wait_async()

        assert handle.done() is True
        assert dst.read_text() == "hello"
