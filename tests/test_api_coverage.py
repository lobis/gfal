"""Additional unit tests for api.py to increase code coverage.

Covers StatResult properties, _map_error paths, _existing_file_matches
compare modes, _preserve_times error handling, split_timestamp_ns edge cases,
_is_eos_host, and eos_mtime_url.
"""

from __future__ import annotations

import errno
import stat
from unittest.mock import MagicMock, patch

from gfal.core.api import (
    AsyncGfalClient,
    CopyOptions,
    StatResult,
    _is_eos_host,
    eos_app_url,
    eos_mtime_url,
    split_timestamp_ns,
)
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
# StatResult property accessors
# ---------------------------------------------------------------------------


class TestStatResultProperties:
    def _make_stat(self) -> StatResult:
        return StatResult(
            info={"type": "file", "size": 1024},
            st_size=1024,
            st_mode=stat.S_IFREG | 0o644,
            st_uid=1000,
            st_gid=1000,
            st_nlink=1,
            st_mtime=1700000000.0,
            st_atime=1700000001.0,
            st_ctime=1700000002.0,
        )

    def test_mode_property(self):
        s = self._make_stat()
        assert s.mode == s.st_mode

    def test_uid_property(self):
        s = self._make_stat()
        assert s.uid == 1000

    def test_gid_property(self):
        s = self._make_stat()
        assert s.gid == 1000

    def test_nlink_property(self):
        s = self._make_stat()
        assert s.nlink == 1

    def test_mtime_property(self):
        s = self._make_stat()
        assert s.mtime == 1700000000.0

    def test_atime_property(self):
        s = self._make_stat()
        assert s.atime == 1700000001.0

    def test_ctime_property(self):
        s = self._make_stat()
        assert s.ctime == 1700000002.0

    def test_size_property(self):
        s = self._make_stat()
        assert s.size == 1024


# ---------------------------------------------------------------------------
# _map_error
# ---------------------------------------------------------------------------


class TestMapError:
    """Test AsyncGfalClient._map_error for various exception types."""

    def _map(self, exc: Exception, url: str = "file:///test") -> GfalError:
        client = AsyncGfalClient()
        return client._map_error(exc, url)

    def test_gfal_error_passthrough(self):
        orig = GfalError("already mapped", errno.EIO)
        result = self._map(orig)
        assert result is orig

    def test_file_not_found(self):
        result = self._map(FileNotFoundError("missing"))
        assert isinstance(result, GfalFileNotFoundError)

    def test_permission_error(self):
        result = self._map(PermissionError("denied"))
        assert isinstance(result, GfalPermissionError)

    def test_file_exists_error(self):
        result = self._map(FileExistsError("exists"))
        assert isinstance(result, GfalFileExistsError)

    def test_is_a_directory_error(self):
        result = self._map(IsADirectoryError("is dir"))
        assert isinstance(result, GfalIsADirectoryError)

    def test_not_a_directory_error(self):
        result = self._map(NotADirectoryError("not dir"))
        assert isinstance(result, GfalNotADirectoryError)

    def test_timeout_error(self):
        result = self._map(TimeoutError("timed out"))
        assert isinstance(result, GfalTimeoutError)

    def test_http_403_status(self):
        exc = Exception("forbidden")
        exc.status = 403
        result = self._map(exc)
        assert isinstance(result, GfalPermissionError)

    def test_http_404_status(self):
        exc = Exception("not found")
        exc.status = 404
        result = self._map(exc)
        assert isinstance(result, GfalFileNotFoundError)

    def test_errno_enoent(self):
        exc = OSError("not found")
        exc.errno = errno.ENOENT
        result = self._map(exc)
        assert isinstance(result, GfalFileNotFoundError)

    def test_errno_eacces(self):
        exc = OSError("denied")
        exc.errno = errno.EACCES
        result = self._map(exc)
        assert isinstance(result, GfalPermissionError)

    def test_errno_eexist(self):
        exc = OSError("exists")
        exc.errno = errno.EEXIST
        result = self._map(exc)
        assert isinstance(result, GfalFileExistsError)

    def test_errno_eisdir(self):
        exc = OSError("is directory")
        exc.errno = errno.EISDIR
        result = self._map(exc)
        assert isinstance(result, GfalIsADirectoryError)

    def test_errno_enotdir(self):
        exc = OSError("not a directory")
        exc.errno = errno.ENOTDIR
        result = self._map(exc)
        assert isinstance(result, GfalNotADirectoryError)

    def test_errno_etimedout(self):
        exc = OSError("timed out")
        exc.errno = errno.ETIMEDOUT
        result = self._map(exc)
        assert isinstance(result, GfalTimeoutError)

    def test_errno_zero_maps_to_eio(self):
        exc = OSError("unknown")
        exc.errno = 0
        result = self._map(exc)
        assert isinstance(result, GfalError)
        assert result.errno == errno.EIO

    def test_aiohttp_ssl_error(self):
        import aiohttp

        exc = aiohttp.ClientSSLError(MagicMock(), MagicMock())
        result = self._map(exc)
        assert isinstance(result, GfalError)
        assert result.errno == errno.EHOSTDOWN

    def test_aiohttp_connection_error(self):
        import aiohttp

        exc = aiohttp.ClientConnectionError("connection refused")
        result = self._map(exc)
        assert isinstance(result, GfalError)
        assert result.errno == errno.ECONNREFUSED

    def test_aiohttp_connection_error_with_errno(self):
        import aiohttp

        exc = aiohttp.ClientConnectionError("conn error")
        exc.errno = errno.ECONNRESET
        result = self._map(exc)
        assert isinstance(result, GfalError)
        assert result.errno == errno.ECONNRESET

    def test_aiohttp_connection_error_non_int_errno(self):
        import aiohttp

        exc = aiohttp.ClientConnectionError("conn error")
        exc.errno = "not an int"
        result = self._map(exc)
        assert isinstance(result, GfalError)
        assert result.errno == errno.ECONNREFUSED

    def test_aiohttp_connection_in_cause_chain_non_int_errno(self):
        import aiohttp

        inner = aiohttp.ClientConnectionError("conn error")
        inner.errno = "not-an-int"
        outer = RuntimeError("wrapped")
        outer.__cause__ = inner
        result = self._map(outer)
        assert isinstance(result, GfalError)
        assert result.errno == errno.ECONNREFUSED

    def test_aiohttp_connection_in_cause_chain_with_errno(self):
        import aiohttp

        inner = aiohttp.ClientConnectionError("conn reset")
        inner.errno = errno.ECONNRESET
        outer = RuntimeError("wrapped")
        outer.__cause__ = inner
        result = self._map(outer)
        assert isinstance(result, GfalError)
        assert result.errno == errno.ECONNRESET

    def test_aiohttp_ssl_in_cause_chain(self):
        import aiohttp

        inner = aiohttp.ClientSSLError(MagicMock(), MagicMock())
        outer = FileNotFoundError("wrapped")
        outer.__cause__ = inner
        result = self._map(outer)
        assert isinstance(result, GfalError)
        assert result.errno == errno.EHOSTDOWN

    def test_aiohttp_connection_in_cause_chain(self):
        import aiohttp

        inner = aiohttp.ClientConnectionError("conn refused")
        outer = OSError("wrapped")
        outer.__cause__ = inner
        result = self._map(outer)
        assert isinstance(result, GfalError)
        assert result.errno == errno.ECONNREFUSED

    def test_aiohttp_connection_in_context_chain(self):
        import aiohttp

        inner = aiohttp.ClientConnectionError("conn error")
        outer = RuntimeError("wrapped")
        outer.__context__ = inner
        result = self._map(outer)
        assert isinstance(result, GfalError)
        assert result.errno == errno.ECONNREFUSED

    def test_xrootd_not_found_message(self):
        exc = Exception(
            "root://server//path: [ERROR] Server responded with an error: "
            "[3011] No such file or directory"
        )
        result = self._map(exc)
        assert isinstance(result, GfalFileNotFoundError)

    def test_generic_error(self):
        exc = RuntimeError("something unexpected")
        result = self._map(exc)
        assert isinstance(result, GfalError)

    def test_empty_error_message(self):
        exc = NotImplementedError()
        result = self._map(exc)
        assert isinstance(result, GfalError)
        assert "(NotImplementedError)" in str(result)


# ---------------------------------------------------------------------------
# _existing_file_matches — compare modes
# ---------------------------------------------------------------------------


class TestExistingFileMatches:
    def _make_client(self):
        return AsyncGfalClient()

    def _make_stat(self, size=100, mtime=1000.0):
        return StatResult(
            info={"type": "file", "size": size},
            st_size=size,
            st_mode=stat.S_IFREG | 0o644,
            st_uid=0,
            st_gid=0,
            st_nlink=1,
            st_mtime=mtime,
            st_atime=mtime,
            st_ctime=mtime,
        )

    def test_compare_none_returns_false(self):
        client = self._make_client()
        options = CopyOptions(compare=None)
        result = client._existing_file_matches_source(
            src_fs=MagicMock(),
            src_path="/src",
            src_st=self._make_stat(),
            dst_fs=MagicMock(),
            dst_path="/dst",
            dst_url="file:///dst",
            options=options,
        )
        assert result is False

    def test_compare_none_string_returns_true(self):
        client = self._make_client()
        options = CopyOptions(compare="none")
        warnings_collected = []
        result = client._existing_file_matches_source(
            src_fs=MagicMock(),
            src_path="/src",
            src_st=self._make_stat(),
            dst_fs=MagicMock(),
            dst_path="/dst",
            dst_url="file:///dst",
            options=options,
            warn_callback=warnings_collected.append,
        )
        assert result is True
        assert any("Skipping" in w for w in warnings_collected)

    def test_compare_size_match(self):
        client = self._make_client()
        options = CopyOptions(compare="size")
        dst_fs = MagicMock()
        dst_fs.info.return_value = {"type": "file", "size": 100}

        warnings_collected = []
        result = client._existing_file_matches_source(
            src_fs=MagicMock(),
            src_path="/src",
            src_st=self._make_stat(size=100),
            dst_fs=dst_fs,
            dst_path="/dst",
            dst_url="file:///dst",
            options=options,
            warn_callback=warnings_collected.append,
        )
        assert result is True

    def test_compare_size_mismatch(self):
        client = self._make_client()
        options = CopyOptions(compare="size")
        dst_fs = MagicMock()
        dst_fs.info.return_value = {"type": "file", "size": 200}

        result = client._existing_file_matches_source(
            src_fs=MagicMock(),
            src_path="/src",
            src_st=self._make_stat(size=100),
            dst_fs=dst_fs,
            dst_path="/dst",
            dst_url="file:///dst",
            options=options,
        )
        assert result is False

    def test_compare_size_error(self):
        client = self._make_client()
        options = CopyOptions(compare="size")
        dst_fs = MagicMock()
        dst_fs.info.side_effect = FileNotFoundError("not found")

        warnings_collected = []
        result = client._existing_file_matches_source(
            src_fs=MagicMock(),
            src_path="/src",
            src_st=self._make_stat(),
            dst_fs=dst_fs,
            dst_path="/dst",
            dst_url="file:///dst",
            options=options,
            warn_callback=warnings_collected.append,
        )
        assert result is False
        assert any("size compare failed" in w for w in warnings_collected)

    def test_compare_size_mtime_match(self):
        client = self._make_client()
        options = CopyOptions(compare="size_mtime")
        dst_fs = MagicMock()
        dst_fs.info.return_value = {"type": "file", "size": 100}

        with patch.object(StatResult, "from_info") as mock_from_info:
            mock_from_info.return_value = self._make_stat(size=100, mtime=1000.0)
            warnings_collected = []
            result = client._existing_file_matches_source(
                src_fs=MagicMock(),
                src_path="/src",
                src_st=self._make_stat(size=100, mtime=1000.0),
                dst_fs=dst_fs,
                dst_path="/dst",
                dst_url="file:///dst",
                options=options,
                warn_callback=warnings_collected.append,
            )
            assert result is True

    def test_compare_size_mtime_mismatch(self):
        client = self._make_client()
        options = CopyOptions(compare="size_mtime")
        dst_fs = MagicMock()
        dst_fs.info.return_value = {"type": "file", "size": 100}

        with patch.object(StatResult, "from_info") as mock_from_info:
            mock_from_info.return_value = self._make_stat(size=100, mtime=2000.0)
            result = client._existing_file_matches_source(
                src_fs=MagicMock(),
                src_path="/src",
                src_st=self._make_stat(size=100, mtime=1000.0),
                dst_fs=dst_fs,
                dst_path="/dst",
                dst_url="file:///dst",
                options=options,
            )
            assert result is False

    def test_compare_size_mtime_error(self):
        client = self._make_client()
        options = CopyOptions(compare="size_mtime")
        dst_fs = MagicMock()
        dst_fs.info.side_effect = FileNotFoundError("not found")

        warnings_collected = []
        result = client._existing_file_matches_source(
            src_fs=MagicMock(),
            src_path="/src",
            src_st=self._make_stat(),
            dst_fs=dst_fs,
            dst_path="/dst",
            dst_url="file:///dst",
            options=options,
            warn_callback=warnings_collected.append,
        )
        assert result is False
        assert any("size_mtime compare failed" in w for w in warnings_collected)

    def test_compare_unknown_returns_false(self):
        client = self._make_client()
        options = CopyOptions(compare="bogus_mode")
        result = client._existing_file_matches_source(
            src_fs=MagicMock(),
            src_path="/src",
            src_st=self._make_stat(),
            dst_fs=MagicMock(),
            dst_path="/dst",
            dst_url="file:///dst",
            options=options,
        )
        assert result is False


# ---------------------------------------------------------------------------
# split_timestamp_ns
# ---------------------------------------------------------------------------


class TestSplitTimestampNs:
    def test_normal_value(self):
        seconds, nanos = split_timestamp_ns(1700000000.5)
        assert seconds == 1700000000
        assert 0 <= nanos < 1_000_000_000

    def test_overflow_nanoseconds(self):
        # A timestamp that produces nanoseconds >= 1e9 due to floating point
        # This tests the overflow correction path
        seconds, nanos = split_timestamp_ns(0.9999999999)
        assert 0 <= nanos < 1_000_000_000

    def test_exact_integer(self):
        seconds, nanos = split_timestamp_ns(42.0)
        assert seconds == 42
        assert nanos == 0


# ---------------------------------------------------------------------------
# _is_eos_host
# ---------------------------------------------------------------------------


class TestIsEosHost:
    def test_none_hostname(self):
        assert _is_eos_host(None) is False

    def test_empty_hostname(self):
        assert _is_eos_host("") is False

    def test_valid_eos_hostname(self):
        assert _is_eos_host("eospilot.cern.ch") is True

    def test_eos_cern_ch(self):
        assert _is_eos_host("eos.cern.ch") is True

    def test_non_eos_hostname(self):
        assert _is_eos_host("example.org") is False

    def test_eos_in_name_but_not_prefix(self):
        assert _is_eos_host("myeos.cern.ch") is False


# ---------------------------------------------------------------------------
# eos_mtime_url
# ---------------------------------------------------------------------------


class TestEosMtimeUrl:
    def test_non_eos_url_returns_none(self):
        result = eos_mtime_url("https://example.org/file", 1700000000.0)
        assert result is None

    def test_non_http_scheme_returns_none(self):
        result = eos_mtime_url("ftp://eospilot.cern.ch/file", 1700000000.0)
        assert result is None

    def test_eos_url_adds_mtime(self):
        result = eos_mtime_url("https://eospilot.cern.ch/file", 1700000000.0)
        assert result is not None
        assert "eos.mtime=" in result


# ---------------------------------------------------------------------------
# eos_app_url
# ---------------------------------------------------------------------------


class TestEosAppUrl:
    def test_non_eos_returns_none(self):
        result = eos_app_url("https://example.org/file", "myapp")
        assert result is None

    def test_non_http_scheme_returns_none(self):
        result = eos_app_url("ftp://eospilot.cern.ch/file", "myapp")
        assert result is None

    def test_eos_url_adds_app(self):
        result = eos_app_url("https://eospilot.cern.ch/file", "myapp")
        assert result is not None
        assert "eos.app=myapp" in result


# ---------------------------------------------------------------------------
# _preserve_times error handling
# ---------------------------------------------------------------------------


class TestPreserveTimes:
    def test_preserve_times_oserror_warns(self, tmp_path):
        client = AsyncGfalClient()
        src_st = StatResult(
            info={"type": "file", "size": 100},
            st_size=100,
            st_mode=stat.S_IFREG | 0o644,
            st_uid=0,
            st_gid=0,
            st_nlink=1,
            st_mtime=1700000000.0,
            st_atime=1700000001.0,
            st_ctime=1700000002.0,
        )
        options = CopyOptions(preserve_times=True)

        warnings_collected = []
        # Use a non-existent local path to trigger OSError
        client._preserve_times(
            src_st=src_st,
            dst_url="file:///nonexistent/path/file.txt",
            dst_path="/nonexistent/path/file.txt",
            options=options,
            warn_callback=warnings_collected.append,
        )
        assert any("could not preserve times" in w for w in warnings_collected)
