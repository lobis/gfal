"""Unit tests for the fsspec integration layer (fs.py)."""

import stat as stat_module

import pytest

from gfal_cli.fs import StatInfo, isdir, normalize_url, stat, url_to_fs

# ---------------------------------------------------------------------------
# normalize_url
# ---------------------------------------------------------------------------


def test_normalize_bare_path(tmp_path):
    f = tmp_path / "foo.txt"
    result = normalize_url(str(f))
    assert result.startswith("file://")
    assert "foo.txt" in result


def test_normalize_file_url_unchanged():
    url = "file:///tmp/foo.txt"
    assert normalize_url(url) == url


def test_normalize_dav():
    result = normalize_url("dav://host/path")
    assert result == "http://host/path"


def test_normalize_davs():
    result = normalize_url("davs://host/path")
    assert result == "https://host/path"


def test_normalize_sentinel():
    assert normalize_url("-") == "-"


def test_normalize_http_unchanged():
    url = "http://example.com/file"
    assert normalize_url(url) == url


def test_normalize_https_unchanged():
    url = "https://example.com/file"
    assert normalize_url(url) == url


def test_normalize_root_unchanged():
    url = "root://eosuser.cern.ch//eos/user/x/xyz/file"
    assert normalize_url(url) == url


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

    def test_file_no_mode_synthesised(self):
        info = {"type": "file", "size": 512}
        si = StatInfo(info)
        assert stat_module.S_ISREG(si.st_mode)
        assert stat_module.S_IMODE(si.st_mode) == 0o644

    def test_default_size_zero(self):
        si = StatInfo({})
        assert si.st_size == 0

    def test_default_uid_gid(self):
        si = StatInfo({})
        assert si.st_uid == 0
        assert si.st_gid == 0

    def test_default_nlink(self):
        si = StatInfo({})
        assert si.st_nlink == 1

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

    def test_size_from_none(self):
        """size=None should be treated as 0."""
        si = StatInfo({"size": None})
        assert si.st_size == 0


# ---------------------------------------------------------------------------
# url_to_fs
# ---------------------------------------------------------------------------


class TestUrlToFs:
    def test_file_uri(self, tmp_path):
        f = tmp_path / "test.txt"
        f.write_text("hello")
        fso, path = url_to_fs(f.as_uri())
        assert path == str(f)

    def test_bare_path(self, tmp_path):
        f = tmp_path / "test.txt"
        f.write_text("hello")
        fso, path = url_to_fs(str(f))
        assert path == str(f)

    def test_file_fs_can_read(self, tmp_path):
        f = tmp_path / "test.txt"
        f.write_bytes(b"data")
        fso, path = url_to_fs(f.as_uri())
        with fso.open(path, "rb") as fh:
            assert fh.read() == b"data"


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
