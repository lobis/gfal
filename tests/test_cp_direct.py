"""Direct unit tests for CommandCopy execute_cp (src/gfal/cli/copy.py).

These tests instantiate CommandCopy and set self.params directly (as a
SimpleNamespace) to avoid subprocess overhead and ensure coverage is collected
in the pytest process.
"""

import sys
from types import SimpleNamespace

import pytest

from gfal.cli.copy import (
    CommandCopy,
    _checksum_fs,
    _finalise_hasher,
    _is_special_file,
    _make_hasher,
    _parse_checksum_arg,
    _tpc_applicable,
    _update_hasher,
)


def _make_cmd():
    cmd = CommandCopy()
    cmd.prog = "gfal-cp"
    return cmd


def _default_params(**kwargs):
    defaults = {
        "cert": None,
        "key": None,
        "timeout": 1800,
        "ssl_verify": True,
        "verbose": 0,
        "log_file": None,
        "force": False,
        "parent": False,
        "checksum": None,
        "checksum_mode": "both",
        "recursive": False,
        "from_file": None,
        "dry_run": False,
        "abort_on_failure": False,
        "transfer_timeout": 0,
        "tpc": False,
        "tpc_only": False,
        "tpc_mode": "pull",
        "copy_mode": None,
        "just_copy": False,
        "disable_cleanup": False,
        "no_delegation": False,
        "evict": False,
        "scitag": None,
        "nbstreams": None,
        "tcp_buffersize": None,
        "src_spacetoken": None,
        "dst_spacetoken": None,
    }
    defaults.update(kwargs)
    return SimpleNamespace(**defaults)


# ---------------------------------------------------------------------------
# execute_cp: basic copy
# ---------------------------------------------------------------------------


class TestExecuteCp:
    def test_copy_basic(self, tmp_path, capsys):
        src = tmp_path / "src.txt"
        dst = tmp_path / "dst.txt"
        src.write_bytes(b"hello world")
        cmd = _make_cmd()
        cmd.params = _default_params(src=src.as_uri(), dst=[dst.as_uri()])
        rc = cmd.execute_cp()
        assert rc == 0
        assert dst.read_bytes() == b"hello world"

    def test_copy_empty_file(self, tmp_path, capsys):
        src = tmp_path / "empty.txt"
        dst = tmp_path / "out.txt"
        src.write_bytes(b"")
        cmd = _make_cmd()
        cmd.params = _default_params(src=src.as_uri(), dst=[dst.as_uri()])
        rc = cmd.execute_cp()
        assert rc == 0
        assert dst.read_bytes() == b""

    def test_copy_missing_source_returns_error(self, tmp_path, capsys):
        dst = tmp_path / "dst.txt"
        cmd = _make_cmd()
        cmd.params = _default_params(
            src=(tmp_path / "no_such").as_uri(),
            dst=[dst.as_uri()],
        )
        rc = cmd.execute_cp()
        assert rc != 0

    def test_copy_to_directory(self, tmp_path, capsys):
        src = tmp_path / "src.txt"
        dstdir = tmp_path / "dstdir"
        src.write_bytes(b"hello")
        dstdir.mkdir()
        cmd = _make_cmd()
        cmd.params = _default_params(src=src.as_uri(), dst=[dstdir.as_uri()])
        rc = cmd.execute_cp()
        assert rc == 0
        assert (dstdir / "src.txt").read_bytes() == b"hello"

    def test_copy_force_overwrites(self, tmp_path, capsys):
        src = tmp_path / "src.txt"
        dst = tmp_path / "dst.txt"
        src.write_bytes(b"new content")
        dst.write_bytes(b"old content")
        cmd = _make_cmd()
        cmd.params = _default_params(src=src.as_uri(), dst=[dst.as_uri()], force=True)
        rc = cmd.execute_cp()
        assert rc == 0
        assert dst.read_bytes() == b"new content"

    def test_copy_no_force_fails_if_dst_exists(self, tmp_path):
        src = tmp_path / "src.txt"
        dst = tmp_path / "dst.txt"
        src.write_bytes(b"new")
        dst.write_bytes(b"old")
        cmd = _make_cmd()
        cmd.params = _default_params(src=src.as_uri(), dst=[dst.as_uri()], force=False)
        rc = cmd.execute_cp()
        assert rc != 0

    def test_copy_from_file_and_src_returns_error(self, tmp_path):
        src = tmp_path / "src.txt"
        src.write_bytes(b"x")
        list_file = tmp_path / "list.txt"
        list_file.write_text(src.as_uri() + "\n")
        cmd = _make_cmd()
        cmd.params = _default_params(
            src=src.as_uri(),
            dst=[(tmp_path / "dst.txt").as_uri()],
            from_file=str(list_file),
        )
        rc = cmd.execute_cp()
        assert rc == 1

    def test_copy_no_source_returns_error(self, tmp_path, capsys):
        cmd = _make_cmd()
        cmd.params = _default_params(src=None, dst=[(tmp_path / "dst.txt").as_uri()])
        rc = cmd.execute_cp()
        assert rc == 1

    def test_copy_from_file(self, tmp_path, capsys):
        src = tmp_path / "src.txt"
        src.write_bytes(b"from file source")
        dst_dir = tmp_path / "out"
        dst_dir.mkdir()
        list_file = tmp_path / "list.txt"
        list_file.write_text(src.as_uri() + "\n")
        dst = tmp_path / "out" / "dest.txt"
        cmd = _make_cmd()
        cmd.params = _default_params(
            src=None,
            dst=[dst.as_uri()],
            from_file=str(list_file),
        )
        rc = cmd.execute_cp()
        assert rc == 0

    def test_copy_dry_run(self, tmp_path, capsys):
        src = tmp_path / "src.txt"
        dst = tmp_path / "dst.txt"
        src.write_bytes(b"data")
        cmd = _make_cmd()
        cmd.params = _default_params(src=src.as_uri(), dst=[dst.as_uri()], dry_run=True)
        rc = cmd.execute_cp()
        assert rc == 0
        assert not dst.exists()  # dry-run: no actual copy
        out = capsys.readouterr().out
        assert "Copy" in out or "Copying" in out

    def test_copy_recursive_directory(self, tmp_path, capsys):
        src = tmp_path / "srcdir"
        dst = tmp_path / "dstdir"
        src.mkdir()
        (src / "f1.txt").write_bytes(b"a")
        (src / "f2.txt").write_bytes(b"b")
        cmd = _make_cmd()
        cmd.params = _default_params(
            src=src.as_uri(), dst=[dst.as_uri()], recursive=True
        )
        rc = cmd.execute_cp()
        assert rc == 0
        assert (dst / "f1.txt").read_bytes() == b"a"
        assert (dst / "f2.txt").read_bytes() == b"b"

    def test_copy_directory_without_recursive_skips(self, tmp_path, capsys):
        src = tmp_path / "srcdir"
        dst = tmp_path / "dstdir"
        src.mkdir()
        cmd = _make_cmd()
        cmd.params = _default_params(
            src=src.as_uri(), dst=[dst.as_uri()], recursive=False
        )
        rc = cmd.execute_cp()
        assert rc == 0  # skips, doesn't fail

    def test_copy_with_checksum_adler32(self, tmp_path, capsys):

        data = b"hello world"
        src = tmp_path / "src.bin"
        dst = tmp_path / "dst.bin"
        src.write_bytes(data)
        cmd = _make_cmd()
        cmd.params = _default_params(
            src=src.as_uri(),
            dst=[dst.as_uri()],
            checksum="ADLER32",
            checksum_mode="both",
        )
        rc = cmd.execute_cp()
        assert rc == 0
        assert dst.read_bytes() == data

    def test_copy_scitag_invalid_range(self, tmp_path):
        src = tmp_path / "src.txt"
        src.write_bytes(b"x")
        cmd = _make_cmd()
        cmd.params = _default_params(
            src=src.as_uri(),
            dst=[(tmp_path / "dst.txt").as_uri()],
            scitag=10,  # out of range [65, 65535]
        )
        rc = cmd.execute_cp()
        assert rc == 1

    def test_copy_ignored_flags_warn(self, tmp_path, capsys):
        src = tmp_path / "src.txt"
        dst = tmp_path / "dst.txt"
        src.write_bytes(b"x")
        cmd = _make_cmd()
        cmd.params = _default_params(
            src=src.as_uri(),
            dst=[dst.as_uri()],
            nbstreams=4,
            tcp_buffersize=65536,
            src_spacetoken="TOKEN",
            dst_spacetoken="TOKEN",
        )
        rc = cmd.execute_cp()
        assert rc == 0

    def test_copy_abort_on_failure(self, tmp_path):
        src1 = tmp_path / "src1.txt"
        src1.write_bytes(b"data")
        no_src = tmp_path / "no_such.txt"
        dst1 = tmp_path / "dst1.txt"
        dst2 = tmp_path / "dst2.txt"
        # Two destinations; first should succeed, second should fail? No, abort_on_failure
        # affects multiple copies, not single. Test with missing source.
        cmd = _make_cmd()
        cmd.params = _default_params(
            src=no_src.as_uri(),
            dst=[dst1.as_uri(), dst2.as_uri()],
            abort_on_failure=True,
        )
        rc = cmd.execute_cp()
        assert rc != 0

    def test_copy_copy_mode_streamed(self, tmp_path, capsys):
        src = tmp_path / "src.txt"
        dst = tmp_path / "dst.txt"
        src.write_bytes(b"data")
        cmd = _make_cmd()
        cmd.params = _default_params(
            src=src.as_uri(),
            dst=[dst.as_uri()],
            copy_mode="streamed",
        )
        rc = cmd.execute_cp()
        assert rc == 0
        assert dst.read_bytes() == b"data"

    def test_copy_parent_creates_dirs(self, tmp_path, capsys):
        src = tmp_path / "src.txt"
        src.write_bytes(b"hello")
        dst = tmp_path / "newdir" / "dst.txt"
        cmd = _make_cmd()
        cmd.params = _default_params(src=src.as_uri(), dst=[dst.as_uri()], parent=True)
        rc = cmd.execute_cp()
        assert rc == 0
        assert dst.read_bytes() == b"hello"


# ---------------------------------------------------------------------------
# Checksum helpers
# ---------------------------------------------------------------------------


class TestChecksumHelpers:
    def test_parse_checksum_arg_alg_only(self):
        alg, expected = _parse_checksum_arg("ADLER32")
        assert alg == "ADLER32"
        assert expected is None

    def test_parse_checksum_arg_with_value(self):
        alg, expected = _parse_checksum_arg("ADLER32:abc123")
        assert alg == "ADLER32"
        assert expected == "abc123"

    def test_make_hasher_adler32(self):
        h = _make_hasher("ADLER32")
        assert h[0] == "ADLER32"
        assert h[1] == 1

    def test_make_hasher_crc32(self):
        h = _make_hasher("CRC32")
        assert h[0] == "CRC32"
        assert h[1] == 0

    def test_make_hasher_md5(self):

        h = _make_hasher("MD5")
        assert hasattr(h, "hexdigest")

    def test_update_hasher_adler32(self):
        h = _make_hasher("ADLER32")
        _update_hasher(h, "ADLER32", b"hello")
        assert h[1] != 1

    def test_update_hasher_crc32(self):
        h = _make_hasher("CRC32")
        _update_hasher(h, "CRC32", b"hello")
        assert h[1] != 0

    def test_update_hasher_md5(self):
        h = _make_hasher("MD5")
        _update_hasher(h, "MD5", b"hello")
        assert h.hexdigest() != ""

    def test_finalise_hasher_adler32(self):
        import zlib

        data = b"hello world"
        h = _make_hasher("ADLER32")
        _update_hasher(h, "ADLER32", data)
        result = _finalise_hasher(h, "ADLER32")
        expected = f"{zlib.adler32(data) & 0xFFFFFFFF:08x}"
        assert result == expected

    def test_finalise_hasher_md5(self):
        import hashlib

        data = b"hello"
        h = _make_hasher("MD5")
        _update_hasher(h, "MD5", data)
        result = _finalise_hasher(h, "MD5")
        assert result == hashlib.md5(data).hexdigest()

    def test_checksum_fs_adler32(self, tmp_path):
        import zlib

        import fsspec

        data = b"hello world"
        f = tmp_path / "test.bin"
        f.write_bytes(data)
        fso = fsspec.filesystem("file")
        result = _checksum_fs(fso, str(f), "ADLER32")
        expected = f"{zlib.adler32(data) & 0xFFFFFFFF:08x}"
        assert result == expected


# ---------------------------------------------------------------------------
# _tpc_applicable
# ---------------------------------------------------------------------------


class TestTpcApplicable:
    def test_http_to_http(self):
        assert _tpc_applicable("http://a.com/f", "http://b.com/f") is True

    def test_https_to_https(self):
        assert _tpc_applicable("https://a.com/f", "https://b.com/f") is True

    def test_http_to_https(self):
        assert _tpc_applicable("http://a.com/f", "https://b.com/f") is True

    def test_root_to_root(self):
        assert _tpc_applicable("root://a.com//f", "root://b.com//f") is True

    def test_file_to_file(self):
        assert _tpc_applicable("file:///a/f", "file:///b/f") is False

    def test_http_to_root(self):
        assert _tpc_applicable("http://a.com/f", "root://b.com//f") is False


# ---------------------------------------------------------------------------
# _is_special_file
# ---------------------------------------------------------------------------


class TestIsSpecialFile:
    def test_regular_file_not_special(self, tmp_path):
        f = tmp_path / "file.txt"
        f.write_text("x")
        assert _is_special_file(str(f)) is False

    def test_directory_not_special(self, tmp_path):
        assert _is_special_file(str(tmp_path)) is False

    def test_nonexistent_not_special(self, tmp_path):
        assert _is_special_file(str(tmp_path / "no_such")) is False

    @pytest.mark.skipif(sys.platform == "win32", reason="No /dev/null on Windows")
    def test_char_device_is_special(self):
        assert _is_special_file("/dev/null") is True


# ---------------------------------------------------------------------------
# execute_cp: copy to stdout ("-")
# ---------------------------------------------------------------------------


class TestExecuteCpToStdout:
    def test_copy_to_stdout(self, tmp_path, capsys):
        src = tmp_path / "src.txt"
        src.write_bytes(b"hello stdout")
        cmd = _make_cmd()
        cmd.params = _default_params(src=src.as_uri(), dst=["-"])
        rc = cmd.execute_cp()
        assert rc == 0

    def test_copy_mode_pull_sets_tpc(self, tmp_path, capsys):
        """copy_mode=pull sets tpc=True and tpc_mode=pull, but falls back gracefully."""
        src = tmp_path / "src.txt"
        dst = tmp_path / "dst.txt"
        src.write_bytes(b"data")
        cmd = _make_cmd()
        cmd.params = _default_params(
            src=src.as_uri(),
            dst=[dst.as_uri()],
            copy_mode="pull",
        )
        rc = cmd.execute_cp()
        # Should succeed (TPC not applicable for file://, falls back to streaming)
        assert rc == 0
        assert dst.read_bytes() == b"data"

    def test_copy_mode_push_sets_tpc(self, tmp_path, capsys):
        """copy_mode=push sets tpc=True and tpc_mode=push."""
        src = tmp_path / "src.txt"
        dst = tmp_path / "dst.txt"
        src.write_bytes(b"data")
        cmd = _make_cmd()
        cmd.params = _default_params(
            src=src.as_uri(),
            dst=[dst.as_uri()],
            copy_mode="push",
        )
        rc = cmd.execute_cp()
        assert rc == 0
        assert dst.read_bytes() == b"data"

    def test_copy_directory_over_file_fails(self, tmp_path):
        """Copying a directory over an existing file should fail."""
        src = tmp_path / "srcdir"
        src.mkdir()
        dst = tmp_path / "dst.txt"
        dst.write_bytes(b"existing")
        cmd = _make_cmd()
        cmd.params = _default_params(
            src=src.as_uri(), dst=[dst.as_uri()], force=True, recursive=False
        )
        rc = cmd.execute_cp()
        # Should fail because src is dir and dst is a file
        assert rc != 0

    def test_copy_dry_run_directory(self, tmp_path, capsys):
        """Dry-run with a directory: should print Mkdir."""
        src = tmp_path / "srcdir"
        dst = tmp_path / "dstdir"
        src.mkdir()
        (src / "f.txt").write_bytes(b"content")
        cmd = _make_cmd()
        cmd.params = _default_params(
            src=src.as_uri(), dst=[dst.as_uri()], dry_run=True, recursive=True
        )
        rc = cmd.execute_cp()
        assert rc == 0
        assert not dst.exists()
        out = capsys.readouterr().out
        assert "Mkdir" in out or "Copy" in out


class TestTransferTimeout:
    def test_transfer_timeout_completes_normally(self, tmp_path, capsys):
        """With a generous timeout, a small file copy should succeed."""
        src = tmp_path / "src.txt"
        dst = tmp_path / "dst.txt"
        src.write_bytes(b"hello world")
        cmd = _make_cmd()
        cmd.params = _default_params(
            src=src.as_uri(),
            dst=[dst.as_uri()],
            transfer_timeout=60,  # generous timeout
        )
        rc = cmd.execute_cp()
        assert rc == 0
        assert dst.read_bytes() == b"hello world"
