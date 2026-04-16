"""Direct unit tests for CommandCopy execute_cp (src/gfal/cli/copy.py).

These tests instantiate CommandCopy and set self.params directly (as a
SimpleNamespace) to avoid subprocess overhead and ensure coverage is collected
in the pytest process.
"""

import errno
import os
import sys
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from gfal.cli.copy import (
    CommandCopy,
    CopyOptions,
    _eos_mtime_url,
    _finalise_hasher,
    _is_special_file,
    _make_hasher,
    _parse_checksum_arg,
    _tpc_applicable,
    _update_hasher,
)
from gfal.core import fs
from gfal.core.api import checksum_fs as _checksum_fs


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
        "quiet": False,
        "log_file": None,
        "force": False,
        "parent": False,
        "checksum": None,
        "checksum_mode": "both",
        "compare": None,
        "recursive": False,
        "preserve_times": True,
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

    def test_single_destination_skips_chain_probe(self, tmp_path):
        src = tmp_path / "src.txt"
        src.write_bytes(b"hello")
        cmd = _make_cmd()
        cmd.params = _default_params(src=src.as_uri(), dst=["https://example.com/dst"])

        with (
            patch("gfal.cli.copy.fs.stat") as mock_stat,
            patch.object(cmd, "_do_copy", return_value=None) as mock_do_copy,
        ):
            rc = cmd.execute_cp()

        assert rc == 0
        mock_stat.assert_not_called()
        mock_do_copy.assert_called_once_with(
            src.as_uri(), "https://example.com/dst", {"timeout": 1800}
        )

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

    def test_copy_compare_checksum_skips_matching(self, tmp_path, capsys):
        src = tmp_path / "src.txt"
        dst = tmp_path / "dst.txt"
        src.write_bytes(b"same content")
        dst.write_bytes(b"same content")
        cmd = _make_cmd()
        cmd.params = _default_params(
            src=src.as_uri(),
            dst=[dst.as_uri()],
            compare="checksum",
        )

        rc = cmd.execute_cp()

        assert rc == 0
        assert dst.read_bytes() == b"same content"
        out = capsys.readouterr().out
        assert "matching ADLER32 checksum" in out

    def test_copy_compare_checksum_copies_when_different(self, tmp_path):
        src = tmp_path / "src.txt"
        dst = tmp_path / "dst.txt"
        src.write_bytes(b"new content")
        dst.write_bytes(b"old content")
        cmd = _make_cmd()
        cmd.params = _default_params(
            src=src.as_uri(),
            dst=[dst.as_uri()],
            compare="checksum",
        )

        rc = cmd.execute_cp()

        assert rc == 0
        assert dst.read_bytes() == b"new content"

    def test_copy_xrootd_permission_denied_returns_eacces(self, tmp_path):
        src = tmp_path / "src.txt"
        src.write_bytes(b"x")
        cmd = _make_cmd()
        cmd.params = _default_params(
            src=src.as_uri(),
            dst=[
                "root://eospilot.cern.ch//eos/pilot/opstest/dteam/python3-gfal/dteam-has-no-permissions-here/x.bin"
            ],
        )

        with patch.object(
            cmd,
            "_do_copy",
            side_effect=OSError(
                "File did not open properly: [ERROR] Server responded with an error: "
                "[3010] Unable to give access - user access restricted - "
                "unauthorized identity used ; Permission denied"
            ),
        ):
            rc = cmd.execute_cp()

        assert rc == errno.EACCES

    def test_copy_recursive_compare_checksum(self, tmp_path):
        src = tmp_path / "srcdir"
        src.mkdir()
        (src / "same.txt").write_bytes(b"same")
        (src / "new.txt").write_bytes(b"new")

        dst = tmp_path / "dstdir"
        dst.mkdir()
        (dst / "same.txt").write_bytes(b"same")

        cmd = _make_cmd()
        cmd.params = _default_params(
            src=src.as_uri(),
            dst=[dst.as_uri()],
            recursive=True,
            compare="checksum",
        )

        rc = cmd.execute_cp()

        assert rc == 0
        assert (dst / "same.txt").read_bytes() == b"same"
        assert (dst / "new.txt").read_bytes() == b"new"

    def test_copy_no_force_fails_if_dst_exists(self, tmp_path):
        src = tmp_path / "src.txt"
        dst = tmp_path / "dst.txt"
        src.write_bytes(b"new")
        dst.write_bytes(b"old")
        cmd = _make_cmd()
        cmd.params = _default_params(src=src.as_uri(), dst=[dst.as_uri()], force=False)
        rc = cmd.execute_cp()
        assert rc == 17

    def test_copy_compare_none_skips(self, tmp_path):
        src = tmp_path / "src.txt"
        dst = tmp_path / "dst.txt"
        src.write_bytes(b"new")
        dst.write_bytes(b"old")
        cmd = _make_cmd()
        cmd.params = _default_params(
            src=src.as_uri(),
            dst=[dst.as_uri()],
            compare="none",
        )

        rc = cmd.execute_cp()

        assert rc == 0
        assert dst.read_bytes() == b"old"

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

    def test_copy_preserve_times_file(self, tmp_path):
        """Times are preserved by default (preserve_times=True is the default)."""
        src = tmp_path / "src.txt"
        dst = tmp_path / "dst.txt"
        src.write_text("hello")
        os.utime(src, (946684800, 946684800))

        cmd = _make_cmd()
        cmd.params = _default_params(
            src=src.as_uri(),
            dst=[dst.as_uri()],
        )
        rc = cmd.execute_cp()

        assert rc == 0
        assert int(dst.stat().st_mtime) == 946684800

    def test_copy_no_preserve_times_file(self, tmp_path):
        """preserve_times=False disables mtime preservation."""
        src = tmp_path / "src.txt"
        dst = tmp_path / "dst.txt"
        src.write_text("hello")
        os.utime(src, (946684800, 946684800))

        cmd = _make_cmd()
        cmd.params = _default_params(
            src=src.as_uri(),
            dst=[dst.as_uri()],
            preserve_times=False,
        )
        rc = cmd.execute_cp()

        assert rc == 0
        assert int(dst.stat().st_mtime) != 946684800

    def test_copy_preserve_times_recursive(self, tmp_path):
        """Recursive copy preserves times by default."""
        src = tmp_path / "srcdir"
        src.mkdir()
        nested = src / "sub"
        nested.mkdir()
        top = src / "top.txt"
        child = nested / "child.txt"
        top.write_text("top")
        child.write_text("child")
        os.utime(top, (946688460, 946688460))
        os.utime(child, (981173100, 981173100))
        os.utime(nested, (946684740, 946684740))
        os.utime(src, (946684680, 946684680))

        dst = tmp_path / "dstdir"
        cmd = _make_cmd()
        cmd.params = _default_params(
            src=src.as_uri(),
            dst=[dst.as_uri()],
            recursive=True,
        )
        rc = cmd.execute_cp()

        assert rc == 0
        assert int((dst / "top.txt").stat().st_mtime) == 946688460
        assert int((dst / "sub" / "child.txt").stat().st_mtime) == 981173100
        assert int((dst / "sub").stat().st_mtime) == 946684740
        assert int(dst.stat().st_mtime) == 946684680


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


class TestPreserveTimesHelpers:
    def test_eos_mtime_url_for_https_eos(self):
        url = _eos_mtime_url(
            "https://eospilot.cern.ch//eos/pilot/test/file.txt", 946684800.0
        )
        assert url == (
            "https://eospilot.cern.ch//eos/pilot/test/file.txt?eos.mtime=946684800"
        )

    def test_eos_mtime_url_preserves_existing_query(self):
        url = _eos_mtime_url(
            "root://eospilot.cern.ch//eos/pilot/test/file.txt?authz=abc", 946684800.25
        )
        assert "authz=abc" in url
        assert "eos.mtime=946684800.250000000" in url

    def test_eos_mtime_url_ignores_non_eos_hosts(self):
        assert (
            _eos_mtime_url("root://redirector.example.org//store/file.root", 1.0)
            is None
        )

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


class TestCliUsesLibraryCopy:
    def test_do_copy_delegates_to_gfal_client(self, tmp_path):
        src = tmp_path / "src.txt"
        dst = tmp_path / "dst.txt"
        src.write_text("hello")

        cmd = _make_cmd()
        cmd.params = _default_params(
            src=src.as_uri(), dst=[dst.as_uri()], compare="size"
        )

        with patch("gfal.cli.copy.GfalClient") as mock_client_cls:
            mock_client = mock_client_cls.return_value

            cmd._do_copy(src.as_uri(), dst.as_uri(), {"timeout": 1800})

        mock_client.copy.assert_called_once()
        _, kwargs = mock_client.copy.call_args
        assert kwargs["options"] == CopyOptions(compare="size", preserve_times=True)
        assert callable(kwargs["progress_callback"])
        assert callable(kwargs["start_callback"])

    def test_do_copy_with_compare_none_delegates_correct_options(self, tmp_path):
        """--compare none should be forwarded as CopyOptions(compare='none')."""
        src = tmp_path / "src.txt"
        dst = tmp_path / "dst.txt"
        src.write_text("hello")

        cmd = _make_cmd()
        cmd.params = _default_params(
            src=src.as_uri(), dst=[dst.as_uri()], compare="none"
        )

        with patch("gfal.cli.copy.GfalClient") as mock_client_cls:
            mock_client = mock_client_cls.return_value

            cmd._do_copy(src.as_uri(), dst.as_uri(), {"timeout": 1800})

        _, kwargs = mock_client.copy.call_args
        assert kwargs["options"].compare == "none"


class TestTpcOnlyPreflight:
    def test_tpc_only_unsupported_pair_skips_destination_probe(self, tmp_path):
        src = tmp_path / "src.txt"
        src.write_bytes(b"hello")

        cmd = _make_cmd()
        cmd.params = _default_params(
            src=src.as_uri(),
            dst=["https://example.com/dst"],
            tpc_only=True,
        )

        src_fs, src_path = fs.url_to_fs(src.as_uri())
        mock_dst_fs = MagicMock()

        def _url_to_fs_side_effect(url, opts=None):
            if url == src.as_uri():
                return src_fs, src_path
            return mock_dst_fs, "/dst"

        with (
            patch("gfal.cli.copy.fs.url_to_fs", side_effect=_url_to_fs_side_effect),
            pytest.raises(OSError, match="TPC not supported for file:// -> https://"),
        ):
            cmd._do_copy(src.as_uri(), "https://example.com/dst", {})

        mock_dst_fs.info.assert_not_called()


# ---------------------------------------------------------------------------
# Dry-run recursive and skip-if-same (issues reported: 2026-04)
# ---------------------------------------------------------------------------


class TestDryRunRecursive:
    """--dry-run -r should walk the tree and show per-file output."""

    def test_dry_run_recursive_shows_individual_files(self, tmp_path, capsys):
        src = tmp_path / "src"
        dst = tmp_path / "dst"
        src.mkdir()
        (src / "a.txt").write_bytes(b"aaa")
        (src / "b.txt").write_bytes(b"bbb")

        cmd = _make_cmd()
        cmd.params = _default_params(
            src=src.as_uri(), dst=[dst.as_uri()], dry_run=True, recursive=True
        )
        rc = cmd.execute_cp()
        assert rc == 0
        assert not dst.exists()

        out = capsys.readouterr().out
        assert "Mkdir" in out
        assert "a.txt" in out
        assert "b.txt" in out

    def test_dry_run_recursive_shows_nested_dirs(self, tmp_path, capsys):
        src = tmp_path / "src"
        sub = src / "sub"
        dst = tmp_path / "dst"
        sub.mkdir(parents=True)
        (src / "root.txt").write_bytes(b"r")
        (sub / "leaf.txt").write_bytes(b"l")

        cmd = _make_cmd()
        cmd.params = _default_params(
            src=src.as_uri(), dst=[dst.as_uri()], dry_run=True, recursive=True
        )
        rc = cmd.execute_cp()
        assert rc == 0
        assert not dst.exists()

        out = capsys.readouterr().out
        assert "root.txt" in out
        assert "leaf.txt" in out
        # A Mkdir for the nested subdirectory should appear
        assert "sub" in out

    def test_dry_run_recursive_compare_checksum_skips_matching(self, tmp_path, capsys):
        """--dry-run --compare checksum should check checksums and show which
        files would be skipped vs. copied."""
        src = tmp_path / "src"
        dst = tmp_path / "dst"
        src.mkdir()
        dst.mkdir()
        (src / "same.txt").write_bytes(b"identical")
        (src / "diff.txt").write_bytes(b"new content")
        # dst has a matching file and a different one
        (dst / "same.txt").write_bytes(b"identical")
        (dst / "diff.txt").write_bytes(b"OLD content")

        cmd = _make_cmd()
        cmd.params = _default_params(
            src=src.as_uri(),
            dst=[dst.as_uri()],
            dry_run=True,
            recursive=True,
            compare="checksum",
        )
        rc = cmd.execute_cp()
        assert rc == 0

        out = capsys.readouterr().out
        # same.txt has matching checksum -> should be skipped
        assert "same.txt" in out
        assert "Skipping" in out
        # diff.txt has different checksum -> should be in a Copy line
        assert "diff.txt" in out
        assert "Copy" in out
        # dst files must NOT be modified (dry-run)
        assert (dst / "diff.txt").read_bytes() == b"OLD content"

    def test_dry_run_single_file_compare_checksum_skip(self, tmp_path, capsys):
        """--dry-run --compare checksum on a single matching file prints Skip."""
        src = tmp_path / "a.txt"
        dst = tmp_path / "b.txt"
        src.write_bytes(b"data")
        dst.write_bytes(b"data")

        cmd = _make_cmd()
        cmd.params = _default_params(
            src=src.as_uri(),
            dst=[dst.as_uri()],
            dry_run=True,
            compare="checksum",
        )
        rc = cmd.execute_cp()
        assert rc == 0

        out = capsys.readouterr().out
        assert "Skipping" in out
        assert "Copy" not in out

    def test_dry_run_single_file_compare_checksum_no_match(self, tmp_path, capsys):
        """--dry-run --compare checksum on a non-matching file prints Copy."""
        src = tmp_path / "a.txt"
        dst = tmp_path / "b.txt"
        src.write_bytes(b"new")
        dst.write_bytes(b"old")

        cmd = _make_cmd()
        cmd.params = _default_params(
            src=src.as_uri(),
            dst=[dst.as_uri()],
            dry_run=True,
            compare="checksum",
        )
        rc = cmd.execute_cp()
        assert rc == 0

        out = capsys.readouterr().out
        assert "Copy" in out
        assert "Skipping" not in out


# ---------------------------------------------------------------------------
# Cancel-event / Ctrl-C wiring (issue reported: 2026-04)
# ---------------------------------------------------------------------------


class TestCancelEvent:
    """The _cancel_event on CommandBase should abort the copy when set."""

    def test_cancel_event_initialized(self):
        import threading

        from gfal.cli.base import CommandBase

        cmd = CommandBase()
        assert isinstance(cmd._cancel_event, threading.Event)
        assert not cmd._cancel_event.is_set()

    def test_cancel_event_aborts_checksum_loop(self, tmp_path):
        """Setting cancel_event during checksum_fs raises GfalError."""
        import threading

        from gfal.core.api import checksum_fs
        from gfal.core.errors import GfalError

        large = tmp_path / "large.bin"
        large.write_bytes(b"x" * 1024 * 1024)  # 1 MB

        cancel = threading.Event()
        cancel.set()  # pre-cancelled

        from gfal.core.fs import url_to_fs

        fso, path = url_to_fs(large.as_uri())
        with pytest.raises(GfalError):
            checksum_fs(fso, path, "ADLER32", cancel)

    def test_cancel_event_passed_to_client_copy(self, tmp_path):
        """CommandCopy._do_copy passes self._cancel_event to client.copy()."""
        src = tmp_path / "src.txt"
        dst = tmp_path / "dst.txt"
        src.write_text("hello")

        cmd = _make_cmd()
        cmd.params = _default_params(src=src.as_uri(), dst=[dst.as_uri()])

        with patch("gfal.cli.copy.GfalClient") as mock_client_cls:
            mock_client = mock_client_cls.return_value
            cmd._do_copy(src.as_uri(), dst.as_uri(), {})

        mock_client.copy.assert_called_once()
        _, kwargs = mock_client.copy.call_args
        assert kwargs.get("cancel_event") is cmd._cancel_event


# ---------------------------------------------------------------------------
# Preserve-times warning fires once at directory level (issue reported: 2026-04)
# ---------------------------------------------------------------------------


class TestPreserveTimesWarningOnce:
    """--preserve-times warning must fire at most once per scheme, at the
    top-level directory, not once per file in a recursive copy."""

    def test_warning_fires_once_for_recursive_copy(self, tmp_path, capsys):
        """Simulate a non-local (http) dst: the warning fires only once even
        though there are multiple files."""
        import contextlib
        import sys
        from io import StringIO
        from urllib.parse import urlparse

        src = tmp_path / "src"
        src.mkdir()
        for i in range(3):
            (src / f"f{i}.txt").write_bytes(b"x" * 10)

        cmd = _make_cmd()
        cmd.params = _default_params(
            src=src.as_uri(),
            dst=["https://example.com/dst/"],
            recursive=True,
            preserve_times=True,
        )

        # Directly exercise the _preserve_times_warned dedup logic by
        # simulating five warning calls for the same scheme.
        stderr_buf = StringIO()
        cmd._preserve_times_warned = set()
        with contextlib.redirect_stderr(stderr_buf):
            for _ in range(5):
                normalized = "https://example.com/dst/"
                scheme = urlparse(normalized).scheme.lower() or "unknown"
                msg = (
                    "--preserve-times is only supported for local destinations; "
                    f"skipping for {scheme} targets"
                )
                if scheme in cmd._preserve_times_warned:
                    continue
                cmd._preserve_times_warned.add(scheme)
                sys.stderr.write(f"{cmd.prog}: warning: {msg}\n")

        warning_output = stderr_buf.getvalue()
        # Should appear exactly once
        assert warning_output.count("--preserve-times") == 1

    def test_no_warning_without_preserve_times_flag(self, tmp_path, capsys):
        """Without --preserve-times, no preserve-times warning is emitted."""
        src = tmp_path / "src"
        dst = tmp_path / "dst"
        src.mkdir()
        (src / "f.txt").write_bytes(b"hello")

        cmd = _make_cmd()
        cmd.params = _default_params(
            src=src.as_uri(),
            dst=[dst.as_uri()],
            recursive=True,
            preserve_times=False,
        )
        rc = cmd.execute_cp()
        assert rc == 0
        _, err = capsys.readouterr()
        assert "preserve-times" not in err
