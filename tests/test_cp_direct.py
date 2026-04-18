"""Direct unit tests for CommandCopy execute_cp (src/gfal/cli/copy.py).

These tests instantiate CommandCopy and set self.params directly (as a
SimpleNamespace) to avoid subprocess overhead and ensure coverage is collected
in the pytest process.
"""

import errno
import os
import sys
from types import SimpleNamespace
from unittest.mock import ANY, MagicMock, patch

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
    _TransferDisplay,
    _update_hasher,
)
from gfal.core import fs
from gfal.core.api import AsyncGfalClient as _AsyncGfalClient
from gfal.core.api import checksum_fs as _checksum_fs
from gfal.core.api import eos_app_url as _eos_app_url


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
        "limit": None,
        "dry_run": False,
        "abort_on_failure": False,
        "transfer_timeout": 0,
        "tpc": False,
        "tpc_only": False,
        "tpc_mode": "pull",
        "copy_mode": None,
        "parallel": 1,
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

    def test_copy_recursive_continues_after_existing_file_error(self, tmp_path, capsys):
        src = tmp_path / "srcdir"
        dst = tmp_path / "dstdir"
        src.mkdir()
        dst.mkdir()
        (src / "exists.txt").write_bytes(b"src")
        (src / "new.txt").write_bytes(b"new")
        (dst / "exists.txt").write_bytes(b"dst")
        cmd = _make_cmd()
        cmd.params = _default_params(
            src=src.as_uri(),
            dst=[dst.as_uri()],
            recursive=True,
        )

        rc = cmd.execute_cp()

        assert rc == errno.EEXIST
        assert (dst / "exists.txt").read_bytes() == b"dst"
        assert (dst / "new.txt").read_bytes() == b"new"
        captured = capsys.readouterr()
        assert "overwrite is not set" in captured.err

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

    def test_copy_rejects_non_positive_parallel(self, tmp_path, capsys):
        src = tmp_path / "src.txt"
        dst = tmp_path / "dst.txt"
        src.write_bytes(b"hello")
        cmd = _make_cmd()
        cmd.params = _default_params(src=src.as_uri(), dst=[dst.as_uri()], parallel=0)

        rc = cmd.execute_cp()

        assert rc == 1
        err = capsys.readouterr().err
        assert "--parallel must be at least 1" in err

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


class TestEosAppUrl:
    def test_eos_app_url_for_https_eos(self):
        url = _eos_app_url(
            "https://eospilot.cern.ch//eos/pilot/test/file.txt", "python3-gfal-cli"
        )
        assert url == (
            "https://eospilot.cern.ch//eos/pilot/test/file.txt?eos.app=python3-gfal-cli"
        )

    def test_eos_app_url_for_xrootd_eos(self):
        url = _eos_app_url(
            "root://eoshome.cern.ch//eos/home/user/file.txt", "python3-gfal-sync"
        )
        assert "eos.app=python3-gfal-sync" in url

    def test_eos_app_url_preserves_existing_query(self):
        url = _eos_app_url(
            "root://eospilot.cern.ch//eos/pilot/test/file.txt?authz=abc",
            "python3-gfal-async",
        )
        assert "authz=abc" in url
        assert "eos.app=python3-gfal-async" in url

    def test_eos_app_url_does_not_override_existing_app(self):
        url = _eos_app_url(
            "https://eospilot.cern.ch//eos/pilot/test/file.txt?eos.app=custom-app",
            "python3-gfal-cli",
        )
        assert "eos.app=custom-app" in url
        assert "python3-gfal-cli" not in url

    def test_eos_app_url_ignores_non_eos_cern_hosts(self):
        # Non-EOS host at cern.ch
        assert (
            _eos_app_url("https://lxplus.cern.ch/some/path", "python3-gfal-cli") is None
        )

    def test_eos_app_url_ignores_non_cern_hosts(self):
        # EOS-like hostname but not cern.ch
        assert (
            _eos_app_url("root://eos.example.org//store/file.root", "python3-gfal-cli")
            is None
        )

    def test_eos_app_url_ignores_local_files(self):
        assert _eos_app_url("/tmp/local/file.txt", "python3-gfal-cli") is None

    def test_eos_app_url_ignores_stdin_sentinel(self):
        # The '-' sentinel should pass through unchanged via _url()
        # (eos_app_url itself returns None for non-EOS URLs)
        assert _eos_app_url("-", "python3-gfal-cli") is None

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


class TestUrlPathJoin:
    """_url_path_join must insert the filename into the URL path, not the query string."""

    def test_plain_url_join(self):
        result = _AsyncGfalClient._url_path_join(
            "https://eospilot.cern.ch//eos/pilot/dir", "file.txt"
        )
        assert result == "https://eospilot.cern.ch//eos/pilot/dir/file.txt"

    def test_join_with_query_string(self):
        # When eos.app has already been injected into the directory URL the
        # filename must appear in the path, not appended after '?eos.app=…'.
        base = "https://eospilot.cern.ch//eos/pilot/dir?eos.app=python3-gfal-cli"
        result = _AsyncGfalClient._url_path_join(base, "file.txt")
        assert result == (
            "https://eospilot.cern.ch//eos/pilot/dir/file.txt?eos.app=python3-gfal-cli"
        )

    def test_join_strips_trailing_slash_before_appending(self):
        base = "https://eospilot.cern.ch//eos/pilot/dir/?eos.app=python3-gfal-cli"
        result = _AsyncGfalClient._url_path_join(base, "file.txt")
        assert "/dir/file.txt" in result
        assert "?eos.app=python3-gfal-cli" in result
        assert "/dir//file.txt" not in result

    def test_join_xrootd_url(self):
        base = "root://eospilot.cern.ch//eos/pilot/dir?eos.app=python3-gfal-sync"
        result = _AsyncGfalClient._url_path_join(base, "data.root")
        assert result == (
            "root://eospilot.cern.ch//eos/pilot/dir/data.root?eos.app=python3-gfal-sync"
        )

    def test_join_local_path(self):
        result = _AsyncGfalClient._url_path_join("file:///tmp/dir", "out.txt")
        assert result == "file:///tmp/dir/out.txt"


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
        assert kwargs["options"] == CopyOptions(
            compare="size",
            preserve_times=True,
            tpc="auto",
        )
        assert callable(kwargs["progress_callback"])
        assert callable(kwargs["start_callback"])
        assert kwargs["cancel_event"] is cmd._cancel_event

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

    def test_build_copy_options_marks_preserve_times_explicit(self):
        cmd = _make_cmd()
        cmd.argv = ["gfal-cp", "--preserve-times", "src", "dst"]
        cmd.params = _default_params(src="src", dst=["dst"], preserve_times=True)

        opts = cmd._build_copy_options()

        assert opts.preserve_times is True
        assert opts.preserve_times_explicit is True
        assert opts.tpc == "auto"

    def test_build_copy_options_copy_mode_streamed_disables_tpc(self):
        cmd = _make_cmd()
        cmd.params = _default_params(src="src", dst=["dst"], copy_mode="streamed")

        opts = cmd._build_copy_options()

        assert opts.tpc == "never"

    def test_recursive_parallelism_defaults_to_sequential(self):
        cmd = _make_cmd()
        cmd.params = _default_params(src="src", dst=["dst"])

        assert cmd._recursive_parallelism("https://a", "https://b") == 1

    def test_recursive_parallelism_uses_parallel_flag(self):
        cmd = _make_cmd()
        cmd.params = _default_params(src="src", dst=["dst"], parallel=7)

        assert cmd._recursive_parallelism("https://a", "https://b") == 7

    def test_predicted_transfer_mode_uses_streaming_for_copy_mode_streamed(self):
        cmd = _make_cmd()
        cmd.params = _default_params(
            src="https://src.example/file",
            dst=["https://dst.example/file"],
            copy_mode="streamed",
        )

        assert (
            cmd._predicted_transfer_mode(
                "https://src.example/file",
                "https://dst.example/file",
            )
            == "streamed"
        )

    def test_warn_copy_message_routes_skip_through_live_output(self):
        cmd = _make_cmd()
        cmd.params = _default_params(src="src", dst=["dst"])

        with patch("gfal.cli.copy.print_live_message") as mock_live_message:
            cmd._warn_copy_message(
                "Skipping existing file https://example.org/dst (matching size)",
                "https://example.org/dst",
            )

        mock_live_message.assert_called_once_with(
            "Skipping existing file https://example.org/dst (matching size)"
        )

    def test_do_copy_non_tty_reports_tpc_mode(self, tmp_path, capsys):
        src = tmp_path / "src.txt"
        dst = tmp_path / "dst.txt"
        src.write_text("hello")

        cmd = _make_cmd()
        cmd.params = _default_params(src=src.as_uri(), dst=[dst.as_uri()])

        with patch("gfal.cli.copy.GfalClient") as mock_client_cls:
            mock_client = mock_client_cls.return_value
            mock_client.stat.side_effect = [
                SimpleNamespace(st_size=5, is_dir=lambda: False),
                FileNotFoundError(),
                SimpleNamespace(st_size=5),
            ]

            def _copy_side_effect(*args, **kwargs):
                kwargs["transfer_mode_callback"]("tpc-pull")
                kwargs["start_callback"]()

            mock_client.copy.side_effect = _copy_side_effect
            cmd._do_copy(src.as_uri(), dst.as_uri(), {"timeout": 1800})

        out = capsys.readouterr().out
        assert "TPC pull" in out

    def test_do_copy_tty_tpc_progress_finishes_at_full_size(self, tmp_path):
        src = tmp_path / "src.txt"
        dst = tmp_path / "dst.txt"
        src.write_text("hello")

        cmd = _make_cmd()
        cmd.params = _default_params(src=src.as_uri(), dst=[dst.as_uri()])

        class _FakeProgress:
            def __init__(self, label):
                self.label = label
                self.calls = []

            def start(self):
                self.calls.append(("start",))

            def update(self, **kwargs):
                self.calls.append(("update", kwargs))

            def stop(self, success, status=None):
                self.calls.append(("stop", success, status))

        fake_instances = []

        def _make_progress(label):
            progress = _FakeProgress(label)
            fake_instances.append(progress)
            return progress

        with (
            patch("gfal.cli.copy.GfalClient") as mock_client_cls,
            patch("gfal.cli.copy.Progress", side_effect=_make_progress),
            patch("gfal.cli.copy.sys.stdout.isatty", return_value=True),
        ):
            mock_client = mock_client_cls.return_value
            mock_client.stat.side_effect = [
                SimpleNamespace(st_size=5, is_dir=lambda: False),
                FileNotFoundError(),
                SimpleNamespace(st_size=5),
            ]

            def _copy_side_effect(*args, **kwargs):
                kwargs["transfer_mode_callback"]("tpc-pull")
                kwargs["start_callback"]()
                kwargs["progress_callback"](3)

            mock_client.copy.side_effect = _copy_side_effect
            cmd._do_copy(src.as_uri(), dst.as_uri(), {"timeout": 1800})

        assert len(fake_instances) == 1
        progress = fake_instances[0]
        assert progress.label == "Copying src.txt (TPC pull)"
        assert progress.calls[0] == ("start",)
        assert progress.calls[1] == ("update", {"total_size": 5})
        assert progress.calls[2] == (
            "update",
            {
                "curr_size": 3,
                "total_size": 5,
                "elapsed": progress.calls[2][1]["elapsed"],
            },
        )
        assert progress.calls[3] == (
            "update",
            {
                "curr_size": 5,
                "total_size": 5,
                "elapsed": progress.calls[3][1]["elapsed"],
            },
        )
        assert progress.calls[4] == ("stop", True, None)

    def test_recursive_top_level_children_emit_history_lines(self, tmp_path, capsys):
        src = tmp_path / "srcdir"
        dst = tmp_path / "dstdir"
        src.mkdir()
        dst.mkdir()
        (src / "one.txt").write_text("one")
        (src / "two.txt").write_text("two")

        cmd = _make_cmd()
        cmd.params = _default_params(
            src=src.as_uri(), dst=[dst.as_uri()], recursive=True
        )

        class _DoneHandle:
            def done(self):
                return True

            def wait(self, timeout=None):
                return None

            def cancel(self):
                return None

        started = []

        def _start_copy(self, src_url, dst_url, **kwargs):
            started.append((src_url, dst_url))
            kwargs["transfer_mode_callback"]("tpc-pull")
            kwargs["start_callback"]()
            return _DoneHandle()

        fake_scan_progress = MagicMock()

        with (
            patch("gfal.cli.copy.Progress") as mock_progress,
            patch("gfal.cli.copy.CountProgress", return_value=fake_scan_progress),
            patch("gfal.cli.copy.print_live_message") as mock_live_message,
            patch("gfal.cli.copy.sys.stdout.isatty", return_value=True),
            patch("gfal.core.api.GfalClient.start_copy", new=_start_copy),
            patch("gfal.core.api.AsyncGfalClient._preserve_times", return_value=None),
        ):
            cmd._do_copy(src.as_uri(), dst.as_uri(), {"timeout": 1800})

        assert sorted(started) == [
            ((src / "one.txt").as_uri(), (dst / "one.txt").as_uri()),
            ((src / "two.txt").as_uri(), (dst / "two.txt").as_uri()),
        ]
        mock_progress.assert_not_called()
        fake_scan_progress.start.assert_called_once()
        fake_scan_progress.stop.assert_called_once_with(True)
        messages = [str(call.args[0]) for call in mock_live_message.call_args_list]
        assert any("one.txt" in message and "copied" in message for message in messages)
        assert any("two.txt" in message and "copied" in message for message in messages)
        assert any("[1/2]" in message for message in messages)
        assert any("[2/2]" in message for message in messages)
        assert any("Scan complete" in message for message in messages)
        assert any("Starting transfers" in message for message in messages)
        assert any("Copy complete" in message for message in messages)

    def test_recursive_tty_uses_history_lines_not_rich_bars(self, tmp_path, capsys):
        src = tmp_path / "srcdir"
        dst = tmp_path / "dstdir"
        src.mkdir()
        dst.mkdir()
        (src / "one.txt").write_text("one")
        (src / "two.txt").write_text("two")

        cmd = _make_cmd()
        cmd.params = _default_params(
            src=src.as_uri(), dst=[dst.as_uri()], recursive=True
        )

        class _DoneHandle:
            def done(self):
                return True

            def wait(self, timeout=None):
                del timeout
                return None

            def cancel(self):
                return None

        def _start_copy(self, src_url, dst_url, **kwargs):
            kwargs["transfer_mode_callback"]("tpc-pull")
            kwargs["start_callback"]()
            return _DoneHandle()

        fake_scan_progress = MagicMock()
        events = []
        fake_scan_progress.stop.side_effect = lambda *args, **kwargs: events.append(
            ("stop", args, kwargs)
        )

        with (
            patch("gfal.cli.copy.Progress") as mock_progress,
            patch("gfal.cli.copy.CountProgress", return_value=fake_scan_progress),
            patch("gfal.cli.copy.sys.stdout.isatty", return_value=True),
            patch("gfal.cli.copy.print_live_message") as mock_live_message,
            patch("gfal.core.api.GfalClient.start_copy", new=_start_copy),
            patch("gfal.core.api.AsyncGfalClient._preserve_times", return_value=None),
        ):
            mock_live_message.side_effect = lambda message: events.append(
                ("message", message)
            )
            cmd._do_copy(src.as_uri(), dst.as_uri(), {"timeout": 1800})

        mock_progress.assert_not_called()
        fake_scan_progress.start.assert_called_once()
        fake_scan_progress.stop.assert_called_once_with(True)
        messages = [str(call.args[0]) for call in mock_live_message.call_args_list]
        assert any("one.txt" in message and "copied" in message for message in messages)
        assert any("two.txt" in message and "copied" in message for message in messages)
        assert any("[1/2]" in message for message in messages)
        assert any("[2/2]" in message for message in messages)
        assert any("Scan complete" in message for message in messages)
        final_summary_index = next(
            index
            for index, event in enumerate(events)
            if event[0] == "message" and "Copy complete" in str(event[1])
        )
        stop_index = next(
            index for index, event in enumerate(events) if event[0] == "stop"
        )
        assert stop_index < final_summary_index

    def test_history_status_line_includes_size_rate_and_elapsed(self):
        display = _TransferDisplay(
            "https://example.org/src.bin",
            "https://example.org/dst.bin",
            src_size=1_048_576,
            transfer_mode="streamed",
            history_only=True,
        )
        display.progress_started = True

        with patch("gfal.cli.copy.time.monotonic", side_effect=[102.0]):
            display.transfer_start = 100.0
            line = display._history_status_line(True)

        assert "Copying src.bin (streamed) [DONE]" in line
        assert "1.0 MB" in line
        assert "512.0 KB/s" in line
        assert "00:00:02" in line

    def test_recursive_scan_spinner_marks_failure_when_listing_raises(self):
        cmd = _make_cmd()
        cmd.params = _default_params(src="src", dst=["dst"], recursive=True)

        fake_client = MagicMock()
        fake_client.stat.side_effect = FileNotFoundError()

        fake_src_fs = MagicMock()
        fake_src_fs.ls.side_effect = RuntimeError("listing failed")
        fake_spinner = MagicMock()

        with (
            patch(
                "gfal.cli.copy.fs.url_to_fs",
                side_effect=[(fake_src_fs, "/src"), (MagicMock(), "/dst")],
            ),
            patch("gfal.cli.copy.Spinner", return_value=fake_spinner),
            pytest.raises(RuntimeError, match="listing failed"),
        ):
            cmd._copy_directory_parallel(
                fake_client,
                "https://example.org/src",
                "https://example.org/dst",
                {"timeout": 1800},
                SimpleNamespace(),
            )

        fake_spinner.start.assert_called_once()
        fake_spinner.stop.assert_called_once_with(False)

    def test_recursive_scan_uses_live_count_progress(self, tmp_path):
        src = tmp_path / "srcdir"
        dst = tmp_path / "dstdir"
        src.mkdir()
        dst.mkdir()

        entries = [str(src / f"file-{index:03d}.txt") for index in range(251)]

        cmd = _make_cmd()
        cmd.params = _default_params(
            src=src.as_uri(),
            dst=[dst.as_uri()],
            recursive=True,
            limit=1,
        )

        class _DoneHandle:
            def done(self):
                return True

            def wait(self, timeout=None):
                del timeout
                return None

            def cancel(self):
                return None

        fake_client = MagicMock()
        fake_client.stat.side_effect = [
            FileNotFoundError(),
            SimpleNamespace(is_dir=lambda: False, st_size=1),
        ]
        fake_client.start_copy.return_value = _DoneHandle()
        fake_client._async_client = MagicMock()

        fake_src_fs = MagicMock()
        fake_src_fs.ls.return_value = entries
        fake_dst_fs = MagicMock()
        fake_dst_fs.ls.return_value = []
        fake_spinner = MagicMock()
        fake_scan_progress = MagicMock()

        with (
            patch(
                "gfal.cli.copy.fs.url_to_fs",
                side_effect=[(fake_src_fs, str(src)), (fake_dst_fs, str(dst))],
            ),
            patch("gfal.cli.copy.Spinner", return_value=fake_spinner),
            patch("gfal.cli.copy.CountProgress", return_value=fake_scan_progress),
            patch("gfal.cli.copy.sys.stdout.isatty", return_value=True),
        ):
            cmd._copy_directory_parallel(
                fake_client,
                src.as_uri(),
                dst.as_uri(),
                {"timeout": 1800},
                SimpleNamespace(),
            )

        fake_spinner.start.assert_called_once()
        fake_spinner.stop.assert_called_once_with(True)
        fake_scan_progress.start.assert_called_once()
        completed_updates = [
            call.kwargs["completed"]
            for call in fake_scan_progress.update.call_args_list
            if "completed" in call.kwargs
        ]
        assert 1 in completed_updates
        assert 250 in completed_updates
        assert completed_updates[-1] == 251
        fake_scan_progress.stop.assert_any_call(True)

    def test_recursive_scan_summary_reports_copy_and_skip_counts(self, tmp_path):
        src = tmp_path / "srcdir"
        dst = tmp_path / "dstdir"
        src.mkdir()
        dst.mkdir()
        (src / "copy-me.txt").write_text("copy me")
        (src / "skip-me.txt").write_text("skip me")
        (dst / "skip-me.txt").write_text("already here")

        cmd = _make_cmd()
        cmd.params = _default_params(
            src=src.as_uri(),
            dst=[dst.as_uri()],
            recursive=True,
            compare="none",
        )

        class _DoneHandle:
            def done(self):
                return True

            def wait(self, timeout=None):
                del timeout
                return None

            def cancel(self):
                return None

        with (
            patch(
                "gfal.core.api.GfalClient.start_copy",
                return_value=_DoneHandle(),
            ),
            patch("gfal.core.api.AsyncGfalClient._preserve_times", return_value=None),
            patch("gfal.cli.copy.print_live_message") as mock_live_message,
        ):
            cmd._do_copy(src.as_uri(), dst.as_uri(), {"timeout": 1800})

        assert any(
            call.args[0]
            == "Recursive scan complete: 2 files, 1 queued to copy, 1 already present and likely skipped"
            for call in mock_live_message.call_args_list
        )
        assert any(
            call.args[0].startswith("Recursive copy complete: 2 copied, elapsed ")
            for call in mock_live_message.call_args_list
        )

    def test_recursive_limit_caps_started_children_and_summary(self, tmp_path):
        src = tmp_path / "srcdir"
        dst = tmp_path / "dstdir"
        src.mkdir()
        dst.mkdir()
        for name in ("one.txt", "two.txt", "three.txt"):
            (src / name).write_text(name)

        cmd = _make_cmd()
        cmd.params = _default_params(
            src=src.as_uri(),
            dst=[dst.as_uri()],
            recursive=True,
            compare="none",
            limit=2,
        )

        started = []

        class _DoneHandle:
            def done(self):
                return True

            def wait(self, timeout=None):
                del timeout
                return None

            def cancel(self):
                return None

        def _start_copy(src_url, dst_url, **kwargs):
            del kwargs
            started.append((src_url, dst_url))
            return _DoneHandle()

        with (
            patch("gfal.core.api.GfalClient.start_copy", side_effect=_start_copy),
            patch("gfal.core.api.AsyncGfalClient._preserve_times", return_value=None),
            patch("gfal.cli.copy.print_live_message") as mock_live_message,
        ):
            cmd._do_copy(src.as_uri(), dst.as_uri(), {"timeout": 1800})

        assert len(started) == 2
        assert any(
            call.args[0]
            == "Recursive scan complete: 3 files, 2 queued to copy, 0 already present and likely skipped (limited to 2)"
            for call in mock_live_message.call_args_list
        )
        assert any(
            call.args[0].startswith("Recursive copy complete: 2 copied, elapsed ")
            for call in mock_live_message.call_args_list
        )

    def test_recursive_directory_child_does_not_set_progress_total(self, tmp_path):
        src = tmp_path / "srcdir"
        dst = tmp_path / "dstdir"
        src.mkdir()
        dst.mkdir()

        child_src = (src / "nested").as_uri()
        child_dst = (dst / "nested").as_uri()

        cmd = _make_cmd()
        cmd.params = _default_params(
            src=src.as_uri(), dst=[dst.as_uri()], recursive=True
        )

        class _FakeDisplay:
            def __init__(self, *args, **kwargs):
                del args, kwargs
                self.total_sizes = []
                self.show_progress = True

            def start(self):
                return None

            def set_total_size(self, total_size):
                self.total_sizes.append(total_size)

            def update(self, *args, **kwargs):
                del args, kwargs
                return None

            def set_mode(self, *args, **kwargs):
                del args, kwargs
                return None

            def finish(self, *args, **kwargs):
                del args, kwargs
                return None

        class _DoneHandle:
            def done(self):
                return True

            def wait(self, timeout=None):
                del timeout
                return None

            def cancel(self):
                return None

        fake_displays = []

        def _make_display(*args, **kwargs):
            display = _FakeDisplay(*args, **kwargs)
            fake_displays.append(display)
            return display

        fake_client = MagicMock()
        fake_client.stat.side_effect = [
            SimpleNamespace(is_dir=lambda: True),
            SimpleNamespace(is_dir=lambda: True, st_size=4096),
        ]
        fake_client.start_copy.return_value = _DoneHandle()
        fake_client._async_client = MagicMock()

        fake_src_fs = MagicMock()
        fake_src_fs.ls.return_value = [str(src / "nested")]

        with (
            patch(
                "gfal.cli.copy.fs.url_to_fs",
                side_effect=[(fake_src_fs, str(src)), (MagicMock(), str(dst))],
            ),
            patch("gfal.cli.copy._TransferDisplay", side_effect=_make_display),
        ):
            cmd._copy_directory_parallel(
                fake_client,
                src.as_uri(),
                dst.as_uri(),
                {"timeout": 1800},
                SimpleNamespace(),
            )

        assert len(fake_displays) == 1
        assert fake_displays[0].total_sizes == []
        fake_client.start_copy.assert_called_once_with(
            child_src,
            child_dst,
            options=cmd._build_copy_options(),
            progress_callback=fake_displays[0].update,
            start_callback=fake_displays[0].start,
            warn_callback=ANY,
            transfer_mode_callback=fake_displays[0].set_mode,
            error_callback=cmd._child_error_callback,
            traverse_callback=cmd._traverse_callback,
            cancel_event=cmd._cancel_event,
        )

    def test_recursive_child_failure_is_reported_once(self, tmp_path):
        src = tmp_path / "srcdir"
        dst = tmp_path / "dstdir"
        src.mkdir()
        dst.mkdir()
        child = src / "one.txt"
        child.write_text("one")

        cmd = _make_cmd()
        cmd.params = _default_params(
            src=src.as_uri(), dst=[dst.as_uri()], recursive=True
        )

        failure = PermissionError("denied")

        class _DoneHandle:
            def done(self):
                return True

            def wait(self, timeout=None):
                del timeout
                raise failure

            def cancel(self):
                return None

        def _start_copy(*args, **kwargs):
            kwargs["error_callback"](args[0], args[1], failure)
            return _DoneHandle()

        with (
            patch("gfal.core.api.GfalClient.start_copy", side_effect=_start_copy),
            patch("gfal.core.api.AsyncGfalClient._preserve_times", return_value=None),
            patch.object(cmd, "_print_error") as mock_print_error,
            pytest.raises(Exception) as excinfo,
        ):
            cmd._do_copy(src.as_uri(), dst.as_uri(), {"timeout": 1800})

        assert "1 recursive transfer(s) failed" in str(excinfo.value)
        mock_print_error.assert_called_once_with(failure)

    def test_do_copy_tty_compare_skip_marks_progress_as_skipped(self, tmp_path):
        src = tmp_path / "src.txt"
        dst = tmp_path / "dst.txt"
        src.write_text("hello")

        cmd = _make_cmd()
        cmd.params = _default_params(
            src=src.as_uri(),
            dst=[dst.as_uri()],
            compare="size",
        )

        class _FakeProgress:
            def __init__(self, label):
                self.label = label
                self.calls = []

            def start(self):
                self.calls.append(("start",))

            def update(self, **kwargs):
                self.calls.append(("update", kwargs))

            def set_description(self, label):
                self.calls.append(("set_description", label))
                self.label = label

            def stop(self, success, status=None):
                self.calls.append(("stop", success, status))

        fake_instances = []

        def _make_progress(label):
            progress = _FakeProgress(label)
            fake_instances.append(progress)
            return progress

        with (
            patch("gfal.cli.copy.GfalClient") as mock_client_cls,
            patch("gfal.cli.copy.Progress", side_effect=_make_progress),
            patch("gfal.cli.copy.sys.stdout.isatty", return_value=True),
            patch("gfal.cli.copy.print_live_message") as mock_live_message,
        ):
            mock_client = mock_client_cls.return_value
            mock_client.stat.side_effect = [
                SimpleNamespace(st_size=5, is_dir=lambda: False),
                SimpleNamespace(st_size=5, is_dir=lambda: False),
                SimpleNamespace(st_size=5),
            ]

            def _copy_side_effect(*args, **kwargs):
                kwargs["warn_callback"](
                    f"Skipping existing file {dst.as_uri()} (matching size)"
                )

            mock_client.copy.side_effect = _copy_side_effect
            cmd._do_copy(src.as_uri(), dst.as_uri(), {"timeout": 1800})

        progress = fake_instances[0]
        assert ("stop", True, "skipped") in progress.calls
        mock_live_message.assert_not_called()

    def test_recursive_top_level_children_use_start_copy_not_copy(self, tmp_path):
        src = tmp_path / "srcdir"
        dst = tmp_path / "dstdir"
        src.mkdir()
        dst.mkdir()
        (src / "one.txt").write_text("one")

        cmd = _make_cmd()
        cmd.params = _default_params(
            src=src.as_uri(), dst=[dst.as_uri()], recursive=True
        )

        class _DoneHandle:
            def done(self):
                return True

            def wait(self, timeout=None):
                return None

            def cancel(self):
                return None

        with (
            patch("gfal.core.api.GfalClient.copy") as mock_copy,
            patch(
                "gfal.core.api.GfalClient.start_copy",
                return_value=_DoneHandle(),
            ) as mock_start_copy,
            patch("gfal.core.api.AsyncGfalClient._preserve_times", return_value=None),
        ):
            cmd._do_copy(src.as_uri(), dst.as_uri(), {"timeout": 1800})

        mock_copy.assert_not_called()
        mock_start_copy.assert_called_once()
        _, kwargs = mock_start_copy.call_args
        assert kwargs["cancel_event"] is cmd._cancel_event


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
