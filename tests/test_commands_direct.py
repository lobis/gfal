"""Direct unit tests for GfalCommands execute_* methods (src/gfal/cli/commands.py).

These tests instantiate GfalCommands and set self.params directly (as a
SimpleNamespace) to avoid subprocess overhead and ensure coverage is collected
in the pytest process.
"""

import sys
import zlib
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from gfal.cli.commands import GfalCommands


def _make_cmd(prog="gfal-test"):
    """Create a GfalCommands instance with a prog attribute set."""
    cmd = GfalCommands()
    cmd.prog = prog
    return cmd


def _default_params(**kwargs):
    """Return a SimpleNamespace with default common params."""
    defaults = {
        "cert": None,
        "key": None,
        "timeout": 1800,
        "ssl_verify": True,
        "verbose": 0,
        "log_file": None,
        "ipv4_only": False,
        "ipv6_only": False,
    }
    defaults.update(kwargs)
    return SimpleNamespace(**defaults)


# ---------------------------------------------------------------------------
# execute_mkdir
# ---------------------------------------------------------------------------


class TestExecuteMkdir:
    def test_mkdir_creates_directory(self, tmp_path):
        d = tmp_path / "newdir"
        cmd = _make_cmd("gfal-mkdir")
        cmd.params = _default_params(
            directory=[d.as_uri()],
            mode="755",
            parents=False,
        )
        rc = cmd.execute_mkdir()
        assert rc is None or rc == 0
        assert d.is_dir()

    def test_mkdir_parents(self, tmp_path):
        d = tmp_path / "a" / "b" / "c"
        cmd = _make_cmd("gfal-mkdir")
        cmd.params = _default_params(
            directory=[d.as_uri()],
            mode="755",
            parents=True,
        )
        rc = cmd.execute_mkdir()
        assert rc is None or rc == 0
        assert d.is_dir()

    def test_mkdir_invalid_mode_returns_error(self, tmp_path, monkeypatch):
        monkeypatch.setenv("GFAL_CLI_GFAL2", "1")
        d = tmp_path / "newdir"
        cmd = _make_cmd("gfal-mkdir")
        cmd.params = _default_params(
            directory=[d.as_uri()],
            mode="abc",  # invalid octal
            parents=False,
        )
        rc = cmd.execute_mkdir()
        assert rc == 1
        assert not d.exists()

    def test_mkdir_invalid_mode_rich_output(self, tmp_path, monkeypatch):
        monkeypatch.delenv("GFAL_CLI_GFAL2", raising=False)
        d = tmp_path / "newdir"
        cmd = _make_cmd("gfal-mkdir")
        cmd.params = _default_params(
            directory=[d.as_uri()],
            mode="xyz",
            parents=False,
        )
        rc = cmd.execute_mkdir()
        assert rc == 1

    def test_mkdir_multiple_directories(self, tmp_path):
        d1 = tmp_path / "dir1"
        d2 = tmp_path / "dir2"
        cmd = _make_cmd("gfal-mkdir")
        cmd.params = _default_params(
            directory=[d1.as_uri(), d2.as_uri()],
            mode="755",
            parents=False,
        )
        rc = cmd.execute_mkdir()
        assert rc is None or rc == 0
        assert d1.is_dir()
        assert d2.is_dir()

    def test_mkdir_error_propagates_to_rc(self, tmp_path):
        """Creating a directory with a nonexistent parent should set rc."""
        d = tmp_path / "no_parent" / "newdir"
        cmd = _make_cmd("gfal-mkdir")
        cmd.params = _default_params(
            directory=[d.as_uri()],
            mode="755",
            parents=False,
        )
        rc = cmd.execute_mkdir()
        assert rc != 0


# ---------------------------------------------------------------------------
# execute_cat
# ---------------------------------------------------------------------------


class TestExecuteCat:
    def test_cat_outputs_file_content(self, tmp_path, capsys):
        f = tmp_path / "test.txt"
        f.write_bytes(b"hello world")
        cmd = _make_cmd("gfal-cat")
        cmd.params = _default_params(
            file=[f.as_uri()],
            bytes=False,
        )
        rc = cmd.execute_cat()
        assert rc is None or rc == 0

    def test_cat_nonexistent_returns_error(self, tmp_path):
        cmd = _make_cmd("gfal-cat")
        cmd.params = _default_params(
            file=[(tmp_path / "no_such.txt").as_uri()],
            bytes=False,
        )
        rc = cmd.execute_cat()
        assert rc != 0

    def test_cat_passes_ipv6_only_to_client(self):
        cmd = _make_cmd("gfal-cat")
        cmd.params = _default_params(
            file=["file:///tmp/test.txt"],
            bytes=False,
            ipv6_only=True,
        )
        fake_file = MagicMock()
        fake_file.__enter__.return_value.read.side_effect = [b"", b""]

        with patch("gfal.cli.commands.GfalClient") as mock_client_cls:
            mock_client_cls.return_value.open.return_value = fake_file
            rc = cmd.execute_cat()

        assert rc is None or rc == 0
        assert mock_client_cls.call_args.kwargs["ipv6_only"] is True
        assert mock_client_cls.call_args.kwargs["ipv4_only"] is False


# ---------------------------------------------------------------------------
# execute_stat
# ---------------------------------------------------------------------------


class TestExecuteStat:
    def test_stat_regular_file(self, tmp_path, capsys, monkeypatch):
        monkeypatch.setenv("GFAL_CLI_GFAL2", "1")
        f = tmp_path / "test.txt"
        f.write_bytes(b"hello world")
        cmd = _make_cmd("gfal-stat")
        cmd.params = _default_params(file=[f.as_uri()])
        rc = cmd.execute_stat()
        captured = capsys.readouterr()
        assert rc is None or rc == 0
        assert "11" in captured.out

    def test_stat_nonexistent_returns_error(self, tmp_path, monkeypatch):
        monkeypatch.setenv("GFAL_CLI_GFAL2", "1")
        cmd = _make_cmd("gfal-stat")
        cmd.params = _default_params(file=[(tmp_path / "no_such").as_uri()])
        rc = cmd.execute_stat()
        assert rc != 0

    def test_stat_multiple_files(self, tmp_path, capsys, monkeypatch):
        monkeypatch.setenv("GFAL_CLI_GFAL2", "1")
        a = tmp_path / "a.txt"
        b = tmp_path / "b.txt"
        a.write_bytes(b"hello")
        b.write_bytes(b"world!!")
        cmd = _make_cmd("gfal-stat")
        cmd.params = _default_params(file=[a.as_uri(), b.as_uri()])
        rc = cmd.execute_stat()
        captured = capsys.readouterr()
        assert rc is None or rc == 0
        assert "5" in captured.out
        assert "7" in captured.out

    def test_stat_directory(self, tmp_path, capsys, monkeypatch):
        monkeypatch.setenv("GFAL_CLI_GFAL2", "1")
        cmd = _make_cmd("gfal-stat")
        cmd.params = _default_params(file=[tmp_path.as_uri()])
        rc = cmd.execute_stat()
        captured = capsys.readouterr()
        assert rc is None or rc == 0
        assert "directory" in captured.out

    def test_stat_rich_mode(self, tmp_path, monkeypatch):
        monkeypatch.delenv("GFAL_CLI_GFAL2", raising=False)
        f = tmp_path / "test.txt"
        f.write_bytes(b"hello")
        cmd = _make_cmd("gfal-stat")
        cmd.params = _default_params(file=[f.as_uri()])
        rc = cmd.execute_stat()
        assert rc is None or rc == 0

    def test_stat_passes_ipv4_only_to_client(self, monkeypatch):
        monkeypatch.setenv("GFAL_CLI_GFAL2", "1")
        cmd = _make_cmd("gfal-stat")
        cmd.params = _default_params(file=["file:///tmp/test.txt"], ipv4_only=True)
        fake_stat = SimpleNamespace(
            st_size=1,
            st_mode=0o100644,
            st_uid=0,
            st_gid=0,
            st_atime=0,
            st_mtime=0,
            st_ctime=0,
        )

        with patch("gfal.cli.commands.GfalClient") as mock_client_cls:
            mock_client_cls.return_value.stat.return_value = fake_stat
            rc = cmd.execute_stat()

        assert rc == 0
        assert mock_client_cls.call_args.kwargs["ipv4_only"] is True
        assert mock_client_cls.call_args.kwargs["ipv6_only"] is False


# ---------------------------------------------------------------------------
# execute_rename
# ---------------------------------------------------------------------------


class TestExecuteRename:
    def test_rename_file(self, tmp_path):
        src = tmp_path / "old.txt"
        dst = tmp_path / "new.txt"
        src.write_text("content")
        cmd = _make_cmd("gfal-rename")
        cmd.params = _default_params(
            source=src.as_uri(),
            destination=dst.as_uri(),
        )
        rc = cmd.execute_rename()
        assert rc is None or rc == 0
        assert not src.exists()
        assert dst.read_text() == "content"


# ---------------------------------------------------------------------------
# execute_chmod
# ---------------------------------------------------------------------------


@pytest.mark.skipif(sys.platform == "win32", reason="chmod semantics differ on Windows")
class TestExecuteChmod:
    def test_chmod_file(self, tmp_path):
        f = tmp_path / "file.txt"
        f.write_text("x")
        cmd = _make_cmd("gfal-chmod")
        cmd.params = _default_params(
            mode="600",
            file=[f.as_uri()],
        )
        rc = cmd.execute_chmod()
        assert rc is None or rc == 0
        assert (f.stat().st_mode & 0o777) == 0o600

    def test_chmod_invalid_mode_returns_error_gfal2(self, tmp_path, monkeypatch):
        monkeypatch.setenv("GFAL_CLI_GFAL2", "1")
        f = tmp_path / "file.txt"
        f.write_text("x")
        cmd = _make_cmd("gfal-chmod")
        cmd.params = _default_params(
            mode="xyz",
            file=[f.as_uri()],
        )
        rc = cmd.execute_chmod()
        assert rc == 1

    def test_chmod_invalid_mode_returns_error_rich(self, tmp_path, monkeypatch):
        monkeypatch.delenv("GFAL_CLI_GFAL2", raising=False)
        f = tmp_path / "file.txt"
        f.write_text("x")
        cmd = _make_cmd("gfal-chmod")
        cmd.params = _default_params(
            mode="notoctal",
            file=[f.as_uri()],
        )
        rc = cmd.execute_chmod()
        assert rc == 1

    def test_chmod_multiple_files(self, tmp_path):
        f1 = tmp_path / "f1.txt"
        f2 = tmp_path / "f2.txt"
        f1.write_text("x")
        f2.write_text("x")
        cmd = _make_cmd("gfal-chmod")
        cmd.params = _default_params(
            mode="644",
            file=[f1.as_uri(), f2.as_uri()],
        )
        rc = cmd.execute_chmod()
        assert rc is None or rc == 0


# ---------------------------------------------------------------------------
# execute_sum
# ---------------------------------------------------------------------------


class TestExecuteSum:
    def test_sum_adler32(self, tmp_path, capsys):
        data = b"hello world"
        f = tmp_path / "test.bin"
        f.write_bytes(data)
        expected = f"{zlib.adler32(data) & 0xFFFFFFFF:08x}"
        cmd = _make_cmd("gfal-sum")
        cmd.params = _default_params(
            file=f.as_uri(),
            checksum_type="ADLER32",
        )
        rc = cmd.execute_sum()
        captured = capsys.readouterr()
        assert rc is None or rc == 0
        assert expected in captured.out

    def test_sum_nonexistent_returns_error(self, tmp_path):
        cmd = _make_cmd("gfal-sum")
        cmd.params = _default_params(
            file=(tmp_path / "no_such").as_uri(),
            checksum_type="ADLER32",
        )
        rc = cmd.execute_sum()
        assert rc == 1


# ---------------------------------------------------------------------------
# execute_xattr
# ---------------------------------------------------------------------------


class TestExecuteXattr:
    def test_xattr_list_no_support_returns_error(self, tmp_path):
        """Local filesystem doesn't support xattr; should return a non-zero code."""
        f = tmp_path / "file.txt"
        f.write_text("x")
        cmd = _make_cmd("gfal-xattr")
        cmd.params = _default_params(
            file=f.as_uri(),
            attribute=None,
        )
        rc = cmd.execute_xattr()
        assert rc != 0

    def test_xattr_get_no_support_returns_error(self, tmp_path):
        f = tmp_path / "file.txt"
        f.write_text("x")
        cmd = _make_cmd("gfal-xattr")
        cmd.params = _default_params(
            file=f.as_uri(),
            attribute="user.test",
        )
        rc = cmd.execute_xattr()
        assert rc != 0

    def test_xattr_set_no_support_returns_error(self, tmp_path):
        f = tmp_path / "file.txt"
        f.write_text("x")
        cmd = _make_cmd("gfal-xattr")
        cmd.params = _default_params(
            file=f.as_uri(),
            attribute="user.key=value",
        )
        rc = cmd.execute_xattr()
        assert rc != 0


# ---------------------------------------------------------------------------
# execute_save
# ---------------------------------------------------------------------------


class TestExecuteSave:
    def test_save_writes_stdin_to_file(self, tmp_path, monkeypatch):
        """execute_save should read from sys.stdin.buffer and write to the file."""
        import io

        f = tmp_path / "out.txt"
        cmd = _make_cmd("gfal-save")
        cmd.params = _default_params(file=f.as_uri())

        # Inject mock stdin
        fake_stdin = io.BytesIO(b"hello save")
        monkeypatch.setattr(
            "sys.stdin", type("FakeStdin", (), {"buffer": fake_stdin})()
        )

        rc = cmd.execute_save()
        assert rc is None or rc == 0
        assert f.read_bytes() == b"hello save"


# ---------------------------------------------------------------------------
# Additional stat tests
# ---------------------------------------------------------------------------


class TestStatMixedExistingNonexistent:
    def test_mixed_files_returns_nonzero(self, tmp_path, capsys, monkeypatch):
        monkeypatch.setenv("GFAL_CLI_GFAL2", "1")
        a = tmp_path / "a.txt"
        a.write_text("x")
        cmd = _make_cmd("gfal-stat")
        cmd.params = _default_params(file=[a.as_uri(), (tmp_path / "no_such").as_uri()])
        rc = cmd.execute_stat()
        # a.txt stats ok, no_such fails → rc != 0
        assert rc != 0
        captured = capsys.readouterr()
        assert "a.txt" in captured.out


# ---------------------------------------------------------------------------
# chmod error on nonexistent file
# ---------------------------------------------------------------------------


@pytest.mark.skipif(sys.platform == "win32", reason="chmod semantics differ on Windows")
class TestChmodErrors:
    def test_chmod_nonexistent_returns_error(self, tmp_path):
        cmd = _make_cmd("gfal-chmod")
        cmd.params = _default_params(
            mode="644",
            file=[(tmp_path / "no_such").as_uri()],
        )
        rc = cmd.execute_chmod()
        assert rc != 0
