"""Direct unit tests for CommandRm execute_rm (src/gfal/cli/rm.py).

These tests instantiate CommandRm and set self.params directly (as a
SimpleNamespace) to avoid subprocess overhead and ensure coverage is collected
in the pytest process.
"""

import errno
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from gfal.cli.rm import CommandRm


def _make_cmd():
    cmd = CommandRm()
    cmd.prog = "gfal-rm"
    return cmd


def _default_params(**kwargs):
    defaults = {
        "cert": None,
        "key": None,
        "timeout": 1800,
        "ssl_verify": True,
        "verbose": 0,
        "log_file": None,
        "recursive": False,
        "dry_run": False,
        "just_delete": False,
        "from_file": None,
        "bulk": False,
        "ipv4_only": False,
        "ipv6_only": False,
    }
    defaults.update(kwargs)
    return SimpleNamespace(**defaults)


# ---------------------------------------------------------------------------
# execute_rm
# ---------------------------------------------------------------------------


class TestExecuteRm:
    def test_rm_file(self, tmp_path, capsys):
        f = tmp_path / "file.txt"
        f.write_text("x")
        cmd = _make_cmd()
        cmd.params = _default_params(file=[f.as_uri()])
        rc = cmd.execute_rm()
        assert rc == 0
        assert not f.exists()
        assert "DELETED" in capsys.readouterr().out

    def test_rm_nonexistent_sets_enoent(self, tmp_path, capsys):
        cmd = _make_cmd()
        cmd.params = _default_params(file=[(tmp_path / "no_such").as_uri()])
        rc = cmd.execute_rm()
        assert rc == errno.ENOENT
        assert "MISSING" in capsys.readouterr().out

    def test_rm_directory_without_recursive_fails(self, tmp_path, capsys):
        d = tmp_path / "dir"
        d.mkdir()
        cmd = _make_cmd()
        cmd.params = _default_params(file=[d.as_uri()], recursive=False)
        rc = cmd.execute_rm()
        assert rc != 0

    def test_rm_directory_recursive(self, tmp_path, capsys):
        d = tmp_path / "mydir"
        d.mkdir()
        (d / "f1.txt").write_text("a")
        (d / "f2.txt").write_text("b")
        cmd = _make_cmd()
        cmd.params = _default_params(file=[d.as_uri()], recursive=True)
        rc = cmd.execute_rm()
        assert rc == 0
        assert not d.exists()

    def test_rm_dry_run_skips_deletion(self, tmp_path, capsys):
        f = tmp_path / "file.txt"
        f.write_text("x")
        cmd = _make_cmd()
        cmd.params = _default_params(file=[f.as_uri()], dry_run=True)
        rc = cmd.execute_rm()
        assert rc == 0
        assert f.exists()  # file should not be deleted
        assert "SKIP" in capsys.readouterr().out

    def test_rm_just_delete_skips_stat(self, tmp_path, capsys):
        f = tmp_path / "file.txt"
        f.write_text("x")
        cmd = _make_cmd()
        cmd.params = _default_params(file=[f.as_uri()], just_delete=True)
        rc = cmd.execute_rm()
        assert rc == 0
        assert not f.exists()

    def test_rm_no_uri_returns_einval(self, tmp_path, capsys):
        cmd = _make_cmd()
        cmd.params = _default_params(file=[])
        rc = cmd.execute_rm()
        assert rc == errno.EINVAL

    def test_rm_from_file_and_positional_returns_einval(self, tmp_path):
        f = tmp_path / "file.txt"
        f.write_text("x")
        list_file = tmp_path / "list.txt"
        list_file.write_text(f.as_uri() + "\n")
        cmd = _make_cmd()
        cmd.params = _default_params(
            file=[f.as_uri()],
            from_file=str(list_file),
        )
        rc = cmd.execute_rm()
        assert rc == errno.EINVAL

    def test_rm_from_file(self, tmp_path, capsys):
        f1 = tmp_path / "f1.txt"
        f2 = tmp_path / "f2.txt"
        f1.write_text("a")
        f2.write_text("b")
        list_file = tmp_path / "list.txt"
        list_file.write_text(f1.as_uri() + "\n" + f2.as_uri() + "\n")
        cmd = _make_cmd()
        cmd.params = _default_params(file=[], from_file=str(list_file))
        rc = cmd.execute_rm()
        assert rc == 0
        assert not f1.exists()
        assert not f2.exists()

    def test_rm_multiple_files(self, tmp_path, capsys):
        f1 = tmp_path / "f1.txt"
        f2 = tmp_path / "f2.txt"
        f1.write_text("a")
        f2.write_text("b")
        cmd = _make_cmd()
        cmd.params = _default_params(file=[f1.as_uri(), f2.as_uri()])
        rc = cmd.execute_rm()
        assert rc == 0
        assert not f1.exists()
        assert not f2.exists()

    def test_rm_passes_ipv4_only_to_client(self):
        cmd = _make_cmd()
        cmd.params = _default_params(file=["file:///tmp/test.txt"], ipv4_only=True)

        with patch("gfal.cli.rm.GfalClient") as mock_client_cls:
            mock_client_cls.return_value.stat.side_effect = errno.ENOENT
            cmd._do_rm = lambda *args, **kwargs: None
            rc = cmd.execute_rm()

        assert rc == 0
        assert mock_client_cls.call_args.kwargs["ipv4_only"] is True
        assert mock_client_cls.call_args.kwargs["ipv6_only"] is False


# ---------------------------------------------------------------------------
# _do_rmdir
# ---------------------------------------------------------------------------


class TestDoRmdir:
    def test_rmdir_empty_directory(self, tmp_path, capsys):
        d = tmp_path / "empty"
        d.mkdir()
        from gfal.core.api import GfalClient

        client = GfalClient()
        cmd = _make_cmd()
        cmd.params = _default_params(recursive=True, dry_run=False)
        cmd._do_rmdir(d.as_uri(), client)
        assert not d.exists()

    def test_rmdir_not_recursive_raises(self, tmp_path):
        d = tmp_path / "dir"
        d.mkdir()
        from gfal.core.api import GfalClient

        client = GfalClient()
        cmd = _make_cmd()
        cmd.params = _default_params(recursive=False, dry_run=False)
        with pytest.raises(IsADirectoryError):
            cmd._do_rmdir(d.as_uri(), client)

    def test_rmdir_dry_run(self, tmp_path, capsys):
        d = tmp_path / "dir"
        d.mkdir()
        (d / "file.txt").write_text("x")
        from gfal.core.api import GfalClient

        client = GfalClient()
        cmd = _make_cmd()
        cmd.params = _default_params(recursive=True, dry_run=True)
        cmd._do_rmdir(d.as_uri(), client)
        out = capsys.readouterr().out
        assert "SKIP" in out
        assert d.exists()

    def test_rmdir_nested(self, tmp_path, capsys):
        d = tmp_path / "dir"
        d.mkdir()
        sub = d / "sub"
        sub.mkdir()
        (sub / "f.txt").write_text("x")
        from gfal.core.api import GfalClient

        client = GfalClient()
        cmd = _make_cmd()
        cmd.params = _default_params(recursive=True, dry_run=False)
        cmd._do_rmdir(d.as_uri(), client)
        assert not d.exists()


# ---------------------------------------------------------------------------
# _set_error
# ---------------------------------------------------------------------------


class TestSetError:
    def test_set_error_first_time(self):
        cmd = _make_cmd()
        cmd._set_error(5)
        assert cmd.return_code == 5

    def test_set_error_not_overwritten(self):
        cmd = _make_cmd()
        cmd._set_error(5)
        cmd._set_error(10)
        # First error wins
        assert cmd.return_code == 5


# ---------------------------------------------------------------------------
# Additional error path tests
# ---------------------------------------------------------------------------


class TestDoRmErrors:
    def test_do_rm_unexpected_exception_sets_error(self, tmp_path, capsys):
        """When rm raises an unexpected exception, it should set the return code and print FAILED."""
        from unittest.mock import patch

        cmd = _make_cmd()
        cmd.params = _default_params(
            just_delete=False,
            dry_run=False,
        )

        # Mock client.stat to succeed (return a file), and client.rm to fail
        f = tmp_path / "file.txt"
        f.write_text("x")
        from gfal.core.api import GfalClient

        client = GfalClient()

        with patch.object(client, "rm", side_effect=OSError("random error")):
            # Also mock stat to return a file (not directory)
            import stat as stat_mod

            from gfal.core.fs import StatInfo

            fake_stat = StatInfo({
                "name": str(f),
                "size": 1,
                "type": "file",
                "mode": stat_mod.S_IFREG | 0o644,
            })
            with patch.object(client, "stat", return_value=fake_stat):
                cmd._do_rm(f.as_uri(), client)

        out = capsys.readouterr().out
        assert "FAILED" in out
        assert cmd.return_code != 0

    def test_do_rmdir_listing_exception_continues(self, tmp_path, capsys):
        """If listing fails during rmdir, it should continue and try to remove the dir."""
        from unittest.mock import patch

        d = tmp_path / "dir"
        d.mkdir()
        from gfal.core.api import GfalClient

        client = GfalClient()

        cmd = _make_cmd()
        cmd.params = _default_params(recursive=True, dry_run=False)

        # Mock ls to raise, but rmdir should still be called
        with patch.object(client, "ls", side_effect=Exception("ls failed")):
            cmd._do_rmdir(d.as_uri(), client)
        # The directory itself should be removed (since ls failed, entries=[], then rmdir)
        assert not d.exists()
