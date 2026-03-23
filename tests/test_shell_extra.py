"""Direct unit tests for shell.py (src/gfal/cli/shell.py).

Tests exercise _all_commands(), _print_gfal_help(), and main() directly
within the pytest process to increase coverage.
"""

import sys

import pytest

from gfal.cli.shell import _all_commands, _print_gfal_help

# ---------------------------------------------------------------------------
# _all_commands
# ---------------------------------------------------------------------------


class TestAllCommands:
    def test_returns_list_of_tuples(self):
        result = _all_commands()
        assert isinstance(result, list)
        assert all(isinstance(item, tuple) and len(item) == 2 for item in result)

    def test_contains_expected_commands(self):
        names = {name for name, _ in _all_commands()}
        for expected in (
            "ls",
            "cp",
            "stat",
            "mkdir",
            "rm",
            "cat",
            "save",
            "rename",
            "chmod",
            "sum",
            "xattr",
        ):
            assert expected in names, f"{expected!r} not in _all_commands()"

    def test_sorted(self):
        result = _all_commands()
        names = [name for name, _ in result]
        assert names == sorted(names)

    def test_docs_are_strings(self):
        for _name, doc in _all_commands():
            assert isinstance(doc, str)


# ---------------------------------------------------------------------------
# _print_gfal_help
# ---------------------------------------------------------------------------


class TestPrintGfalHelp:
    def test_prints_to_stdout(self, capsys):
        _print_gfal_help(to=sys.stdout)
        captured = capsys.readouterr()
        # Should mention at least one subcommand
        combined = captured.out + captured.err
        assert "ls" in combined or "cp" in combined or "gfal" in combined.lower()

    def test_prints_to_stderr(self, capsys):
        _print_gfal_help(to=sys.stderr)
        captured = capsys.readouterr()
        combined = captured.out + captured.err
        assert len(combined) > 0


# ---------------------------------------------------------------------------
# main() — in-process, catch SystemExit
# ---------------------------------------------------------------------------


class TestMain:
    def _run_main(self, *argv):
        """Invoke main() in-process and return the SystemExit code."""
        from gfal.cli.shell import main

        with pytest.raises(SystemExit) as exc_info:
            main(list(argv))
        return exc_info.value.code

    def test_gfal_no_args_exits_zero(self):
        rc = self._run_main("gfal")
        assert rc == 0

    def test_gfal_help_exits_zero(self):
        rc = self._run_main("gfal", "--help")
        assert rc == 0

    def test_gfal_dash_h_exits_zero(self):
        rc = self._run_main("gfal", "-h")
        assert rc == 0

    def test_gfal_version_exits_zero(self, capsys):
        rc = self._run_main("gfal", "--version")
        assert rc == 0
        captured = capsys.readouterr()
        assert "gfal" in captured.out.lower()

    def test_gfal_dash_v_version(self, capsys):
        rc = self._run_main("gfal", "-V")
        assert rc == 0
        captured = capsys.readouterr()
        assert "gfal" in captured.out.lower()

    def test_gfal_version_subcommand(self, capsys):
        rc = self._run_main("gfal", "version")
        assert rc == 0
        captured = capsys.readouterr()
        assert "gfal" in captured.out.lower()

    def test_gfal_help_subcommand(self, capsys):
        rc = self._run_main("gfal", "help")
        assert rc == 0

    def test_gfal_unknown_flag_exits_one(self, capsys):
        rc = self._run_main("gfal", "--unknown-flag-xyz")
        assert rc == 1

    def test_gfal_ls_dispatches_ok(self, tmp_path):
        (tmp_path / "hello.txt").write_text("hi")
        rc = self._run_main("gfal", "ls", tmp_path.as_uri())
        assert rc == 0

    def test_gfal_hyphenated_ls_dispatches_ok(self, tmp_path):
        (tmp_path / "hello.txt").write_text("hi")
        rc = self._run_main("gfal-ls", tmp_path.as_uri())
        assert rc == 0

    def test_unknown_command_exits_nonzero(self, capsys):
        rc = self._run_main("gfal-unknown_cmd_xyz_abc")
        assert rc == 1
        captured = capsys.readouterr()
        assert "Unknown command" in captured.err

    def test_gfal_stat_dispatches_ok(self, tmp_path):
        f = tmp_path / "test.txt"
        f.write_text("hello")
        rc = self._run_main("gfal", "stat", f.as_uri())
        assert rc == 0

    def test_gfal_cp_dispatches_ok(self, tmp_path):
        src = tmp_path / "src.txt"
        dst = tmp_path / "dst.txt"
        src.write_bytes(b"hello")
        rc = self._run_main("gfal", "cp", src.as_uri(), dst.as_uri())
        assert rc == 0
        assert dst.read_bytes() == b"hello"

    def test_gfal_mkdir_dispatches_ok(self, tmp_path):
        d = tmp_path / "newdir"
        rc = self._run_main("gfal", "mkdir", d.as_uri())
        assert rc == 0
        assert d.is_dir()

    def test_gfal_rm_dispatches_ok(self, tmp_path):
        f = tmp_path / "file.txt"
        f.write_text("x")
        rc = self._run_main("gfal", "rm", f.as_uri())
        assert rc == 0
        assert not f.exists()

    def test_help_flag_on_subcommand(self, capsys):
        rc = self._run_main("gfal-ls", "--help")
        assert rc == 0
        captured = capsys.readouterr()
        combined = captured.out + captured.err
        assert "ls" in combined.lower() or "list" in combined.lower()
