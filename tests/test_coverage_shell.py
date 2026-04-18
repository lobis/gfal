"""Tests targeting uncovered lines in src/gfal/cli/shell.py.

Covers:
- _ensure_xrootd_dylib_path() — macOS dylib re-exec logic (mocked)
- _print_gfal_help() — ImportError branch when rich_click unavailable
- _build_completion_group() — building Click Group for shell completion
"""

import builtins
import sys
import types

import click

# ---------------------------------------------------------------------------
# _ensure_xrootd_dylib_path
# ---------------------------------------------------------------------------


class TestEnsureXrootdDylibPath:
    """Exercise every branch in _ensure_xrootd_dylib_path()."""

    def test_not_darwin_returns_immediately(self, monkeypatch):
        """Non-darwin platform → early return, no pyxrootd import attempted."""
        monkeypatch.setattr(sys, "platform", "linux")
        from gfal.cli.shell import _ensure_xrootd_dylib_path

        # Should return without side effects
        _ensure_xrootd_dylib_path()

    def test_darwin_pyxrootd_import_error(self, monkeypatch):
        """darwin + pyxrootd not installed → early return."""
        monkeypatch.setattr(sys, "platform", "darwin")

        real_import = builtins.__import__

        def _fake_import(name, *args, **kwargs):
            if name == "pyxrootd":
                raise ImportError("No module named 'pyxrootd'")
            return real_import(name, *args, **kwargs)

        monkeypatch.setattr(builtins, "__import__", _fake_import)
        from gfal.cli.shell import _ensure_xrootd_dylib_path

        _ensure_xrootd_dylib_path()

    def test_darwin_plugin_dir_already_in_path(self, monkeypatch):
        """darwin + pyxrootd present + plugin dir already in DYLD_LIBRARY_PATH → return."""
        monkeypatch.setattr(sys, "platform", "darwin")

        fake_pyxrootd = types.ModuleType("pyxrootd")
        fake_pyxrootd.__file__ = "/fake/lib/pyxrootd/__init__.py"
        monkeypatch.setitem(sys.modules, "pyxrootd", fake_pyxrootd)

        monkeypatch.setenv("DYLD_LIBRARY_PATH", "/fake/lib/pyxrootd:/other")

        from gfal.cli.shell import _ensure_xrootd_dylib_path

        _ensure_xrootd_dylib_path()

    def test_darwin_argv0_not_a_file(self, monkeypatch):
        """darwin + pyxrootd + plugin dir NOT in path + argv[0] is not a file → return."""
        monkeypatch.setattr(sys, "platform", "darwin")

        fake_pyxrootd = types.ModuleType("pyxrootd")
        fake_pyxrootd.__file__ = "/fake/lib/pyxrootd/__init__.py"
        monkeypatch.setitem(sys.modules, "pyxrootd", fake_pyxrootd)

        monkeypatch.delenv("DYLD_LIBRARY_PATH", raising=False)
        monkeypatch.setattr(sys, "argv", ["-c", "some_code"])

        from gfal.cli.shell import _ensure_xrootd_dylib_path

        _ensure_xrootd_dylib_path()

    def test_darwin_execve_called_with_empty_current(self, monkeypatch, tmp_path):
        """darwin + pyxrootd + not in path + argv[0] is a file → os.execve called."""
        monkeypatch.setattr(sys, "platform", "darwin")

        fake_pyxrootd = types.ModuleType("pyxrootd")
        fake_pyxrootd.__file__ = "/fake/lib/pyxrootd/__init__.py"
        monkeypatch.setitem(sys.modules, "pyxrootd", fake_pyxrootd)

        monkeypatch.delenv("DYLD_LIBRARY_PATH", raising=False)

        # argv[0] must be a real file
        fake_bin = tmp_path / "gfal"
        fake_bin.write_text("#!/bin/bash")
        monkeypatch.setattr(sys, "argv", [str(fake_bin), "ls"])

        execve_calls = []

        def mock_execve(executable, args, env):
            execve_calls.append((executable, args, env))

        monkeypatch.setattr("os.execve", mock_execve)

        from gfal.cli.shell import _ensure_xrootd_dylib_path

        _ensure_xrootd_dylib_path()

        assert len(execve_calls) == 1
        _, _, new_env = execve_calls[0]
        assert new_env["DYLD_LIBRARY_PATH"] == "/fake/lib/pyxrootd"

    def test_darwin_execve_called_with_existing_dyld(self, monkeypatch, tmp_path):
        """Existing DYLD_LIBRARY_PATH is preserved and prepended to."""
        monkeypatch.setattr(sys, "platform", "darwin")

        fake_pyxrootd = types.ModuleType("pyxrootd")
        fake_pyxrootd.__file__ = "/fake/lib/pyxrootd/__init__.py"
        monkeypatch.setitem(sys.modules, "pyxrootd", fake_pyxrootd)

        monkeypatch.setenv("DYLD_LIBRARY_PATH", "/some/other/path")

        fake_bin = tmp_path / "gfal"
        fake_bin.write_text("#!/bin/bash")
        monkeypatch.setattr(sys, "argv", [str(fake_bin), "ls"])

        execve_calls = []

        def mock_execve(executable, args, env):
            execve_calls.append((executable, args, env))

        monkeypatch.setattr("os.execve", mock_execve)

        from gfal.cli.shell import _ensure_xrootd_dylib_path

        _ensure_xrootd_dylib_path()

        assert len(execve_calls) == 1
        _, _, new_env = execve_calls[0]
        assert new_env["DYLD_LIBRARY_PATH"] == "/fake/lib/pyxrootd:/some/other/path"

    def test_darwin_execve_args_correct(self, monkeypatch, tmp_path):
        """os.execve receives sys.executable and [sys.executable] + sys.argv."""
        monkeypatch.setattr(sys, "platform", "darwin")

        fake_pyxrootd = types.ModuleType("pyxrootd")
        fake_pyxrootd.__file__ = "/fake/lib/pyxrootd/__init__.py"
        monkeypatch.setitem(sys.modules, "pyxrootd", fake_pyxrootd)

        monkeypatch.delenv("DYLD_LIBRARY_PATH", raising=False)

        fake_bin = tmp_path / "gfal"
        fake_bin.write_text("#!/bin/bash")
        fake_argv = [str(fake_bin), "stat", "/some/file"]
        monkeypatch.setattr(sys, "argv", fake_argv)

        execve_calls = []

        def mock_execve(executable, args, env):
            execve_calls.append((executable, args, env))

        monkeypatch.setattr("os.execve", mock_execve)

        from gfal.cli.shell import _ensure_xrootd_dylib_path

        _ensure_xrootd_dylib_path()

        executable, args, _ = execve_calls[0]
        assert executable == sys.executable
        assert args == [sys.executable] + fake_argv


# ---------------------------------------------------------------------------
# _print_gfal_help — ImportError branch (no rich_click)
# ---------------------------------------------------------------------------


class TestPrintGfalHelpNoRichClick:
    """Exercise the except ImportError branch in _print_gfal_help.

    Click's ``grp(["--help"], standalone_mode=True)`` writes help text directly
    to stdout.  The ``to`` parameter only receives the trailing newline.
    We therefore use ``capsys`` to capture the full output.
    """

    def test_falls_back_to_plain_click(self, monkeypatch, capsys):
        """When rich_click is not importable, plain click is used."""
        real_import = builtins.__import__

        def _block_rich_click(name, *args, **kwargs):
            if name == "rich_click":
                raise ImportError("No module named 'rich_click'")
            return real_import(name, *args, **kwargs)

        monkeypatch.setattr(builtins, "__import__", _block_rich_click)

        from gfal.cli import shell

        shell._print_gfal_help(to=sys.stdout)
        captured = capsys.readouterr()

        # Should still produce help output with known commands
        assert "ls" in captured.out
        assert "gfal" in captured.out.lower()

    def test_output_ends_with_newline(self, monkeypatch, capsys):
        """_print_gfal_help always appends a trailing newline via to.write."""
        real_import = builtins.__import__

        def _block_rich_click(name, *args, **kwargs):
            if name == "rich_click":
                raise ImportError("No module named 'rich_click'")
            return real_import(name, *args, **kwargs)

        monkeypatch.setattr(builtins, "__import__", _block_rich_click)

        from gfal.cli import shell

        shell._print_gfal_help(to=sys.stdout)
        captured = capsys.readouterr()
        assert captured.out.endswith("\n")

    def test_contains_epilog_text(self, monkeypatch, capsys):
        """Help output includes the epilog regardless of click variant."""
        real_import = builtins.__import__

        def _block_rich_click(name, *args, **kwargs):
            if name == "rich_click":
                raise ImportError("No module named 'rich_click'")
            return real_import(name, *args, **kwargs)

        monkeypatch.setattr(builtins, "__import__", _block_rich_click)

        from gfal.cli import shell

        shell._print_gfal_help(to=sys.stdout)
        captured = capsys.readouterr()
        assert "gfal <command> --help" in captured.out


# ---------------------------------------------------------------------------
# _build_completion_group
# ---------------------------------------------------------------------------


class TestBuildCompletionGroup:
    """Exercise _build_completion_group() and verify the returned Click Group."""

    def test_returns_click_group(self):
        from gfal.cli.shell import _build_completion_group

        grp = _build_completion_group()
        assert isinstance(grp, click.Group)

    def test_group_name_is_gfal(self):
        from gfal.cli.shell import _build_completion_group

        grp = _build_completion_group()
        assert grp.name == "gfal"

    def test_contains_expected_commands(self):
        from gfal.cli.shell import _build_completion_group

        grp = _build_completion_group()
        cmd_names = set(grp.list_commands(ctx=None))
        for expected in ("ls", "cp", "stat", "mkdir", "rm", "cat"):
            assert expected in cmd_names, f"{expected!r} missing from completion group"

    def test_commands_have_params(self):
        """Each command in the group should have Click params (flags/args)."""
        from gfal.cli.shell import _build_completion_group

        grp = _build_completion_group()
        # Check that 'ls' has some parameters (e.g. --long, --all)
        ls_cmd = grp.get_command(ctx=None, cmd_name="ls")
        assert ls_cmd is not None
        param_names = [p.name for p in ls_cmd.params]
        assert len(param_names) > 0

    def test_no_duplicate_commands(self):
        """_build_completion_group should de-duplicate via the 'seen' set."""
        from gfal.cli.shell import _build_completion_group

        grp = _build_completion_group()
        cmd_list = grp.list_commands(ctx=None)
        assert len(cmd_list) == len(set(cmd_list))

    def test_fallback_to_plain_click_group(self, monkeypatch):
        """When rich_click is unavailable, falls back to click.Group."""
        real_import = builtins.__import__

        def _block_rich_click(name, *args, **kwargs):
            if name == "rich_click":
                raise ImportError("No module named 'rich_click'")
            return real_import(name, *args, **kwargs)

        monkeypatch.setattr(builtins, "__import__", _block_rich_click)

        from gfal.cli.shell import _build_completion_group

        grp = _build_completion_group()
        assert isinstance(grp, click.Group)
        assert grp.name == "gfal"
        # Still has commands
        assert len(grp.list_commands(ctx=None)) > 0
