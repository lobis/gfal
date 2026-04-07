"""Tests for shell.py: dispatch and error handling."""

import pytest

from gfal.cli.shell import _find_command

# ---------------------------------------------------------------------------
# _find_command
# ---------------------------------------------------------------------------


class TestFindCommand:
    def test_known_command(self):
        cls, method = _find_command("ls")
        assert method.__name__ == "execute_ls"

    def test_cp_command(self):
        cls, method = _find_command("cp")
        assert method.__name__ == "execute_cp"

    def test_unknown_raises(self):
        with pytest.raises(ValueError, match="Unknown command"):
            _find_command("nonexistent_command")


# ---------------------------------------------------------------------------
# main() via subprocess
# ---------------------------------------------------------------------------


class TestMainEntrypoint:
    def test_version_flag(self):
        from helpers import run_gfal

        # --version exits with 0 and prints version info
        rc, out, err = run_gfal("ls", "--version")
        assert rc == 0
        assert "gfal" in out or "gfal" in err

    def test_unknown_command(self):
        import subprocess
        import sys

        from helpers import _subprocess_env

        script = (
            "import sys; sys.argv=['gfal', 'unknown_cmd_xyz'];"
            "from gfal.cli.shell import main; main()"
        )
        proc = subprocess.run(
            [sys.executable, "-c", script],
            capture_output=True,
            text=True,
            encoding="utf-8",
            env=_subprocess_env(),
        )
        assert proc.returncode != 0
        assert "Unknown command" in proc.stderr

    def test_hyphenated_entrypoint_is_rejected(self):
        import subprocess
        import sys

        from helpers import _subprocess_env

        script = (
            "import sys; sys.argv=['gfal-ls', '/tmp'];"
            "from gfal.cli.shell import main; main()"
        )
        proc = subprocess.run(
            [sys.executable, "-c", script],
            capture_output=True,
            text=True,
            encoding="utf-8",
            env=_subprocess_env(),
        )
        assert proc.returncode != 0
        assert "gfal <command>" in proc.stderr


# ---------------------------------------------------------------------------
# gfal parent command
# ---------------------------------------------------------------------------


class TestGfalParentCommand:
    def test_gfal_version_subcommand(self):
        """``gfal version`` prints the package version and exits 0."""

        # Reuse run_gfal but override the program name to "gfal" with "version"
        import subprocess
        import sys

        from helpers import _subprocess_env

        script = (
            "import sys; sys.argv=['gfal', 'version'];"
            "from gfal.cli.shell import main; main()"
        )
        proc = subprocess.run(
            [sys.executable, "-c", script],
            capture_output=True,
            text=True,
            encoding="utf-8",
            env=_subprocess_env(),
        )
        assert proc.returncode == 0
        assert "gfal" in proc.stdout.lower() or "gfal" in proc.stderr.lower()

    def test_gfal_help_subcommand(self):
        """``gfal help`` exits 0 and lists available commands."""
        import subprocess
        import sys

        from helpers import _subprocess_env

        script = (
            "import sys; sys.argv=['gfal', 'help'];"
            "from gfal.cli.shell import main; main()"
        )
        proc = subprocess.run(
            [sys.executable, "-c", script],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            env=_subprocess_env(),
        )
        assert proc.returncode == 0
        combined = proc.stdout + proc.stderr
        # Help output should mention at least one known subcommand
        assert "ls" in combined or "cp" in combined or "copy" in combined

    def test_gfal_dispatches_to_ls(self, tmp_path):
        """``gfal ls <url>`` dispatches correctly to the ls command."""
        import subprocess
        import sys

        from helpers import _subprocess_env

        (tmp_path / "hello.txt").write_text("hi")

        script = (
            f"import sys; sys.argv=['gfal', 'ls', '{tmp_path.as_uri()}'];"
            "from gfal.cli.shell import main; main()"
        )
        proc = subprocess.run(
            [sys.executable, "-c", script],
            capture_output=True,
            text=True,
            encoding="utf-8",
            env=_subprocess_env(),
        )
        assert proc.returncode == 0
        assert "hello.txt" in proc.stdout


# ---------------------------------------------------------------------------
# Common ignored args across all commands
# ---------------------------------------------------------------------------


class TestCommonIgnoredArgs:
    """Verify that -D, -C, -4, -6 are accepted by every command without crashing."""

    def test_definition_on_stat(self, tmp_path):
        from helpers import run_gfal

        f = tmp_path / "f.txt"
        f.write_text("x")
        rc, out, err = run_gfal("stat", "-D", "CORE:CHECKSUM_CHECK=0", f.as_uri())
        assert rc == 0

    def test_client_info_on_ls(self, tmp_path):
        from helpers import run_gfal

        f = tmp_path / "f.txt"
        f.write_text("x")
        rc, out, err = run_gfal("ls", "-C", "myapp/1.0", tmp_path.as_uri())
        assert rc == 0

    def test_ipv4_on_ls(self, tmp_path):
        from helpers import run_gfal

        f = tmp_path / "f.txt"
        f.write_text("x")
        rc, out, err = run_gfal("ls", "-4", tmp_path.as_uri())
        assert rc == 0

    def test_ipv6_on_ls(self, tmp_path):
        from helpers import run_gfal

        f = tmp_path / "f.txt"
        f.write_text("x")
        rc, out, err = run_gfal("ls", "-6", tmp_path.as_uri())
        assert rc == 0
