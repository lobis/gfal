"""Tests for shell tab-completion support."""

import subprocess
import sys
from pathlib import Path
from typing import Optional

import pytest

from helpers import _subprocess_env


def _run_completion(
    argv_suffix: list,
    env_extra: Optional[dict] = None,
):
    """Run ``gfal ...`` in a subprocess and return (returncode, stdout, stderr)."""
    script = (
        "import sys; sys.argv=['gfal'] + sys.argv[1:];"
        "from gfal.cli.shell import main; main()"
    )
    env = _subprocess_env()
    if env_extra:
        env = {**env, **env_extra}
    proc = subprocess.run(
        [sys.executable, "-c", script, *argv_suffix],
        capture_output=True,
        text=True,
        encoding="utf-8",
        env=env,
    )
    return proc.returncode, proc.stdout, proc.stderr


class TestCompletionCommand:
    """``gfal completion`` subcommand prints shell-specific setup lines."""

    def test_bash(self):
        rc, out, _ = _run_completion(["completion", "bash"])
        assert rc == 0
        assert "_GFAL_COMPLETE=bash_source gfal" in out

    def test_zsh(self):
        rc, out, _ = _run_completion(["completion", "zsh"])
        assert rc == 0
        assert "_GFAL_COMPLETE=zsh_source gfal" in out

    def test_fish(self):
        rc, out, _ = _run_completion(["completion", "fish"])
        assert rc == 0
        assert "_GFAL_COMPLETE=fish_source gfal" in out

    def test_autodetect_bash(self):
        rc, out, _ = _run_completion(["completion"], env_extra={"SHELL": "/bin/bash"})
        assert rc == 0
        assert "_GFAL_COMPLETE=bash_source gfal" in out

    def test_autodetect_zsh(self):
        rc, out, _ = _run_completion(
            ["completion"], env_extra={"SHELL": "/usr/bin/zsh"}
        )
        assert rc == 0
        assert "_GFAL_COMPLETE=zsh_source gfal" in out

    def test_autodetect_fish(self):
        rc, out, _ = _run_completion(
            ["completion"], env_extra={"SHELL": "/usr/bin/fish"}
        )
        assert rc == 0
        assert "_GFAL_COMPLETE=fish_source gfal" in out

    def test_unsupported_shell_exits_nonzero(self):
        rc, out, err = _run_completion(["completion", "powershell"])
        assert rc != 0
        assert "unsupported shell" in err or "unsupported shell" in out

    def test_no_shell_no_env_exits_nonzero(self):
        rc, _, _ = _run_completion(["completion"], env_extra={"SHELL": ""})
        assert rc != 0


class TestClickShellCompletion:
    """Click's ``_GFAL_COMPLETE`` env-var mechanism returns completion data."""

    @staticmethod
    def _require_usable_bash():
        """Skip if ``bash`` is missing or unusable on this platform."""
        try:
            version_check = subprocess.run(
                ["bash", "--version"],
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                env=_subprocess_env(),
            )
        except FileNotFoundError:
            pytest.skip("bash not installed")

        if version_check.returncode != 0:
            pytest.skip("bash is not usable in this environment")

    def test_bash_source_script_is_generated(self):
        """``_GFAL_COMPLETE=bash_source gfal`` emits a bash completion function."""
        rc, out, _ = _run_completion([], env_extra={"_GFAL_COMPLETE": "bash_source"})
        assert rc == 0
        assert "_gfal_completion" in out
        assert "complete" in out

    def test_zsh_source_script_is_generated(self):
        """``_GFAL_COMPLETE=zsh_source gfal`` emits a zsh completion function."""
        rc, out, _ = _run_completion([], env_extra={"_GFAL_COMPLETE": "zsh_source"})
        assert rc == 0
        assert "autoload -Uz compinit" in out
        assert "compinit" in out
        assert "_gfal_completion" in out

    def test_zsh_wrapper_bootstraps_compinit(self):
        """The generated zsh setup works even in a fresh shell without compinit."""
        try:
            version_check = subprocess.run(
                ["zsh", "--version"],
                capture_output=True,
                text=True,
                encoding="utf-8",
                env=_subprocess_env(),
            )
        except FileNotFoundError:
            pytest.skip("zsh not installed")

        if version_check.returncode != 0:
            pytest.skip("zsh --version failed")

        gfal_dir = str(Path(sys.executable).parent)
        zsh_script = """
            export PATH="$1:$PATH"
            eval "$(_GFAL_COMPLETE=zsh_source gfal)"
            whence compdef >/dev/null 2>&1
            whence _gfal_completion >/dev/null 2>&1
        """
        proc = subprocess.run(
            ["zsh", "-f", "-c", zsh_script, "zsh", gfal_dir],
            capture_output=True,
            text=True,
            encoding="utf-8",
            env=_subprocess_env(),
        )
        assert proc.returncode == 0, proc.stderr

    def test_bash_completes_subcommands(self):
        """Typing ``gfal <TAB>`` offers the known subcommands."""
        rc, out, _ = _run_completion(
            [""],
            env_extra={
                "_GFAL_COMPLETE": "bash_complete",
                "COMP_WORDS": "gfal ",
                "COMP_CWORD": "1",
            },
        )
        assert rc == 0
        assert "ls" in out
        assert "cp" in out
        assert "rm" in out
        assert "completion" in out

    def test_bash_completes_subcommands_without_trailing_space(self):
        """Typing ``gfal<TAB>`` still offers subcommands instead of files."""
        self._require_usable_bash()
        gfal_dir = str(Path(sys.executable).parent)
        bash_script = """
            export PATH="$1:$PATH"
            eval "$(_GFAL_COMPLETE=bash_source gfal)"
            COMP_WORDS=(gfal)
            COMP_CWORD=0
            COMPREPLY=()
            _gfal_completion gfal
            printf '%s\n' "${COMPREPLY[@]}"
        """
        proc = subprocess.run(
            ["bash", "-lc", bash_script, "bash", gfal_dir],
            capture_output=True,
            text=True,
            encoding="utf-8",
            env=_subprocess_env(),
        )
        assert proc.returncode == 0, proc.stderr
        assert "ls" in proc.stdout
        assert "cp" in proc.stdout
        assert "completion" in proc.stdout

    def test_bash_completes_partial_subcommand(self):
        """Typing ``gfal l<TAB>`` narrows to commands starting with 'l'."""
        rc, out, _ = _run_completion(
            ["l"],
            env_extra={
                "_GFAL_COMPLETE": "bash_complete",
                "COMP_WORDS": "gfal l",
                "COMP_CWORD": "1",
            },
        )
        assert rc == 0
        assert "ls" in out

    def test_bash_completes_subcommand_options(self):
        """Typing ``gfal ls --<TAB>`` offers ls-specific flags."""
        rc, out, _ = _run_completion(
            ["ls", "--"],
            env_extra={
                "_GFAL_COMPLETE": "bash_complete",
                "COMP_WORDS": "gfal ls --",
                "COMP_CWORD": "2",
            },
        )
        assert rc == 0
        assert "--long" in out
        assert "--all" in out
        assert "--help" in out
