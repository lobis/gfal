"""Direct unit tests for tape/token commands (src/gfal/cli/tape.py).

These tests call execute_* methods directly (no subprocess) to improve coverage
of the lines that currently aren't reached (the return 1 lines).
"""

from types import SimpleNamespace
from unittest.mock import patch

from gfal.cli.tape import CommandTape


def _make_cmd(prog):
    cmd = CommandTape()
    cmd.prog = prog
    return cmd


def _default_params(**kwargs):
    defaults = {
        "cert": None,
        "key": None,
        "timeout": 1800,
        "ssl_verify": True,
        "verbose": 0,
        "log_file": None,
    }
    defaults.update(kwargs)
    return SimpleNamespace(**defaults)


class TestExecuteBringonline:
    def test_returns_one(self, tmp_path):
        f = tmp_path / "file.txt"
        f.write_text("x")
        cmd = _make_cmd("gfal-bringonline")
        cmd.params = _default_params(
            surl=f.as_uri(),
            pin_lifetime=None,
            desired_request_time=None,
            staging_metadata=None,
            polling_timeout=None,
            from_file=None,
        )
        rc = cmd.execute_bringonline()
        assert rc == 1

    def test_writes_not_supported_to_stderr(self, tmp_path, capsys):
        f = tmp_path / "file.txt"
        f.write_text("x")
        cmd = _make_cmd("gfal-bringonline")
        cmd.params = _default_params(
            surl=f.as_uri(),
            pin_lifetime=None,
            desired_request_time=None,
            staging_metadata=None,
            polling_timeout=None,
            from_file=None,
        )
        cmd.execute_bringonline()
        captured = capsys.readouterr()
        assert (
            "not supported" in captured.err.lower() or "gfal2" in captured.err.lower()
        )


class TestExecuteArchivepoll:
    def test_returns_one(self, tmp_path):
        f = tmp_path / "file.txt"
        f.write_text("x")
        cmd = _make_cmd("gfal-archivepoll")
        cmd.params = _default_params(
            surl=f.as_uri(),
            polling_timeout=None,
            from_file=None,
        )
        rc = cmd.execute_archivepoll()
        assert rc == 1

    def test_writes_not_supported_to_stderr(self, tmp_path, capsys):
        f = tmp_path / "file.txt"
        f.write_text("x")
        cmd = _make_cmd("gfal-archivepoll")
        cmd.params = _default_params(
            surl=f.as_uri(),
            polling_timeout=None,
            from_file=None,
        )
        cmd.execute_archivepoll()
        captured = capsys.readouterr()
        assert (
            "not supported" in captured.err.lower() or "gfal2" in captured.err.lower()
        )


class TestExecuteEvict:
    def test_returns_one(self, tmp_path):
        f = tmp_path / "file.txt"
        f.write_text("x")
        cmd = _make_cmd("gfal-evict")
        cmd.params = _default_params(
            file=f.as_uri(),
            token=None,
        )
        rc = cmd.execute_evict()
        assert rc == 1

    def test_writes_not_supported_to_stderr(self, tmp_path, capsys):
        f = tmp_path / "file.txt"
        f.write_text("x")
        cmd = _make_cmd("gfal-evict")
        cmd.params = _default_params(
            file=f.as_uri(),
            token=None,
        )
        cmd.execute_evict()
        captured = capsys.readouterr()
        assert (
            "not supported" in captured.err.lower() or "gfal2" in captured.err.lower()
        )


class TestExecuteToken:
    def test_unsupported_non_eos_path_returns_one(self, tmp_path):
        f = tmp_path / "file.txt"
        f.write_text("x")
        cmd = _make_cmd("gfal-token")
        cmd.params = _default_params(
            path=f.as_uri(),
            write=False,
            validity=720,
            ssh_host=None,
            eos_instance="root://eospilot.cern.ch",
            tree=False,
            no_tree=False,
            issuer=None,
            activities=[],
        )
        rc = cmd.execute_token()
        assert rc == 1

    def test_unsupported_non_eos_path_reports_clear_error(self, tmp_path, capsys):
        f = tmp_path / "file.txt"
        f.write_text("x")
        cmd = _make_cmd("gfal-token")
        cmd.params = _default_params(
            path=f.as_uri(),
            write=False,
            validity=720,
            ssh_host=None,
            eos_instance="root://eospilot.cern.ch",
            tree=False,
            no_tree=False,
            issuer=None,
            activities=[],
        )
        cmd.execute_token()
        captured = capsys.readouterr()
        assert "eos token path" in captured.err.lower() or (
            "unsupported eos token path" in captured.err.lower()
        )

    def test_generates_write_tree_token_over_ssh(self, capsys):
        cmd = _make_cmd("gfal-token")
        cmd.params = _default_params(
            path="root://eospilot.cern.ch//eos/pilot/test/lobisapa/iaxo/",
            write=True,
            validity=720,
            ssh_host="eospilot",
            eos_instance="root://eospilot.cern.ch",
            tree=True,
            no_tree=False,
            issuer=None,
            activities=[],
        )

        with (
            patch("gfal.cli.tape.time.time", return_value=1_700_000_000),
            patch("gfal.cli.tape.subprocess.run") as mock_run,
        ):
            mock_run.return_value = SimpleNamespace(
                returncode=0,
                stdout="zteos64:TOKEN\n",
                stderr="",
            )
            rc = cmd.execute_token()

        assert rc == 0
        assert capsys.readouterr().out == "zteos64:TOKEN\n"
        mock_run.assert_called_once_with(
            [
                "ssh",
                "eospilot",
                "eos",
                "root://eospilot.cern.ch",
                "token",
                "--path",
                "/eos/pilot/test/lobisapa/iaxo/",
                "--permission",
                "rwx",
                "--expires",
                str(1_700_000_000 + 720 * 60),
                "--tree",
            ],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            check=False,
        )

    def test_default_read_token_uses_rx_and_auto_tree_for_directory(self):
        cmd = _make_cmd("gfal-token")
        cmd.params = _default_params(
            path="/eos/pilot/test/lobisapa/iaxo/",
            write=False,
            validity=60,
            ssh_host=None,
            eos_instance="root://eospilot.cern.ch",
            tree=False,
            no_tree=False,
            issuer=None,
            activities=[],
        )

        with (
            patch("gfal.cli.tape.time.time", return_value=10),
            patch("gfal.cli.tape.subprocess.run") as mock_run,
        ):
            mock_run.return_value = SimpleNamespace(
                returncode=0,
                stdout="zteos64:READ\n",
                stderr="",
            )
            rc = cmd.execute_token()

        assert rc == 0
        argv = mock_run.call_args.args[0]
        assert argv[1] == "eospilot"
        assert argv[argv.index("--permission") + 1] == "rx"
        assert str(10 + 60 * 60) == argv[argv.index("--expires") + 1]
        assert "--tree" in argv

    def test_no_tree_suppresses_directory_tree_flag(self):
        cmd = _make_cmd("gfal-token")
        cmd.params = _default_params(
            path="/eos/pilot/test/lobisapa/iaxo/",
            write=False,
            validity=60,
            ssh_host="eospilot",
            eos_instance="root://eospilot.cern.ch",
            tree=False,
            no_tree=True,
            issuer=None,
            activities=[],
        )

        with patch("gfal.cli.tape.subprocess.run") as mock_run:
            mock_run.return_value = SimpleNamespace(
                returncode=0,
                stdout="zteos64:READ\n",
                stderr="",
            )
            rc = cmd.execute_token()

        assert rc == 0
        assert "--tree" not in mock_run.call_args.args[0]

    def test_stdout_contains_only_first_token_line(self, capsys):
        cmd = _make_cmd("gfal-token")
        cmd.params = _default_params(
            path="/eos/pilot/test/lobisapa/iaxo/",
            write=True,
            validity=720,
            ssh_host="eospilot",
            eos_instance="root://eospilot.cern.ch",
            tree=True,
            no_tree=False,
            issuer=None,
            activities=[],
        )

        with patch("gfal.cli.tape.subprocess.run") as mock_run:
            mock_run.return_value = SimpleNamespace(
                returncode=0,
                stdout="zteos64:PRIVATE\nextra noise\n",
                stderr="",
            )
            rc = cmd.execute_token()

        assert rc == 0
        assert capsys.readouterr().out == "zteos64:PRIVATE\n"
