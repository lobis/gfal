"""Direct unit tests for tape stub commands (src/gfal/cli/tape.py).

These tests call execute_* methods directly (no subprocess) to improve coverage
of the lines that currently aren't reached (the return 1 lines).
"""

from types import SimpleNamespace

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
    def test_returns_one(self, tmp_path):
        f = tmp_path / "file.txt"
        f.write_text("x")
        cmd = _make_cmd("gfal-token")
        cmd.params = _default_params(
            path=f.as_uri(),
            write=False,
            validity=None,
            issuer=None,
            activities=[],
        )
        rc = cmd.execute_token()
        assert rc == 1

    def test_writes_not_supported_to_stderr(self, tmp_path, capsys):
        f = tmp_path / "file.txt"
        f.write_text("x")
        cmd = _make_cmd("gfal-token")
        cmd.params = _default_params(
            path=f.as_uri(),
            write=False,
            validity=None,
            issuer=None,
            activities=[],
        )
        cmd.execute_token()
        captured = capsys.readouterr()
        assert (
            "not supported" in captured.err.lower() or "gfal2" in captured.err.lower()
        )
