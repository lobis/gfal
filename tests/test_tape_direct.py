"""Direct unit tests for tape/token commands (src/gfal/cli/tape.py).

These tests call execute_* methods directly (no subprocess) to improve coverage
of command behavior without requiring real storage endpoints.
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
    def test_retrieves_token(self, tmp_path, monkeypatch, capsys):
        f = tmp_path / "file.txt"
        f.write_text("x")
        calls = []

        def fake_retrieve_token(*args, **kwargs):
            calls.append((args, kwargs))
            return "issued-token"

        monkeypatch.setattr("gfal.cli.tape.retrieve_token", fake_retrieve_token)
        cmd = _make_cmd("gfal-token")
        cmd.params = _default_params(
            path=f.as_uri(),
            write=False,
            validity=60,
            issuer=None,
            activities=[],
        )
        rc = cmd.execute_token()
        captured = capsys.readouterr()
        assert rc == 0
        assert captured.out == "issued-token\n"
        assert captured.err == ""
        assert calls[0][0] == (f.as_uri(),)
        assert calls[0][1]["issuer"] is None
        assert calls[0][1]["validity"] == 60
        assert calls[0][1]["write_access"] is False
        assert calls[0][1]["activities"] is None

    def test_forwards_write_and_custom_activities(self, tmp_path, monkeypatch):
        f = tmp_path / "file.txt"
        f.write_text("x")
        calls = []

        def fake_retrieve_token(*args, **kwargs):
            calls.append((args, kwargs))
            return "custom-token"

        monkeypatch.setattr("gfal.cli.tape.retrieve_token", fake_retrieve_token)
        cmd = _make_cmd("gfal-token")
        cmd.params = _default_params(
            path=f.as_uri(),
            write=True,
            validity=15,
            issuer="https://issuer.example",
            activities=["LIST", "MANAGE"],
        )
        cmd.execute_token()
        assert calls[0][1]["issuer"] == "https://issuer.example"
        assert calls[0][1]["validity"] == 15
        assert calls[0][1]["write_access"] is True
        assert calls[0][1]["activities"] == ["LIST", "MANAGE"]

    def test_verbose_default_message(self, tmp_path, monkeypatch, capsys):
        f = tmp_path / "file.txt"
        f.write_text("x")
        monkeypatch.setattr("gfal.cli.tape.retrieve_token", lambda *a, **k: "tok")
        cmd = _make_cmd("gfal-token")
        cmd.params = _default_params(
            path=f.as_uri(),
            write=False,
            validity=60,
            issuer=None,
            activities=[],
            verbose=1,
        )
        cmd.execute_token()
        captured = capsys.readouterr()
        assert "Will use default activities for read access" in captured.out
        assert captured.out.endswith("tok\n")

    def test_verbose_custom_activities_message(self, tmp_path, monkeypatch, capsys):
        f = tmp_path / "file.txt"
        f.write_text("x")
        monkeypatch.setattr("gfal.cli.tape.retrieve_token", lambda *a, **k: "tok")
        cmd = _make_cmd("gfal-token")
        cmd.params = _default_params(
            path=f.as_uri(),
            write=True,
            validity=60,
            issuer=None,
            activities=["DELETE"],
            verbose=1,
        )
        cmd.execute_token()
        captured = capsys.readouterr()
        assert "Will use user-provided activities" in captured.out

    def test_negative_validity_returns_one(self, tmp_path, capsys):
        f = tmp_path / "file.txt"
        f.write_text("x")
        cmd = _make_cmd("gfal-token")
        cmd.params = _default_params(
            path=f.as_uri(),
            write=False,
            validity=-1,
            issuer=None,
            activities=[],
        )
        rc = cmd.execute_token()
        captured = capsys.readouterr()
        assert rc == 1
        assert "Validity must be a number >= 0" in captured.err
