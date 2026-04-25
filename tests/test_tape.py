"""Tests for tape stub commands and EOS token CLI parsing.

The tape/staging commands require the native gfal2 C library and are not
supported in this fsspec-based implementation.  Each command must:
  - exit with a non-zero return code (1)
  - print a message to stderr explaining the limitation
  - accept its documented CLI flags without error (backwards compatibility)
"""

from helpers import run_gfal

# ---------------------------------------------------------------------------
# bringonline
# ---------------------------------------------------------------------------


class TestBringonline:
    def test_exits_nonzero(self, tmp_path):
        f = tmp_path / "file.txt"
        f.write_text("x")
        rc, out, err = run_gfal("bringonline", f.as_uri())
        assert rc != 0

    def test_prints_not_supported(self, tmp_path):
        f = tmp_path / "file.txt"
        f.write_text("x")
        rc, out, err = run_gfal("bringonline", f.as_uri())
        assert "not supported" in err.lower() or "gfal2" in err.lower()

    def test_pin_lifetime_accepted(self, tmp_path):
        f = tmp_path / "file.txt"
        f.write_text("x")
        rc, out, err = run_gfal("bringonline", "--pin-lifetime", "3600", f.as_uri())
        # Should fail gracefully (not with "unrecognised argument")
        assert "unrecognised" not in err
        assert "error: argument" not in err or "pin-lifetime" not in err

    def test_desired_request_time_accepted(self, tmp_path):
        f = tmp_path / "file.txt"
        f.write_text("x")
        rc, out, err = run_gfal(
            "bringonline", "--desired-request-time", "7200", f.as_uri()
        )
        assert "unrecognised" not in err

    def test_polling_timeout_accepted(self, tmp_path):
        f = tmp_path / "file.txt"
        f.write_text("x")
        rc, out, err = run_gfal("bringonline", "--polling-timeout", "60", f.as_uri())
        assert "unrecognised" not in err

    def test_help_exits_zero(self):
        rc, out, err = run_gfal("bringonline", "--help")
        assert rc == 0

    def test_from_file_accepted(self, tmp_path):
        list_file = tmp_path / "list.txt"
        list_file.write_text("")
        rc, out, err = run_gfal("bringonline", "--from-file", str(list_file))
        assert "unrecognised" not in err


# ---------------------------------------------------------------------------
# archivepoll
# ---------------------------------------------------------------------------


class TestArchivepoll:
    def test_exits_nonzero(self, tmp_path):
        f = tmp_path / "file.txt"
        f.write_text("x")
        rc, out, err = run_gfal("archivepoll", f.as_uri())
        assert rc != 0

    def test_prints_not_supported(self, tmp_path):
        f = tmp_path / "file.txt"
        f.write_text("x")
        rc, out, err = run_gfal("archivepoll", f.as_uri())
        assert "not supported" in err.lower() or "gfal2" in err.lower()

    def test_polling_timeout_accepted(self, tmp_path):
        f = tmp_path / "file.txt"
        f.write_text("x")
        rc, out, err = run_gfal("archivepoll", "--polling-timeout", "120", f.as_uri())
        assert "unrecognised" not in err

    def test_help_exits_zero(self):
        rc, out, err = run_gfal("archivepoll", "--help")
        assert rc == 0

    def test_from_file_accepted(self, tmp_path):
        list_file = tmp_path / "list.txt"
        list_file.write_text("")
        rc, out, err = run_gfal("archivepoll", "--from-file", str(list_file))
        assert "unrecognised" not in err


# ---------------------------------------------------------------------------
# evict
# ---------------------------------------------------------------------------


class TestEvict:
    def test_exits_nonzero(self, tmp_path):
        f = tmp_path / "file.txt"
        f.write_text("x")
        rc, out, err = run_gfal("evict", f.as_uri())
        assert rc != 0

    def test_prints_not_supported(self, tmp_path):
        f = tmp_path / "file.txt"
        f.write_text("x")
        rc, out, err = run_gfal("evict", f.as_uri())
        assert "not supported" in err.lower() or "gfal2" in err.lower()

    def test_help_exits_zero(self):
        rc, out, err = run_gfal("evict", "--help")
        assert rc == 0


# ---------------------------------------------------------------------------
# token
# ---------------------------------------------------------------------------


class TestToken:
    def test_exits_nonzero(self, tmp_path):
        f = tmp_path / "file.txt"
        f.write_text("x")
        rc, out, err = run_gfal("token", f.as_uri())
        assert rc != 0

    def test_prints_not_supported(self, tmp_path):
        f = tmp_path / "file.txt"
        f.write_text("x")
        rc, out, err = run_gfal("token", f.as_uri())
        assert "eos token path" in err.lower() or "unsupported eos token path" in err

    def test_write_flag_accepted(self, tmp_path):
        f = tmp_path / "file.txt"
        f.write_text("x")
        rc, out, err = run_gfal("token", "--write", f.as_uri())
        assert "unrecognised" not in err

    def test_validity_accepted(self, tmp_path):
        f = tmp_path / "file.txt"
        f.write_text("x")
        rc, out, err = run_gfal("token", "--validity", "60", f.as_uri())
        assert "unrecognised" not in err

    def test_issuer_accepted(self, tmp_path):
        f = tmp_path / "file.txt"
        f.write_text("x")
        rc, out, err = run_gfal(
            "token", "--issuer", "https://issuer.example", f.as_uri()
        )
        assert "unrecognised" not in err

    def test_eos_token_options_accepted(self, tmp_path):
        f = tmp_path / "file.txt"
        f.write_text("x")
        rc, out, err = run_gfal(
            "token",
            "--ssh-host",
            "eospilot",
            "--eos-instance",
            "root://eospilot.cern.ch",
            "--tree",
            "--no-tree",
            "--output-file",
            str(tmp_path / "token"),
            f.as_uri(),
        )
        assert "unrecognised" not in err

    def test_help_exits_zero(self):
        rc, out, err = run_gfal("token", "--help")
        assert rc == 0
        assert "--authz-token-file" in out
        assert "--output-file" in out
