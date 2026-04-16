"""Tests for base.py: surl type converter, @arg decorator, CommandBase."""

from pathlib import Path
from urllib.parse import urlparse

import pytest

from gfal.cli.base import CommandBase, arg, surl

# ---------------------------------------------------------------------------
# surl() type converter
# ---------------------------------------------------------------------------


class TestSurl:
    def test_bare_path_becomes_file_url(self, tmp_path):
        f = tmp_path / "foo.txt"
        result = surl(str(f))
        assert result == f.as_uri()

    def test_bare_relative_path_becomes_absolute(self):
        result = surl("some/relative/path.txt")
        parsed = urlparse(result)
        assert parsed.scheme == "file"
        # URL path always starts with "/" for absolute file:// URIs on all platforms
        assert parsed.path.startswith("/")

    def test_file_url_unchanged(self):
        url = "file:///tmp/foo.txt"
        assert surl(url) == url

    def test_http_url_unchanged(self):
        url = "http://example.com/file"
        assert surl(url) == url

    def test_https_url_unchanged(self):
        url = "https://example.com/file"
        assert surl(url) == url

    def test_root_url_unchanged(self):
        url = "root://eosuser.cern.ch//eos/user/file"
        assert surl(url) == url

    def test_dash_sentinel_unchanged(self):
        assert surl("-") == "-"


# ---------------------------------------------------------------------------
# @arg decorator
# ---------------------------------------------------------------------------


class TestArgDecorator:
    def test_attaches_arguments(self):
        @arg("--foo", type=str)
        @arg("--bar", action="store_true")
        def my_func(self):
            pass

        assert hasattr(my_func, "arguments")
        assert len(my_func.arguments) == 2

    def test_argument_order_preserved(self):
        """Arguments are stored in the order decorators execute (outermost first)."""

        @arg("--second")
        @arg("--first")
        def my_func(self):
            pass

        # @arg("--first") runs first (inner), inserts at 0 → ["--first"]
        # @arg("--second") runs second (outer), inserts at 0 → ["--second", "--first"]
        args = [a[0][0] for a in my_func.arguments]
        assert args == ["--second", "--first"]

    def test_no_duplicate_arguments(self):
        @arg("--foo", type=str)
        @arg("--foo", type=str)
        def my_func(self):
            pass

        assert len(my_func.arguments) == 1


# ---------------------------------------------------------------------------
# CommandBase
# ---------------------------------------------------------------------------


class TestCommandBase:
    def test_initial_return_code(self):
        cb = CommandBase()
        assert cb.return_code == -1

    def test_get_subclasses_non_empty(self):
        """At least GfalCommands, CommandCopy, CommandLs, CommandRm exist."""
        import gfal.cli.shell  # noqa: F401 — triggers subclass registration

        subs = CommandBase.get_subclasses()
        assert len(subs) >= 4

    def test_common_args_present(self, tmp_path):
        """All commands should accept -v/-q, -t, -E, --key, --log-file, --no-verify."""
        from helpers import run_gfal

        f = tmp_path / "test.txt"
        f.write_text("x")

        # -v should not cause an error
        rc, out, err = run_gfal("stat", "-v", f.as_uri())
        assert rc == 0

        # -q should not cause an error
        rc, out, err = run_gfal("stat", "-q", f.as_uri())
        assert rc == 0

    def test_timeout_arg(self, tmp_path):
        from helpers import run_gfal

        f = tmp_path / "test.txt"
        f.write_text("x")
        rc, out, err = run_gfal("stat", "-t", "10", f.as_uri())
        assert rc == 0

    def test_verbose_levels(self, tmp_path):
        from helpers import run_gfal

        f = tmp_path / "test.txt"
        f.write_text("x")
        # -vvv should work (debug level)
        rc, out, err = run_gfal("stat", "-vvv", f.as_uri())
        assert rc == 0

    def test_log_file(self, tmp_path):
        from helpers import run_gfal

        f = tmp_path / "test.txt"
        f.write_text("x")
        log = tmp_path / "test.log"
        rc, out, err = run_gfal("stat", "-vvv", "--log-file", str(log), f.as_uri())
        assert rc == 0
        # Log file should exist (may or may not have content depending on log level)
        assert log.exists()


# ---------------------------------------------------------------------------
# _format_error
# ---------------------------------------------------------------------------


class TestFormatError:
    def test_file_not_found_no_strerror(self):
        """fsspec-style FileNotFoundError: just a URL, no strerror."""
        e = FileNotFoundError("root://server//path/to/file")
        assert e.strerror is None
        msg = CommandBase._format_error(e)
        assert "root://server//path/to/file" in msg
        assert "No such file or directory" in msg

    def test_permission_error_no_strerror(self):
        e = PermissionError("file:///restricted")
        msg = CommandBase._format_error(e)
        assert "Permission denied" in msg

    def test_is_a_directory_no_strerror(self):
        e = IsADirectoryError("file:///some/dir")
        msg = CommandBase._format_error(e)
        assert "Is a directory" in msg

    def test_not_a_directory_no_strerror(self):
        e = NotADirectoryError("file:///some/file")
        msg = CommandBase._format_error(e)
        assert "Not a directory" in msg

    def test_file_exists_no_strerror(self):
        e = FileExistsError("file:///existing")
        msg = CommandBase._format_error(e)
        assert "File exists" in msg

    def test_real_os_error_not_doubled(self):
        """Real OS FileNotFoundError already has strerror in str(e) — don't double it."""
        try:
            Path("/tmp/nonexistent_xyz_test_abc").stat()
        except FileNotFoundError as e:
            msg = CommandBase._format_error(e)
            assert msg.count("No such file or directory") == 1

    def test_generic_exception(self):
        """Non-OSError exceptions just use str(e)."""
        e = ValueError("something went wrong")
        msg = CommandBase._format_error(e)
        assert msg == "something went wrong"

    def test_os_error_with_strerror_not_in_msg(self):
        """OSError with strerror that isn't in str(e) gets it appended."""
        e = OSError("custom message")
        e.strerror = "custom OS error"
        msg = CommandBase._format_error(e)
        assert "custom message" in msg
        assert "custom OS error" in msg

    @pytest.mark.parametrize(
        "exc_type, expected_desc",
        [
            (FileNotFoundError, "No such file or directory"),
            (PermissionError, "Permission denied"),
            (IsADirectoryError, "Is a directory"),
            (NotADirectoryError, "Not a directory"),
            (FileExistsError, "File exists"),
            (TimeoutError, "Operation timed out"),
        ],
    )
    def test_all_known_types(self, exc_type, expected_desc):
        e = exc_type("some://url")
        msg = CommandBase._format_error(e)
        assert expected_desc in msg


# ---------------------------------------------------------------------------
# Error output via CLI
# ---------------------------------------------------------------------------


class TestErrorOutput:
    def test_nonexistent_file_shows_description(self, tmp_path):
        """Error for a missing file should say 'No such file or directory'."""
        from helpers import run_gfal

        rc, out, err = run_gfal("stat", (tmp_path / "no_such").as_uri())
        assert rc != 0
        assert "No such file or directory" in err

    def test_nonexistent_ls_shows_description(self, tmp_path):
        from helpers import run_gfal

        rc, out, err = run_gfal("ls", (tmp_path / "no_such").as_uri())
        assert rc != 0
        assert "No such file or directory" in err

    def test_error_message_contains_progr_prefix(self, tmp_path):
        """Error lines must start with the program name."""
        from helpers import run_gfal

        rc, out, err = run_gfal("stat", (tmp_path / "missing").as_uri())
        assert rc != 0
        assert err.startswith("gfal stat:")


# ---------------------------------------------------------------------------
# _format_error with empty str(e)
# ---------------------------------------------------------------------------


class TestFormatErrorEmptyStr:
    """_format_error must handle exceptions whose str() is empty."""

    def test_not_implemented_error_empty_str(self):
        cb = CommandBase()
        e = NotImplementedError()
        assert str(e) == ""
        result = cb._format_error(e)
        # Should not return empty string; fall back to class name or type hint
        assert result != ""

    def test_value_error_empty_str(self):
        cb = CommandBase()
        e = ValueError()
        result = cb._format_error(e)
        assert result != ""

    def test_not_implemented_format_error_unit(self):
        """Unit test: _format_error on NotImplementedError() returns non-empty string."""
        cb = CommandBase()
        result = cb._format_error(NotImplementedError())
        assert isinstance(result, str)
        assert result != ""


# ---------------------------------------------------------------------------
# surl extra cases
# ---------------------------------------------------------------------------


class TestSurlExtra:
    def test_xroot_url_unchanged(self):
        url = "xroot://server.example.com//data/file"
        assert surl(url) == url

    def test_dav_url_unchanged(self):
        """dav:// URLs should pass through surl unchanged (normalisation is in fs.py)."""
        url = "dav://example.com/path"
        assert surl(url) == url

    def test_empty_path_is_cwd(self):
        """surl of a relative path (no scheme) resolves to a file:// URI."""
        result = surl("somefile.txt")
        assert result.startswith("file://")
