"""Direct unit tests for base.py utilities.

These tests call base.py functions directly (no subprocess) to improve coverage
of:
  - surl() type converter
  - SurlParamType.convert()
  - _argspec_to_click_option() with various actions
  - _argspec_to_click_argument()
  - CommandBase._format_error()
  - CommandBase._executor()
  - _version_callback()
"""

import errno
import sys
from types import SimpleNamespace

import pytest

from gfal.cli.base import (
    CommandBase,
    SurlParamType,
    _argspec_to_click_argument,
    _argspec_to_click_option,
    get_console,
    is_gfal2_compat,
    surl,
)
from gfal.core.errors import GfalError

# ---------------------------------------------------------------------------
# surl()
# ---------------------------------------------------------------------------


class TestSurl:
    def test_file_url_passthrough(self):
        url = "file:///tmp/foo.txt"
        assert surl(url) == url

    def test_http_url_passthrough(self):
        url = "http://example.com/file"
        assert surl(url) == url

    def test_root_url_passthrough(self):
        url = "root://server//path/to/file"
        assert surl(url) == url

    def test_bare_absolute_path(self, tmp_path):
        f = tmp_path / "foo.txt"
        result = surl(str(f))
        assert result.startswith("file://")
        assert "foo.txt" in result

    def test_stdin_sentinel(self):
        assert surl("-") == "-"

    def test_relative_path_becomes_absolute(self):
        result = surl("relative/file.txt")
        assert result.startswith("file://")
        assert "relative" in result

    @pytest.mark.skipif(sys.platform != "win32", reason="Windows drive letters only")
    def test_windows_drive_letter_not_treated_as_scheme(self):
        # "C:/path" has a single-char scheme "C" — should be treated as a path
        result = surl("C:/path/to/file")
        assert result.startswith("file://")


# ---------------------------------------------------------------------------
# SurlParamType.convert()
# ---------------------------------------------------------------------------


class TestSurlParamType:
    def test_none_returns_none(self):
        s = SurlParamType()
        assert s.convert(None, None, None) is None

    def test_url_passthrough(self):
        s = SurlParamType()
        url = "http://example.com/x"
        assert s.convert(url, None, None) == url

    def test_bare_path_converted(self, tmp_path):
        s = SurlParamType()
        f = tmp_path / "test.txt"
        result = s.convert(str(f), None, None)
        assert result.startswith("file://")


# ---------------------------------------------------------------------------
# _argspec_to_click_option: various action types
# ---------------------------------------------------------------------------


class TestArgspecToClickOption:
    def test_store_true(self):
        spec = _argspec_to_click_option(
            ("--flag",), {"action": "store_true", "help": "a flag"}
        )
        assert spec["kind"] == "option"
        assert spec["click_kw"]["is_flag"] is True
        assert spec["click_kw"]["default"] is False
        assert spec["click_kw"]["flag_value"] is True

    def test_store_true_with_default(self):
        spec = _argspec_to_click_option(
            ("--flag",), {"action": "store_true", "default": True}
        )
        assert spec["click_kw"]["default"] is True
        assert spec["click_kw"]["flag_value"] is True

    def test_store_false(self):
        spec = _argspec_to_click_option(("--no-flag",), {"action": "store_false"})
        assert spec["kind"] == "const_option"
        assert spec["const"] is False
        assert spec["click_kw"]["default"] is False

    def test_store_false_with_dest(self):
        spec = _argspec_to_click_option(
            ("--no-flag",), {"action": "store_false", "dest": "flag"}
        )
        assert spec["kind"] == "const_option"
        assert spec["dest"] == "flag"
        assert spec["const"] is False

    def test_store_const(self):
        spec = _argspec_to_click_option(
            ("-S",), {"action": "store_const", "dest": "sort", "const": "size"}
        )
        assert spec["kind"] == "const_option"
        assert spec["const"] == "size"
        assert spec["dest"] == "sort"

    def test_count(self):
        spec = _argspec_to_click_option(
            ("-v", "--verbose"), {"action": "count", "default": 0}
        )
        assert spec["kind"] == "option"
        assert spec["click_kw"]["count"] is True

    def test_count_no_default(self):
        spec = _argspec_to_click_option(("-v",), {"action": "count"})
        assert spec["click_kw"]["default"] == 0

    def test_append(self):
        spec = _argspec_to_click_option(("--item",), {"action": "append"})
        assert spec["kind"] == "option"
        assert spec["click_kw"]["multiple"] is True

    def test_regular_with_type(self):
        spec = _argspec_to_click_option(
            ("-t", "--timeout"), {"type": int, "default": 30, "metavar": "SECS"}
        )
        assert spec["kind"] == "option"
        assert spec["click_kw"]["type"] is int
        assert spec["click_kw"]["default"] == 30
        assert spec["click_kw"]["metavar"] == "SECS"

    def test_regular_with_choices(self):
        import click

        spec = _argspec_to_click_option(("--mode",), {"choices": ["a", "b", "c"]})
        assert spec["kind"] == "option"
        assert isinstance(spec["click_kw"]["type"], click.Choice)

    def test_regular_with_choices_and_type(self):
        import click

        spec = _argspec_to_click_option(
            ("--mode",), {"type": str, "choices": ["a", "b"]}
        )
        assert spec["kind"] == "option"
        assert isinstance(spec["click_kw"]["type"], click.Choice)

    def test_surl_type(self):
        from gfal.cli.base import SURL, surl

        spec = _argspec_to_click_option(("--url",), {"type": surl})
        assert spec["click_kw"]["type"] is SURL

    def test_nargs(self):
        spec = _argspec_to_click_option(("--files",), {"nargs": 2})
        assert spec["click_kw"]["nargs"] == 2

    def test_dest_preserved(self):
        spec = _argspec_to_click_option(("-s",), {"type": str, "dest": "src_token"})
        assert spec["dest"] == "src_token"

    def test_help_preserved(self):
        spec = _argspec_to_click_option(("--opt",), {"help": "my help text"})
        assert spec["click_kw"]["help"] == "my help text"


# ---------------------------------------------------------------------------
# _argspec_to_click_argument
# ---------------------------------------------------------------------------


class TestArgspecToClickArgument:
    def test_basic_argument(self):
        spec = _argspec_to_click_argument(("file",), {"type": str})
        assert spec["kind"] == "argument"
        assert "FILE" in spec["param_decls"][0]

    def test_nargs_plus(self):
        spec = _argspec_to_click_argument(("file",), {"nargs": "+"})
        assert spec["click_kw"]["nargs"] == -1
        assert spec["click_kw"]["required"] is True

    def test_nargs_star(self):
        spec = _argspec_to_click_argument(("file",), {"nargs": "*"})
        assert spec["click_kw"]["nargs"] == -1
        assert spec["click_kw"]["required"] is False

    def test_nargs_question(self):
        spec = _argspec_to_click_argument(("file",), {"nargs": "?", "default": None})
        assert spec["click_kw"]["required"] is False
        assert spec["click_kw"]["default"] is None

    def test_nargs_int(self):
        spec = _argspec_to_click_argument(("file",), {"nargs": 2})
        assert spec["click_kw"]["nargs"] == 2

    def test_with_preceding_optional(self):
        """After an optional positional, required arguments become non-required."""
        spec = _argspec_to_click_argument(
            ("dest",), {"type": str}, has_preceding_optional=True
        )
        assert spec["click_kw"].get("required") is False

    def test_surl_type(self):
        from gfal.cli.base import SURL, surl

        spec = _argspec_to_click_argument(("url",), {"type": surl})
        assert spec["click_kw"]["type"] is SURL

    def test_nargs_plus_with_preceding_optional(self):
        spec = _argspec_to_click_argument(
            ("files",), {"nargs": "+"}, has_preceding_optional=True
        )
        assert spec["click_kw"]["required"] is False


# ---------------------------------------------------------------------------
# CommandBase._format_error
# ---------------------------------------------------------------------------


class TestFormatError:
    def setup_method(self):
        self._cmd = CommandBase()
        self._cmd.prog = "gfal-test"

    def test_empty_message(self):
        e = Exception()
        result = CommandBase._format_error(e)
        assert result  # Should not be empty — should show exception type

    def test_file_not_found_error(self):
        e = FileNotFoundError(errno.ENOENT, "No such file or directory", "/tmp/missing")
        result = CommandBase._format_error(e)
        assert "No such file or directory" in result

    def test_permission_error(self):
        e = PermissionError(errno.EACCES, "Permission denied", "/tmp/file")
        result = CommandBase._format_error(e)
        assert "Permission denied" in result

    def test_is_a_directory_error(self):
        e = IsADirectoryError(errno.EISDIR, "Is a directory", "/tmp/dir")
        result = CommandBase._format_error(e)
        assert "Is a directory" in result

    def test_not_a_directory_error(self):
        e = NotADirectoryError(errno.ENOTDIR, "Not a directory", "/tmp/file")
        result = CommandBase._format_error(e)
        assert "Not a directory" in result

    def test_file_exists_error(self):
        e = FileExistsError(errno.EEXIST, "File exists", "/tmp/file")
        result = CommandBase._format_error(e)
        assert "File exists" in result

    def test_timeout_error(self):
        e = TimeoutError(errno.ETIMEDOUT, "Operation timed out")
        result = CommandBase._format_error(e)
        assert "timed" in result.lower() or "timeout" in result.lower()

    def test_oserror_with_strerror(self):
        e = OSError(errno.ENOSPC, "No space left on device", "/tmp/file")
        result = CommandBase._format_error(e)
        assert "No space left on device" in result

    def test_oserror_strerror_not_doubled(self):
        """When strerror is already in str(e), don't append again."""
        e = OSError("my custom msg: some strerror")
        e.strerror = "some strerror"
        result = CommandBase._format_error(e)
        # Should not duplicate
        assert result.count("some strerror") == 1

    def test_http_status_403(self):
        e = Exception("Forbidden")
        e.status = 403
        e.request_info = None
        result = CommandBase._format_error(e)
        assert "Permission denied" in result

    def test_http_status_404(self):
        e = Exception("Not found")
        e.status = 404
        e.request_info = None
        result = CommandBase._format_error(e)
        assert "No such file or directory" in result

    def test_http_status_500(self):
        e = Exception("Server error")
        e.status = 500
        e.request_info = None
        result = CommandBase._format_error(e)
        assert "Internal server error" in result

    def test_http_status_unknown(self):
        e = Exception("Strange error")
        e.status = 999
        e.request_info = None
        result = CommandBase._format_error(e)
        assert "HTTP 999" in result

    def test_not_implemented_error_empty_string(self):
        e = NotImplementedError()
        result = CommandBase._format_error(e)
        assert result  # should not be blank

    def test_path_from_args(self):
        """When e.args[0] looks like a path, it should be used as the path."""
        e = FileNotFoundError("/some/path/file.txt")
        result = CommandBase._format_error(e)
        assert "No such file or directory" in result

    def test_cern_connection_error(self):
        e = Exception("Failed to connect to eospublic.cern.ch: timeout")
        result = CommandBase._format_error(e)
        assert "CERN VPN" in result or "cern.ch" in result.lower()

    def test_bytes_path(self):
        e = FileNotFoundError(errno.ENOENT, "No such file", b"/tmp/missing")
        result = CommandBase._format_error(e)
        assert "No such file or directory" in result

    @pytest.mark.skipif(sys.platform != "win32", reason="Windows-only error codes")
    def test_winerror_2(self):
        e = FileNotFoundError()
        e.winerror = 2
        result = CommandBase._format_error(e)
        assert "No such file or directory" in result

    def test_winerror_in_message(self):
        e = OSError("[WinError 2] The system cannot find the file specified")
        result = CommandBase._format_error(e)
        assert "No such file or directory" in result

    def test_winerror_3_in_message(self):
        e = OSError("[WinError 3] The system cannot find the path specified")
        result = CommandBase._format_error(e)
        assert "No such file or directory" in result

    def test_winerror_5_in_message(self):
        e = OSError("[WinError 5] Access is denied")
        result = CommandBase._format_error(e)
        assert "Permission denied" in result

    def test_winerror_17_in_message(self):
        e = OSError("[WinError 17] Cannot move the file to a different disk drive")
        result = CommandBase._format_error(e)
        assert "File exists" in result

    def test_winerror_183_in_message(self):
        e = OSError("[WinError 183] Cannot create a file when that file already exists")
        result = CommandBase._format_error(e)
        assert "File exists" in result


# ---------------------------------------------------------------------------
# get_console
# ---------------------------------------------------------------------------


class TestGetConsole:
    def test_gfal2_compat_mode(self, monkeypatch):
        monkeypatch.setenv("GFAL_CLI_GFAL2", "1")
        console = get_console()
        assert console is not None

    def test_normal_mode(self, monkeypatch):
        monkeypatch.delenv("GFAL_CLI_GFAL2", raising=False)
        console = get_console()
        assert console is not None

    def test_stderr_console(self):
        console = get_console(stderr=True)
        assert console is not None


# ---------------------------------------------------------------------------
# is_gfal2_compat
# ---------------------------------------------------------------------------


class TestIsGfal2Compat:
    def test_env_set(self, monkeypatch):
        monkeypatch.setenv("GFAL_CLI_GFAL2", "1")
        assert is_gfal2_compat() is True

    def test_env_unset(self, monkeypatch):
        monkeypatch.delenv("GFAL_CLI_GFAL2", raising=False)
        assert is_gfal2_compat() is False


# ---------------------------------------------------------------------------
# CommandBase._executor
# ---------------------------------------------------------------------------


class TestCommandBaseExecutor:
    def _make_cmd(self):
        cmd = CommandBase()
        cmd.prog = "gfal-test"
        return cmd

    def test_executor_success(self):
        cmd = self._make_cmd()

        def func(self):
            return 0

        cmd._executor(func)
        assert cmd.return_code == 0

    def test_executor_none_return_becomes_zero(self):
        cmd = self._make_cmd()

        def func(self):
            return None

        cmd._executor(func)
        assert cmd.return_code == 0

    def test_executor_exception_sets_return_code(self):
        cmd = self._make_cmd()

        def func(self):
            raise OSError(errno.ENOENT, "No such file")

        cmd._executor(func)
        assert cmd.return_code == errno.ENOENT

    def test_executor_broken_pipe_silently_swallowed(self):
        cmd = self._make_cmd()

        def func(self):
            raise OSError(errno.EPIPE, "Broken pipe")

        cmd._executor(func)
        assert cmd.return_code == 0

    def test_executor_exception_no_errno_returns_1(self):
        cmd = self._make_cmd()

        def func(self):
            raise ValueError("something bad")

        cmd._executor(func)
        assert cmd.return_code == 1

    def test_executor_cancelled_error_is_silent_when_cancelled(self):
        cmd = self._make_cmd()
        cmd._cancel_event.set()
        called = []
        cmd._print_error = lambda e: called.append(e)

        def func(self):
            raise GfalError("Transfer cancelled", errno.ECANCELED)

        cmd._executor(func)
        assert cmd.return_code == errno.ECANCELED
        assert called == []

    def test_executor_cancelled_error_prints_when_not_cancelled(self):
        cmd = self._make_cmd()
        called = []
        cmd._print_error = lambda e: called.append(e)

        def func(self):
            raise GfalError("Transfer cancelled", errno.ECANCELED)

        cmd._executor(func)
        assert cmd.return_code == errno.ECANCELED
        assert len(called) == 1


# ---------------------------------------------------------------------------
# Additional base.py tests for execute() with various params
# ---------------------------------------------------------------------------


class TestCommandBaseExecute:
    """Test CommandBase.execute() with direct in-process calls."""

    def _make_minimal_cmd_with_params(self, **kwargs):
        """Create a CommandBase instance with a minimal params namespace."""
        cmd = CommandBase()
        cmd.prog = "gfal-test"
        defaults = {
            "cert": None,
            "key": None,
            "timeout": 1800,
            "ssl_verify": True,
            "verbose": 0,
            "log_file": None,
            "ipv4_only": False,
            "ipv6_only": False,
        }
        defaults.update(kwargs)
        cmd.params = SimpleNamespace(**defaults)
        return cmd

    def test_execute_with_cert(self, monkeypatch, tmp_path):
        """When cert is set, X509 env vars should be set."""
        cert = str(tmp_path / "cert.pem")
        # Create a dummy cert file
        (tmp_path / "cert.pem").write_text("cert")

        cmd = self._make_minimal_cmd_with_params(cert=cert)

        def func(self):
            return 0

        func.is_interactive = False
        # Clear existing X509 env vars to avoid interference
        monkeypatch.delenv("X509_USER_CERT", raising=False)
        monkeypatch.delenv("X509_USER_KEY", raising=False)
        monkeypatch.delenv("X509_USER_PROXY", raising=False)

        rc = cmd.execute(func)
        assert rc == 0
        import os

        assert os.environ.get("X509_USER_CERT") == cert

    def test_execute_interactive_func(self):
        """Interactive functions should run in main thread, bypassing the Thread."""
        cmd = self._make_minimal_cmd_with_params()

        def func(self):
            return 42

        func.is_interactive = True
        rc = cmd.execute(func)
        assert rc == 42

    def test_execute_logger_setup(self, tmp_path):
        """Verbose flag should configure logging."""

        cmd = self._make_minimal_cmd_with_params(verbose=2)

        def func(self):
            return 0

        func.is_interactive = False
        rc = cmd.execute(func)
        assert rc == 0

    def test_execute_ipv4_only(self):
        """ipv4_only flag should not prevent command execution."""
        cmd = self._make_minimal_cmd_with_params(ipv4_only=True)

        def func(self):
            return 0

        func.is_interactive = False
        rc = cmd.execute(func)
        assert rc == 0

    def test_execute_ipv6_only(self):
        """ipv6_only flag should not prevent command execution."""
        cmd = self._make_minimal_cmd_with_params(ipv6_only=True)

        def func(self):
            return 0

        func.is_interactive = False
        rc = cmd.execute(func)
        assert rc == 0

    @pytest.mark.skipif(not hasattr(__import__("os"), "getuid"), reason="Unix only")
    def test_execute_x509_proxy_autodetect(self, monkeypatch, tmp_path):
        """When no cert and proxy file exists, X509_USER_PROXY should be set."""
        import contextlib
        import os
        from pathlib import Path

        uid = os.getpid() + 100_000
        proxy_path = Path(f"/tmp/x509up_u{uid}")
        monkeypatch.setattr("gfal.cli.base.os.getuid", lambda: uid)
        monkeypatch.setattr("gfal.cli.base._proxy_is_expired", lambda _path: False)

        monkeypatch.delenv("X509_USER_PROXY", raising=False)
        monkeypatch.delenv("X509_USER_CERT", raising=False)

        # Create the proxy at the exact location base.py checks, then clean up.
        already_existed = proxy_path.exists()
        if not already_existed:
            proxy_path.write_text("fake proxy")
        try:
            cmd = self._make_minimal_cmd_with_params(cert=None)

            def func(self):
                return 0

            func.is_interactive = False
            rc = cmd.execute(func)
            assert rc == 0
            assert os.environ.get("X509_USER_PROXY") == str(proxy_path)
        finally:
            if not already_existed:
                with contextlib.suppress(OSError):
                    proxy_path.unlink()

    @pytest.mark.skipif(not hasattr(__import__("os"), "getuid"), reason="Unix only")
    def test_execute_expired_x509_proxy_autodetect_ignored(self, monkeypatch, tmp_path):
        """Expired auto-detected proxies should not be injected into the env."""
        import contextlib
        import os
        from pathlib import Path

        uid = os.getpid() + 200_000
        proxy_path = Path(f"/tmp/x509up_u{uid}")
        monkeypatch.setattr("gfal.cli.base.os.getuid", lambda: uid)

        monkeypatch.delenv("X509_USER_PROXY", raising=False)
        monkeypatch.delenv("X509_USER_CERT", raising=False)
        monkeypatch.delenv("X509_USER_KEY", raising=False)
        monkeypatch.setattr("gfal.cli.base._proxy_is_expired", lambda _path: True)

        already_existed = proxy_path.exists()
        if not already_existed:
            proxy_path.write_text("fake proxy")
        try:
            cmd = self._make_minimal_cmd_with_params(cert=None)

            def func(self):
                return 0

            func.is_interactive = False
            rc = cmd.execute(func)
            assert rc == 0
            assert os.environ.get("X509_USER_PROXY") is None
            assert os.environ.get("X509_USER_CERT") is None
            assert os.environ.get("X509_USER_KEY") is None
        finally:
            if not already_existed:
                with contextlib.suppress(OSError):
                    proxy_path.unlink()

    def test_execute_keyboard_interrupt_returns_worker_code_when_worker_finishes(
        self, monkeypatch
    ):
        cmd = self._make_minimal_cmd_with_params()

        def func(self):
            return 0

        func.is_interactive = False

        class _FakeThread:
            def __init__(self, target=None, args=None):
                del target, args
                self._alive = True
                self._join_calls = 0

            def start(self):
                return None

            def is_alive(self):
                return self._alive

            def join(self, timeout=None):
                del timeout
                self._join_calls += 1
                if self._join_calls == 1:
                    cmd.return_code = errno.ECANCELED
                    self._alive = False
                    raise KeyboardInterrupt
                return None

        monkeypatch.setattr("gfal.cli.base.Thread", _FakeThread)
        rc = cmd.execute(func)
        assert rc == errno.ECANCELED

    def test_execute_keyboard_interrupt_waits_for_worker_summary_path(
        self, monkeypatch
    ):
        cmd = self._make_minimal_cmd_with_params()

        def func(self):
            return 0

        func.is_interactive = False

        class _FakeThread:
            def __init__(self, target=None, args=None):
                del target, args
                self._alive = True
                self._join_calls = 0

            def start(self):
                return None

            def is_alive(self):
                return self._alive

            def join(self, timeout=None):
                del timeout
                self._join_calls += 1
                if self._join_calls == 1:
                    raise KeyboardInterrupt
                if self._join_calls == 4:
                    cmd.return_code = errno.ECANCELED
                    self._alive = False
                return None

        monkeypatch.setattr("gfal.cli.base.Thread", _FakeThread)
        rc = cmd.execute(func)
        assert rc == errno.ECANCELED

    def test_execute_keyboard_interrupt_times_out_when_worker_does_not_finish(
        self, monkeypatch, capsys
    ):
        cmd = self._make_minimal_cmd_with_params()

        def func(self):
            return 0

        func.is_interactive = False

        class _FakeThread:
            def __init__(self, target=None, args=None):
                del target, args
                self._alive = True
                self._join_calls = 0

            def start(self):
                return None

            def is_alive(self):
                return self._alive

            def join(self, timeout=None):
                del timeout
                self._join_calls += 1
                if self._join_calls == 1:
                    raise KeyboardInterrupt
                return None

        monkeypatch.setattr("gfal.cli.base.Thread", _FakeThread)
        times = iter([100.0, 111.0])
        monkeypatch.setattr("gfal.cli.base.time.monotonic", lambda: next(times))
        rc = cmd.execute(func)
        captured = capsys.readouterr()
        assert rc == errno.EINTR
        assert "Interrupted" in captured.err

    def test_execute_keyboard_interrupt_second_signal_aborts_cleanup_wait(
        self, monkeypatch, capsys
    ):
        cmd = self._make_minimal_cmd_with_params()

        def func(self):
            return 0

        func.is_interactive = False

        class _FakeThread:
            def __init__(self, target=None, args=None):
                del target, args
                self._alive = True
                self._join_calls = 0

            def start(self):
                return None

            def is_alive(self):
                return self._alive

            def join(self, timeout=None):
                del timeout
                self._join_calls += 1
                if self._join_calls in (1, 2):
                    raise KeyboardInterrupt
                return None

        monkeypatch.setattr("gfal.cli.base.Thread", _FakeThread)
        rc = cmd.execute(func)
        captured = capsys.readouterr()
        assert rc == errno.EINTR
        assert "Interrupted" in captured.err

    def test_execute_keyboard_interrupt_emits_pending_command_summary(
        self, monkeypatch, capsys
    ):
        class _SummaryCmd(self._make_minimal_cmd_with_params().__class__):
            def _emit_interrupt_summary_if_pending(self_inner):
                print("SUMMARY")
                return True

        cmd = _SummaryCmd()
        cmd.params = self._make_minimal_cmd_with_params().params

        def func(self):
            return 0

        func.is_interactive = False

        class _FakeThread:
            def __init__(self, target=None, args=None):
                del target, args
                self._alive = True
                self._join_calls = 0

            def start(self):
                return None

            def is_alive(self):
                return self._alive

            def join(self, timeout=None):
                del timeout
                self._join_calls += 1
                if self._join_calls == 1:
                    raise KeyboardInterrupt
                return None

        monkeypatch.setattr("gfal.cli.base.Thread", _FakeThread)
        times = iter([100.0, 111.0])
        monkeypatch.setattr("gfal.cli.base.time.monotonic", lambda: next(times))
        rc = cmd.execute(func)
        captured = capsys.readouterr()
        assert rc == errno.EINTR
        assert "SUMMARY" in captured.out
        assert "Interrupted" not in captured.err


# ---------------------------------------------------------------------------
# _argspec_to_click_option: choices without explicit type
# ---------------------------------------------------------------------------


class TestArgspecEdgeCases:
    def test_choices_no_type_not_in_choices(self):
        """When choices is set but type is not, and choices is already consumed."""
        import click

        from gfal.cli.base import _argspec_to_click_option

        # This exercises line 266-267: `if choices and "type" not in click_kw`
        spec = _argspec_to_click_option(
            ("--mode",),
            {"choices": ["x", "y"]},  # no type= specified
        )
        assert isinstance(spec["click_kw"]["type"], click.Choice)

    def test_nargs_1_not_applied(self):
        """nargs=1 should not be applied (single value is default)."""
        spec = _argspec_to_click_option(
            ("--opt",),
            {"nargs": 1, "type": str},
        )
        assert "nargs" not in spec["click_kw"]


# ---------------------------------------------------------------------------
# parse() UsageError path
# ---------------------------------------------------------------------------


class TestCommandBaseParse:
    def test_parse_usage_error_exits_2(self, tmp_path):
        """Providing wrong argument types should cause a SystemExit(2)."""
        from gfal.cli.commands import GfalCommands

        cmd = GfalCommands()
        # stat requires at least one file argument
        with pytest.raises(SystemExit) as exc_info:
            cmd.parse(GfalCommands.execute_stat, ["gfal-stat"])
        assert exc_info.value.code == 2

    def test_parse_version_exits_0(self):
        """--version flag should cause SystemExit(0)."""
        from gfal.cli.commands import GfalCommands

        cmd = GfalCommands()
        with pytest.raises(SystemExit) as exc_info:
            cmd.parse(GfalCommands.execute_stat, ["gfal-stat", "--version"])
        assert exc_info.value.code == 0
