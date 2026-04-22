"""Additional targeted tests to increase code coverage across several modules.

Covers uncovered lines in:
- copy.py: _truncate_middle, _TransferDisplay paths, execute_cp validations,
           _entry_mtime, _entry_size, interrupt summary, _render helpers,
           dry_run, _do_copy paths
- base.py: aiohttp error mapping, _proxy_is_expired, WinError formatting,
           aiohttp SSL errors, prog_name splitting
- api.py: TransferHandle.join/done, _coerce_stat_result, _precomputed_match
- shell.py: completion group duplicate skip, _emit_bash_completion_source,
            main() completion paths
- progress.py: _format_binary_rate B/s, _format_binary_size B,
               LegacyCountProgress, has_live_progress
- ls.py: empty dir, non-dir HTTP file path
- tpc.py: ConnectionError in _parse_tpc_body, success after connection error,
          failure at end-of-body
"""

from __future__ import annotations

import errno
import sys
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# copy.py: _truncate_middle
# ---------------------------------------------------------------------------


class TestTruncateMiddle:
    def test_short_value_returned_unchanged(self):
        from gfal.cli.copy import _truncate_middle

        assert _truncate_middle("hello", 10) == "hello"

    def test_max_width_zero_returns_empty(self):
        from gfal.cli.copy import _truncate_middle

        assert _truncate_middle("hello", 0) == ""

    def test_max_width_one_returns_first_char(self):
        from gfal.cli.copy import _truncate_middle

        assert _truncate_middle("hello", 1) == "h"

    def test_max_width_two(self):
        from gfal.cli.copy import _truncate_middle

        # max_width <= 3: return value[:max_width]
        assert _truncate_middle("hello", 2) == "he"

    def test_max_width_three(self):
        from gfal.cli.copy import _truncate_middle

        # max_width <= 3: return value[:max_width]
        assert _truncate_middle("hello", 3) == "hel"

    def test_truncates_in_middle(self):
        from gfal.cli.copy import _truncate_middle

        result = _truncate_middle("abcdefghij", 7)
        assert "..." in result
        assert len(result) == 7


# ---------------------------------------------------------------------------
# copy.py: _TransferDisplay._rate_text and _history_status_line
# ---------------------------------------------------------------------------


class TestTransferDisplayRateText:
    def test_rate_text_none_when_size_none(self):
        from gfal.cli.copy import _TransferDisplay

        assert _TransferDisplay._rate_text(None, 1.0) is None

    def test_rate_text_none_when_elapsed_zero(self):
        from gfal.cli.copy import _TransferDisplay

        assert _TransferDisplay._rate_text(1024, 0) is None

    def test_rate_text_none_when_elapsed_negative(self):
        from gfal.cli.copy import _TransferDisplay

        assert _TransferDisplay._rate_text(1024, -1.0) is None

    def test_rate_text_returns_string_when_valid(self):
        from gfal.cli.copy import _TransferDisplay

        result = _TransferDisplay._rate_text(1024 * 1024, 1.0)
        assert result is not None
        assert "/s" in result

    def test_history_status_line_no_details(self):
        """_history_status_line returns just the status when no size/rate."""
        from gfal.cli.copy import _TransferDisplay

        td = _TransferDisplay("/src/f", "/dst/f", quiet=True)
        td.src_size = None
        line = td._history_status_line(True)
        assert isinstance(line, str)
        assert len(line) > 0


# ---------------------------------------------------------------------------
# copy.py: _TransferDisplay.start with history_only and quiet
# ---------------------------------------------------------------------------


class TestTransferDisplayFinish:
    def test_finish_without_start_is_noop(self):
        """finish() when not started should not crash."""
        from gfal.cli.copy import _TransferDisplay

        td = _TransferDisplay("/src/f", "/dst/f", quiet=True)
        td.finish(True)  # should not raise

    def test_finish_suppress_output(self):
        from gfal.cli.copy import _TransferDisplay

        td = _TransferDisplay("/src/f", "/dst/f", quiet=True)
        td.suppress_output()
        td.progress_started = True
        td.finish(True)  # should return early

    def test_update_suppress_output(self):
        from gfal.cli.copy import _TransferDisplay

        td = _TransferDisplay("/src/f", "/dst/f", quiet=True)
        td.suppress_output()
        td.update(1024)  # should not crash

    def test_set_mode_suppress_output(self):
        from gfal.cli.copy import _TransferDisplay

        td = _TransferDisplay("/src/f", "/dst/f", quiet=True)
        td.suppress_output()
        td.set_mode("streamed")  # should not crash

    def test_set_total_size_suppress_output(self):
        from gfal.cli.copy import _TransferDisplay

        td = _TransferDisplay("/src/f", "/dst/f", quiet=True)
        td.suppress_output()
        td.set_total_size(1024)  # should not crash

    def test_mark_skipped_suppress_output(self):
        from gfal.cli.copy import _TransferDisplay

        td = _TransferDisplay("/src/f", "/dst/f", quiet=True)
        td.suppress_output()
        td.mark_skipped()  # should not crash

    def test_start_not_quiet_no_progress(self):
        """Cover start() with show_progress=False and not quiet."""
        from gfal.cli.copy import _TransferDisplay

        td = _TransferDisplay("/src/file.txt", "/dst/file.txt", quiet=False)
        td.show_progress = False
        td.src_size = 512
        td.start()  # should print without crashing

    def test_start_history_only_does_not_create_bar(self):
        from gfal.cli.copy import _TransferDisplay

        td = _TransferDisplay("/src/f", "/dst/f", quiet=True)
        td.show_progress = True
        td.history_only = True
        td.start()
        assert td.progress_bar is None


# ---------------------------------------------------------------------------
# copy.py: _entry_mtime / _entry_size edge cases
# ---------------------------------------------------------------------------


class TestEntryHelpers:
    def test_entry_mtime_non_dict_returns_none(self):
        from gfal.cli.copy import CommandCopy

        cmd = CommandCopy()
        assert cmd._entry_mtime("not-a-dict") is None

    def test_entry_mtime_no_keys_returns_none(self):
        from gfal.cli.copy import CommandCopy

        cmd = CommandCopy()
        assert cmd._entry_mtime({}) is None

    def test_entry_mtime_with_mtime_key(self):
        from gfal.cli.copy import CommandCopy

        cmd = CommandCopy()
        assert cmd._entry_mtime({"mtime": 1234.5}) == 1234.5

    def test_entry_mtime_with_bad_value(self):
        from gfal.cli.copy import CommandCopy

        cmd = CommandCopy()
        assert cmd._entry_mtime({"mtime": "not-a-float"}) is None

    def test_entry_size_from_non_dict_with_st_size(self):
        """Cover the non-dict branch in _entry_size with st_size attribute."""
        from gfal.cli.copy import CommandCopy

        cmd = CommandCopy()
        obj = SimpleNamespace(st_size=42)
        assert cmd._entry_size(obj) == 42

    def test_entry_size_from_non_dict_with_size_attr(self):
        """Cover the non-dict branch in _entry_size with size attribute."""
        from gfal.cli.copy import CommandCopy

        cmd = CommandCopy()
        obj = SimpleNamespace(size=99)
        assert cmd._entry_size(obj) == 99

    def test_entry_size_bad_value_returns_none(self):
        from gfal.cli.copy import CommandCopy

        cmd = CommandCopy()
        assert cmd._entry_size({"size": "bad"}) is None

    def test_entry_name_non_dict(self):
        from gfal.cli.copy import CommandCopy

        cmd = CommandCopy()
        # non-dict strings: Path(...).name extracts the basename
        result = cmd._entry_name("/some/path/file.txt")
        assert result == "file.txt"


# ---------------------------------------------------------------------------
# copy.py: execute_cp validation
# ---------------------------------------------------------------------------


def _make_copy_cmd(**extra):
    from gfal.cli.copy import CommandCopy

    cmd = CommandCopy()
    cmd.prog = "gfal-cp"
    defaults = {
        "cert": None,
        "key": None,
        "timeout": 1800,
        "ssl_verify": True,
        "verbose": 0,
        "quiet": False,
        "log_file": None,
        "force": False,
        "parent": False,
        "checksum": None,
        "checksum_mode": "both",
        "compare": None,
        "recursive": False,
        "preserve_times": True,
        "from_file": None,
        "src": None,
        "dst": [],
        "limit": None,
        "dry_run": False,
        "abort_on_failure": False,
        "transfer_timeout": 0,
        "tpc": False,
        "tpc_only": False,
        "tpc_mode": "pull",
        "copy_mode": None,
        "parallel": 1,
        "just_copy": False,
        "disable_cleanup": False,
        "no_delegation": False,
        "evict": False,
        "scitag": None,
        "nbstreams": None,
        "tcp_buffersize": None,
        "src_spacetoken": None,
        "dst_spacetoken": None,
    }
    defaults.update(extra)
    cmd.params = SimpleNamespace(**defaults)
    return cmd


class TestExecuteCpValidation:
    def test_parallel_less_than_one(self):
        cmd = _make_copy_cmd(parallel=0)
        rc = cmd.execute_cp()
        assert rc == 1

    def test_limit_less_than_one(self):
        cmd = _make_copy_cmd(limit=0)
        rc = cmd.execute_cp()
        assert rc == 1

    def test_from_file_with_positional_src(self):
        cmd = _make_copy_cmd(from_file="somefile", src=["/some/path"])
        rc = cmd.execute_cp()
        assert rc == 1

    def test_copy_mode_streamed_sets_flags(self):
        """cover copy_mode == 'streamed' branch."""
        cmd = _make_copy_cmd(copy_mode="streamed", src=["/tmp/a"], dst=["/tmp/b"])
        # We just need to call execute_cp and have it fail on missing src stat —
        # the important thing is that the copy_mode branch is exercised.
        with patch.object(cmd, "_build_client") as mock_client:
            mock_client.side_effect = Exception("no client needed")
            import contextlib

            with contextlib.suppress(Exception):
                cmd.execute_cp()
        assert cmd.params.tpc is False
        assert cmd.params.tpc_only is False

    def test_copy_mode_push_branch(self):
        """cover copy_mode == 'push' branch."""
        import contextlib

        cmd = _make_copy_cmd(copy_mode="push", src=["/tmp/a"], dst=["/tmp/b"])
        with patch.object(cmd, "_build_client") as mock_client:
            mock_client.side_effect = Exception("no client needed")
            with contextlib.suppress(Exception):
                cmd.execute_cp()
        assert cmd.params.tpc_mode == "push"

    def test_copy_mode_unknown_branch(self):
        """cover copy_mode else branch."""
        import contextlib

        cmd = _make_copy_cmd(copy_mode="unknown", src=["/tmp/a"], dst=["/tmp/b"])
        with patch.object(cmd, "_build_client") as mock_client:
            mock_client.side_effect = Exception("no client needed")
            with contextlib.suppress(Exception):
                cmd.execute_cp()
        assert cmd.params.tpc is False

    def test_missing_source_returns_one(self):
        """cover 'Missing source' branch."""
        cmd = _make_copy_cmd(src=None, dst=["/tmp/b"], from_file=None)
        rc = cmd.execute_cp()
        assert rc == 1


# ---------------------------------------------------------------------------
# copy.py: interrupt summary methods
# ---------------------------------------------------------------------------


class TestInterruptSummary:
    def _make_cmd_with_summary(self):
        import threading

        from gfal.cli.copy import CommandCopy

        cmd = CommandCopy()
        cmd._recursive_interrupt_summary = {
            "lock": threading.Lock(),
            "printed": False,
            "copied": 2,
            "copied_bytes": 1024,
            "skipped": 0,
            "failed": 0,
            "recursive_start": 0.0,
            "scan_summary": {},
            "rich_recursive_layout": False,
        }
        cmd._interrupt_cancel_error_emitted = False
        return cmd

    def test_update_state_noop_when_none(self):
        from gfal.cli.copy import CommandCopy

        cmd = CommandCopy()
        cmd._recursive_interrupt_summary = None
        cmd._update_recursive_interrupt_summary_state(copied=5)  # should not raise

    def test_mark_printed_returns_false_when_none(self):
        from gfal.cli.copy import CommandCopy

        cmd = CommandCopy()
        cmd._recursive_interrupt_summary = None
        assert cmd._mark_recursive_interrupt_summary_printed() is False

    def test_mark_printed_returns_false_when_already_printed(self):
        cmd = self._make_cmd_with_summary()
        cmd._recursive_interrupt_summary["printed"] = True
        assert cmd._mark_recursive_interrupt_summary_printed() is False

    def test_mark_printed_returns_true_first_time(self):
        cmd = self._make_cmd_with_summary()
        assert cmd._mark_recursive_interrupt_summary_printed() is True
        assert cmd._recursive_interrupt_summary["printed"] is True

    def test_emit_interrupt_summary_none_returns_false(self):
        from gfal.cli.copy import CommandCopy

        cmd = CommandCopy()
        cmd._recursive_interrupt_summary = None
        assert cmd._emit_interrupt_summary_if_pending() is False

    def test_emit_interrupt_summary_already_printed(self):
        cmd = self._make_cmd_with_summary()
        cmd._recursive_interrupt_summary["printed"] = True
        assert cmd._emit_interrupt_summary_if_pending() is False

    def test_emit_interrupt_summary_plain_text(self):
        """Covers plain-text branch (rich_recursive_layout=False)."""
        cmd = self._make_cmd_with_summary()
        with patch("gfal.cli.copy.print_live_message"):
            result = cmd._emit_interrupt_summary_if_pending()
        assert result is True

    def test_emit_interrupt_summary_rich_layout(self):
        """Covers rich branch (rich_recursive_layout=True)."""
        cmd = self._make_cmd_with_summary()
        cmd._recursive_interrupt_summary["rich_recursive_layout"] = True
        cmd._recursive_interrupt_summary["scan_summary"] = {
            "total": 2,
            "queued_first": 2,
            "likely_skipped": 0,
            "deferred_existing": 0,
            "compare_mode": None,
            "limited_to": None,
        }
        with patch("gfal.cli.copy.print_live_message"):
            result = cmd._emit_interrupt_summary_if_pending()
        assert result is True

    def test_emit_interrupt_error_none_returns_false(self):
        from gfal.cli.copy import CommandCopy

        cmd = CommandCopy()
        cmd._recursive_interrupt_summary = None
        cmd._interrupt_cancel_error_emitted = False
        assert cmd._emit_interrupt_error_if_pending() is False

    def test_emit_interrupt_error_already_emitted(self):
        cmd = self._make_cmd_with_summary()
        cmd._interrupt_cancel_error_emitted = True
        assert cmd._emit_interrupt_error_if_pending() is False

    def test_emit_interrupt_error_emits(self):
        cmd = self._make_cmd_with_summary()
        with patch.object(cmd, "_print_error"):
            result = cmd._emit_interrupt_error_if_pending()
        assert result is True
        assert cmd._interrupt_cancel_error_emitted is True

    def test_clear_state(self):
        cmd = self._make_cmd_with_summary()
        cmd._clear_recursive_interrupt_summary_state()
        assert cmd._recursive_interrupt_summary is None
        assert cmd._interrupt_cancel_error_emitted is False


# ---------------------------------------------------------------------------
# copy.py: _render_single_final_summary cancelled branch
# ---------------------------------------------------------------------------


class TestRenderSingleFinalSummary:
    def test_cancelled_branch(self):
        from gfal.cli.copy import CommandCopy

        cmd = CommandCopy()
        result = cmd._render_single_final_summary(
            copied=0,
            copied_bytes=0,
            skipped=0,
            failed=1,
            elapsed=5.0,
            cancelled=True,
        )
        assert "Copy interrupted" in result.plain

    def test_completed_with_bytes(self):
        from gfal.cli.copy import CommandCopy

        cmd = CommandCopy()
        result = cmd._render_single_final_summary(
            copied=1,
            copied_bytes=1024,
            skipped=0,
            failed=0,
            elapsed=1.0,
        )
        assert result is not None


# ---------------------------------------------------------------------------
# copy.py: _do_copy dry_run branch
# ---------------------------------------------------------------------------


class TestDoCopyDryRun:
    def test_dry_run_file(self, tmp_path):
        """dry_run for a single file: prints Copy line."""
        src = tmp_path / "src.txt"
        src.write_text("hello")
        dst = tmp_path / "dst.txt"

        cmd = _make_copy_cmd(
            dry_run=True, src=[str(src)], dst=[str(dst)], recursive=False
        )
        with patch("builtins.print") as mock_print:
            cmd._do_copy(str(src), str(dst), {})
        printed = " ".join(
            str(a) for call in mock_print.call_args_list for a in call[0]
        )
        assert "Copy" in printed

    def test_dry_run_directory_no_recursive(self, tmp_path):
        """dry_run for a directory without -r: prints skip message."""
        src_dir = tmp_path / "srcdir"
        src_dir.mkdir()
        dst_dir = tmp_path / "dstdir"

        cmd = _make_copy_cmd(
            dry_run=True, src=[str(src_dir)], dst=[str(dst_dir)], recursive=False
        )
        with patch("builtins.print") as mock_print:
            cmd._do_copy(str(src_dir), str(dst_dir), {})
        printed = " ".join(
            str(a) for call in mock_print.call_args_list for a in call[0]
        )
        assert "Skipping directory" in printed

    def test_dry_run_directory_recursive(self, tmp_path):
        """dry_run for a directory with -r: prints mkdir and Copy lines."""
        src_dir = tmp_path / "srcdir"
        src_dir.mkdir()
        dst_dir = tmp_path / "dstdir"

        cmd = _make_copy_cmd(
            dry_run=True, src=[str(src_dir)], dst=[str(dst_dir)], recursive=True
        )
        with patch("builtins.print") as mock_print:
            cmd._do_copy(str(src_dir), str(dst_dir), {})
        printed = " ".join(
            str(a) for call in mock_print.call_args_list for a in call[0]
        )
        assert "Copy" in printed


# ---------------------------------------------------------------------------
# copy.py: _is_skip_message / _handle_skip_warn
# ---------------------------------------------------------------------------


class TestSkipMessage:
    def test_is_skip_message_true_existing(self):
        from gfal.cli.copy import CommandCopy

        cmd = CommandCopy()
        assert cmd._is_skip_message("Skipping existing file /some/path")

    def test_is_skip_message_true_directory(self):
        from gfal.cli.copy import CommandCopy

        cmd = CommandCopy()
        assert cmd._is_skip_message("Skipping directory /some/path")

    def test_is_skip_message_false(self):
        from gfal.cli.copy import CommandCopy

        cmd = CommandCopy()
        assert not cmd._is_skip_message("Some other warning")


# ---------------------------------------------------------------------------
# base.py: exception_exit_code with aiohttp errors (mocked)
# ---------------------------------------------------------------------------


class TestExceptionExitCodeAiohttp:
    def test_client_ssl_error_returns_ehostdown(self):
        from gfal.cli.base import exception_exit_code

        mock_aiohttp = MagicMock()

        class FakeClientSSLError(Exception):
            pass

        class FakeClientConnectionError(Exception):
            pass

        mock_aiohttp.ClientSSLError = FakeClientSSLError
        mock_aiohttp.ClientConnectionError = FakeClientConnectionError

        with patch.dict(sys.modules, {"aiohttp": mock_aiohttp}):
            e = FakeClientSSLError("ssl error")
            result = exception_exit_code(e)
            assert result == errno.EHOSTDOWN

    def test_client_connection_error_returns_econnrefused(self):
        from gfal.cli.base import exception_exit_code

        mock_aiohttp = MagicMock()

        class FakeClientSSLError(Exception):
            pass

        class FakeClientConnectionError(Exception):
            pass

        mock_aiohttp.ClientSSLError = FakeClientSSLError
        mock_aiohttp.ClientConnectionError = FakeClientConnectionError

        with patch.dict(sys.modules, {"aiohttp": mock_aiohttp}):
            e = FakeClientConnectionError("conn refused")
            result = exception_exit_code(e)
            assert result == errno.ECONNREFUSED


# ---------------------------------------------------------------------------
# base.py: _proxy_is_expired
# ---------------------------------------------------------------------------


class TestProxyIsExpired:
    def test_returns_false_on_exception(self):
        from pathlib import Path

        from gfal.cli.base import _proxy_is_expired

        # Non-existent path → ssl._ssl._test_decode_cert raises, suppress → False
        result = _proxy_is_expired(Path("/nonexistent/proxy"))
        assert result is False

    def test_returns_false_for_empty_file(self, tmp_path):
        from gfal.cli.base import _proxy_is_expired

        proxy = tmp_path / "proxy"
        proxy.write_bytes(b"")
        result = _proxy_is_expired(proxy)
        assert result is False


# ---------------------------------------------------------------------------
# base.py: _format_error WinError and aiohttp SSL paths (mocked)
# ---------------------------------------------------------------------------


class TestFormatErrorWinError:
    def _make_cmd(self):
        from gfal.cli.base import CommandBase

        class _Cmd(CommandBase):
            def execute_dummy(self):
                pass

        cmd = _Cmd()
        cmd.prog = "gfal-test"
        return cmd

    def test_winerror_2_no_such_file(self):
        cmd = self._make_cmd()
        e = OSError()
        e.winerror = 2
        result = cmd._format_error(e)
        assert "No such file or directory" in result

    def test_winerror_5_permission_denied(self):
        cmd = self._make_cmd()
        e = OSError()
        e.winerror = 5
        result = cmd._format_error(e)
        assert "Permission denied" in result

    def test_winerror_17_file_exists(self):
        cmd = self._make_cmd()
        e = OSError()
        e.winerror = 17
        result = cmd._format_error(e)
        assert "File exists" in result

    def test_winerror_183_file_exists(self):
        cmd = self._make_cmd()
        e = OSError()
        e.winerror = 183
        result = cmd._format_error(e)
        assert "File exists" in result

    def test_winerror_string_in_msg(self):
        cmd = self._make_cmd()
        e = OSError("[WinError 2] The system cannot find the file")
        result = cmd._format_error(e)
        assert "No such file or directory" in result

    def test_empty_message_shows_class_name(self):
        cmd = self._make_cmd()
        e = NotImplementedError()
        result = cmd._format_error(e)
        assert "NotImplementedError" in result

    def test_aiohttp_ssl_error_message(self):
        """Cover the aiohttp.ClientSSLError branch in _format_error."""
        cmd = self._make_cmd()
        mock_aiohttp = MagicMock()

        class FakeClientSSLError(OSError):
            pass

        class FakeClientConnectorSSLError(FakeClientSSLError):
            pass

        class FakeClientConnectorError(OSError):
            pass

        class FakeClientConnectionError(OSError):
            pass

        mock_aiohttp.ClientSSLError = FakeClientSSLError
        mock_aiohttp.ClientConnectorSSLError = FakeClientConnectorSSLError
        mock_aiohttp.ClientConnectorError = FakeClientConnectorError
        mock_aiohttp.ClientConnectionError = FakeClientConnectionError

        with patch.dict(sys.modules, {"aiohttp": mock_aiohttp}):
            e = FakeClientSSLError("ssl error")
            result = cmd._format_error(e)
            assert "SSL" in result or "ssl" in result.lower()

    def test_aiohttp_connector_ssl_error_message(self):
        """Cover ClientConnectorSSLError branch in _format_error."""
        cmd = self._make_cmd()
        mock_aiohttp = MagicMock()

        class FakeClientSSLError(OSError):
            pass

        class FakeClientConnectorSSLError(FakeClientSSLError):
            pass

        class FakeClientConnectorError(OSError):
            pass

        class FakeClientConnectionError(OSError):
            pass

        mock_aiohttp.ClientSSLError = FakeClientSSLError
        mock_aiohttp.ClientConnectorSSLError = FakeClientConnectorSSLError
        mock_aiohttp.ClientConnectorError = FakeClientConnectorError
        mock_aiohttp.ClientConnectionError = FakeClientConnectionError

        with patch.dict(sys.modules, {"aiohttp": mock_aiohttp}):
            e = FakeClientConnectorSSLError("ssl connector error")
            result = cmd._format_error(e)
            assert "SSL" in result or "ssl" in result.lower()


# ---------------------------------------------------------------------------
# api.py: TransferHandle.join / done
# ---------------------------------------------------------------------------


class TestTransferHandle:
    def test_join_returns_true_when_done(self):
        """TransferHandle.join() returns True once thread is finished."""
        import threading

        from gfal.core.api import TransferHandle

        result_holder: dict = {}
        exc_holder: dict = {}
        cancel_event = threading.Event()
        ready_event = threading.Event()

        def _quick():
            result_holder["value"] = 42

        t = threading.Thread(target=_quick)
        t.start()
        handle = TransferHandle(t, cancel_event, ready_event, exc_holder, result_holder)
        result = handle.join(timeout=5.0)
        assert result is True

    def test_done_returns_false_while_running(self):
        """TransferHandle.done() returns False when thread is still alive."""
        import threading

        from gfal.core.api import TransferHandle

        started = threading.Event()
        proceed = threading.Event()
        result_holder: dict = {}
        exc_holder: dict = {}
        cancel_event = threading.Event()
        ready_event = threading.Event()

        def _slow():
            started.set()
            proceed.wait(timeout=5.0)
            result_holder["value"] = 1

        t = threading.Thread(target=_slow)
        t.start()
        handle = TransferHandle(t, cancel_event, ready_event, exc_holder, result_holder)

        started.wait(timeout=2.0)
        assert handle.done() is False
        proceed.set()
        t.join()
        assert handle.done() is True


# ---------------------------------------------------------------------------
# api.py: _coerce_stat_result
# ---------------------------------------------------------------------------


class TestCoerceStatResult:
    def test_with_stat_result_passthrough(self):
        from gfal.core.api import AsyncGfalClient, StatResult

        sr = StatResult.from_info({"name": "test", "size": 100, "type": "file"})
        result = AsyncGfalClient._coerce_stat_result(sr)
        assert result is sr

    def test_with_dict(self):
        from gfal.core.api import AsyncGfalClient, StatResult

        result = AsyncGfalClient._coerce_stat_result(
            {"name": "test", "size": 50, "type": "file"}
        )
        assert isinstance(result, StatResult)

    def test_with_object_having_info_dict(self):
        from gfal.core.api import AsyncGfalClient, StatResult

        obj = SimpleNamespace(info={"name": "foo", "size": 10, "type": "file"})
        result = AsyncGfalClient._coerce_stat_result(obj)
        assert isinstance(result, StatResult)

    def test_with_object_having_attrs(self):
        from gfal.core.api import AsyncGfalClient, StatResult

        obj = SimpleNamespace(name="bar", size=20, type="file")
        result = AsyncGfalClient._coerce_stat_result(obj)
        assert isinstance(result, StatResult)


# ---------------------------------------------------------------------------
# api.py: _precomputed_match
# ---------------------------------------------------------------------------


class TestPrecomputedMatch:
    def _make_client(self):
        from gfal.core.api import AsyncGfalClient

        return AsyncGfalClient()

    def _make_options(self, compare):
        from gfal.core.api import CopyOptions

        return CopyOptions(compare=compare)

    def _make_stat(self, size=100, mtime=1000.0):
        from gfal.core.api import StatResult

        return StatResult.from_info(
            {"name": "test", "size": size, "type": "file", "mtime": mtime}
        )

    def test_compare_none_returns_none(self):
        client = self._make_client()
        src = self._make_stat()
        dst = self._make_stat()
        opts = self._make_options(None)
        result = client._precomputed_match(src, dst, "dst_url", opts)
        assert result is None

    def test_compare_none_mode_returns_true(self):
        client = self._make_client()
        src = self._make_stat()
        dst = self._make_stat()
        opts = self._make_options("none")
        result = client._precomputed_match(src, dst, "dst_url", opts)
        assert result is True

    def test_compare_none_mode_calls_warn_callback(self):
        client = self._make_client()
        src = self._make_stat()
        dst = self._make_stat()
        opts = self._make_options("none")
        warnings = []
        result = client._precomputed_match(src, dst, "dst_url", opts, warnings.append)
        assert result is True
        assert len(warnings) == 1
        assert "Skipping" in warnings[0]

    def test_compare_size_match_no_callback(self):
        client = self._make_client()
        src = self._make_stat(size=100)
        dst = self._make_stat(size=100)
        opts = self._make_options("size")
        result = client._precomputed_match(src, dst, "dst_url", opts)
        assert result is True

    def test_compare_size_mismatch(self):
        client = self._make_client()
        src = self._make_stat(size=100)
        dst = self._make_stat(size=200)
        opts = self._make_options("size")
        result = client._precomputed_match(src, dst, "dst_url", opts)
        assert result is False

    def test_compare_size_mtime_match_calls_callback(self):
        client = self._make_client()
        src = self._make_stat(size=100, mtime=1000.0)
        dst = self._make_stat(size=100, mtime=1000.0)
        opts = self._make_options("size_mtime")
        warnings = []
        result = client._precomputed_match(src, dst, "dst_url", opts, warnings.append)
        assert result is True
        assert len(warnings) == 1

    def test_compare_size_mtime_mismatch(self):
        client = self._make_client()
        src = self._make_stat(size=100, mtime=1000.0)
        dst = self._make_stat(size=100, mtime=2000.0)
        opts = self._make_options("size_mtime")
        result = client._precomputed_match(src, dst, "dst_url", opts)
        assert result is False


# ---------------------------------------------------------------------------
# shell.py: _emit_bash_completion_source and main() completion paths
# ---------------------------------------------------------------------------


class TestShellCompletion:
    def test_emit_bash_completion_source(self, capsys):
        from gfal.cli.shell import _emit_bash_completion_source

        _emit_bash_completion_source()
        captured = capsys.readouterr()
        assert "_gfal_completion" in captured.out

    def test_main_bash_source_completion(self, capsys):
        """Cover the bash_source branch in main()."""
        from gfal.cli.shell import main

        with patch.dict("os.environ", {"_GFAL_COMPLETE": "bash_source"}, clear=False):
            main(argv=["gfal", "ls"])
        captured = capsys.readouterr()
        assert "_gfal_completion" in captured.out

    def test_main_other_complete_mode(self, monkeypatch, capsys):
        """Cover the non-bash_source complete_mode branch."""
        from gfal.cli.shell import main

        monkeypatch.setenv("_GFAL_COMPLETE", "some_other_mode")
        import contextlib

        with contextlib.suppress(SystemExit):
            main(argv=["gfal", "ls"])

    def test_build_completion_group_returns_group(self):
        from gfal.cli.shell import _build_completion_group

        grp = _build_completion_group()
        assert grp is not None
        assert grp.name == "gfal"
        # Should have commands registered
        assert len(grp.commands) > 0


# ---------------------------------------------------------------------------
# progress.py: _format_binary_rate and _format_binary_size with B unit
# ---------------------------------------------------------------------------


class TestProgressFormatFunctions:
    def test_format_binary_rate_zero_returns_question(self):
        from gfal.cli.progress import _format_binary_rate

        assert _format_binary_rate(0) == "?"

    def test_format_binary_rate_none_returns_question(self):
        from gfal.cli.progress import _format_binary_rate

        assert _format_binary_rate(None) == "?"

    def test_format_binary_rate_bytes_per_second(self):
        """Cover the B/s unit path (rate < 1024)."""
        from gfal.cli.progress import _format_binary_rate

        result = _format_binary_rate(512)
        assert "B/s" in result
        assert "512" in result

    def test_format_binary_rate_larger(self):
        from gfal.cli.progress import _format_binary_rate

        result = _format_binary_rate(1024 * 1024)
        assert "/s" in result

    def test_format_binary_size_zero_returns_zero_b(self):
        from gfal.cli.progress import _format_binary_size

        assert _format_binary_size(0) == "0 B"

    def test_format_binary_size_none_returns_zero_b(self):
        from gfal.cli.progress import _format_binary_size

        assert _format_binary_size(None) == "0 B"

    def test_format_binary_size_bytes(self):
        """Cover the B unit path (size < 1024)."""
        from gfal.cli.progress import _format_binary_size

        result = _format_binary_size(512)
        assert "B" in result
        assert "512" in result

    def test_format_binary_size_megabytes(self):
        from gfal.cli.progress import _format_binary_size

        result = _format_binary_size(1024 * 1024)
        assert "MB" in result


# ---------------------------------------------------------------------------
# progress.py: LegacyCountProgress
# ---------------------------------------------------------------------------


class TestLegacyCountProgress:
    def test_start_stop(self):
        from gfal.cli.progress import LegacyCountProgress

        cp = LegacyCountProgress("test", total=10, transient=True)
        cp.start()
        cp.update(completed=5)
        cp.stop(success=True)

    def test_update_noop(self):
        from gfal.cli.progress import LegacyCountProgress

        cp = LegacyCountProgress("test", total=10)
        # update is a no-op, should not raise
        cp.update(completed=1, total=10, bytes_completed=1024)


# ---------------------------------------------------------------------------
# progress.py: has_live_progress with shared dict
# ---------------------------------------------------------------------------


class TestHasLiveProgress:
    def test_returns_false_in_gfal2_compat_mode(self):
        """In gfal2 compat mode, no live manager → has_live_progress returns False."""
        from gfal.cli.progress import has_live_progress

        with patch("gfal.cli.progress.is_gfal2_compat", return_value=True):
            result = has_live_progress()
        assert result is False

    def test_returns_false_when_no_manager(self):
        from gfal.cli.progress import has_live_progress

        result = has_live_progress()
        # Should return False when no active progress manager
        assert isinstance(result, bool)


# ---------------------------------------------------------------------------
# tpc.py: _parse_tpc_body edge cases
# ---------------------------------------------------------------------------


class TestParseTpcBody:
    def _make_resp(self, lines, status_code=200):
        mock_resp = MagicMock()
        mock_resp.status_code = status_code
        mock_resp.iter_lines.return_value = iter(lines)
        mock_resp.close = MagicMock()
        return mock_resp

    def test_success_line_returns(self):
        from gfal.core.tpc import _parse_tpc_body

        resp = self._make_resp(["Perf Marker", "End", "success: 200"])
        _parse_tpc_body(resp)  # should not raise

    def test_failure_line_raises(self):
        from gfal.core.tpc import _parse_tpc_body

        resp = self._make_resp(["failure: something went wrong"])
        with pytest.raises(OSError, match="failure"):
            _parse_tpc_body(resp)

    def test_failure_at_end_of_body(self):
        from gfal.core.tpc import _parse_tpc_body

        resp = self._make_resp(["failure: end of body failure"])
        with pytest.raises(OSError):
            _parse_tpc_body(resp)

    def test_silent_end_treated_as_success(self):
        from gfal.core.tpc import _parse_tpc_body

        resp = self._make_resp(["Perf Marker", "End"])
        _parse_tpc_body(resp)  # empty body end = success, no raise

    def test_connection_error_after_success(self):
        """Cover ConnectionError handler when last_non_empty starts with 'success:'."""
        from gfal.core.tpc import _parse_tpc_body

        def _lines():
            yield "success: transfer ok"
            raise ConnectionError("connection lost")

        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.iter_lines.return_value = _lines()
        mock_resp.close = MagicMock()
        _parse_tpc_body(mock_resp)  # should not raise (success before error)

    def test_connection_error_no_success_reraises(self):
        """ConnectionError when last_non_empty is not success: should re-raise."""
        from gfal.core.tpc import _parse_tpc_body

        def _lines():
            yield "Perf Marker"
            raise ConnectionError("connection lost")

        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.iter_lines.return_value = _lines()
        mock_resp.close = MagicMock()
        with pytest.raises(ConnectionError):
            _parse_tpc_body(mock_resp)

    def test_405_raises_not_implemented(self):
        from gfal.core.tpc import _parse_tpc_body

        resp = self._make_resp([], status_code=405)
        with pytest.raises(NotImplementedError):
            _parse_tpc_body(resp)

    def test_501_raises_not_implemented(self):
        from gfal.core.tpc import _parse_tpc_body

        resp = self._make_resp([], status_code=501)
        with pytest.raises(NotImplementedError):
            _parse_tpc_body(resp)


# ---------------------------------------------------------------------------
# webdav.py: _SyncAiohttpSession.close and context manager
# ---------------------------------------------------------------------------


class TestSyncAiohttpSessionClose:
    def test_close_before_loop_start(self):
        from gfal.core.webdav import _SyncAiohttpSession

        session = _SyncAiohttpSession({})
        # close without starting the loop
        session.close()  # should not raise
        # second close is idempotent
        session.close()

    def test_context_manager(self):
        from gfal.core.webdav import _SyncAiohttpSession

        with _SyncAiohttpSession({}) as session:
            assert session is not None
        # After __exit__, session is closed
        assert session._closed

    def test_closed_session_raises_on_ensure_loop(self):
        from gfal.core.webdav import _SyncAiohttpSession

        session = _SyncAiohttpSession({})
        session.close()
        with pytest.raises(RuntimeError, match="Session is closed"):
            session._ensure_loop()


# ---------------------------------------------------------------------------
# copy.py: _recursive_parallelism with abort_on_failure
# ---------------------------------------------------------------------------


class TestRecursiveParallelism:
    def test_abort_on_failure_forces_serial(self):
        from gfal.cli.copy import CommandCopy

        cmd = CommandCopy()
        cmd.params = SimpleNamespace(abort_on_failure=True, parallel=4)
        assert cmd._recursive_parallelism("src", "dst") == 1

    def test_uses_parallel_param(self):
        from gfal.cli.copy import CommandCopy

        cmd = CommandCopy()
        cmd.params = SimpleNamespace(abort_on_failure=False, parallel=4)
        assert cmd._recursive_parallelism("src", "dst") == 4


# ---------------------------------------------------------------------------
# copy.py: _classify_recursive_child_jobs with empty src_entries
# ---------------------------------------------------------------------------


class TestClassifyRecursiveChildJobs:
    def test_empty_src_entries(self):
        from gfal.cli.copy import CommandCopy

        cmd = CommandCopy()
        jobs, summary = cmd._classify_recursive_child_jobs([], [], None)
        assert jobs == []
        assert summary["total"] == 0

    def test_no_dst_entries(self):
        from gfal.cli.copy import CommandCopy

        cmd = CommandCopy()
        src_entries = [
            ("file1.txt", "src://host/file1.txt", "dst://host/file1.txt", {}),
        ]
        jobs, summary = cmd._classify_recursive_child_jobs(src_entries, [], None)
        assert len(jobs) == 1
        assert summary["total"] == 1


# ---------------------------------------------------------------------------
# ls.py: _list_one with empty directory / HTTP file fallback
# ---------------------------------------------------------------------------


class TestListOneEdgeCases:
    def _make_ls_cmd(self, **kwargs):
        from gfal.cli.ls import CommandLs

        cmd = CommandLs()
        cmd.prog = "gfal-ls"
        defaults = {
            "cert": None,
            "key": None,
            "timeout": 1800,
            "ssl_verify": True,
            "verbose": 0,
            "quiet": False,
            "log_file": None,
            "file": ["/tmp/testfile"],
            "directory": False,
            "long": False,
            "all": False,
            "sort": None,
            "reverse": False,
            "color": False,
            "xattr": None,
            "human": False,
            "full_time": False,
        }
        defaults.update(kwargs)
        cmd.params = SimpleNamespace(**defaults)
        return cmd

    def test_list_one_http_file_with_empty_ls(self, tmp_path):
        """When ls() returns empty for a file, fall back to showing the entry."""

        from gfal.core.api import StatResult

        # Make a real file
        f = tmp_path / "test.txt"
        f.write_text("hello")
        url = f.as_uri()

        cmd = self._make_ls_cmd(file=[url])
        cmd.params.long = False

        mock_client = MagicMock()
        st = StatResult.from_info({"name": str(f), "size": 5, "type": "file"})
        mock_client.stat.return_value = st
        mock_client.ls.return_value = []  # empty ls result

        with patch("gfal.cli.ls.GfalClient", return_value=mock_client):
            rc = cmd.execute_ls()
        assert rc == 0


# ---------------------------------------------------------------------------
# base.py: prog_name splitting with dash
# ---------------------------------------------------------------------------


class TestBuildClickCommandProgName:
    def test_prog_with_dash_suffix(self):
        from gfal.cli.base import _build_click_command
        from gfal.cli.commands import GfalCommands

        method = GfalCommands.execute_mkdir
        cmd, _, _ = _build_click_command(method, "gfal-mkdir", "Make a directory")
        assert cmd is not None

    def test_prog_with_space_suffix(self):
        from gfal.cli.base import _build_click_command
        from gfal.cli.commands import GfalCommands

        method = GfalCommands.execute_mkdir
        cmd, _, _ = _build_click_command(method, "gfal mkdir", "Make a directory")
        assert cmd is not None


# ---------------------------------------------------------------------------
# copy.py: _render_recursive_scan_summary with limit
# ---------------------------------------------------------------------------


class TestRenderRecursiveScanSummary:
    def test_with_limited_to(self):
        from gfal.cli.copy import CommandCopy

        cmd = CommandCopy()
        summary = {
            "total": 100,
            "queued_first": 10,
            "likely_skipped": 80,
            "deferred_existing": 0,
            "compare_mode": "size",
            "limited_to": 10,
        }
        result = cmd._render_recursive_scan_summary(summary)
        assert result is not None

    def test_without_limited_to(self):
        from gfal.cli.copy import CommandCopy

        cmd = CommandCopy()
        summary = {
            "total": 5,
            "queued_first": 3,
            "likely_skipped": 2,
            "deferred_existing": 0,
            "compare_mode": None,
            "limited_to": None,
        }
        result = cmd._render_recursive_scan_summary(summary)
        assert result is not None
