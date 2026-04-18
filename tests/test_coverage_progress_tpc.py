"""Tests to cover uncovered lines in progress.py, tpc.py, and __init__.py."""

import importlib
import sys
import threading
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from gfal.cli.progress import (
    CountProgress,
    LegacyProgress,
    LegacySpinner,
    Progress,
    RichCountProgress,
    RichProgress,
    RichSpinner,
    Spinner,
    TuiProgress,
    _format_hms,
    print_live_message,
)
from gfal.core import tpc as tpc_mod

# ---------------------------------------------------------------------------
# __init__.py — PackageNotFoundError path
# ---------------------------------------------------------------------------


class TestInitVersionFallback:
    def test_version_fallback_on_package_not_found(self, monkeypatch):
        """When importlib.metadata.version raises PackageNotFoundError,
        __version__ should fall back to '0.0.0+unknown'."""
        from importlib.metadata import PackageNotFoundError

        original_version = importlib.metadata.version

        def _fake_version(name):
            if name == "gfal":
                raise PackageNotFoundError(name)
            return original_version(name)

        monkeypatch.setattr(importlib.metadata, "version", _fake_version)

        # Remove the cached module so it gets re-imported
        saved = sys.modules.pop("gfal", None)
        try:
            import gfal as gfal_mod

            importlib.reload(gfal_mod)
            assert gfal_mod.__version__ == "0.0.0+unknown"
        finally:
            if saved is not None:
                sys.modules["gfal"] = saved


# ---------------------------------------------------------------------------
# progress.py — _format_hms edge cases
# ---------------------------------------------------------------------------


class TestFormatHms:
    def test_zero_seconds(self):
        assert _format_hms(0) == "0:00:00"

    def test_negative_clamped_to_zero(self):
        assert _format_hms(-10) == "0:00:00"

    def test_one_hour(self):
        assert _format_hms(3600) == "1:00:00"

    def test_mixed(self):
        assert _format_hms(3661) == "1:01:01"

    def test_large_value(self):
        assert _format_hms(86400) == "24:00:00"


# ---------------------------------------------------------------------------
# progress.py — Progress() factory
# ---------------------------------------------------------------------------


class TestProgressFactory:
    def test_returns_tui_progress_when_callback_given(self):
        cb = lambda *a, **kw: None  # noqa: E731
        result = Progress("label", tui_callback=cb)
        assert isinstance(result, TuiProgress)

    def test_returns_legacy_when_gfal2_compat(self, monkeypatch):
        monkeypatch.setattr("gfal.cli.progress.is_gfal2_compat", lambda: True)
        result = Progress("label")
        assert isinstance(result, LegacyProgress)

    def test_returns_rich_when_not_gfal2_compat(self, monkeypatch):
        monkeypatch.setattr("gfal.cli.progress.is_gfal2_compat", lambda: False)
        result = Progress("label")
        assert isinstance(result, RichProgress)

    def test_count_progress_returns_rich_when_not_gfal2_compat(self, monkeypatch):
        monkeypatch.setattr("gfal.cli.progress.is_gfal2_compat", lambda: False)
        result = CountProgress("label", 3)
        assert isinstance(result, RichCountProgress)

    def test_count_progress_preserves_transient_override(self, monkeypatch):
        monkeypatch.setattr("gfal.cli.progress.is_gfal2_compat", lambda: False)
        result = CountProgress("label", 3, transient=False)
        assert isinstance(result, RichCountProgress)
        assert result.transient is False


# ---------------------------------------------------------------------------
# progress.py — Spinner() factory
# ---------------------------------------------------------------------------


class TestSpinnerFactory:
    def test_returns_legacy_spinner_when_gfal2_compat(self, monkeypatch):
        monkeypatch.setattr("gfal.cli.progress.is_gfal2_compat", lambda: True)
        result = Spinner("label")
        assert isinstance(result, LegacySpinner)

    def test_returns_rich_spinner_when_not_gfal2_compat(self, monkeypatch):
        monkeypatch.setattr("gfal.cli.progress.is_gfal2_compat", lambda: False)
        result = Spinner("label")
        assert isinstance(result, RichSpinner)


# ---------------------------------------------------------------------------
# progress.py — print_live_message gfal2 compat path
# ---------------------------------------------------------------------------


class TestPrintLiveMessageGfal2:
    def test_prints_plain_when_gfal2_compat(self, monkeypatch, capsys):
        monkeypatch.setattr("gfal.cli.progress.is_gfal2_compat", lambda: True)
        print_live_message("hello world")
        captured = capsys.readouterr()
        assert "hello world" in captured.out


# ---------------------------------------------------------------------------
# progress.py — TuiProgress
# ---------------------------------------------------------------------------


class TestTuiProgress:
    def test_init(self):
        cb = MagicMock()
        tp = TuiProgress(cb)
        assert tp.callback is cb
        assert tp.size == 0
        assert tp.value == 0
        assert not tp._is_child

    def test_branched_returns_child(self):
        cb = MagicMock()
        tp = TuiProgress(cb)
        child = tp.branched("path1", "path2")
        assert isinstance(child, TuiProgress)
        assert child._is_child is True
        assert child.callback is cb

    def test_set_size_ignored_on_parent(self):
        cb = MagicMock()
        tp = TuiProgress(cb)
        tp.set_size(1024)
        # Parent ignores set_size, callback not triggered with size update
        assert tp.size == 0

    def test_set_size_on_child(self):
        cb = MagicMock()
        tp = TuiProgress(cb, _is_child=True)
        tp.set_size(2048)
        assert tp.size == 2048
        cb.assert_called_with(0, 2048)

    def test_set_size_none_becomes_zero(self):
        cb = MagicMock()
        tp = TuiProgress(cb, _is_child=True)
        tp.set_size(None)
        assert tp.size == 0

    def test_relative_update_ignored_on_parent(self):
        cb = MagicMock()
        tp = TuiProgress(cb)
        tp.relative_update(100)
        assert tp.value == 0

    def test_relative_update_on_child(self):
        cb = MagicMock()
        tp = TuiProgress(cb, _is_child=True)
        tp.relative_update(50)
        tp.relative_update(30)
        assert tp.value == 80
        cb.assert_called_with(80, 0)

    def test_absolute_update(self):
        cb = MagicMock()
        tp = TuiProgress(cb)
        tp.absolute_update(999)
        assert tp.value == 999
        cb.assert_called_with(999, 0)

    def test_trigger_with_none_callback(self):
        tp = TuiProgress(None)
        tp.absolute_update(10)  # should not raise

    def test_branch_coro_returns_coro(self):
        tp = TuiProgress(MagicMock())
        sentinel = object()
        assert tp.branch_coro(sentinel) is sentinel

    def test_stop_calls_callback_with_finished(self):
        cb = MagicMock()
        tp = TuiProgress(cb)
        tp.value = 100
        tp.size = 200
        tp.stop(success=True)
        cb.assert_called_with(100, 200, finished=True, success=True)

    def test_stop_failure(self):
        cb = MagicMock()
        tp = TuiProgress(cb)
        tp.stop(success=False)
        cb.assert_called_with(0, 0, finished=True, success=False)

    def test_stop_no_callback(self):
        tp = TuiProgress(None)
        tp.stop(success=True)  # should not raise

    def test_total_property(self):
        tp = TuiProgress(None)
        assert tp.total == 0
        tp.size = 500
        assert tp.total == 500

    def test_current_property(self):
        tp = TuiProgress(None)
        assert tp.current == 0
        tp.value = 42
        assert tp.current == 42


# ---------------------------------------------------------------------------
# progress.py — RichProgress additional branches
# ---------------------------------------------------------------------------


class TestRichProgressExtraBranches:
    def test_manager_configures_persistent_rich_progress(self, monkeypatch):
        import types

        created_kwargs = {}

        def _column(*args, **kwargs):
            del args, kwargs
            return object()

        class _FakeBackend:
            def __init__(self, *args, **kwargs):
                del args
                created_kwargs.update(kwargs)

        fake_progress_module = types.ModuleType("rich.progress")
        fake_progress_module.BarColumn = _column
        fake_progress_module.DownloadColumn = _column
        fake_progress_module.SpinnerColumn = _column
        fake_progress_module.TextColumn = _column
        fake_progress_module.TimeElapsedColumn = object
        fake_progress_module.TransferSpeedColumn = _column
        fake_progress_module.Progress = _FakeBackend

        fake_text_module = types.ModuleType("rich.text")
        fake_text_module.Text = _column

        monkeypatch.setitem(sys.modules, "rich.progress", fake_progress_module)
        monkeypatch.setitem(sys.modules, "rich.text", fake_text_module)
        monkeypatch.setattr(
            "gfal.cli.progress.get_console", lambda stderr=False: object()
        )
        monkeypatch.setattr(RichProgress, "_shared", None, raising=False)
        monkeypatch.setattr(
            RichProgress, "_shared_init_lock", threading.Lock(), raising=False
        )

        RichProgress._manager()

        assert created_kwargs["transient"] is False

    def test_count_manager_configures_transient_rich_progress(self, monkeypatch):
        import types

        created_kwargs = {}

        def _column(*args, **kwargs):
            del args, kwargs
            return object()

        class _FakeBackend:
            def __init__(self, *args, **kwargs):
                del args
                created_kwargs.update(kwargs)

        fake_progress_module = types.ModuleType("rich.progress")
        fake_progress_module.BarColumn = _column
        fake_progress_module.ProgressColumn = object
        fake_progress_module.SpinnerColumn = _column
        fake_progress_module.TextColumn = _column
        fake_progress_module.TimeElapsedColumn = object
        fake_progress_module.Progress = _FakeBackend

        monkeypatch.setitem(sys.modules, "rich.progress", fake_progress_module)
        monkeypatch.setattr(
            "gfal.cli.progress.get_console", lambda stderr=False: object()
        )
        monkeypatch.setattr(RichCountProgress, "_shared", {}, raising=False)
        monkeypatch.setattr(
            RichCountProgress, "_shared_init_lock", threading.Lock(), raising=False
        )

        RichCountProgress("label", 3)._manager()

        assert created_kwargs["transient"] is True
        assert created_kwargs["redirect_stdout"] is False
        assert created_kwargs["redirect_stderr"] is False

    def _make_backend(self, task_total=None):
        class _FakeRichBackend:
            def __init__(self):
                self.calls = []
                self.tasks = []

            def start(self):
                self.calls.append(("start",))

            def add_task(self, description, total=None):
                task_id = len(self.tasks)
                self.tasks.append(SimpleNamespace(total=total))
                return task_id

            def update(self, task_id, **kwargs):
                self.calls.append(("update", task_id, kwargs))
                task = self.tasks[task_id]
                if "total" in kwargs:
                    task.total = kwargs["total"]

            def refresh(self):
                self.calls.append(("refresh",))

            def stop_task(self, task_id):
                self.calls.append(("stop_task", task_id))

            def stop(self):
                self.calls.append(("stop",))

        backend = _FakeRichBackend()
        if task_total is not None:
            backend.tasks.append(SimpleNamespace(total=task_total))
        return backend

    def _patch_shared(self, monkeypatch, backend):
        monkeypatch.setattr(
            RichProgress,
            "_shared",
            SimpleNamespace(
                lock=threading.Lock(),
                progress=backend,
                started=False,
                active=0,
            ),
            raising=False,
        )

    def test_update_before_start_is_noop(self, monkeypatch):
        backend = self._make_backend()
        self._patch_shared(monkeypatch, backend)
        p = RichProgress("test")
        p.update(curr_size=100, total_size=200)
        # No update calls because not started
        assert not any(c[0] == "update" for c in backend.calls)

    def test_set_description_before_start_is_noop(self, monkeypatch):
        backend = self._make_backend()
        self._patch_shared(monkeypatch, backend)
        p = RichProgress("test")
        p.set_description("new label")
        assert not any(c[0] == "update" for c in backend.calls)

    def test_stop_before_start_is_noop(self, monkeypatch):
        backend = self._make_backend()
        self._patch_shared(monkeypatch, backend)
        p = RichProgress("test")
        p.stop(success=True)
        assert not any(c[0] == "stop_task" for c in backend.calls)

    def test_start_twice_is_noop(self, monkeypatch):
        backend = self._make_backend()
        self._patch_shared(monkeypatch, backend)
        p = RichProgress("test")
        p.start()
        p.start()  # second start should be a no-op
        assert backend.calls.count(("start",)) == 1

    def test_stop_failure_without_total(self, monkeypatch):
        """stop(success=False) when task.total is None — the failure branch."""
        backend = self._make_backend()
        self._patch_shared(monkeypatch, backend)
        p = RichProgress("test")
        p.start()
        p.stop(success=False)
        # Should have FAILED in the description update
        assert any(
            c[0] == "update" and "FAILED" in str(c[2].get("description", ""))
            for c in backend.calls
        )

    def test_stop_success_with_total_completes(self, monkeypatch):
        """stop(success=True) with total set completes the task bar."""
        backend = self._make_backend()
        self._patch_shared(monkeypatch, backend)
        p = RichProgress("test")
        p.start()
        p.update(total_size=1000)
        p.stop(success=True)
        assert any(
            c[0] == "update" and "DONE" in str(c[2].get("description", ""))
            for c in backend.calls
        )

    def test_update_with_curr_size_only(self, monkeypatch):
        """update with only curr_size sets 'completed' kwarg."""
        backend = self._make_backend()
        self._patch_shared(monkeypatch, backend)
        p = RichProgress("test")
        p.start()
        p.update(curr_size=512)
        assert any(
            c[0] == "update" and c[2].get("completed") == 512 for c in backend.calls
        )


# ---------------------------------------------------------------------------
# progress.py — RichSpinner additional branches
# ---------------------------------------------------------------------------


class TestRichSpinnerExtraBranches:
    def test_start_twice_is_noop(self, monkeypatch):
        calls = []

        class _FakeStatus:
            def start(self):
                calls.append("start")

            def stop(self):
                calls.append("stop")

        class _FakeConsole:
            def status(self, label):
                return _FakeStatus()

        monkeypatch.setattr(
            "gfal.cli.progress.get_console", lambda stderr=False: _FakeConsole()
        )
        s = RichSpinner("test")
        s.start()
        s.start()  # should be a no-op
        assert calls.count("start") == 1

    def test_stop_before_start_is_noop(self, monkeypatch):
        monkeypatch.setattr(
            "gfal.cli.progress.get_console", lambda stderr=False: MagicMock()
        )
        s = RichSpinner("test")
        s.stop()  # should not raise


# ---------------------------------------------------------------------------
# progress.py — LegacyProgress._render() branches
# ---------------------------------------------------------------------------


class TestLegacyProgressRender:
    def _make_progress(self):
        import datetime

        p = LegacyProgress("test")
        p.started = True
        p.start_time = datetime.datetime.now()
        p.dots = 0
        return p

    def test_render_with_percentage(self, capsys):
        p = self._make_progress()
        p.status = {
            "percentage": 50.0,
            "rate": 1024.0,
            "curr_size": 512,
        }
        p._render()
        captured = capsys.readouterr()
        assert "50%" in captured.out
        assert "test" in captured.out

    def test_render_with_total_size_only(self, capsys):
        p = self._make_progress()
        p.status = {"total_size": 2048}
        p._render()
        captured = capsys.readouterr()
        assert "File size:" in captured.out

    def test_render_with_curr_size_only(self, capsys):
        p = self._make_progress()
        p.status = {"curr_size": 1024}
        p._render()
        captured = capsys.readouterr()
        assert "test" in captured.out

    def test_render_with_curr_size_and_rate(self, capsys):
        p = self._make_progress()
        p.status = {"curr_size": 1024, "rate": 512}
        p._render()
        captured = capsys.readouterr()
        assert "/s" in captured.out

    def test_render_no_status(self, capsys):
        p = self._make_progress()
        p.status = None
        p._render()
        captured = capsys.readouterr()
        assert "test" in captured.out

    def test_render_increments_dots(self):
        p = self._make_progress()
        p.status = None
        assert p.dots == 0
        p._render()
        assert p.dots == 1
        p._render()
        assert p.dots == 2
        p._render()
        assert p.dots == 3
        p._render()
        assert p.dots == 0  # wraps around


# ---------------------------------------------------------------------------
# progress.py — LegacySpinner
# ---------------------------------------------------------------------------


class TestLegacySpinner:
    def test_start_and_stop(self, capsys):
        s = LegacySpinner("scanning")
        s.start()
        s.stop(success=True)
        captured = capsys.readouterr()
        assert "scanning" in captured.out
        assert "[DONE]" in captured.out

    def test_stop_failure(self, capsys):
        s = LegacySpinner("scanning")
        s.start()
        s.stop(success=False)
        captured = capsys.readouterr()
        assert "[FAILED]" in captured.out

    def test_stop_skipped(self, capsys):
        s = LegacySpinner("scanning")
        s.start()
        s.stop(success=True, status="skipped")
        captured = capsys.readouterr()
        assert "[SKIPPED]" in captured.out


# ---------------------------------------------------------------------------
# progress.py — _HAS_FCNTL = False path
# ---------------------------------------------------------------------------


class TestHasFcntlFalse:
    def test_terminal_width_without_fcntl(self, monkeypatch):
        import gfal.cli.progress as progress_mod

        monkeypatch.setattr(progress_mod, "_HAS_FCNTL", False)
        width = LegacyProgress._terminal_width()
        assert isinstance(width, int)
        assert width > 0


# ---------------------------------------------------------------------------
# progress.py — _PinnedElapsedColumn.render()
# ---------------------------------------------------------------------------


class TestPinnedElapsedColumn:
    def test_render_with_final_elapsed(self, monkeypatch):
        """When task has final_elapsed field, render returns it directly."""

        class _FakeText:
            def __init__(self, text, style=None):
                self.text = text
                self.style = style

        # We need to access the class created inside _manager, so we replicate
        # the relevant logic
        from rich.progress import TimeElapsedColumn

        class _PinnedElapsedColumn(TimeElapsedColumn):
            def render(self, task):
                final_elapsed = task.fields.get("final_elapsed")
                if final_elapsed:
                    return _FakeText(final_elapsed, style="progress.elapsed")
                return super().render(task)

        task = SimpleNamespace(
            fields={"final_elapsed": "1:23:45"},
            elapsed=100,
            finished=False,
            started=True,
        )
        col = _PinnedElapsedColumn()
        result = col.render(task)
        assert result.text == "1:23:45"
        assert result.style == "progress.elapsed"

    def test_render_without_final_elapsed(self):
        """When no final_elapsed, delegates to parent render."""
        from rich.progress import TimeElapsedColumn

        class _PinnedElapsedColumn(TimeElapsedColumn):
            def render(self, task):
                final_elapsed = task.fields.get("final_elapsed")
                if final_elapsed:
                    return SimpleNamespace(text=final_elapsed)
                return super().render(task)

        task = SimpleNamespace(
            fields={},
            elapsed=10.0,
            finished=False,
            started=True,
        )
        col = _PinnedElapsedColumn()
        result = col.render(task)
        # The parent returns a rich Text object, just verify it's not None
        assert result is not None


# ---------------------------------------------------------------------------
# tpc.py — _parse_tpc_body additional branches
# ---------------------------------------------------------------------------


class TestParseTpcBodyExtra:
    def _make_resp(self, status_code, lines=None, raise_for_status_exc=None):
        resp = SimpleNamespace(
            status_code=status_code,
            iter_lines=lambda decode_unicode=True: iter(lines or []),
            raise_for_status=lambda: None,
            close=lambda: None,
        )
        if raise_for_status_exc:
            resp.raise_for_status = lambda: (_ for _ in ()).throw(raise_for_status_exc)
        return resp

    def test_non_2xx_calls_raise_for_status(self):
        """Non-2xx, non-405/501 should call raise_for_status()."""

        class FakeHTTPError(Exception):
            pass

        resp = self._make_resp(403, raise_for_status_exc=FakeHTTPError("Forbidden"))
        with pytest.raises(FakeHTTPError, match="Forbidden"):
            tpc_mod._parse_tpc_body(resp)

    def test_perf_markers_with_progress_callback(self):
        """Progress callback should receive cumulative bytes from perf markers."""
        lines = [
            "Perf Marker",
            "  Stripe Bytes Transferred: 524288",
            "End",
            "Perf Marker",
            "  Stripe Bytes Transferred: 1048576",
            "End",
            "success: Created",
        ]
        resp = self._make_resp(202, lines)
        received = []
        tpc_mod._parse_tpc_body(resp, progress_callback=received.append)
        assert received == [524288, 1048576]

    def test_perf_marker_no_callback_no_error(self):
        """Perf markers with no callback should not error."""
        lines = [
            "Perf Marker",
            "  Stripe Bytes Transferred: 100",
            "End",
            "success: ok",
        ]
        resp = self._make_resp(202, lines)
        tpc_mod._parse_tpc_body(resp)  # should not raise

    def test_perf_marker_with_zero_bytes(self):
        """Marker with 0 bytes transferred should not trigger callback."""
        lines = [
            "Perf Marker",
            "End",
            "success: ok",
        ]
        resp = self._make_resp(202, lines)
        received = []
        tpc_mod._parse_tpc_body(resp, progress_callback=received.append)
        assert received == []

    def test_connection_error_after_success(self):
        """ConnectionError after a success line should not propagate."""

        def _iter_lines(decode_unicode=True):
            yield "success: done"
            raise ConnectionError("reset")

        resp = SimpleNamespace(
            status_code=202,
            iter_lines=_iter_lines,
            raise_for_status=lambda: None,
            close=lambda: None,
        )
        # success line seen before ConnectionError, should return normally
        tpc_mod._parse_tpc_body(resp)

    def test_connection_error_without_success(self):
        """ConnectionError without prior success should propagate."""

        def _iter_lines(decode_unicode=True):
            yield "Perf Marker"
            raise ConnectionError("connection reset")

        resp = SimpleNamespace(
            status_code=202,
            iter_lines=_iter_lines,
            raise_for_status=lambda: None,
            close=lambda: None,
        )
        with pytest.raises(ConnectionError, match="connection reset"):
            tpc_mod._parse_tpc_body(resp)

    def test_failure_at_body_end(self):
        """Body ends with a failure line after iter_lines completes."""
        lines = [
            "Perf Marker",
            "  Stripe Bytes Transferred: 100",
            "End",
            "failure: transfer aborted",
        ]
        resp = self._make_resp(202, lines)
        with pytest.raises(OSError, match="transfer aborted"):
            tpc_mod._parse_tpc_body(resp)

    def test_empty_body_treated_as_success(self):
        resp = self._make_resp(200, [])
        tpc_mod._parse_tpc_body(resp)  # should not raise

    def test_invalid_stripe_bytes_ignored(self):
        """Non-numeric Stripe Bytes Transferred value should be silently ignored."""
        lines = [
            "Perf Marker",
            "  Stripe Bytes Transferred: not-a-number",
            "End",
            "success: ok",
        ]
        resp = self._make_resp(202, lines)
        received = []
        tpc_mod._parse_tpc_body(resp, progress_callback=received.append)
        # marker_bytes stays 0, so callback not called
        assert received == []

    def test_close_called_even_on_error(self):
        """resp.close() should be called in the finally block."""
        closed = []
        resp = SimpleNamespace(
            status_code=202,
            iter_lines=lambda decode_unicode=True: iter(["failure: server error"]),
            raise_for_status=lambda: None,
            close=lambda: closed.append(True),
        )
        with pytest.raises(OSError):
            tpc_mod._parse_tpc_body(resp)
        assert closed == [True]

    def test_failure_line_after_end_of_body(self):
        """Body ends without success/failure, but last line is failure."""
        lines = ["failure: quota exceeded"]
        resp = self._make_resp(202, lines)
        with pytest.raises(OSError, match="quota exceeded"):
            tpc_mod._parse_tpc_body(resp)


# ---------------------------------------------------------------------------
# tpc.py — _http_tpc additional branches
# ---------------------------------------------------------------------------


class TestHttpTpcExtra:
    def _make_session(self, status_code=201, lines=None):
        resp = SimpleNamespace(
            status_code=status_code,
            iter_lines=lambda decode_unicode=True: iter(lines or []),
            raise_for_status=lambda: None,
            close=lambda: None,
        )
        session = MagicMock()
        session.request.return_value = resp
        return session, resp

    def test_push_mode_headers(self):
        session, _ = self._make_session(201)
        with patch.object(tpc_mod, "_build_session", return_value=session):
            result = tpc_mod._http_tpc(
                "https://src.example.com/file",
                "https://dst.example.com/file",
                {},
                mode="push",
                timeout=None,
                verbose=False,
                scitag=None,
            )
        assert result is True
        args, kwargs = session.request.call_args
        assert args[1] == "https://src.example.com/file"
        assert kwargs["headers"]["Destination"] == "https://dst.example.com/file"
        assert "Source" not in kwargs["headers"]

    def test_verbose_pull(self, capsys):
        session, _ = self._make_session(201)
        with patch.object(tpc_mod, "_build_session", return_value=session):
            tpc_mod._http_tpc(
                "https://src.example.com/file",
                "https://dst.example.com/file",
                {},
                mode="pull",
                timeout=None,
                verbose=True,
                scitag=None,
            )
        captured = capsys.readouterr()
        assert "[TPC pull]" in captured.err

    def test_verbose_push(self, capsys):
        session, _ = self._make_session(201)
        with patch.object(tpc_mod, "_build_session", return_value=session):
            tpc_mod._http_tpc(
                "https://src.example.com/file",
                "https://dst.example.com/file",
                {},
                mode="push",
                timeout=None,
                verbose=True,
                scitag=None,
            )
        captured = capsys.readouterr()
        assert "[TPC push]" in captured.err

    def test_scitag_header(self):
        session, _ = self._make_session(201)
        with patch.object(tpc_mod, "_build_session", return_value=session):
            tpc_mod._http_tpc(
                "https://src.example.com/file",
                "https://dst.example.com/file",
                {},
                mode="pull",
                timeout=None,
                verbose=False,
                scitag=42,
            )
        _, kwargs = session.request.call_args
        assert kwargs["headers"]["SciTag"] == "42"

    def test_start_callback_called(self):
        session, _ = self._make_session(201)
        called = []
        with patch.object(tpc_mod, "_build_session", return_value=session):
            tpc_mod._http_tpc(
                "https://src.example.com/file",
                "https://dst.example.com/file",
                {},
                mode="pull",
                timeout=None,
                verbose=False,
                scitag=None,
                start_callback=lambda: called.append(True),
            )
        assert called == [True]


# ---------------------------------------------------------------------------
# tpc.py — _xrootd_tpc branches
# ---------------------------------------------------------------------------


class TestXrootdTpc:
    def test_import_error_raises_not_implemented(self, monkeypatch):
        """When XRootD is not installed, _xrootd_tpc raises NotImplementedError."""
        import builtins

        original_import = builtins.__import__

        def _fake_import(name, *args, **kwargs):
            if name == "XRootD" or name.startswith("XRootD."):
                raise ImportError("No module named 'XRootD'")
            return original_import(name, *args, **kwargs)

        monkeypatch.setattr(builtins, "__import__", _fake_import)
        with pytest.raises(
            NotImplementedError, match="XRootD support is not installed"
        ):
            tpc_mod._xrootd_tpc(
                "root://src.example.com//file",
                "root://dst.example.com//file",
                timeout=None,
                verbose=False,
            )

    def _setup_xrootd(self, monkeypatch, fake_process):
        """Set up fake XRootD modules so ``from XRootD import client`` works."""
        fake_client = SimpleNamespace(CopyProcess=lambda: fake_process)
        fake_xrootd = SimpleNamespace(client=fake_client)
        monkeypatch.setitem(sys.modules, "XRootD", fake_xrootd)
        monkeypatch.setitem(sys.modules, "XRootD.client", fake_client)

    def test_verbose_output(self, monkeypatch, capsys):
        """Verbose mode writes to stderr."""
        ok_status = SimpleNamespace(ok=True, message="")

        fake_process = MagicMock()
        fake_process.prepare.return_value = ok_status
        fake_process.run.return_value = (ok_status, None)

        self._setup_xrootd(monkeypatch, fake_process)

        tpc_mod._xrootd_tpc(
            "root://src.example.com//file",
            "root://dst.example.com//file",
            timeout=None,
            verbose=True,
        )
        captured = capsys.readouterr()
        assert "[TPC xrootd]" in captured.err

    def test_timeout_passed(self, monkeypatch):
        ok_status = SimpleNamespace(ok=True, message="")
        fake_process = MagicMock()
        fake_process.prepare.return_value = ok_status
        fake_process.run.return_value = (ok_status, None)

        self._setup_xrootd(monkeypatch, fake_process)

        tpc_mod._xrootd_tpc(
            "root://src.example.com//file",
            "root://dst.example.com//file",
            timeout=60,
            verbose=False,
        )
        _, kwargs = fake_process.add_job.call_args
        assert kwargs["tpctimeout"] == 60

    def test_prepare_failure_raises_oserror(self, monkeypatch):
        fail_status = SimpleNamespace(ok=False, message="prepare error")
        fake_process = MagicMock()
        fake_process.prepare.return_value = fail_status

        self._setup_xrootd(monkeypatch, fake_process)

        with pytest.raises(OSError, match="prepare failed"):
            tpc_mod._xrootd_tpc(
                "root://src.example.com//file",
                "root://dst.example.com//file",
                timeout=None,
                verbose=False,
            )

    def test_prepare_tpc_not_supported_raises_not_implemented(self, monkeypatch):
        fail_status = SimpleNamespace(
            ok=False, message="TPC not supported by this server"
        )
        fake_process = MagicMock()
        fake_process.prepare.return_value = fail_status

        self._setup_xrootd(monkeypatch, fake_process)

        with pytest.raises(NotImplementedError, match="TPC not supported"):
            tpc_mod._xrootd_tpc(
                "root://src.example.com//file",
                "root://dst.example.com//file",
                timeout=None,
                verbose=False,
            )

    def test_run_failure_raises_oserror(self, monkeypatch):
        ok_status = SimpleNamespace(ok=True, message="")
        fail_status = SimpleNamespace(ok=False, message="transfer error")
        fake_process = MagicMock()
        fake_process.prepare.return_value = ok_status
        fake_process.run.return_value = (fail_status, None)

        self._setup_xrootd(monkeypatch, fake_process)

        with pytest.raises(OSError, match="TPC failed"):
            tpc_mod._xrootd_tpc(
                "root://src.example.com//file",
                "root://dst.example.com//file",
                timeout=None,
                verbose=False,
            )

    def test_run_tpc_not_supported_raises_not_implemented(self, monkeypatch):
        ok_status = SimpleNamespace(ok=True, message="")
        fail_status = SimpleNamespace(
            ok=False, message="TPC not supported on this endpoint"
        )
        fake_process = MagicMock()
        fake_process.prepare.return_value = ok_status
        fake_process.run.return_value = (fail_status, None)

        self._setup_xrootd(monkeypatch, fake_process)

        with pytest.raises(NotImplementedError, match="TPC not supported"):
            tpc_mod._xrootd_tpc(
                "root://src.example.com//file",
                "root://dst.example.com//file",
                timeout=None,
                verbose=False,
            )

    def test_prepare_returns_tuple(self, monkeypatch):
        """prepare() may return a tuple (status, ...) instead of a bare status."""
        ok_status = SimpleNamespace(ok=True, message="")
        fake_process = MagicMock()
        fake_process.prepare.return_value = (ok_status, None)
        fake_process.run.return_value = (ok_status, None)

        self._setup_xrootd(monkeypatch, fake_process)

        result = tpc_mod._xrootd_tpc(
            "root://src.example.com//file",
            "root://dst.example.com//file",
            timeout=None,
            verbose=False,
        )
        assert result is True

    def test_run_returns_bare_status(self, monkeypatch):
        """run() may return a bare status instead of a tuple."""
        ok_status = SimpleNamespace(ok=True, message="")
        fake_process = MagicMock()
        fake_process.prepare.return_value = ok_status
        fake_process.run.return_value = ok_status

        self._setup_xrootd(monkeypatch, fake_process)

        result = tpc_mod._xrootd_tpc(
            "root://src.example.com//file",
            "root://dst.example.com//file",
            timeout=None,
            verbose=False,
        )
        assert result is True

    def test_job_results_failure(self, monkeypatch):
        """Individual job results with a failing status should raise."""
        ok_status = SimpleNamespace(ok=True, message="")
        fail_job_status = SimpleNamespace(ok=False, message="checksum mismatch")
        fake_process = MagicMock()
        fake_process.prepare.return_value = ok_status
        fake_process.run.return_value = (
            ok_status,
            [SimpleNamespace(status=fail_job_status)],
        )

        self._setup_xrootd(monkeypatch, fake_process)

        with pytest.raises(OSError, match="job failed.*checksum mismatch"):
            tpc_mod._xrootd_tpc(
                "root://src.example.com//file",
                "root://dst.example.com//file",
                timeout=None,
                verbose=False,
            )

    def test_job_results_tpc_not_supported(self, monkeypatch):
        """Job result with 'tpc not supported' raises NotImplementedError."""
        ok_status = SimpleNamespace(ok=True, message="")
        fail_job = SimpleNamespace(ok=False, message="TPC not supported for this path")
        fake_process = MagicMock()
        fake_process.prepare.return_value = ok_status
        fake_process.run.return_value = (
            ok_status,
            [SimpleNamespace(status=fail_job)],
        )

        self._setup_xrootd(monkeypatch, fake_process)

        with pytest.raises(NotImplementedError, match="TPC not supported"):
            tpc_mod._xrootd_tpc(
                "root://src.example.com//file",
                "root://dst.example.com//file",
                timeout=None,
                verbose=False,
            )

    def test_start_callback_called(self, monkeypatch):
        ok_status = SimpleNamespace(ok=True, message="")
        fake_process = MagicMock()
        fake_process.prepare.return_value = ok_status
        fake_process.run.return_value = (ok_status, None)

        self._setup_xrootd(monkeypatch, fake_process)

        called = []
        tpc_mod._xrootd_tpc(
            "root://src.example.com//file",
            "root://dst.example.com//file",
            timeout=None,
            verbose=False,
            start_callback=lambda: called.append(True),
        )
        assert called == [True]

    def test_job_results_ok_status(self, monkeypatch):
        """Job result with ok status should succeed."""
        ok_status = SimpleNamespace(ok=True, message="")
        ok_job = SimpleNamespace(ok=True, message="")
        fake_process = MagicMock()
        fake_process.prepare.return_value = ok_status
        fake_process.run.return_value = (
            ok_status,
            [SimpleNamespace(status=ok_job)],
        )

        self._setup_xrootd(monkeypatch, fake_process)

        result = tpc_mod._xrootd_tpc(
            "root://src.example.com//file",
            "root://dst.example.com//file",
            timeout=None,
            verbose=False,
        )
        assert result is True

    def test_job_result_no_status_attribute(self, monkeypatch):
        """Job result with no status attribute should be treated as OK."""
        ok_status = SimpleNamespace(ok=True, message="")
        fake_process = MagicMock()
        fake_process.prepare.return_value = ok_status
        fake_process.run.return_value = (
            ok_status,
            [SimpleNamespace()],  # no status attribute
        )

        self._setup_xrootd(monkeypatch, fake_process)

        result = tpc_mod._xrootd_tpc(
            "root://src.example.com//file",
            "root://dst.example.com//file",
            timeout=None,
            verbose=False,
        )
        assert result is True
