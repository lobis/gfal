"""Tests for gfal_cli.progress — Progress bar unit tests."""

import threading
from types import SimpleNamespace

from gfal.cli.progress import LegacyProgress as Progress
from gfal.cli.progress import RichProgress, RichSpinner, print_live_message


class TestProgressInit:
    def test_initial_state(self):
        p = Progress("test")
        assert p.label == "test"
        assert not p.started
        assert not p.stopped
        assert p.status is None

    def test_update_sets_status(self):
        p = Progress("test")
        p.update(curr_size=1024, total_size=2048)
        assert p.status is not None
        assert p.status["curr_size"] == 1024
        assert p.status["total_size"] == 2048

    def test_update_computes_percentage(self):
        p = Progress("test")
        p.update(curr_size=500, total_size=1000, elapsed=1.0)
        assert p.status["percentage"] == 50.0
        assert p.status["rate"] == 500.0

    def test_update_zero_elapsed(self):
        p = Progress("test")
        p.update(curr_size=500, total_size=1000, elapsed=0)
        assert "percentage" not in p.status

    def test_stop_without_start(self):
        """stop() on never-started Progress should be harmless."""
        p = Progress("test")
        p.stop(True)  # Should not raise


class TestProgressRateStr:
    def test_bytes_per_second(self):
        assert Progress._rate_str(100) == "100B/s"

    def test_kilobytes(self):
        result = Progress._rate_str(1024)
        assert "K" in result
        assert "/s" in result

    def test_megabytes(self):
        result = Progress._rate_str(1024 * 1024)
        assert "M" in result

    def test_gigabytes(self):
        result = Progress._rate_str(1024**3)
        assert "G" in result


class TestProgressSizeStr:
    def test_bytes(self):
        result = Progress._size_str(100)
        assert "B" in result
        assert "/s" not in result

    def test_kilobytes(self):
        result = Progress._size_str(1024)
        assert "KB" in result or "K" in result


class TestProgressSizeStrFull:
    def test_zero(self):
        result = Progress._size_str(0)
        assert result == "0B"

    def test_exactly_1kb(self):
        result = Progress._size_str(1024)
        assert "K" in result
        assert "B" in result

    def test_megabytes(self):
        result = Progress._size_str(1024 * 1024)
        assert "M" in result
        assert "B" in result

    def test_gigabytes(self):
        result = Progress._size_str(1024**3)
        assert "G" in result
        assert "B" in result

    def test_no_per_second(self):
        """_size_str must not contain '/s'."""
        for size in [0, 100, 1024, 1024**2, 1024**3]:
            assert "/s" not in Progress._size_str(size)

    def test_always_ends_with_b(self):
        """_size_str always ends with 'B'."""
        for size in [0, 100, 1024, 1024**2]:
            assert Progress._size_str(size).endswith("B")


class TestProgressRateStrFull:
    def test_zero(self):
        result = Progress._rate_str(0)
        assert "/s" in result
        assert "B" in result

    def test_exactly_1kb(self):
        result = Progress._rate_str(1024)
        assert "K" in result
        assert "/s" in result

    def test_gigabytes(self):
        result = Progress._rate_str(1024**3)
        assert "G" in result

    def test_terabytes(self):
        result = Progress._rate_str(1024**4)
        assert "T" in result

    def test_always_ends_with_s(self):
        for rate in [0, 500, 1024, 1024**2]:
            assert Progress._rate_str(rate).endswith("/s")

    def test_high_value_compact(self):
        """100 KB/s should not show decimal places."""
        result = Progress._rate_str(100 * 1024)
        assert "." not in result


class TestProgressLifecycle:
    def test_start_sets_started_flag(self):
        p = Progress("test")
        p.start()
        assert p.started
        p.stop(True)

    def test_start_twice_is_safe(self):
        """Calling start() again on an already-started Progress is a no-op."""
        p = Progress("test")
        p.start()
        p.start()  # should not raise or start a second thread
        p.stop(True)

    def test_stop_before_start_is_safe(self):
        """stop() on a never-started Progress must not raise."""
        p = Progress("test")
        p.stop(True)

    def test_stop_sets_stopped_flag(self):
        p = Progress("test")
        p.start()
        p.stop(True)
        assert p.stopped

    def test_stop_writes_done(self, capsys):
        p = Progress("Copying myfile.txt")
        p.start()
        p.stop(True)
        captured = capsys.readouterr()
        assert "[DONE]" in captured.out
        assert "Copying myfile.txt" in captured.out

    def test_stop_writes_failed(self, capsys):
        p = Progress("Copying myfile.txt")
        p.start()
        p.stop(False)
        captured = capsys.readouterr()
        assert "[FAILED]" in captured.out

    def test_stop_writes_skipped(self, capsys):
        p = Progress("Copying myfile.txt")
        p.start()
        p.stop(True, status="skipped")
        captured = capsys.readouterr()
        assert "[SKIPPED]" in captured.out

    def test_stop_shows_elapsed(self, capsys):
        p = Progress("X")
        p.start()
        p.stop(True)
        captured = capsys.readouterr()
        assert "after" in captured.out
        assert "s" in captured.out


class TestProgressUpdateEdgeCases:
    def test_update_only_total_no_percentage(self):
        p = Progress("test")
        p.update(total_size=1024)
        assert p.status["total_size"] == 1024
        assert "percentage" not in p.status

    def test_update_curr_zero_no_percentage(self):
        """curr_size=0 with elapsed>0 should not produce a percentage."""
        p = Progress("test")
        p.update(curr_size=0, total_size=1000, elapsed=1.0)
        assert "percentage" not in p.status

    def test_update_replaces_previous(self):
        p = Progress("test")
        p.update(curr_size=100, total_size=1000, elapsed=1.0)
        p.update(curr_size=500, total_size=1000, elapsed=2.0)
        assert p.status["curr_size"] == 500

    def test_update_with_explicit_rate(self):
        p = Progress("test")
        p.update(rate=1024)
        assert p.status["rate"] == 1024

    def test_update_with_all_params(self):
        p = Progress("test")
        p.update(curr_size=500, total_size=1000, elapsed=1.0)
        assert p.status["percentage"] == 50.0
        assert p.status["rate"] == 500.0


class TestRichProgress:
    def test_rich_progress_refreshes_on_updates_and_stop(self, monkeypatch):
        class _FakeRichBackend:
            def __init__(self):
                self.calls = []
                self.tasks = []

            def start(self):
                self.calls.append(("start",))

            def add_task(self, description, total=None):
                task_id = len(self.tasks)
                self.tasks.append(SimpleNamespace(total=total))
                self.calls.append(("add_task", description, total))
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

        progress = RichProgress("Copying example.txt (TPC pull)")
        progress.start()
        progress.update(total_size=10)
        progress.set_description("Copying example.txt (streamed)")
        progress.stop(True)

        assert ("refresh",) in backend.calls
        assert ("stop_task", 0) in backend.calls
        assert backend.calls[-1] == ("stop",)

    def test_rich_progress_stop_marks_skipped(self, monkeypatch):
        class _FakeRichBackend:
            def __init__(self):
                self.calls = []
                self.tasks = [SimpleNamespace(total=10)]

            def start(self):
                self.calls.append(("start",))

            def add_task(self, description, total=None):
                self.calls.append(("add_task", description, total))
                return 0

            def update(self, task_id, **kwargs):
                self.calls.append(("update", task_id, kwargs))

            def refresh(self):
                self.calls.append(("refresh",))

            def stop_task(self, task_id):
                self.calls.append(("stop_task", task_id))

            def stop(self):
                self.calls.append(("stop",))

        backend = _FakeRichBackend()
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

        progress = RichProgress("Copying example.txt")
        progress.start()
        progress.stop(True, status="skipped")

        assert (
            "update",
            0,
            {"description": "Copying example.txt [yellow]\\[SKIPPED][/]"},
        ) in backend.calls


class TestRichSpinner:
    def test_rich_spinner_uses_status_not_progress_rows(self, monkeypatch):
        calls = []

        class _FakeStatus:
            def start(self):
                calls.append(("start",))

            def stop(self):
                calls.append(("stop",))

        class _FakeConsole:
            def status(self, label):
                calls.append(("status", label))
                return _FakeStatus()

        monkeypatch.setattr(
            "gfal.cli.progress.get_console", lambda stderr=False: _FakeConsole()
        )

        spinner = RichSpinner("Scanning example")
        spinner.start()
        spinner.stop()

        assert calls == [
            ("status", "Scanning example"),
            ("start",),
            ("stop",),
        ]


class TestPrintLiveMessage:
    def test_live_message_uses_active_progress_console(self, monkeypatch):
        printed = []
        refreshed = []

        class _FakeConsole:
            def print(self, message, markup=False, highlight=False):
                printed.append((message, markup, highlight))

        manager = SimpleNamespace(
            lock=threading.Lock(),
            progress=SimpleNamespace(
                console=_FakeConsole(),
                refresh=lambda: refreshed.append(True),
            ),
            started=True,
            active=1,
        )
        monkeypatch.setattr(RichProgress, "_shared", manager, raising=False)

        print_live_message("Skipping existing file dst")

        assert printed == [("Skipping existing file dst", False, False)]
        assert refreshed == [True]
