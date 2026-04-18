import contextlib
import datetime
import math
import shutil
import struct
import sys
import threading
import time
from types import SimpleNamespace

try:
    import fcntl
    import termios

    _HAS_FCNTL = True
except ImportError:
    _HAS_FCNTL = False


from fsspec.callbacks import Callback

from gfal.cli.base import get_console, is_gfal2_compat


def _format_hms(total_seconds):
    total_seconds = max(0, int(total_seconds))
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{hours}:{minutes:02d}:{seconds:02d}"


def Progress(label, tui_callback=None):
    if tui_callback:
        return TuiProgress(tui_callback)
    if is_gfal2_compat():
        return LegacyProgress(label)
    return RichProgress(label)


def CountProgress(label, total):
    if is_gfal2_compat():
        return LegacyCountProgress(label, total)
    return RichCountProgress(label, total)


def Spinner(label):
    if is_gfal2_compat():
        return LegacySpinner(label)
    return RichSpinner(label)


def _active_live_manager():
    if is_gfal2_compat():
        return None

    for progress_cls in (RichProgress, RichCountProgress):
        manager = getattr(progress_cls, "_shared", None)
        if (
            manager is not None
            and getattr(manager, "started", False)
            and getattr(manager, "active", 0) > 0
        ):
            return manager
    return None


def print_live_message(message):
    manager = _active_live_manager()
    if manager is not None:
        with manager.lock:
            if getattr(manager, "kind", None) == "count" and manager.started:
                with contextlib.suppress(Exception):
                    manager.progress.stop()
                manager.started = False
                manager.progress.console.print(message, markup=False, highlight=False)
                with contextlib.suppress(Exception):
                    manager.progress.start()
                manager.started = True
            else:
                manager.progress.console.print(message, markup=False, highlight=False)
            manager.progress.refresh()
        return

    print(message)


def _final_status_text(label, success, status=None):
    outcome = "SKIPPED" if status == "skipped" else "DONE" if success else "FAILED"
    return f"{label} [{outcome}]"


def _should_emit_live_final_message(success, status=None):
    del success, status
    return True


def has_live_progress():
    """Return True when Rich progress is currently managing live output."""
    return _active_live_manager() is not None


class TuiProgress(Callback):
    """Callback that bridges fsspec's callback API to the TUI progress modal.

    fsspec's ``get()``/``put()`` use the callback at **two levels**:

    1. **Parent** (this instance): ``set_size(num_files)`` and
       ``relative_update(1)`` per file — tracks *file count*.
    2. **Child** (returned by ``branched()``): ``set_size(byte_total)`` and
       ``relative_update(chunk_bytes)`` — tracks *bytes* for each file.

    We only care about byte-level progress, so:
    - ``branched()`` returns a child ``TuiProgress`` that shares the same
      user-facing callback.
    - The parent silently ignores the file-count updates.
    """

    def __init__(self, callback, *, _is_child=False):
        super().__init__()
        self.callback = callback
        self.size = 0
        self.value = 0
        self._is_child = _is_child

    # -- fsspec branching: create a child for per-file byte progress ----------

    def branched(self, path_1, path_2, **kwargs):
        """Return a child callback that tracks byte-level progress."""
        return TuiProgress(self.callback, _is_child=True)

    # -- progress updates -----------------------------------------------------

    def set_size(self, size):
        if not self._is_child:
            return  # ignore file-count size from get()/put()
        self.size = size or 0
        self._trigger()

    def relative_update(self, inc=1):
        if not self._is_child:
            return  # ignore file-count ticks from get()/put()
        self.value += inc
        self._trigger()

    def absolute_update(self, value):
        self.value = value
        self._trigger()

    def _trigger(self):
        if self.callback:
            self.callback(self.value, self.size)

    def branch_coro(self, coro):
        return coro

    def stop(self, success=True, status=None):
        del status
        if self.callback:
            self.callback(self.value, self.size, finished=True, success=success)

    @property
    def total(self):
        return self.size or 0

    @property
    def current(self):
        return self.value


class RichProgress:
    _shared = None
    _shared_init_lock = threading.Lock()

    def __init__(self, label):
        self.label = label
        self._started_flag = False
        self.task_id = None
        self._started_at = None

    @classmethod
    def _manager(cls):
        if cls._shared is None:
            with cls._shared_init_lock:
                if cls._shared is None:
                    from rich.progress import (
                        BarColumn,
                        DownloadColumn,
                        SpinnerColumn,
                        TextColumn,
                        TimeElapsedColumn,
                        TransferSpeedColumn,
                    )
                    from rich.progress import (
                        Progress as _RichProgress,
                    )
                    from rich.text import Text

                    class _PinnedElapsedColumn(TimeElapsedColumn):
                        def render(self, task):
                            final_elapsed = task.fields.get("final_elapsed")
                            if final_elapsed:
                                return Text(final_elapsed, style="progress.elapsed")
                            return super().render(task)

                    cls._shared = SimpleNamespace(
                        lock=threading.Lock(),
                        progress=_RichProgress(
                            SpinnerColumn(),
                            TextColumn("[progress.description]{task.description}"),
                            BarColumn(),
                            DownloadColumn(),
                            TransferSpeedColumn(),
                            _PinnedElapsedColumn(),
                            console=get_console(stderr=False),
                            expand=True,
                            transient=False,
                            refresh_per_second=20,
                        ),
                        kind="progress",
                        started=False,
                        active=0,
                    )
        return cls._shared

    def start(self):
        manager = self._manager()
        with manager.lock:
            if self._started_flag:
                return
            if not manager.started:
                manager.progress.start()
                manager.started = True
            self.task_id = manager.progress.add_task(self.label, total=None)
            self._started_at = time.monotonic()
            manager.active += 1
            self._started_flag = True
            manager.progress.refresh()

    def update(self, curr_size=None, total_size=None, rate=None, elapsed=None):
        manager = self._manager()
        with manager.lock:
            if not self._started_flag:
                return
            kwargs = {}
            if curr_size is not None:
                kwargs["completed"] = curr_size
            if total_size is not None:
                kwargs["total"] = total_size
            manager.progress.update(self.task_id, **kwargs)
            manager.progress.refresh()

    def set_description(self, label):
        self.label = label
        manager = self._manager()
        with manager.lock:
            if not self._started_flag:
                return
            manager.progress.update(self.task_id, description=label)
            manager.progress.refresh()

    def stop(self, success, status=None):
        manager = self._manager()
        with manager.lock:
            if not self._started_flag:
                return
            self._started_flag = False
            with contextlib.suppress(IndexError, KeyError, AttributeError):
                task = manager.progress.tasks[self.task_id]
                elapsed_text = (
                    _format_hms(time.monotonic() - self._started_at)
                    if self._started_at is not None
                    else None
                )
                if success and task.total is not None:
                    manager.progress.update(
                        self.task_id,
                        completed=task.total,
                        final_elapsed=elapsed_text,
                    )
                elif elapsed_text is not None and status != "skipped":
                    manager.progress.update(
                        self.task_id,
                        final_elapsed=elapsed_text,
                    )
                final_message = _final_status_text(self.label, success, status)
                removed = False
                remove_task = getattr(manager.progress, "remove_task", None)
                if remove_task is not None:
                    with contextlib.suppress(Exception):
                        remove_task(self.task_id)
                        removed = True
                if not removed:
                    with contextlib.suppress(Exception):
                        manager.progress.stop_task(self.task_id)
                    if status == "skipped":
                        manager.progress.update(
                            self.task_id,
                            description=f"{self.label} [yellow]\\[SKIPPED][/]",
                        )
                    elif success:
                        manager.progress.update(
                            self.task_id, description=f"{self.label} [green]\\[DONE][/]"
                        )
                    else:
                        manager.progress.update(
                            self.task_id,
                            description=f"{self.label} [red]\\[FAILED][/]",
                        )
                manager.progress.refresh()
                console = getattr(manager.progress, "console", None)
                if console is not None and _should_emit_live_final_message(
                    success, status
                ):
                    with contextlib.suppress(Exception):
                        console.print(final_message, markup=False, highlight=False)
            manager.progress.refresh()
            self._started_at = None
            manager.active = max(0, manager.active - 1)
            if manager.started and manager.active == 0:
                manager.progress.stop()
                manager.started = False


class RichSpinner:
    def __init__(self, label):
        self.label = label
        self._started_flag = False
        self._status = None

    def start(self):
        if self._started_flag:
            return
        console = get_console(stderr=False)
        self._status = console.status(self.label)
        self._status.start()
        self._started_flag = True

    def stop(self, success=True, status=None):
        del success, status
        if not self._started_flag:
            return
        self._started_flag = False
        with contextlib.suppress(Exception):
            self._status.stop()


class RichCountProgress:
    _shared = None
    _shared_init_lock = threading.Lock()

    def __init__(self, label, total):
        self.label = label
        self.total = total
        self._started_flag = False
        self.task_id = None

    @classmethod
    def _manager(cls):
        if cls._shared is None:
            with cls._shared_init_lock:
                if cls._shared is None:
                    from rich.progress import (
                        BarColumn,
                        SpinnerColumn,
                        TextColumn,
                        TimeElapsedColumn,
                    )
                    from rich.progress import Progress as _RichProgress

                    cls._shared = SimpleNamespace(
                        lock=threading.Lock(),
                        progress=_RichProgress(
                            SpinnerColumn(),
                            TextColumn("[progress.description]{task.description}"),
                            BarColumn(),
                            TextColumn("{task.completed}/{task.total} files"),
                            TimeElapsedColumn(),
                            console=get_console(stderr=False),
                            expand=True,
                            transient=True,
                            refresh_per_second=10,
                            redirect_stdout=False,
                            redirect_stderr=False,
                        ),
                        kind="count",
                        started=False,
                        active=0,
                    )
        return cls._shared

    def start(self):
        manager = self._manager()
        with manager.lock:
            if self._started_flag:
                return
            if not manager.started:
                manager.progress.start()
                manager.started = True
            self.task_id = manager.progress.add_task(self.label, total=self.total)
            manager.active += 1
            self._started_flag = True
            manager.progress.refresh()

    def update(self, completed=None, total=None):
        manager = self._manager()
        with manager.lock:
            if not self._started_flag:
                return
            kwargs = {}
            if completed is not None:
                kwargs["completed"] = completed
            if total is not None:
                kwargs["total"] = total
            manager.progress.update(self.task_id, **kwargs)
            manager.progress.refresh()

    def stop(self, success=True, status=None):
        del success, status
        manager = self._manager()
        with manager.lock:
            if not self._started_flag:
                return
            self._started_flag = False
            remove_task = getattr(manager.progress, "remove_task", None)
            if remove_task is not None:
                with contextlib.suppress(Exception):
                    remove_task(self.task_id)
            else:
                with contextlib.suppress(Exception):
                    manager.progress.stop_task(self.task_id)
            manager.progress.refresh()
            manager.active = max(0, manager.active - 1)
            if manager.started and manager.active == 0:
                manager.progress.stop()
                manager.started = False


class LegacyProgress:
    def __init__(self, label):
        self.label = label
        self.started = False
        self.stopped = False
        self.status = None
        self.lock = threading.Lock()

    def start(self):
        with self.lock:
            if self.started or self.stopped:
                return
            self.started = True
        self.start_time = datetime.datetime.now()
        self.dots = 0
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def _run(self):
        while True:
            with self.lock:
                if self.stopped:
                    break
                self._render()
            time.sleep(0.5)

    def _render(self):
        elapsed = (datetime.datetime.now() - self.start_time).total_seconds()
        sys.stdout.write("\r")
        label = self.label + ("." * self.dots).ljust(3)
        time_str = f"  {elapsed:.0f}s "
        sys.stdout.write(label + time_str)

        s = self.status
        if s:
            width = self._terminal_width()
            if s.get("percentage") is not None:
                pct_str = f"{int(round(s['percentage']))}% "
                rate_str = self._rate_str(s["rate"])
                size_str = " " + self._size_str(s["curr_size"]) + " "
                used = (
                    len(label)
                    + len(time_str)
                    + len(pct_str)
                    + len(size_str)
                    + len(rate_str)
                )
                bar_w = max(5, width - used - 2)
                filled = max(1, int(round(s["percentage"] * bar_w / 100.0)))
                bar = "[" + "=" * (filled - 1) + ">" + " " * (bar_w - filled) + "]"
                sys.stdout.write(pct_str + bar + size_str + rate_str)
            elif s.get("total_size"):
                sys.stdout.write(
                    " File size: {}".format(self._size_str(s["total_size"]))
                )
            elif s.get("curr_size"):
                sys.stdout.write(self._size_str(s["curr_size"]))
                if s.get("rate"):
                    sys.stdout.write(" " + self._rate_str(s["rate"]))

        sys.stdout.flush()
        self.dots = (self.dots + 1) % 4

    def update(self, curr_size=None, total_size=None, rate=None, elapsed=None):
        with self.lock:
            self.status = {}
            if curr_size is not None:
                self.status["curr_size"] = curr_size
            if total_size is not None:
                self.status["total_size"] = total_size
            if curr_size and elapsed and total_size and elapsed > 0:
                self.status["rate"] = curr_size / elapsed
                self.status["percentage"] = (curr_size / total_size) * 100.0
            elif rate is not None:
                self.status["rate"] = rate

    def stop(self, success, status=None):
        if not self.started:
            return
        with self.lock:
            self.stopped = True
        if hasattr(self, "_thread"):
            self._thread.join(timeout=2)
        elapsed = (datetime.datetime.now() - self.start_time).total_seconds()
        outcome = "SKIPPED" if status == "skipped" else "DONE" if success else "FAILED"
        msg = f"{self.label}   [{outcome}]  after {elapsed:.0f}s"
        sys.stdout.write("\r" + msg + " " * max(0, self._terminal_width() - len(msg)))
        sys.stdout.flush()

    @staticmethod
    def _terminal_width():
        if _HAS_FCNTL:
            try:
                data = fcntl.ioctl(
                    sys.stdin.fileno(),
                    termios.TIOCGWINSZ,
                    struct.pack("HHHH", 0, 0, 0, 0),
                )
                _, w, _, _ = struct.unpack("HHHH", data)
                return w
            except Exception:
                pass
        return shutil.get_terminal_size(fallback=(80, 24)).columns

    @staticmethod
    def _rate_str(rate):
        symbols = ["B", "K", "M", "G", "T", "P"]
        deg = 0
        while float(rate) >= 1024.0 and deg < len(symbols) - 1:
            rate = float(rate) / 1024.0
            deg += 1
        digits = len(str(math.floor(rate)))
        prec = max(0, 3 - digits) if deg > 0 else 0
        return f"{round(rate, prec):.{prec}f}{symbols[deg]}/s"

    @staticmethod
    def _size_str(size):
        s = LegacyProgress._rate_str(size)
        s = s[:-2]
        if not s.endswith("B"):
            s += "B"
        return s


class LegacySpinner:
    def __init__(self, label):
        self.label = label
        self._progress = LegacyProgress(label)

    def start(self):
        self._progress.start()

    def stop(self, success=True, status=None):
        self._progress.stop(success, status=status)


class LegacyCountProgress:
    def __init__(self, label, total):
        del total
        self._spinner = LegacySpinner(label)

    def start(self):
        self._spinner.start()

    def update(self, completed=None, total=None):
        del completed, total

    def stop(self, success=True, status=None):
        self._spinner.stop(success=success, status=status)
