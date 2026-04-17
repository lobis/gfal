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


def Progress(label, tui_callback=None):
    if tui_callback:
        return TuiProgress(tui_callback)
    if is_gfal2_compat():
        return LegacyProgress(label)
    return RichProgress(label)


def Spinner(label):
    if is_gfal2_compat():
        return LegacySpinner(label)
    return RichSpinner(label)


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

    def stop(self, success=True):
        if self.callback:
            self.callback(self.value, self.size, finished=True, success=success)

    @property
    def total(self):
        return self.size or 0

    @property
    def current(self):
        return self.value


class RichProgress:
    def __init__(self, label):
        self.label = label
        self._started_flag = False
        self.task_id = None

    @classmethod
    def _manager(cls):
        if not hasattr(cls, "_shared"):
            from rich.progress import (
                BarColumn,
                DownloadColumn,
                SpinnerColumn,
                TextColumn,
                TimeRemainingColumn,
                TransferSpeedColumn,
            )
            from rich.progress import (
                Progress as _RichProgress,
            )

            cls._shared = SimpleNamespace(
                lock=threading.Lock(),
                progress=_RichProgress(
                    SpinnerColumn(),
                    TextColumn("[progress.description]{task.description}"),
                    BarColumn(),
                    DownloadColumn(),
                    TransferSpeedColumn(),
                    TimeRemainingColumn(),
                    console=get_console(stderr=False),
                    expand=True,
                    transient=False,
                    refresh_per_second=20,
                ),
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

    def stop(self, success):
        manager = self._manager()
        with manager.lock:
            if not self._started_flag:
                return
            self._started_flag = False
            try:
                task = manager.progress.tasks[self.task_id]
                if success and task.total is not None:
                    manager.progress.update(self.task_id, completed=task.total)
                with contextlib.suppress(Exception):
                    manager.progress.stop_task(self.task_id)
                if success:
                    manager.progress.update(
                        self.task_id, description=f"{self.label} [green]\\[DONE][/]"
                    )
                else:
                    manager.progress.update(
                        self.task_id, description=f"{self.label} [red]\\[FAILED][/]"
                    )
            except Exception:
                pass
            manager.progress.refresh()
            manager.active = max(0, manager.active - 1)
            if manager.started and manager.active == 0:
                manager.progress.stop()
                manager.started = False


class RichSpinner:
    def __init__(self, label):
        self.label = label
        self._started_flag = False
        self.task_id = None

    def start(self):
        manager = RichProgress._manager()
        with manager.lock:
            if self._started_flag:
                return
            if not manager.started:
                manager.progress.start()
                manager.started = True
            self.task_id = manager.progress.add_task(self.label, total=None)
            manager.active += 1
            self._started_flag = True
            manager.progress.refresh()

    def stop(self, success=True):
        del success
        manager = RichProgress._manager()
        with manager.lock:
            if not self._started_flag:
                return
            self._started_flag = False
            with contextlib.suppress(Exception):
                manager.progress.remove_task(self.task_id)
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

    def stop(self, success):
        if not self.started:
            return
        with self.lock:
            self.stopped = True
        if hasattr(self, "_thread"):
            self._thread.join(timeout=2)
        elapsed = (datetime.datetime.now() - self.start_time).total_seconds()
        outcome = "DONE" if success else "FAILED"
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

    def stop(self, success=True):
        self._progress.stop(success)
