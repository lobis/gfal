"""Benchmark and streaming-regression coverage for copy operations."""

from __future__ import annotations

import json
import os
import socket
import sys
import threading
import time
import uuid
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Optional

import pytest

from gfal.core.api import AsyncGfalClient, CopyOptions, StatResult
from gfal.core.webdav import _STREAM_EOF, _StreamingRequestsPutFile
from helpers import run_gfal

_PILOT_BASE = "https://eospilot.cern.ch//eos/pilot/opstest/dteam/python3-gfal/tmp"
_PILOT_ROOT_BASE = "root://eospilot.cern.ch//eos/pilot/opstest/dteam/python3-gfal/tmp"
_PUBSRC = (
    "https://eospublic.cern.ch//eos/opendata/atlas/rucio/data16_13TeV/"
    "DAOD_PHYSLITE.37020379._000600.pool.root.1"
)
_PUBSRC_ROOT = (
    "root://eospublic.cern.ch//eos/opendata/atlas/rucio/data16_13TeV/"
    "DAOD_PHYSLITE.37020379._000600.pool.root.1"
)


def _find_proxy() -> Optional[str]:
    proxy = os.environ.get("X509_USER_PROXY", "")
    if proxy and Path(proxy).is_file():
        return proxy
    try:
        uid = os.getuid()
    except AttributeError:
        return None
    default = Path(f"/tmp/x509up_u{uid}")
    if default.is_file():
        return str(default)
    return None


def _eospilot_reachable() -> bool:
    try:
        with socket.create_connection(("eospilot.cern.ch", 443), timeout=5):
            return True
    except OSError:
        return False


requires_benchmark_opt_in = pytest.mark.skipif(
    os.environ.get("GFAL_RUN_BENCHMARKS") != "1",
    reason="set GFAL_RUN_BENCHMARKS=1 to run timing benchmarks",
)


@dataclass(frozen=True)
class _BenchmarkRunner:
    name: str
    argv: list[str]
    env: dict[str, str]


@dataclass(frozen=True)
class _BenchmarkResult:
    scenario: str
    runner: str
    elapsed_s: float
    returncode: int
    stdout_tail: str
    stderr_tail: str


def _tail(text: str, lines: int = 8) -> str:
    parts = text.strip().splitlines()
    return "\n".join(parts[-lines:])


def _bench_command(runner: _BenchmarkRunner, args: list[str]) -> _BenchmarkResult:
    started = time.monotonic()
    rc, out, err = run_gfal_binary_argv(
        runner.argv + args,
        env=runner.env,
        timeout=1800,
    )
    elapsed = time.monotonic() - started
    return _BenchmarkResult(
        scenario="",
        runner=runner.name,
        elapsed_s=round(elapsed, 3),
        returncode=rc,
        stdout_tail=_tail(out),
        stderr_tail=_tail(err),
    )


def run_gfal_binary_argv(argv: list[str], *, env: dict[str, str], timeout: int):
    import subprocess

    proc = subprocess.run(
        argv,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        env=env,
        timeout=timeout,
    )
    return proc.returncode, proc.stdout, proc.stderr


class _StreamingCopyState:
    chunk_count = 3
    chunk_bytes = 1024 * 1024
    delay_s = 0.1

    def __init__(self) -> None:
        self.first_put_byte_at: float | None = None
        self.source_finished_at: float | None = None
        self.put_bytes = 0
        self.lock = threading.Lock()


class _DelayedReader:
    def __init__(self, state: _StreamingCopyState) -> None:
        self._state = state
        self._chunks_remaining = state.chunk_count

    def read(self, _size: int) -> bytes:
        if self._chunks_remaining == 0:
            with self._state.lock:
                self._state.source_finished_at = time.monotonic()
            return b""
        self._chunks_remaining -= 1
        time.sleep(self._state.delay_s)
        return b"x" * self._state.chunk_bytes

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class _FakeSourceFs:
    def __init__(self, state: _StreamingCopyState) -> None:
        self._state = state
        self.open_calls = 0
        self.open_stream_read_calls = 0

    def open(self, _path: str, _mode: str):
        self.open_calls += 1
        return _DelayedReader(self._state)

    def open_stream_read(self, _path: str):
        self.open_stream_read_calls += 1
        return _DelayedReader(self._state)


class _FakeUploadSession:
    def __init__(self, state: _StreamingCopyState) -> None:
        self._state = state

    def request_upload_stream(
        self,
        _method: str,
        _url: str,
        *,
        body_queue,
        content_length=None,
        headers=None,
        timeout=None,
    ):
        import concurrent.futures

        del content_length, headers, timeout
        future: concurrent.futures.Future = concurrent.futures.Future()

        def _consume() -> None:
            try:
                while True:
                    item = body_queue.get(timeout=1)
                    if item is _STREAM_EOF:
                        break
                    with self._state.lock:
                        if self._state.first_put_byte_at is None:
                            self._state.first_put_byte_at = time.monotonic()
                        self._state.put_bytes += len(item)
                response = type("Response", (), {"status_code": 201, "headers": {}})()
                future.set_result(response)
            except Exception as exc:
                future.set_exception(exc)

        threading.Thread(target=_consume, daemon=True).start()
        return future


class _FakeDestinationFs:
    def __init__(self, state: _StreamingCopyState) -> None:
        self._session = _FakeUploadSession(state)

    def open(self, path: str, mode: str):
        return _StreamingRequestsPutFile(self._session, path)

    def open_stream_write(self, path: str, *, content_length: int | None = None):
        del content_length
        return _StreamingRequestsPutFile(self._session, path)


class TestStreamingCopyRegression:
    def test_copy_loop_starts_http_upload_before_source_reaches_eof(
        self,
        monkeypatch,
    ):
        state = _StreamingCopyState()
        client = AsyncGfalClient()
        total_size = state.chunk_count * state.chunk_bytes
        src_st = StatResult.from_info(
            {"name": "src", "size": total_size, "type": "file"}
        )
        fake_src_fs = _FakeSourceFs(state)
        fake_dst_fs = _FakeDestinationFs(state)

        monkeypatch.setattr(
            "gfal.core.api.fs.url_to_fs",
            lambda url, storage_options=None: (fake_dst_fs, url),
        )

        client._copy_file(
            "file:///tmp/source.bin",
            fake_src_fs,
            "/tmp/source.bin",
            "https://example.invalid/sink.bin",
            fake_dst_fs,
            "https://example.invalid/sink.bin",
            src_st,
            CopyOptions(tpc="never"),
            None,
            None,
            None,
            None,
            None,
        )

        assert state.first_put_byte_at is not None
        assert state.source_finished_at is not None
        assert state.put_bytes == total_size
        assert state.first_put_byte_at < state.source_finished_at
        assert fake_src_fs.open_stream_read_calls == 1
        assert fake_src_fs.open_calls == 0


def _assert_benchmark_parity(results: list[_BenchmarkResult]) -> None:
    thresholds = {
        "local_to_pilot_256m": 1.15,
        "public_to_pilot_streamed": 1.10,
        "local_to_root_256m": 1.05,
        "public_root_to_root": 1.15,
    }
    by_scenario_runner = {(r.scenario, r.runner): r for r in results}
    for scenario, max_ratio in thresholds.items():
        repo = by_scenario_runner.get((scenario, "repo_gfal"))
        legacy = by_scenario_runner.get((scenario, "gfal2"))
        if repo is None or legacy is None:
            continue
        if repo.returncode != 0 or legacy.returncode != 0:
            continue
        limit = round(legacy.elapsed_s * max_ratio, 3)
        assert repo.elapsed_s <= limit, (
            f"{scenario} parity regression: repo_gfal={repo.elapsed_s:.3f}s "
            f"gfal2={legacy.elapsed_s:.3f}s limit={limit:.3f}s"
        )


class TestBenchmarkParityAssertions:
    def test_assert_benchmark_parity_accepts_near_equal_results(self):
        _assert_benchmark_parity(
            [
                _BenchmarkResult(
                    "public_to_pilot_streamed", "repo_gfal", 6.95, 0, "", ""
                ),
                _BenchmarkResult("public_to_pilot_streamed", "gfal2", 7.00, 0, "", ""),
                _BenchmarkResult("local_to_root_256m", "repo_gfal", 1.62, 0, "", ""),
                _BenchmarkResult("local_to_root_256m", "gfal2", 1.60, 0, "", ""),
            ]
        )

    def test_assert_benchmark_parity_rejects_slow_public_https_copy(self):
        with pytest.raises(AssertionError, match="public_to_pilot_streamed"):
            _assert_benchmark_parity(
                [
                    _BenchmarkResult(
                        "public_to_pilot_streamed",
                        "repo_gfal",
                        8.50,
                        0,
                        "",
                        "",
                    ),
                    _BenchmarkResult(
                        "public_to_pilot_streamed",
                        "gfal2",
                        7.00,
                        0,
                        "",
                        "",
                    ),
                ]
            )


@requires_benchmark_opt_in
@pytest.mark.integration
@pytest.mark.network
class TestCopyBenchmarks:
    def test_copy_benchmark_matrix(self, tmp_path):
        if not _eospilot_reachable():
            pytest.skip("eospilot.cern.ch:443 not reachable")
        proxy = _find_proxy()
        if proxy is None:
            pytest.skip("No X.509 proxy found")

        local_src = tmp_path / "benchmark-local.bin"
        with local_src.open("wb") as handle:
            handle.truncate(256 * 1024 * 1024)
        local_dst = tmp_path / "benchmark-local-copy.bin"

        repo_gfal = str(Path(sys.executable).with_name("gfal"))
        env = dict(os.environ)
        runners = [_BenchmarkRunner("repo_gfal", [repo_gfal, "cp"], env)]

        gfal_copy = Path("/usr/bin/gfal-copy")
        if gfal_copy.is_file():
            gfal2_env = dict(env)
            gfal2_env.setdefault("GFAL_PYTHONBIN", "/usr/bin/python3")
            runners.append(_BenchmarkRunner("gfal2", [str(gfal_copy)], gfal2_env))

        suffix = f"benchmark-{uuid.uuid4().hex[:8]}"
        pilot_dir = f"{_PILOT_BASE}/{suffix}"
        pilot_root_dir = f"{_PILOT_ROOT_BASE}/{suffix}"
        rc, _, err = run_gfal(
            "mkdir",
            "-E",
            proxy,
            "--key",
            proxy,
            pilot_dir,
            env=env,
        )
        assert rc == 0, err

        try:
            scenarios = [
                (
                    "local_to_local_256m",
                    [
                        "-f",
                        "--copy-mode",
                        "streamed",
                        local_src.as_uri(),
                        local_dst.as_uri(),
                    ],
                ),
                (
                    "local_to_pilot_256m",
                    [
                        "-E",
                        proxy,
                        "--key",
                        proxy,
                        "-f",
                        "--copy-mode",
                        "streamed",
                        local_src.as_uri(),
                        f"{pilot_dir}/local-{{runner}}.bin",
                    ],
                ),
                (
                    "public_to_pilot_streamed",
                    [
                        "-E",
                        proxy,
                        "--key",
                        proxy,
                        "-f",
                        "--copy-mode",
                        "streamed",
                        _PUBSRC,
                        f"{pilot_dir}/public-{{runner}}.bin",
                    ],
                ),
                (
                    "local_to_root_256m",
                    [
                        "-E",
                        proxy,
                        "--key",
                        proxy,
                        "-f",
                        "--copy-mode",
                        "streamed",
                        local_src.as_uri(),
                        f"{pilot_root_dir}/local-{{runner}}.bin",
                    ],
                ),
                (
                    "public_root_to_root",
                    [
                        "-E",
                        proxy,
                        "--key",
                        proxy,
                        "-f",
                        "--copy-mode",
                        "streamed",
                        _PUBSRC_ROOT,
                        f"{pilot_root_dir}/public-{{runner}}.bin",
                    ],
                ),
            ]

            results: list[_BenchmarkResult] = []
            for label, args in scenarios:
                for runner in runners:
                    resolved = [
                        arg.format(runner=runner.name) if "{runner}" in arg else arg
                        for arg in args
                    ]
                    result = _bench_command(runner, resolved)
                    results.append(
                        _BenchmarkResult(
                            scenario=label,
                            runner=result.runner,
                            elapsed_s=result.elapsed_s,
                            returncode=result.returncode,
                            stdout_tail=result.stdout_tail,
                            stderr_tail=result.stderr_tail,
                        )
                    )
                    assert result.returncode == 0, (
                        f"{label} failed for {runner.name}: {result.stderr_tail}"
                    )

            _assert_benchmark_parity(results)
            print(json.dumps([asdict(result) for result in results], indent=2))
        finally:
            run_gfal(
                "rm",
                "-r",
                "-E",
                proxy,
                "--key",
                proxy,
                pilot_dir,
                env=env,
            )
