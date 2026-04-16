"""Async-focused direct tests for the library client API."""

import asyncio
import contextlib
import io
import time
from pathlib import Path

import pytest

from gfal.core import fs
from gfal.core.api import AsyncGfalClient, CopyOptions, GfalClient
from gfal.core.errors import (
    GfalError,
    GfalFileExistsError,
    GfalFileNotFoundError,
    GfalTimeoutError,
)


@pytest.mark.asyncio
async def test_async_exists_missing_returns_false(tmp_path):
    client = AsyncGfalClient()
    missing = (tmp_path / "missing.txt").as_uri()

    assert await client.exists(missing) is False


@pytest.mark.asyncio
async def test_async_iterdir_detail_returns_stat_results(tmp_path):
    src = tmp_path / "src"
    src.mkdir()
    (src / "alpha.txt").write_text("alpha")
    (src / "beta.txt").write_text("beta")
    client = AsyncGfalClient()

    entries = await client.iterdir(src.as_uri(), detail=True)

    names = sorted(Path(entry.info["name"]).name for entry in entries)
    assert names == ["alpha.txt", "beta.txt"]
    assert all(entry.is_file() for entry in entries)


@pytest.mark.asyncio
async def test_async_file_operations_roundtrip(tmp_path):
    src = tmp_path / "src.txt"
    renamed = tmp_path / "renamed.txt"
    subdir = tmp_path / "sub"
    client = AsyncGfalClient()

    await client.mkdir(subdir.as_uri())
    with await client.open(src.as_uri(), "wb") as handle:
        handle.write(b"payload")

    await client.rename(src.as_uri(), renamed.as_uri())

    with await client.open(renamed.as_uri(), "rb") as handle:
        assert handle.read() == b"payload"

    await client.rm(renamed.as_uri())
    await client.rmdir(subdir.as_uri())

    assert not renamed.exists()
    assert not subdir.exists()


@pytest.mark.asyncio
async def test_async_copy_local_preserve_times_and_parent_creation(tmp_path):
    src = tmp_path / "src.txt"
    dst = tmp_path / "nested" / "out.txt"
    src.write_text("payload")
    src_ts = 946684800
    src_at = 946684860
    src.touch()
    import os

    os.utime(src, (src_at, src_ts))
    client = AsyncGfalClient()

    await client.copy(
        src.as_uri(),
        dst.as_uri(),
        options=CopyOptions(create_parents=True, preserve_times=True),
    )

    assert dst.read_text() == "payload"
    assert int(dst.stat().st_mtime) == src_ts


@pytest.mark.asyncio
async def test_async_copy_reports_progress_and_start_callback(tmp_path):
    src = tmp_path / "src.bin"
    dst = tmp_path / "dst.bin"
    src.write_bytes(b"x" * (fs.CHUNK_SIZE + 1024))
    client = AsyncGfalClient()
    progress = []
    started = []

    await client.copy(
        src.as_uri(),
        dst.as_uri(),
        options=CopyOptions(),
        progress_callback=progress.append,
        start_callback=lambda: started.append(True),
    )

    assert dst.read_bytes() == src.read_bytes()
    assert started == [True]
    assert progress
    assert progress[-1] == src.stat().st_size


@pytest.mark.asyncio
async def test_async_copy_compare_checksum_warns_and_skips(tmp_path):
    src = tmp_path / "src.txt"
    dst = tmp_path / "dst.txt"
    src.write_text("same content")
    dst.write_text("same content")
    before = dst.stat().st_mtime_ns
    warnings = []
    client = AsyncGfalClient()

    await client.copy(
        src.as_uri(),
        dst.as_uri(),
        options=CopyOptions(compare="checksum"),
        warn_callback=warnings.append,
    )

    assert dst.read_text() == "same content"
    assert dst.stat().st_mtime_ns == before
    assert any("matching ADLER32 checksum" in warning for warning in warnings)


class _RemoteWriteFs:
    def info(self, path):
        raise FileNotFoundError(path)

    def open(self, path, mode):
        assert mode == "wb"
        return contextlib.nullcontext(io.BytesIO())


@pytest.mark.asyncio
async def test_async_copy_remote_preserve_times_warns(monkeypatch, tmp_path):
    src = tmp_path / "src.txt"
    src.write_text("payload")
    client = AsyncGfalClient()
    original_url_to_fs = fs.url_to_fs
    warnings = []

    def _url_to_fs_side_effect(url, storage_options=None, **kwargs):
        if url.startswith("https://example.com/"):
            return _RemoteWriteFs(), "/remote/file.txt"
        return original_url_to_fs(url, storage_options=storage_options, **kwargs)

    monkeypatch.setattr("gfal.core.api.fs.url_to_fs", _url_to_fs_side_effect)

    await client.copy(
        src.as_uri(),
        "https://example.com/file.txt",
        options=CopyOptions(preserve_times=True),
        warn_callback=warnings.append,
    )

    assert any("--preserve-times is only supported" in warning for warning in warnings)


@pytest.mark.asyncio
async def test_async_copy_tpc_only_preflight_rejects_unsupported_pair(tmp_path):
    src = tmp_path / "src.txt"
    src.write_text("payload")
    client = AsyncGfalClient()

    with pytest.raises(OSError, match="TPC not supported for file:// -> https://"):
        await client.copy(
            src.as_uri(),
            "https://example.com/file.txt",
            options=CopyOptions(tpc="only"),
        )


@pytest.mark.asyncio
async def test_async_copy_maps_backend_exceptions(monkeypatch):
    client = AsyncGfalClient()

    def _failing_copy(
        src_url,
        dst_url,
        options,
        progress_callback,
        start_callback,
        warn_callback,
        cancel_event,
    ):
        raise FileExistsError("already exists")

    monkeypatch.setattr(client, "_copy_sync", _failing_copy)

    with pytest.raises(GfalFileExistsError, match="already exists"):
        await client.copy("file:///tmp/src.txt", "file:///tmp/dst.txt")


@pytest.mark.asyncio
async def test_async_start_copy_maps_background_exception(monkeypatch):
    client = AsyncGfalClient()

    def _failing_copy(
        src_url,
        dst_url,
        options,
        progress_callback,
        start_callback,
        warn_callback,
        cancel_event,
    ):
        raise FileNotFoundError("missing")

    monkeypatch.setattr(client, "_copy_sync", _failing_copy)
    handle = client.start_copy("file:///tmp/src.txt", "file:///tmp/dst.txt")

    with pytest.raises(GfalFileNotFoundError, match="missing"):
        await handle.wait_async(timeout=1)


@pytest.mark.asyncio
async def test_async_start_copy_wait_timeout(monkeypatch):
    client = AsyncGfalClient()

    def _slow_copy(
        src_url,
        dst_url,
        options,
        progress_callback,
        start_callback,
        warn_callback,
        cancel_event,
    ):
        time.sleep(0.2)

    monkeypatch.setattr(client, "_copy_sync", _slow_copy)
    handle = client.start_copy("file:///tmp/src.txt", "file:///tmp/dst.txt")

    with pytest.raises(GfalTimeoutError):
        await handle.wait_async(timeout=0.01)

    await handle.wait_async(timeout=1)


@pytest.mark.asyncio
async def test_async_start_copy_cancel_propagates_cancelled_error(monkeypatch):
    client = AsyncGfalClient()

    def _slow_copy(
        src_url,
        dst_url,
        options,
        progress_callback,
        start_callback,
        warn_callback,
        cancel_event,
    ):
        for _ in range(50):
            if cancel_event is not None and cancel_event.is_set():
                raise GfalError("Transfer cancelled", 125)
            time.sleep(0.01)

    monkeypatch.setattr(client, "_copy_sync", _slow_copy)
    handle = client.start_copy("file:///tmp/src.txt", "file:///tmp/dst.txt")
    await asyncio.sleep(0.03)
    handle.cancel()

    with pytest.raises(GfalError, match="Transfer cancelled"):
        await handle.wait_async(timeout=1)


@pytest.mark.asyncio
async def test_async_start_copy_propagates_background_exception(monkeypatch):
    client = AsyncGfalClient()

    def _failing_copy(
        src_url,
        dst_url,
        options,
        progress_callback,
        start_callback,
        warn_callback,
        cancel_event,
    ):
        raise GfalFileExistsError("already exists")

    monkeypatch.setattr(client, "_copy_sync", _failing_copy)
    handle = client.start_copy("file:///tmp/src.txt", "file:///tmp/dst.txt")

    with pytest.raises(GfalFileExistsError):
        await handle.wait_async(timeout=1)


@pytest.mark.asyncio
async def test_sync_facade_works_inside_running_event_loop(tmp_path):
    src = tmp_path / "src.txt"
    dst = tmp_path / "dst.txt"
    src.write_text("payload")
    client = GfalClient()

    stat_result = client.stat(src.as_uri())
    client.copy(src.as_uri(), dst.as_uri())

    assert stat_result.size == len("payload")
    assert dst.read_text() == "payload"
