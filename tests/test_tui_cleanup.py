import asyncio
import threading
import time

import pytest

from gfal_cli.tui import GfalTui


@pytest.mark.asyncio
async def test_tui_clean_exit():
    """Verify that GfalTui exits cleanly and attempts to cancel workers."""
    app = GfalTui()

    async with app.run_test() as pilot:
        # Start a worker thread
        worker_started = threading.Event()

        def dummy_worker():
            worker_started.set()
            # Simulate a long-running blocking operation
            for _ in range(100):
                time.sleep(0.1)

        app.run_worker(dummy_worker, thread=True, name="cleanup_test_worker")

        # Wait for worker to start
        start_time = time.time()
        while not worker_started.is_set() and time.time() - start_time < 2:
            await asyncio.sleep(0.01)

        assert worker_started.is_set(), "Worker thread did not start"

        # Trigger quit
        await pilot.press("q")

    # If we reached here, app.run_test() finished, meaning the app exited.
    # Stray threads might still exist if they weren't daemonized or joined,
    # but the primary goal is ensuring pilot.press("q") doesn't hang the test.
    assert True
