import asyncio
from unittest.mock import MagicMock, patch

import pytest

pytest.importorskip("textual")
from textual.widgets import RichLog, Tree

from gfal.tui import GfalTui


@pytest.mark.asyncio
async def test_tui_checksum_calls_fs_with_algo():
    """
    Verify that action_checksum calls fs.checksum(path, algo) and logs success.
    """
    app = GfalTui()
    # Mock url_to_fs and compute_checksum in tui.py
    mock_fs = MagicMock()
    mock_fs.ls.return_value = []

    node_path = "https://example.com/test_file"

    with (
        patch("gfal.tui.url_to_fs", return_value=(mock_fs, node_path)),
        patch("gfal.tui.compute_checksum", return_value="abc12345") as mock_compute,
        patch("gfal.core.fs.compute_checksum", return_value="abc12345"),
    ):
        async with app.run_test() as pilot:
            await pilot.wait_for_scheduled_animations()

            # We want to test the 'remote' tree (left)
            tree = app.query_one("#left-tree", Tree)
            tree.focus()

            if not tree.root.children:
                tree.root.add("test_file", data=node_path, allow_expand=False)
                # We need to wait for the UI to reflect the new node
                await pilot.pause()

            # Move cursor from root to the first child
            await pilot.press("down")
            await pilot.pause()

            node = tree.cursor_node
            assert node and node.data == node_path, (
                f"Selected node {node} is not the expected file {node_path}"
            )

            log = app.query_one("#log-window", RichLog)
            log.clear()

            # Trigger checksum action
            await pilot.press("c")

            # Wait for background worker to complete
            timeout = 5.0
            start_time = asyncio.get_event_loop().time()
            while True:
                workers = [w for w in app.workers if w.name == "get_checksum"]
                if workers and all(w.is_finished for w in workers):
                    break
                await asyncio.sleep(0.1)
                if asyncio.get_event_loop().time() - start_time > timeout:
                    break

            await pilot.pause(1.0)

            # Verify compute_checksum was called with correct algorithm (uppercase)
            mock_compute.assert_called_with(mock_fs, node_path, "ADLER32")

            # Verify success message in log window
            log_content = "\n".join(str(line.text) for line in log.lines or [])
            assert "abc12345" in log_content
            assert "ADLER32" in log_content


@pytest.mark.asyncio
async def test_tui_checksum_on_directory_shows_warning():
    """
    Verify that action_checksum_request notifies and returns if a directory is selected.
    """
    app = GfalTui()
    mock_fs = MagicMock()
    mock_fs.ls.return_value = []

    with patch("gfal.tui.url_to_fs", return_value=(mock_fs, "root://mock")):
        async with app.run_test() as pilot:
            await pilot.wait_for_scheduled_animations()

            # Focus remote tree (left)
            tree = app.query_one("#left-tree")
            tree.focus()

            # The root node itself is a directory (allow_expand=True)
            tree.select_node(tree.root)

            with patch.object(app, "notify") as mock_notify:
                # Trigger checksum action
                await pilot.press("c")
                await pilot.pause()

                # Verify notification was called with the correct message
                mock_notify.assert_called_with(
                    "Checksum calculation is not supported for directories.",
                    severity="warning",
                )

                # Ensure no checksum worker was started
                workers = [w for w in app.workers if w.name == "get_checksum"]
                assert not workers
