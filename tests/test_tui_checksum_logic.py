import asyncio
from unittest.mock import MagicMock, patch

import pytest
from textual.widgets import RichLog, Tree

from gfal_cli.tui import GfalTui


@pytest.mark.asyncio
async def test_tui_checksum_calls_fs_with_algo():
    """
    Verify that action_checksum calls fs.checksum(path, algo) and logs success.
    """
    app = GfalTui()
    # Mock url_to_fs to return a mocked filesystem
    mock_fs = MagicMock()
    mock_fs.checksum.return_value = "abc12345"
    mock_fs.ls.return_value = []

    async with app.run_test() as pilot:
        await pilot.wait_for_scheduled_animations()

        # We want to test the 'remote' tree
        tree = app.query_one("#left-tree", Tree)
        tree.focus()  # CRITICAL: ensure the tree is focused

        node_path = "https://example.com/test_file"
        if not tree.root.children:
            tree.root.add("test_file", data=node_path)

        node = tree.root.children[0]
        tree.select_node(node)

        log = app.query_one("#log-window", RichLog)
        log.clear()

        with patch("gfal_cli.tui.url_to_fs", return_value=(mock_fs, node_path)):
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

            await pilot.pause(0.5)

            # Verify checksum was called with correct algorithm
            # action_checksum in tui.py uses "ADLER32" as default first try
            mock_fs.checksum.assert_any_call(node_path, "ADLER32")

            # Verify success message in log window
            log_content = "\n".join(str(line.text) for line in log.lines)
            print(f"DEBUG: Log content: {log_content}")
            assert "abc12345" in log_content
            assert "ADLER32" in log_content


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-s", "-n0", __file__]))
