from pathlib import Path
from unittest.mock import patch

import pytest

from gfal_cli.tui import GfalTui, HighlightableDirectoryTree


@pytest.mark.asyncio
async def test_tui_local_checksum_calls_fs():
    """Verify that TUI correctly identifies and computes local checksums."""
    app = GfalTui()
    # Create a dummy local file for testing
    test_file = Path("test_local_checksum.txt")
    test_file.write_text("hello world")

    try:
        async with app.run_test() as pilot:
            # 1. Select a file in the local tree (right pane)
            tree = app.query_one("#right-tree", HighlightableDirectoryTree)
            tree.focus()

            # Refresh local tree to see the file
            await pilot.press("r")

            # Wait for items to load
            for _ in range(50):
                if tree.root.children:
                    break
                await pilot.pause(0.02)

            # Move down to select the file (root is dir)
            await pilot.press("down")
            await pilot.pause()

            # Ensure we are on a file node
            while (
                tree.cursor_node
                and tree.cursor_node.data.path.is_dir()
                and tree.cursor_node != tree.root.children[-1]
            ):
                await pilot.press("down")
                await pilot.pause(0.05)

            # Find the node for our test file
            # Since DirectoryTree might be large, we'll manually set the cursor
            # to a node we know exists or mock the selection.

            with patch("gfal_cli.tui.url_to_fs") as mock_url_to_fs:
                from fsspec.implementations.local import LocalFileSystem

                mock_fs = LocalFileSystem()
                mock_url_to_fs.return_value = (mock_fs, str(test_file.absolute()))

                with patch("gfal_cli.tui.compute_checksum") as mock_compute:
                    mock_compute.return_value = (
                        "5eb63bbbe01eeed093cb22bb8f5acdc3"  # md5 of 'hello world'
                    )

                    # Mock _get_node_path to return our test file
                    with patch.object(
                        GfalTui,
                        "_get_node_path",
                        return_value=str(test_file.absolute()),
                    ):
                        # Trigger checksum action
                        await pilot.press("c")
                        await pilot.pause()

                        # Verify compute_checksum was called
                        # It should be called for ADLER32 first, then stop if it succeeds
                        mock_compute.assert_called()
                        args = mock_compute.call_args[0]
                        assert args[1] == str(test_file.absolute())

                        # Check activity log for success
                        # We need to wait for the worker to finish
                        # Textual workers are tricky to wait for in tests without private APIs
                        # but we can wait a bit.
                        import asyncio

                        await asyncio.sleep(0.5)

                        log = app.query_one("RichLog")
                        log_text = "\n".join(str(line.text) for line in log.lines)
                        assert "SUCCESS Checksum" in log_text
                        assert "5eb63bbbe01eeed093cb22bb8f5acdc3" in log_text
    finally:
        if test_file.exists():
            test_file.unlink()
