from unittest.mock import MagicMock, patch

import pytest
from textual.widgets import Checkbox, Input, Tree

from gfal_cli.tui import GfalTui


@pytest.mark.asyncio
async def test_tui_composition():
    """Verify that the TUI widgets exist after composition."""
    app = GfalTui()
    async with app.run_test():
        # Check for key widgets
        assert app.query_one("#url-input", Input)
        assert app.query_one("#ssl-verify", Checkbox)
        assert app.query_one("#local-tree")
        assert app.query_one("#remote-pane")


@pytest.mark.asyncio
async def test_tui_url_submission():
    """Verify that submitting a URL triggers update_remote."""
    app = GfalTui()
    test_url = "https://example.com/data"

    with patch("gfal_cli.tui.url_to_fs") as mock_url_to_fs:
        mock_fs = MagicMock()
        mock_fs.ls.return_value = [
            {"name": "file1.txt", "type": "file"},
            {"name": "file2.txt", "type": "file"},
        ]
        mock_url_to_fs.return_value = (mock_fs, "/data")

        async with app.run_test() as pilot:
            # Wait for any background workers (like the initial load)
            for _ in range(5):
                await pilot.pause()

            # Set URL and submit
            input_widget = app.query_one("#url-input", Input)
            input_widget.value = test_url
            await pilot.press("enter")

            # Wait for the tree to mount and its initial expand worker
            for _ in range(5):
                await pilot.pause()

            # Find the tree that matches our test URL
            tree = None
            for t in app.query(Tree):
                if str(t.root.label) == test_url:
                    tree = t
                    break

            assert tree is not None, "Could not find a tree with the test URL"

            # Children should be loaded
            labels = [str(node.label) for node in tree.root.children]
            assert "file1.txt" in labels
            assert "file2.txt" in labels

            # Ensure the LAST call was with our test URL and default ssl_verify
            mock_url_to_fs.assert_any_call(test_url, ssl_verify=False)


@pytest.mark.asyncio
async def test_tui_ssl_toggle():
    """Verify that toggling SSL verify works."""
    app = GfalTui()
    test_url = "https://example.com/data"

    with patch("gfal_cli.tui.url_to_fs") as mock_url_to_fs:
        mock_url_to_fs.return_value = (MagicMock(), "/data")

        async with app.run_test() as pilot:
            # Wait for initial load
            for _ in range(5):
                await pilot.pause()

            # Toggle Checkbox
            checkbox = app.query_one("#ssl-verify", Checkbox)
            checkbox.value = True

            # Submit URL
            input_widget = app.query_one("#url-input", Input)
            input_widget.value = test_url
            await pilot.press("enter")
            for _ in range(5):
                await pilot.pause()

            # Ensure ssl_verify=True was passed
            mock_url_to_fs.assert_any_call(test_url, ssl_verify=True)
