from unittest.mock import MagicMock, patch

import pytest
from textual.widgets import Checkbox, Input, Static

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
        # Mock filesystem
        from unittest.mock import MagicMock

        mock_fs = MagicMock()
        mock_fs.ls.return_value = ["file1.txt", "file2.txt"]
        mock_url_to_fs.return_value = (mock_fs, "/data")

        async with app.run_test() as pilot:
            # Set URL and submit
            input_widget = app.query_one("#url-input", Input)
            input_widget.value = test_url
            await pilot.press("enter")

            # Wait for the worker to finish
            await pilot.pause()

            # Verify placeholder update
            placeholder = app.query_one("#remote-placeholder", Static)
            content = str(placeholder.render())
            assert "file1.txt" in content
            assert "file2.txt" in content
            # Ensure ssl_verify=False was passed (default)
            mock_url_to_fs.assert_called_with(test_url, ssl_verify=False)


@pytest.mark.asyncio
async def test_tui_ssl_toggle():
    """Verify that toggling SSL verify works."""
    app = GfalTui()
    test_url = "https://example.com/data"

    with patch("gfal_cli.tui.url_to_fs") as mock_url_to_fs:
        mock_url_to_fs.return_value = (MagicMock(), "/data")

        async with app.run_test() as pilot:
            # Toggle Checkbox
            checkbox = app.query_one("#ssl-verify", Checkbox)
            checkbox.value = True

            # Submit URL
            input_widget = app.query_one("#url-input", Input)
            input_widget.value = test_url
            await pilot.press("enter")
            await pilot.pause()

            # Ensure ssl_verify=True was passed
            mock_url_to_fs.assert_called_with(test_url, ssl_verify=True)
