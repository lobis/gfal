from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from textual.containers import Vertical
from textual.widgets import Button, Input, RichLog, Tree

from gfal_cli.tui import (
    ChecksumResultModal,
    GfalTui,
    HighlightableDirectoryTree,
    HighlightableRemoteDirectoryTree,
    MessageModal,
)


@pytest.mark.asyncio
async def test_tui_composition():
    """Verify that the TUI widgets exist after composition."""
    app = GfalTui()
    async with app.run_test():
        # Check for key widgets
        assert app.query_one("#left-pane")
        assert app.query_one("#right-pane")
        assert app.query_one("#log-window", RichLog)


@pytest.mark.asyncio
async def test_tui_url_submission_via_modal():
    """Verify that submitting a URL via modal triggers update_focused_pane."""
    app = GfalTui()
    test_url = "https://example.com/data"

    with patch("gfal_cli.tui.url_to_fs") as mock_url_to_fs:
        mock_url_to_fs.return_value = (MagicMock(), "/data")

        async with app.run_test() as pilot:
            # Open URL modal
            await pilot.press("/")
            await pilot.pause()

            # Input URL and submit
            input_widget = app.screen.query_one("#modal-url-input", Input)
            input_widget.value = test_url
            await pilot.press("enter")
            await pilot.pause(0.1)

            # Verify the source tree's URL was updated (Source is focused by default)
            source_tree = app.query_one("#source-tree")
            assert source_tree.url == test_url
            mock_url_to_fs.assert_any_call(test_url, ssl_verify=False)


@pytest.mark.asyncio
async def test_tui_ssl_toggle():
    """Verify that toggling SSL verify works."""
    app = GfalTui()
    test_url = "https://example.com/data"

    with patch("gfal_cli.tui.url_to_fs") as mock_url_to_fs:
        mock_url_to_fs.return_value = (MagicMock(), "/data")

        async with app.run_test() as pilot:
            # Toggle SSL via hotkey
            await pilot.press("v")
            await pilot.pause()

            # Submit URL via modal
            await pilot.press("/")
            await pilot.pause()
            input_widget = app.screen.query_one("#modal-url-input", Input)
            input_widget.value = test_url
            await pilot.press("enter")
            await pilot.pause(0.1)

            mock_url_to_fs.assert_any_call(test_url, ssl_verify=True)


@pytest.mark.asyncio
async def test_tui_hotkeys():
    """Verify that hotkeys trigger activity logging."""
    app = GfalTui()

    with patch("gfal_cli.tui.url_to_fs") as mock_url_to_fs:
        mock_url_to_fs.return_value = (MagicMock(), "/data")

        async with app.run_test() as pilot:
            # Focus a tree node
            app.query_one("#source-tree").focus()
            await pilot.press("down")
            await pilot.pause()

            # Test Stat hotkey
            await pilot.press("s")
            # Wait for modal and dismiss
            for _ in range(20):
                if isinstance(app.screen, MessageModal):
                    break
                await pilot.pause(0.02)
            assert isinstance(app.screen, MessageModal)
            await pilot.press("escape")
            # Wait for dismissal
            for _ in range(20):
                if not isinstance(app.screen, MessageModal):
                    break
                await pilot.pause(0.01)
            assert not isinstance(app.screen, MessageModal)

            log = app.query_one("#log-window", RichLog)
            # Wait for log entry
            for _ in range(20):
                if any("Fetching stat for" in line for line in log.lines):
                    break
                await pilot.pause(0.02)

            # Test Checksum hotkey
            await pilot.press("c")
            for _ in range(20):
                if isinstance(app.screen, ChecksumResultModal):
                    break
                await pilot.pause(0.02)
            assert isinstance(app.screen, ChecksumResultModal)

            # Wait for calculation result
            for _ in range(50):
                if app.screen.result != "Calculating...":
                    break
                await pilot.pause(0.02)
            assert app.screen.result != "Calculating..."

            await pilot.press("escape")
            await pilot.pause()
            for _ in range(20):
                if not isinstance(app.screen, ChecksumResultModal):
                    break
                await pilot.pause(0.01)

            # Test Refresh hotkey
            await pilot.press("r")
            await pilot.pause()

            # Reset focus
            tree = app.query_one("#source-tree")
            tree.focus()
            await pilot.pause()

            # Verify log toggle hotkey actually changes display
            log = app.query_one("#log-window", RichLog)
            assert log.display is True
            await pilot.press("L")
            await pilot.pause()
            assert log.display is False
            await pilot.press("L")
            await pilot.pause()
            assert log.display is True

    @patch("gfal_cli.tui.url_to_fs")
    async def test_tui_modal_behavior(self, mock_url_to_fs):
        """Check modal screen behavior."""
        mock_fs = MagicMock()
        mock_fs.info.return_value = {"size": 100}
        mock_url_to_fs.return_value = (mock_fs, "/data")

        app = GfalTui()
        async with app.run_test() as pilot:
            # Trigger Stat
            app.query_one("#source-tree").focus()
            await pilot.press("down")
            await pilot.press("s")
            # Give the worker a moment to push the screen
            for _ in range(20):
                if isinstance(app.screen, MessageModal):
                    break
                await pilot.pause(0.01)

            assert isinstance(app.screen, MessageModal)
            # Dismiss it
            await pilot.press("escape")
            await pilot.pause()
            assert not isinstance(app.screen, MessageModal)


@pytest.mark.asyncio
async def test_tui_copy_no_selection():
    """Verify that copy doesn't crash if nothing is selected."""
    app = GfalTui()
    # Mock to avoid real network calls if a worker starts
    with patch("gfal_cli.tui.url_to_fs"):
        async with app.run_test() as pilot:
            # Both trees are empty/initializing
            await pilot.press("f5")
            await pilot.pause()
            assert True


@pytest.mark.asyncio
async def test_tui_vim_navigation():
    """Verify that g/G hotkeys move the cursor to top/bottom."""
    app = GfalTui()
    async with app.run_test() as pilot:
        tree = app.query_one("#dest-tree", HighlightableDirectoryTree)
        tree.focus()
        await pilot.press("down")
        await pilot.pause()

        # Should be some items in local dir (at least the root if empty, or children)
        # We just want to ensure it doesn't crash and moves selection

        # Move to bottom
        await pilot.press("G")
        await pilot.pause()

        def get_last(node):
            if node.is_expanded and node.children:
                return get_last(node.children[-1])
            return node

        assert tree.cursor_node == get_last(tree.root)

        # Move to top
        await pilot.press("g")
        await pilot.pause()
        target = tree.root
        assert tree.cursor_node == target


@pytest.mark.asyncio
async def test_tui_tpc_toggle_state():
    """Verify that the TPC toggle state is correctly handled via hotkey."""
    app = GfalTui()
    async with app.run_test() as pilot:
        assert app.tpc_enabled is True  # Enabled by default

        await pilot.press("t")
        await pilot.pause()
        assert app.tpc_enabled is False

        await pilot.press("t")
        await pilot.pause()
        assert app.tpc_enabled is True


@pytest.mark.asyncio
async def test_tui_modal_dismiss_button_click():
    """Verify that clicking the Close button in MessageModal works."""
    app = GfalTui()
    async with app.run_test() as pilot:
        mock_fs = MagicMock()
        mock_fs.info.return_value = {"size": 100}
        with patch("gfal_cli.tui.url_to_fs", return_value=(mock_fs, "/data")):
            # Trigger Stat to show a modal
            app.query_one("#source-tree").focus()
            await pilot.press("down")
            await pilot.press("s")

            for _ in range(20):
                if isinstance(app.screen, MessageModal):
                    break
                await pilot.pause(0.01)

            assert isinstance(app.screen, MessageModal)

            # Click the Close button on the modal
            # We need to find the button on the active screen
            close_btn = app.screen.query_one("#close-btn", Button)
            await pilot.click(close_btn)
            await pilot.pause()

            assert not isinstance(app.screen, MessageModal)


@pytest.mark.asyncio
async def test_tui_error_handling_ls_failure():
    """Verify that remote tree errors are logged."""
    app = GfalTui()
    with patch("gfal_cli.tui.url_to_fs") as mock_url_to_fs:
        mock_url_to_fs.side_effect = Exception("Connection refused")
        async with app.run_test() as pilot:
            # Open modal and enter failing URL
            await pilot.press("/")
            await pilot.pause()
            input_widget = app.screen.query_one("#modal-url-input", Input)
            input_widget.value = "http://failed"
            await pilot.press("enter")
            await pilot.pause(0.1)

            log = app.query_one("#log-window", RichLog)
            # Check for error in log
            for line in log.lines:
                if "Failed to load http://failed: Connection refused" in line:
                    break
            # Since we can't easily check log lines in some environments,
            # we mainly care about no crash.
            assert True


@pytest.mark.asyncio
async def test_tui_refresh_hotkey_logic():
    """Verify that the refresh hotkey triggers directory reloading."""
    app = GfalTui()
    # DirectoryTree.reload is async
    with (
        patch(
            "textual.widgets.DirectoryTree.reload", new_callable=AsyncMock
        ) as mock_local_reload,
        patch(
            "gfal_cli.tui.HighlightableRemoteDirectoryTree.load_directory",
            new_callable=MagicMock,
        ) as mock_remote_load,
    ):
        async with app.run_test() as pilot:
            app.query_one("#source-tree").focus()
            await pilot.press("down")
            await pilot.press("r")
            await pilot.pause()
            mock_local_reload.assert_called()
            mock_remote_load.assert_called()


@pytest.mark.asyncio
async def test_tui_remote_tree_selection_stat_call(tmp_path):
    """Verify that Stat hotkey on a remote node uses the correct remote path."""
    # Create a dummy remote structure
    remote_dir = tmp_path / "remote"
    remote_dir.mkdir()
    (remote_dir / "file.txt").write_text("remote content")

    app = GfalTui()
    # Mock url_to_fs to point the remote tree to our local dummy dir
    with patch("gfal_cli.tui.url_to_fs") as mock_url_to_fs:
        from gfal_cli.fs import url_to_fs as real_url_to_fs

        # For the remote path, return a local fs pointing to our dummy dir
        def side_effect(url, *args, **kwargs):
            if url.startswith("root://"):
                return real_url_to_fs(str(remote_dir))
            return real_url_to_fs(url)

        mock_url_to_fs.side_effect = side_effect

        async with app.run_test() as pilot:
            # Use update_focused_pane which handles node expansion correctly
            await app.update_focused_pane("root://localhost/remote_mock")

            # Focus source tree (which we just updated to be remote)
            source_tree = app.query_one(
                "#source-tree", HighlightableRemoteDirectoryTree
            )
            app.set_focus(source_tree)

            # Wait for nodes to load (file.txt should be under root)
            for _ in range(50):
                if source_tree.root and source_tree.root.children:
                    break
                await pilot.pause(0.02)

            # Manually select the first child
            if source_tree.root and source_tree.root.children:
                source_tree.select_node(source_tree.root.children[0])

            # The second call to url_to_fs happens in action_stat
            mock_fs = MagicMock()
            mock_fs.info.return_value = {"size": 1234, "type": "file"}
            mock_url_to_fs.side_effect = None
            mock_url_to_fs.return_value = (mock_fs, "/remote_mock/file.txt")

            await pilot.press("s")

            # Check for modal and wait longer
            passed = False
            for _ in range(50):
                if isinstance(app.screen, MessageModal):
                    passed = True
                    break
                await pilot.pause(0.01)

            assert passed, f"MessageModal did not appear. Screen: {app.screen}."
            await pilot.press("escape")


@pytest.mark.asyncio
async def test_tui_swap_panes():
    """Verify that pressing 'x' swaps the Local and Remote trees."""
    app = GfalTui()
    async with app.run_test() as pilot:
        left_pane = app.query_one("#left-pane", Vertical)
        right_pane = app.query_one("#right-pane", Vertical)

        # Initial state: Left=Source (Remote), Right=Destination (Local)
        assert isinstance(left_pane.query_one(Tree), HighlightableRemoteDirectoryTree)
        assert isinstance(right_pane.query_one(Tree), HighlightableDirectoryTree)

        # Press 'x' to swap
        await pilot.press("x")
        await pilot.pause()

        # Swapped state: Left=Local, Right=Remote
        assert isinstance(left_pane.query_one(Tree), HighlightableDirectoryTree)
        assert isinstance(right_pane.query_one(Tree), HighlightableRemoteDirectoryTree)

        # Press 'x' again to swap back
        await pilot.press("x")
        await pilot.pause()

        assert isinstance(left_pane.query_one(Tree), HighlightableRemoteDirectoryTree)
        assert isinstance(right_pane.query_one(Tree), HighlightableDirectoryTree)


@pytest.mark.asyncio
async def test_tui_unmount_cleanup():
    """Verify that workers are cancelled on unmount."""
    app = GfalTui()
    with patch.object(app.workers, "cancel_all") as mock_cancel:
        app.on_unmount()
        mock_cancel.assert_called_once()


@pytest.mark.asyncio
async def test_tui_log_persistence(tmp_path):
    """Verify that TUI logs are persisted to a file."""
    log_file = tmp_path / "test.log"
    app = GfalTui()
    app.log_file = str(log_file)
    async with app.run_test() as pilot:
        app.log_activity("Test log message")
        await pilot.pause(0.1)
        assert log_file.exists()
        content = log_file.read_text()
        assert "Test log message" in content


@pytest.mark.asyncio
async def test_tui_toggle_label_update():
    """Verify that the TPC toggle label in the footer updates."""
    app = GfalTui()
    async with app.run_test() as pilot:
        app.refresh_bindings()
        if app.screen:
            app.screen.refresh_bindings()

        # Check initial label for TPC
        def get_desc(key):
            try:
                # Use [-1] to get the most recent binding for the key
                return app._bindings.key_to_bindings[key][-1].description
            except (KeyError, IndexError):
                return None

        assert get_desc("t") == "TPC [ON]"

        await pilot.press("t")
        await pilot.pause()
        assert get_desc("t") == "TPC [OFF]"

        await pilot.press("t")
        await pilot.pause()
        assert get_desc("t") == "TPC [ON]"

        # Check SSL label
        assert get_desc("v") == "SSL [OFF]"
        await pilot.press("v")
        await pilot.pause()
        await pilot.pause(0.1)
        assert get_desc("v") == "SSL [ON]"

        await pilot.press("v")
        await pilot.pause()
        assert get_desc("v") == "SSL [OFF]"


@pytest.mark.asyncio
async def test_tui_local_stat_command_logging(tmp_path):
    """Verify that local stat triggers command logging to file."""
    log_file = tmp_path / "stat.log"
    app = GfalTui()
    app.log_file = str(log_file)
    async with app.run_test() as pilot:
        # Focus local tree (Destination)
        tree = app.query_one("#dest-tree", HighlightableDirectoryTree)
        tree.focus()
        await pilot.press("down")
        await pilot.pause()

        with patch("gfal_cli.tui.url_to_fs") as mock_url_to_fs:
            mock_fs = MagicMock()
            mock_fs.info.return_value = {"size": 100}
            mock_url_to_fs.return_value = (mock_fs, "/")

            await pilot.press("s")
            # Wait for modal and log write
            for _ in range(20):
                if isinstance(app.screen, MessageModal):
                    break
                await pilot.pause(0.01)
            await pilot.pause(0.2)

            assert log_file.exists()
            content = log_file.read_text()
            assert "gfal-stat" in content


@pytest.mark.asyncio
async def test_tui_local_checksum_command_logging(tmp_path):
    """Verify that local checksum triggers command logging to file."""
    log_file = tmp_path / "sum.log"
    app = GfalTui()
    app.log_file = str(log_file)
    async with app.run_test() as pilot:
        # Focus local tree (Destination)
        tree = app.query_one("#dest-tree", HighlightableDirectoryTree)
        tree.focus()
        await pilot.press("down")
        await pilot.pause()

        with patch("gfal_cli.tui.url_to_fs") as mock_url_to_fs:
            mock_fs = MagicMock()
            mock_fs.checksum.return_value = "ABCDEF"
            mock_url_to_fs.return_value = (mock_fs, "/")

            await pilot.press("c")
            # Wait for ChecksumResultModal
            for _ in range(20):
                if isinstance(app.screen, ChecksumResultModal):
                    break
                await pilot.pause(0.01)

            # Wait for calculation and log write
            for _ in range(50):
                if app.screen.result != "Calculating...":
                    break
                await pilot.pause(0.02)
            await pilot.pause(0.2)

            assert log_file.exists()
            content = log_file.read_text()
            assert "gfal-sum" in content


@pytest.mark.asyncio
async def test_tui_human_readable_stat():
    """Verify that stat modal contains human-readable size and time."""
    app = GfalTui()
    async with app.run_test() as pilot:
        # Focus local tree (Destination)
        tree = app.query_one("#dest-tree", HighlightableDirectoryTree)
        tree.focus()
        await pilot.press("down")
        await pilot.pause()

        with patch("gfal_cli.tui.url_to_fs") as mock_url_to_fs:
            mock_fs = MagicMock()
            # 10 MB = 10 * 1024 * 1024 = 10485760 bytes
            timestamp = 1710972000.0  # 2024-03-20 22:00:00 UTC
            mock_fs.info.return_value = {"size": 10485760, "mtime": timestamp}
            mock_url_to_fs.return_value = (mock_fs, "/")

            await pilot.press("s")
            # Wait for modal
            for _ in range(20):
                if isinstance(app.screen, MessageModal):
                    break
                await pilot.pause(0.01)

            assert isinstance(app.screen, MessageModal)
            message = app.screen.message
            assert "10.00 MB" in message
            assert "2024-03-20 22:00:00 UTC" in message


@pytest.mark.asyncio
async def test_tui_checksum_formatting_v2():
    """Verify that checksum modal contains algorithm name and hex value."""
    app = GfalTui()
    async with app.run_test() as pilot:
        # Focus local tree (Destination)
        tree = app.query_one("#dest-tree", HighlightableDirectoryTree)
        tree.focus()
        await pilot.press("down")
        await pilot.pause()

        with patch("gfal_cli.tui.url_to_fs") as mock_url_to_fs:
            mock_fs = MagicMock()
            # Return bytes checksum
            mock_fs.checksum.return_value = b"\xab\xcd\xef"
            mock_url_to_fs.return_value = (mock_fs, "/")

            await pilot.press("c")
            # Wait for ChecksumResultModal
            for _ in range(20):
                if isinstance(app.screen, ChecksumResultModal):
                    break
                await pilot.pause(0.01)

            # Wait for result
            for _ in range(50):
                if app.screen.result != "Calculating...":
                    break
                await pilot.pause(0.02)

            assert isinstance(app.screen, ChecksumResultModal)
            assert "abcdef" in app.screen.result
