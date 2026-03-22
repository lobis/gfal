from unittest.mock import MagicMock, patch

import pytest

from gfal_cli.tui import (
    GfalTui,
    HighlightableDirectoryTree,
    HighlightableRemoteDirectoryTree,
    PasteModal,
)


@pytest.mark.asyncio
async def test_tui_yank_functionality():
    """Test that pressing 'y' yanks the selected item and highlights it."""
    app = GfalTui()
    async with app.run_test() as pilot:
        local_tree = app.query_one("#local-tree", HighlightableDirectoryTree)
        # Wait for trees to be ready and have nodes
        for _ in range(20):
            if local_tree.root and local_tree.root.children:
                break
            await pilot.pause(0.01)

        # Move cursor to first child
        await pilot.press("down")
        node = local_tree.cursor_node
        assert node is not None

        path = str(node.data.path)

        # Yank it
        await pilot.press("y")

        assert path in app.yanked_urls

        # Verify highlight in label
        label = local_tree.render_label(node, MagicMock(), MagicMock())
        assert "[YANKED]" in str(label)

        # Verify other tree also has the yanked_url set
        remote_tree = app.query_one("#remote-tree", HighlightableRemoteDirectoryTree)
        assert path in remote_tree.yanked_urls


@pytest.mark.asyncio
async def test_tui_paste_modal_trigger():
    """Test that pressing 'p' on a directory opens the PasteModal."""
    app = GfalTui()
    async with app.run_test() as pilot:
        await pilot.pause()
        # Yank something first
        app.yanked_urls = ["/tmp/source.txt"]

        # Focus remote tree and select a directory (root is a directory)
        remote_tree = app.query_one("#remote-tree", HighlightableRemoteDirectoryTree)
        app.set_focus(remote_tree)
        # root is selected by default usually, if not:
        if not remote_tree.cursor_node:
            await pilot.press("down")

        # Press 'p'
        await pilot.press("p")

        # Check if PasteModal is active
        assert isinstance(app.screen, PasteModal)
        assert app.screen.src_url == "/tmp/source.txt"
        assert app.screen.dst_dir == remote_tree.root.data


@pytest.mark.asyncio
async def test_tui_paste_execution():
    """Test that submitting PasteModal triggers _do_copy."""
    app = GfalTui()
    # We patch _do_copy because it runs in a background thread and we just want to verify trigger
    with patch.object(GfalTui, "_do_copy") as mock_do_copy:
        async with app.run_test() as pilot:
            await pilot.pause()
            app.yanked_urls = ["/tmp/file.txt"]
            remote_tree = app.query_one(
                "#remote-tree", HighlightableRemoteDirectoryTree
            )
            app.set_focus(remote_tree)
            if not remote_tree.cursor_node:
                await pilot.press("down")

            await pilot.press("p")
            assert isinstance(app.screen, PasteModal)

            # Type a name and press enter
            # We don't need to clear exactly, just append or type
            await pilot.press("n", "e", "w", ".", "t", "x", "t", "enter")

            # Wait for any background task to be triggered
            await pilot.pause()

            # The destination path should be remote_root + /new.txt
            expected_dst = remote_tree.root.data.rstrip("/") + "/new.txt"

            # Since _do_copy is run via run_worker, we might need a bit more pause
            import asyncio

            await asyncio.sleep(0.1)

            mock_do_copy.assert_called_once()
            args, _ = mock_do_copy.call_args
            assert args[0] == "/tmp/file.txt"
            assert args[1] == expected_dst
