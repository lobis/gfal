import asyncio
from unittest.mock import MagicMock, patch

import pytest

pytest.importorskip("textual")

from gfal.tui import GfalTui, HighlightableDirectoryTree, PasteModal


@pytest.mark.asyncio
async def test_tui_yank_toggle():
    """Test that pressing 'y' twice on the same file toggles the yank state."""
    app = GfalTui()
    async with app.run_test() as pilot:
        # Wait for DirectoryTree to load the current directory
        tree = app.query_one("#right-tree", HighlightableDirectoryTree)

        for _ in range(50):
            if tree.root.children:
                break
            await asyncio.sleep(0.1)
            await pilot.pause()

        assert len(tree.root.children) > 0
        app.set_focus(tree)

        # Select first item
        await pilot.press("g")
        await pilot.pause()

        node = tree.cursor_node
        url = str(node.data.path)

        # Initial state: nothing yanked
        assert not app.yanked_urls

        # Yank it
        await pilot.press("y")
        await pilot.pause()
        assert url in app.yanked_urls
        assert url in tree.yanked_urls

        # Un-yank it (press y again)
        await pilot.press("y")
        await pilot.pause()
        assert not app.yanked_urls
        assert not tree.yanked_urls

        # Yank it again
        await pilot.press("y")
        await pilot.pause()
        assert url in app.yanked_urls


@pytest.mark.asyncio
async def test_tui_yank_label_immediate(tmp_path):
    """[YANKED] label must appear immediately after pressing y, without moving cursor."""
    (tmp_path / "file.txt").write_text("hello")
    app = GfalTui(dst=str(tmp_path))
    async with app.run_test() as pilot:
        tree = app.query_one("#right-tree", HighlightableDirectoryTree)
        app.set_focus(tree)

        # Wait for children to load
        for _ in range(50):
            if tree.root.children:
                break
            await pilot.pause(0.05)

        await pilot.press("g")
        await pilot.pause()

        node = tree.cursor_node
        assert node is not None
        url = str(node.data.path)

        # Yank — do NOT move cursor afterwards
        await pilot.press("y")
        await pilot.pause()

        assert url in tree.yanked_urls

        # The line cache must be cleared so render_label is called again.
        # Verify by checking the label rendered for the yanked node directly.
        from rich.style import Style

        label = tree.render_label(node, Style(), Style())
        assert "[YANKED]" in str(label), (
            "render_label did not include [YANKED] — line cache was not invalidated"
        )


@pytest.mark.asyncio
async def test_tui_yank_cleared_after_paste(tmp_path):
    """[YANKED] labels must be removed from trees after a paste completes."""
    src = tmp_path / "src.txt"
    src.write_text("hello")
    dst_dir = tmp_path / "dst"
    dst_dir.mkdir()

    app = GfalTui(dst=str(dst_dir))

    # Mock url_to_fs so paste works without real filesystems
    mock_fs = MagicMock()
    mock_fs.get.side_effect = lambda *a, callback=None, **kw: (
        callback.set_size(5)
        or callback.absolute_update(5)
        or callback.stop(success=True)
    )
    mock_fs.put.side_effect = mock_fs.get.side_effect
    mock_url_to_fs = MagicMock(return_value=(mock_fs, str(src)))

    with (
        patch("gfal.core.fs.url_to_fs", mock_url_to_fs),
        patch("gfal.tui.url_to_fs", mock_url_to_fs),
    ):
        async with app.run_test() as pilot:
            tree = app.query_one("#right-tree", HighlightableDirectoryTree)
            app.set_focus(tree)

            for _ in range(50):
                if tree.root.children:
                    break
                await pilot.pause(0.05)

            await pilot.press("g")
            await pilot.pause()

            node = tree.cursor_node
            assert node is not None
            url = str(node.data.path)

            # Yank
            await pilot.press("y")
            await pilot.pause()
            assert url in app.yanked_urls
            assert url in tree.yanked_urls

            # Paste — triggers handle_paste callback which should clear yank state
            app.action_paste()
            await pilot.pause()
            assert isinstance(app.screen, PasteModal)
            app.screen.on_paste()
            await pilot.pause()

            # After paste is confirmed, yanked_urls must be empty in both app and tree
            assert not app.yanked_urls, "app.yanked_urls not cleared after paste"
            assert not tree.yanked_urls, "tree.yanked_urls not cleared after paste"

            from rich.style import Style

            label = tree.render_label(node, Style(), Style())
            assert "[YANKED]" not in str(label), (
                "[YANKED] label still present in tree after paste"
            )
