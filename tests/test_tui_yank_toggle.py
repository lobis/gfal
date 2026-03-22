import asyncio

import pytest

from gfal_cli.tui import GfalTui, HighlightableDirectoryTree


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
