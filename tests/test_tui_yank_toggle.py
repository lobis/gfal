import asyncio

import pytest

from gfal_cli.tui import GfalTui, HighlightableDirectoryTree


@pytest.mark.asyncio
async def test_tui_yank_toggle():
    """Test that pressing 'y' twice on the same file toggles the yank state."""
    app = GfalTui()
    async with app.run_test() as pilot:
        # Wait for DirectoryTree to load the current directory
        tree = app.query_one("#local-tree", HighlightableDirectoryTree)

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
