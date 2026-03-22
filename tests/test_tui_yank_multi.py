import asyncio

import pytest

from gfal_cli.tui import GfalTui, HighlightableDirectoryTree


@pytest.mark.asyncio
async def test_tui_yank_multi():
    """Test that multiple items can be yanked and un-yanked independently."""
    app = GfalTui()
    async with app.run_test() as pilot:
        tree = app.query_one("#local-tree", HighlightableDirectoryTree)

        # Wait for tree to load
        for _ in range(50):
            if tree.root.children:
                break
            await asyncio.sleep(0.1)
            await pilot.pause()

        assert len(tree.root.children) >= 2
        app.set_focus(tree)

        # Yank first item
        await pilot.press("g")
        await pilot.pause()
        url1 = str(tree.cursor_node.data.path)
        await pilot.press("y")
        await pilot.pause()

        # Yank second item
        await pilot.press("j")
        await pilot.pause()
        url2 = str(tree.cursor_node.data.path)
        await pilot.press("y")
        await pilot.pause()

        # Check both are yanked
        assert url1 in app.yanked_urls
        assert url2 in app.yanked_urls
        assert len(app.yanked_urls) == 2

        # Un-yank first item
        await pilot.press("k")  # Back to first
        await pilot.pause()
        await pilot.press("y")
        await pilot.pause()

        assert url1 not in app.yanked_urls
        assert url2 in app.yanked_urls
        assert len(app.yanked_urls) == 1

        # Yank first item again
        await pilot.press("y")
        await pilot.pause()
        assert url1 in app.yanked_urls
        assert len(app.yanked_urls) == 2


# Test removed (redundant with test_tui_cleanup.py)
