import asyncio

import pytest

from gfal_cli.tui import GfalTui, HighlightableDirectoryTree


@pytest.mark.asyncio
async def test_tui_movement_vim_keys():
    """Test that j, k, g, G keys move the cursor correctly."""
    app = GfalTui()
    async with app.run_test() as pilot:
        # Wait for DirectoryTree to load the current directory
        tree = app.query_one("#dest-tree", HighlightableDirectoryTree)

        # We need to wait for children to populate
        for _ in range(50):
            if tree.root.children:
                break
            await asyncio.sleep(0.1)
            await pilot.pause()

        assert len(tree.root.children) > 1

        # With show_root=False, first entry is tree.root.children[0]
        app.set_focus(tree)
        await pilot.pause()

        # Start at the top (g)
        await pilot.press("g")
        await pilot.pause()
        initial_node = tree.cursor_node
        assert initial_node is not None
        # With show_root=True, first entry is tree.root
        assert initial_node == tree.root

        # Move down with 'j'
        await pilot.press("j")
        await pilot.pause()
        down_j_node = tree.cursor_node
        assert down_j_node != initial_node

        # Move up with 'k'
        await pilot.press("k")
        await pilot.pause()
        assert tree.cursor_node == initial_node

        # Move to bottom with 'G'
        await pilot.press("G")
        await pilot.pause()
        bottom_node = tree.cursor_node
        assert bottom_node != initial_node

        # Ensure it is actually the last visible node
        def get_last(node):
            if node.is_expanded and node.children:
                return get_last(node.children[-1])
            return node

        assert bottom_node == get_last(tree.root)

        # Move to top with 'g'
        await pilot.press("g")
        await pilot.pause()
        assert tree.cursor_node == initial_node
