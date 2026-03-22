import pytest
from textual.widgets import Tree

from gfal_cli.tui import (
    GfalTui,
    HighlightableDirectoryTree,
    HighlightableRemoteDirectoryTree,
)


@pytest.mark.asyncio
async def test_tui_swap_panes():
    """Test that pressing 'x' swaps the contents of the left and right panes."""
    app = GfalTui()
    async with app.run_test() as pilot:
        # Wait for trees to initialize
        await pilot.pause()

        left_pane = app.query_one("#left-pane")
        right_pane = app.query_one("#right-pane")

        initial_left_tree = left_pane.query_one(Tree)
        initial_right_tree = right_pane.query_one(Tree)

        # Verify initial types (remote on left, local on right)
        assert isinstance(initial_left_tree, HighlightableRemoteDirectoryTree)
        assert isinstance(initial_right_tree, HighlightableDirectoryTree)

        # Press 'x' to swap
        await pilot.press("x")
        await pilot.pause()

        # Verify swapped trees
        new_left_tree = left_pane.query_one(Tree)
        new_right_tree = right_pane.query_one(Tree)

        # Should be same instances but repositioned
        assert new_left_tree == initial_right_tree
        assert new_right_tree == initial_left_tree

        assert isinstance(new_left_tree, HighlightableDirectoryTree)
        assert isinstance(new_right_tree, HighlightableRemoteDirectoryTree)

        # Press 'x' again to swap back
        await pilot.press("x")
        await pilot.pause()

        assert left_pane.query_one(Tree) == initial_left_tree
        assert right_pane.query_one(Tree) == initial_right_tree
