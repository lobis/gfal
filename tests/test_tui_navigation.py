import pytest

from gfal_cli.tui import GfalTui


@pytest.mark.asyncio
async def test_tui_pane_navigation():
    """Verify that left/right and h/l keys switch focus between panes."""
    app = GfalTui()
    async with app.run_test() as pilot:
        # Check initial focus (should be local tree by default or nothing)
        # In our implementation of _get_focused_tree, it defaults to local-tree if nothing is focused.
        # But here we want to check the actual focused widget.

        # Focus remote tree first
        await pilot.press("right")
        assert app.focused.id == "remote-tree"

        # Focus local tree with left arrow
        await pilot.press("left")
        assert app.focused.id == "local-tree"

        # Focus remote tree with 'l'
        await pilot.press("l")
        assert app.focused.id == "remote-tree"

        # Focus local tree with 'h'
        await pilot.press("h")
        assert app.focused.id == "local-tree"

        # Verify 'L' (shift-l) still works for log toggle (indirectly by checking binding)
        # We can't easily check if log is toggled without checking styles,
        # but we can check if the binding exists.
        def get_key(b):
            if hasattr(b, "key"):
                return b.key
            return b[0]

        binding = next((b for b in app.BINDINGS if get_key(b) == "L"), None)
        assert binding is not None
        # Check action, handling both Binding objects and older tuples
        action = getattr(binding, "action", None)
        if action is None and isinstance(binding, (list, tuple)):
            action = binding[1]
        assert action == "toggle_log"
