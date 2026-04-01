import pytest

pytest.importorskip("textual")

from gfal.tui import GfalTui


@pytest.mark.asyncio
async def test_tui_pane_navigation():
    """Verify that left/right and h/l keys switch focus between panes."""
    app = GfalTui()
    async with app.run_test() as pilot:
        # Check initial focus (should be local tree by default or nothing)
        # Default focus is the left pane (left-tree)
        assert app.focused.id == "left-tree"
        await pilot.press("right")
        assert app.focused.id == "right-tree"

        # Focus remote tree with 'l'
        await pilot.press("l")
        assert app.focused.id == "right-tree"

        # Focus local tree with 'h'
        await pilot.press("h")
        assert app.focused.id == "left-tree"
        await pilot.press("h")
        assert app.focused.id == "left-tree"
        await pilot.press("l")
        assert app.focused.id == "right-tree"

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
