from pathlib import Path

import pytest

from gfal.tui import (
    ChecksumResultModal,
    GfalTui,
    HighlightableDirectoryTree,
    HighlightableRemoteDirectoryTree,
)


@pytest.mark.asyncio
async def test_tui_positional_args():
    """Verify that positional arguments initialize the trees correctly."""
    src = "root://eospublic.cern.ch//eos/user/l/lobis/"
    dst = "/tmp/test_dst"

    app = GfalTui(src=src, dst=dst)
    async with app.run_test():
        left_tree = app.query_one("#left-tree")
        right_tree = app.query_one("#right-tree")

        assert isinstance(left_tree, HighlightableRemoteDirectoryTree)
        assert left_tree.url == src

        assert isinstance(right_tree, HighlightableDirectoryTree)
        # Use Path for cross-platform comparison of path separators
        assert Path(right_tree.path).as_posix().endswith(dst)


@pytest.mark.asyncio
async def test_tui_hidden_bindings():
    """Verify that vim navigation hints are hidden in the footer."""
    app = GfalTui()
    async with app.run_test():
        # Check BINDINGS for 'j', 'k', 'g', 'G', 'h', 'l'
        hidden_keys = {"j", "k", "g", "G", "h", "l"}
        for binding in app.BINDINGS:
            from textual.binding import Binding

            if isinstance(binding, Binding) and binding.key in hidden_keys:
                assert binding.show is False


@pytest.mark.asyncio
async def test_tui_checksum_modal_persistence(tmp_path):
    """Verify that the checksum algorithm is persisted across modal requests."""
    # Create a dummy file to ensure action_checksum_request has a valid target
    test_file = tmp_path / "test.txt"
    test_file.write_text("hello")

    app = GfalTui(src=str(tmp_path))
    app.last_checksum_algo = "MD5"

    async with app.run_test() as pilot:
        # Wait for the tree to load
        await pilot.pause(0.1)

        # Select the file
        tree = app.query_one("#left-tree")
        tree.focus()

        # Wait for tree to load
        for _ in range(50):
            if tree.root.children:
                break
            await pilot.pause(0.02)

        # Move down from root to the file
        await pilot.press("down")
        await pilot.pause()

        # Verify it's not the root
        assert tree.cursor_node != tree.root
        assert not tree.cursor_node.data.path.is_dir()

        # Trigger checksum request (immediately starts calc)
        await pilot.press("c")

        # Poll for ChecksumResultModal
        modal = None
        for _ in range(20):
            if isinstance(app.screen, ChecksumResultModal):
                modal = app.screen
                break
            await pilot.pause(0.05)

        assert modal is not None, "ChecksumResultModal was not pushed"
        assert modal.algo == "MD5"

        # Wait for result (reactive updates)
        for _ in range(50):
            if modal.result != "Calculating...":
                break
            await pilot.pause(0.02)

        assert modal.result != "Calculating..."

        # Change algorithm in the modal
        select = modal.query_one("#algo-select")
        select.value = "SHA1"  # Triggers Select.Changed -> Re-calc
        await pilot.pause(0.1)

        assert app.last_checksum_algo == "SHA1"
        assert modal.algo == "SHA1"
        # Optional: wait for re-calc result
        for _ in range(50):
            if modal.result != "Calculating...":
                break
            await pilot.pause(0.05)
        assert modal.result != "Calculating..."

        # Wait for any background workers to finish before teardown
        await pilot.pause(0.5)
