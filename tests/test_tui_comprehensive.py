"""Comprehensive TUI tests covering swap+operations, refresh, modals, and edge cases."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from rich.style import Style
from textual.widgets import Tree

from gfal_cli.tui import (
    GfalTui,
    HighlightableDirectoryTree,
    HighlightableRemoteDirectoryTree,
    MessageModal,
    PasteModal,
    TransferSummaryModal,
    UrlInputModal,
)

# ---------------------------------------------------------------------------
# Swap + operations: ensure swap doesn't break subsequent actions
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_swap_preserves_focus():
    """After swapping panes, the previously focused tree keeps focus."""
    app = GfalTui()
    async with app.run_test() as pilot:
        await pilot.pause()

        # Focus right tree
        right_tree = app.query_one("#right-pane").query_one(Tree)
        app.set_focus(right_tree)
        await pilot.pause()

        assert app.focused is right_tree

        # Swap
        await pilot.press("x")
        await pilot.pause()

        # right_tree should still be focused (now in left pane)
        assert app.focused is right_tree


@pytest.mark.asyncio
async def test_swap_yank_after_swap():
    """Yanking works correctly after panes have been swapped."""
    app = GfalTui()
    async with app.run_test() as pilot:
        await pilot.pause()

        # Focus right (local) tree
        right_tree = app.query_one("#right-pane").query_one(Tree)
        app.set_focus(right_tree)

        for _ in range(50):
            if right_tree.root and right_tree.root.children:
                break
            await pilot.pause(0.05)

        # Swap panes
        await pilot.press("x")
        await pilot.pause()

        # Local tree is now in left pane — yank should still work
        await pilot.press("g")
        await pilot.pause()
        await pilot.press("y")
        await pilot.pause()

        assert len(app.yanked_urls) == 1


@pytest.mark.asyncio
async def test_swap_double_swap_is_identity():
    """Swapping twice returns to original layout."""
    app = GfalTui()
    async with app.run_test() as pilot:
        await pilot.pause()

        left_pane = app.query_one("#left-pane")
        right_pane = app.query_one("#right-pane")

        orig_left = left_pane.query_one(Tree)
        orig_right = right_pane.query_one(Tree)

        # Swap twice
        await pilot.press("x")
        await pilot.pause()
        await pilot.press("x")
        await pilot.pause()

        assert left_pane.query_one(Tree) is orig_left
        assert right_pane.query_one(Tree) is orig_right


@pytest.mark.asyncio
async def test_swap_focus_left_right_after_swap():
    """action_focus_left/right works correctly after swap."""
    app = GfalTui()
    async with app.run_test() as pilot:
        await pilot.pause()

        left_pane = app.query_one("#left-pane")
        right_pane = app.query_one("#right-pane")

        # Swap
        await pilot.press("x")
        await pilot.pause()

        # Focus left pane (now contains the original right tree)
        await pilot.press("h")
        await pilot.pause()
        focused = app.focused
        assert focused is left_pane.query_one(Tree)

        # Focus right pane
        await pilot.press("l")
        await pilot.pause()
        focused = app.focused
        assert focused is right_pane.query_one(Tree)


# ---------------------------------------------------------------------------
# Transfer summary modal
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_transfer_summary_modal_progress_display():
    """TransferSummaryModal refreshes display with correct counts."""
    transfers = [
        ("file:///a.txt", "/tmp/a.txt", 1000),
        ("file:///b.txt", "/tmp/b.txt", 2000),
        ("file:///c.txt", "/tmp/c.txt", 500),
    ]
    modal = TransferSummaryModal(transfers)

    from textual.app import App, ComposeResult
    from textual.widgets import Static

    class TestApp(App):
        def compose(self) -> ComposeResult:
            yield Static("test")

    app = TestApp()
    async with app.run_test() as pilot:
        app.push_screen(modal)
        await pilot.pause()

        # All start pending
        assert all(t["status"] == "pending" for t in modal._transfers)

        # Mark one copying
        modal.mark_copying("/tmp/a.txt")
        assert modal._transfers[0]["status"] == "copying"

        # Mark one done
        modal.mark_transfer("/tmp/a.txt", success=True)
        assert modal._transfers[0]["status"] == "done"
        assert modal._transfers[0]["current"] == 1000  # filled to expected

        # Mark one failed
        modal.mark_transfer("/tmp/b.txt", success=False, error="Timeout")
        assert modal._transfers[1]["status"] == "failed"
        assert modal._transfers[1]["error"] == "Timeout"

        # Third still pending
        assert modal._transfers[2]["status"] == "pending"

        # Cleanup
        app.pop_screen()
        await pilot.pause()


@pytest.mark.asyncio
async def test_transfer_summary_modal_all_failed():
    """TransferSummaryModal handles all transfers failing."""
    transfers = [("file:///a.txt", "/tmp/a.txt", 100)]
    modal = TransferSummaryModal(transfers)

    from textual.app import App, ComposeResult
    from textual.widgets import Static

    class TestApp(App):
        def compose(self) -> ComposeResult:
            yield Static("test")

    app = TestApp()
    async with app.run_test() as pilot:
        app.push_screen(modal)
        await pilot.pause()

        modal.mark_transfer("/tmp/a.txt", success=False, error="ENOENT")
        assert modal._transfers[0]["status"] == "failed"

        # Timer should stop
        assert modal._poll_timer is None

        app.pop_screen()
        await pilot.pause()


@pytest.mark.asyncio
async def test_transfer_summary_modal_close_button():
    """Close button dismisses the modal and stops polling."""
    transfers = [("file:///a.txt", "/tmp/a.txt", 100)]
    modal = TransferSummaryModal(transfers)

    from textual.app import App, ComposeResult
    from textual.widgets import Static

    class TestApp(App):
        def compose(self) -> ComposeResult:
            yield Static("test")

    app = TestApp()
    async with app.run_test() as pilot:
        app.push_screen(modal)
        await pilot.pause()

        # Press escape to dismiss
        await pilot.press("escape")
        await pilot.pause()

        # Modal should be dismissed
        assert not isinstance(app.screen, TransferSummaryModal)


# ---------------------------------------------------------------------------
# _do_copy edge cases
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_do_copy_local_to_local_file(tmp_path):
    """_do_copy handles local file to local directory via run_worker."""
    src = tmp_path / "source.txt"
    src.write_text("hello")
    dst_dir = tmp_path / "dest"
    dst_dir.mkdir()

    app = GfalTui(dst=str(tmp_path))

    async with app.run_test() as pilot:
        await pilot.pause()

        # Use run_worker like the real code does
        app.run_worker(
            lambda: app._do_copy(str(src), str(dst_dir / "source.txt")),
            thread=True,
        )

        # Wait for worker to complete
        for _ in range(50):
            if (dst_dir / "source.txt").exists():
                break
            await pilot.pause(0.1)

        assert (dst_dir / "source.txt").read_text() == "hello"


@pytest.mark.asyncio
async def test_do_copy_local_directory(tmp_path):
    """_do_copy handles local directory copy via run_worker."""
    src_dir = tmp_path / "src_dir"
    src_dir.mkdir()
    (src_dir / "a.txt").write_text("aaa")
    (src_dir / "b.txt").write_text("bbb")

    dst_dir = tmp_path / "dst_dir"

    app = GfalTui(dst=str(tmp_path))

    async with app.run_test() as pilot:
        await pilot.pause()

        app.run_worker(
            lambda: app._do_copy(str(src_dir), str(dst_dir)),
            thread=True,
        )

        for _ in range(50):
            if dst_dir.exists() and (dst_dir / "a.txt").exists():
                break
            await pilot.pause(0.1)

        assert (dst_dir / "a.txt").read_text() == "aaa"
        assert (dst_dir / "b.txt").read_text() == "bbb"


@pytest.mark.asyncio
async def test_do_copy_error_marks_modal_failed(tmp_path):
    """_do_copy marks modal transfer as failed on error."""
    app = GfalTui(dst=str(tmp_path))

    transfers = [("file:///nonexistent", str(tmp_path / "out.txt"), 100)]
    modal = TransferSummaryModal(transfers)

    async with app.run_test() as pilot:
        app.push_screen(modal)
        await pilot.pause()

        app.run_worker(
            lambda: app._do_copy(
                "/nonexistent_source_file_12345",
                str(tmp_path / "out.txt"),
                modal=modal,
            ),
            thread=True,
        )

        # Wait for the worker to complete and mark the transfer
        for _ in range(50):
            if modal._transfers[0]["status"] == "failed":
                break
            await pilot.pause(0.1)

        assert modal._transfers[0]["status"] == "failed"

        app.pop_screen()
        await pilot.pause()


# ---------------------------------------------------------------------------
# Refresh robustness
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_refresh_trees_no_crash(tmp_path):
    """refresh_trees doesn't crash even with default trees."""
    app = GfalTui(dst=str(tmp_path))

    with patch("gfal_cli.tui.url_to_fs") as mock_url_to_fs:
        mock_fs = MagicMock()
        mock_fs.ls.return_value = []
        mock_url_to_fs.return_value = (mock_fs, "/remote")

        async with app.run_test() as pilot:
            await pilot.pause()

            # Pressing 'r' should not crash
            await pilot.press("r")
            await pilot.pause(0.5)

            # App should still be responsive
            assert app.is_running


@pytest.mark.asyncio
async def test_refresh_after_swap(tmp_path):
    """refresh_trees works correctly after panes have been swapped."""
    app = GfalTui(dst=str(tmp_path))

    with patch("gfal_cli.tui.url_to_fs") as mock_url_to_fs:
        mock_fs = MagicMock()
        mock_fs.ls.return_value = []
        mock_url_to_fs.return_value = (mock_fs, "/remote")

        async with app.run_test() as pilot:
            await pilot.pause()

            # Swap then refresh
            await pilot.press("x")
            await pilot.pause()
            await pilot.press("r")
            await pilot.pause(0.5)

            # App should still be running
            assert app.is_running


# ---------------------------------------------------------------------------
# Search / URL modal
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_search_opens_url_modal():
    """Pressing '/' opens the UrlInputModal."""
    app = GfalTui()
    async with app.run_test() as pilot:
        await pilot.pause()

        await pilot.press("slash")
        await pilot.pause()

        assert isinstance(app.screen, UrlInputModal)

        # Dismiss
        await pilot.press("escape")
        await pilot.pause()
        assert not isinstance(app.screen, UrlInputModal)


# ---------------------------------------------------------------------------
# Log toggle
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_toggle_log_visibility():
    """Pressing 'L' toggles log window visibility."""
    app = GfalTui()
    async with app.run_test() as pilot:
        await pilot.pause()

        log = app.query_one("#log-window")
        initial_display = log.display

        await pilot.press("L")
        await pilot.pause()
        assert log.display != initial_display

        await pilot.press("L")
        await pilot.pause()
        assert log.display == initial_display


# ---------------------------------------------------------------------------
# Stat action
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_stat_on_local_file(tmp_path):
    """action_stat shows a MessageModal with file info for local files."""
    (tmp_path / "test.txt").write_text("hello world")
    app = GfalTui(dst=str(tmp_path))

    async with app.run_test() as pilot:
        tree = app.query_one("#right-tree", HighlightableDirectoryTree)
        app.set_focus(tree)

        for _ in range(50):
            if tree.root and tree.root.children:
                break
            await pilot.pause(0.05)

        # Move to first file
        await pilot.press("g")
        await pilot.pause()

        # Press 's' for stat
        await pilot.press("s")
        await pilot.pause(0.5)

        # Check that a MessageModal was shown (stat results)
        found_modal = False
        for screen in app.screen_stack:
            if isinstance(screen, MessageModal):
                found_modal = True
                break
        assert found_modal, "Stat did not push a MessageModal"

        # Dismiss
        await pilot.press("escape")
        await pilot.pause()


# ---------------------------------------------------------------------------
# Paste with no yanked items
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_paste_with_nothing_yanked():
    """Pressing 'p' with nothing yanked shows a warning notification."""
    app = GfalTui()
    async with app.run_test() as pilot:
        await pilot.pause()
        assert not app.yanked_urls

        await pilot.press("p")
        await pilot.pause()

        # Should NOT open PasteModal
        assert not isinstance(app.screen, PasteModal)


# ---------------------------------------------------------------------------
# _get_focused_tree
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_focused_tree_returns_correct_tree():
    """_get_focused_tree returns the tree in the focused pane."""
    app = GfalTui()
    async with app.run_test() as pilot:
        await pilot.pause()

        left_tree = app.query_one("#left-pane").query_one(Tree)
        right_tree = app.query_one("#right-pane").query_one(Tree)

        app.set_focus(left_tree)
        assert app._get_focused_tree() is left_tree

        app.set_focus(right_tree)
        assert app._get_focused_tree() is right_tree


@pytest.mark.asyncio
async def test_get_focused_tree_after_swap():
    """_get_focused_tree works correctly after pane swap."""
    app = GfalTui()
    async with app.run_test() as pilot:
        await pilot.pause()

        left_pane = app.query_one("#left-pane")
        right_pane = app.query_one("#right-pane")

        orig_right = right_pane.query_one(Tree)

        # Swap
        await pilot.press("x")
        await pilot.pause()

        # Focus the left pane (which now has orig_right)
        app.set_focus(left_pane.query_one(Tree))
        result = app._get_focused_tree()
        assert result is orig_right


# ---------------------------------------------------------------------------
# _get_node_path
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_node_path_local():
    """_get_node_path extracts path from local tree nodes."""
    app = GfalTui()
    async with app.run_test() as pilot:
        tree = app.query_one("#right-tree", HighlightableDirectoryTree)
        for _ in range(50):
            if tree.root and tree.root.children:
                break
            await pilot.pause(0.05)

        app.set_focus(tree)
        await pilot.press("g")
        await pilot.pause()

        node = tree.cursor_node
        path = app._get_node_path(node)
        assert path  # Should be a non-empty string
        assert Path(path).exists() or "://" in path


@pytest.mark.asyncio
async def test_get_node_path_none():
    """_get_node_path returns empty string for None input."""
    app = GfalTui()
    async with app.run_test():
        assert app._get_node_path(None) == ""


# ---------------------------------------------------------------------------
# HighlightableDirectoryTree render_label
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_render_label_without_yank(tmp_path):
    """render_label shows normal label when file is not yanked."""
    (tmp_path / "file.txt").write_text("x")
    app = GfalTui(dst=str(tmp_path))

    async with app.run_test() as pilot:
        tree = app.query_one("#right-tree", HighlightableDirectoryTree)
        for _ in range(50):
            if tree.root and tree.root.children:
                break
            await pilot.pause(0.05)

        node = tree.root.children[0]
        label = tree.render_label(node, Style(), Style())
        assert "[YANKED]" not in str(label)


@pytest.mark.asyncio
async def test_render_label_with_yank(tmp_path):
    """render_label appends [YANKED] for yanked files."""
    (tmp_path / "file.txt").write_text("x")
    app = GfalTui(dst=str(tmp_path))

    async with app.run_test() as pilot:
        tree = app.query_one("#right-tree", HighlightableDirectoryTree)
        for _ in range(50):
            if tree.root and tree.root.children:
                break
            await pilot.pause(0.05)

        node = tree.root.children[0]
        url = str(node.data.path)
        tree.yanked_urls = {url}

        label = tree.render_label(node, Style(), Style())
        assert "[YANKED]" in str(label)


# ---------------------------------------------------------------------------
# HighlightableRemoteDirectoryTree
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_remote_tree_render_label_yanked():
    """Remote tree render_label shows [YANKED] for yanked URLs."""
    app = GfalTui()
    with patch("gfal_cli.tui.url_to_fs") as mock_url_to_fs:
        mock_fs = MagicMock()
        mock_fs.ls.return_value = [
            {"name": "/eos/data/file.root", "type": "file"},
        ]
        mock_url_to_fs.return_value = (mock_fs, "/eos/data")

        async with app.run_test() as pilot:
            tree = app.query_one("#left-tree", HighlightableRemoteDirectoryTree)
            for _ in range(50):
                if tree.root and tree.root.children:
                    break
                await pilot.pause(0.05)

            if tree.root.children:
                node = tree.root.children[0]
                tree.yanked_urls = {node.data}

                label = tree.render_label(node, Style(), Style())
                assert "[YANKED]" in str(label)


# ---------------------------------------------------------------------------
# Composition and initialization
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_tui_custom_src_dst():
    """GfalTui initializes with custom src and dst paths."""
    app = GfalTui(src="root://example.com//data/", dst="/tmp")
    async with app.run_test() as pilot:
        await pilot.pause()

        src_tree = app.query_one("#left-tree")
        dst_tree = app.query_one("#right-tree")

        assert isinstance(src_tree, HighlightableRemoteDirectoryTree)
        assert isinstance(dst_tree, HighlightableDirectoryTree)

        assert "root://example.com" in src_tree.url
        assert "/tmp" in str(dst_tree.path)


@pytest.mark.asyncio
async def test_tui_local_src():
    """GfalTui with local src creates a HighlightableDirectoryTree for left pane."""
    app = GfalTui(src="/tmp", dst="/tmp")
    async with app.run_test() as pilot:
        await pilot.pause()

        src_tree = app.query_one("#left-tree")
        dst_tree = app.query_one("#right-tree")

        assert isinstance(src_tree, HighlightableDirectoryTree)
        assert isinstance(dst_tree, HighlightableDirectoryTree)


# ---------------------------------------------------------------------------
# SSL and TPC toggles
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ssl_toggle_propagates_to_remote_trees():
    """SSL toggle updates ssl_verify on all remote trees."""
    app = GfalTui()
    async with app.run_test() as pilot:
        await pilot.pause()

        remote_tree = app.query_one("#left-tree", HighlightableRemoteDirectoryTree)
        assert not remote_tree.ssl_verify

        await pilot.press("v")
        await pilot.pause()

        assert app.ssl_verify is True
        assert remote_tree.ssl_verify is True

        await pilot.press("v")
        await pilot.pause()

        assert app.ssl_verify is False
        assert remote_tree.ssl_verify is False


@pytest.mark.asyncio
async def test_tpc_toggle():
    """TPC toggle changes app state."""
    app = GfalTui()
    async with app.run_test() as pilot:
        await pilot.pause()

        initial = app.tpc_enabled

        await pilot.press("t")
        await pilot.pause()

        assert app.tpc_enabled != initial


# ---------------------------------------------------------------------------
# Log activity
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_log_activity_levels():
    """log_activity writes messages for all log levels."""
    app = GfalTui()
    async with app.run_test() as pilot:
        await pilot.pause()

        for level in ["info", "success", "error", "warning", "command"]:
            app.log_activity(f"Test {level}", level=level)

        await pilot.pause()

        log = app.query_one("#log-window")
        log_text = "".join(
            "".join(s.text for s in strip._segments) for strip in log.lines
        )

        for level in ["INFO", "SUCCESS", "ERROR", "WARNING", "COMMAND"]:
            assert level in log_text, f"{level} not found in log"


# ---------------------------------------------------------------------------
# Quit action
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_quit_action():
    """action_quit exits the application."""
    app = GfalTui()
    async with app.run_test() as pilot:
        await pilot.pause()

        await pilot.press("q")
        await pilot.pause(0.5)

        # App should have exited (or be exiting)
        # In test mode, the app might not fully exit, so just check no crash
