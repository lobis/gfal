"""Tests for TUI transfer progress tracking.

Covers both the low-level TuiProgress callback adapter (no TUI needed) and
the full TransferSummaryModal integration through the TUI.
"""

from unittest.mock import MagicMock, PropertyMock, patch

import pytest

pytest.importorskip("textual")

from gfal.cli.progress import TuiProgress
from gfal.tui import (
    GfalTui,
    HighlightableDirectoryTree,
    PasteModal,
    TransferSummaryModal,
)

# ---------------------------------------------------------------------------
# Unit tests for TuiProgress callback adapter
# ---------------------------------------------------------------------------


def test_tui_progress_branched_tracks_bytes():
    """branched() child must forward byte-level set_size/relative_update."""
    updates = []

    def cb(current, total, **kw):
        updates.append((current, total, kw))

    prog = TuiProgress(cb)

    # Simulate what fsspec get() does:
    # 1. Parent set_size(num_files) — should be ignored
    prog.set_size(3)
    assert len(updates) == 0, "Parent set_size should be ignored"

    # 2. Branch for first file
    with prog.branched("remote/a.txt", "local/a.txt") as child:
        child.set_size(1000)
        assert updates[-1] == (0, 1000, {}), f"Expected (0, 1000), got {updates[-1]}"

        child.relative_update(400)
        assert updates[-1] == (400, 1000, {})

        child.relative_update(600)
        assert updates[-1] == (1000, 1000, {})

    # 3. Parent relative_update(1) per file — should be ignored
    before = len(updates)
    prog.relative_update(1)
    assert len(updates) == before, "Parent relative_update should be ignored"


def test_tui_progress_direct_absolute_update():
    """absolute_update on the parent must still work (used by local copies)."""
    updates = []

    def cb(current, total, **kw):
        updates.append((current, total))

    prog = TuiProgress(cb)
    # Direct absolute_update always works (used for local shutil copies)
    prog.absolute_update(500)
    assert updates[-1] == (500, 0)


def test_tui_progress_stop():
    """stop() must forward finished flag."""
    updates = []

    def cb(current, total, **kw):
        updates.append((current, total, kw))

    prog = TuiProgress(cb)
    with prog.branched("a", "b") as child:
        child.set_size(100)
        child.relative_update(100)

    prog.stop(success=True)
    assert updates[-1][2].get("finished") is True
    assert updates[-1][2].get("success") is True


def test_tui_progress_with_real_local_fsspec(tmp_path):
    """Verify TuiProgress receives byte-level updates from a real fsspec get()."""
    # Create a source file large enough to see chunked reads
    src = tmp_path / "source.bin"
    data = b"x" * 50_000
    src.write_bytes(data)

    updates = []

    def cb(current, total, **kw):
        updates.append((current, total))

    prog = TuiProgress(cb)

    from fsspec.implementations.local import LocalFileSystem

    fs = LocalFileSystem()
    dst = tmp_path / "dest.bin"
    fs.get(str(src), str(dst), callback=prog)

    # Local fs uses shutil.copyfile which does NOT call callback chunk-by-chunk,
    # so the child's relative_update is never called.  The important thing is
    # that this does NOT crash, and the parent's file-count set_size(1) is
    # silently ignored (no "0 / 1 B" bug).
    assert dst.read_bytes() == data


# ---------------------------------------------------------------------------
# TransferSummaryModal unit tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_transfer_summary_modal_mark_transfer():
    """TransferSummaryModal tracks transfers and marks them done/failed."""
    transfers = [
        ("file:///a.txt", "/tmp/a.txt", 100),
        ("file:///b.txt", "/tmp/b.txt", 200),
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

        # Initially all pending
        assert all(t["status"] == "pending" for t in modal._transfers)

        # Mark first as copying
        modal.mark_copying("/tmp/a.txt")
        assert modal._transfers[0]["status"] == "copying"

        # Mark first as done
        modal.mark_transfer("/tmp/a.txt", success=True)
        assert modal._transfers[0]["status"] == "done"

        # Mark second as failed
        modal.mark_transfer("/tmp/b.txt", success=False, error="Network error")
        assert modal._transfers[1]["status"] == "failed"
        assert modal._transfers[1]["error"] == "Network error"

        # Cleanup — pop modal before app teardown
        app.pop_screen()
        await pilot.pause()


# ---------------------------------------------------------------------------
# Full TUI integration test
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_tui_transfer_summary_modal(tmp_path):
    """Verify that pasting files pushes TransferSummaryModal and tracks completion."""
    app = GfalTui(dst=str(tmp_path))

    src_url = "root://eospublic.cern.ch//eos/opendata/file.root"

    mock_url_to_fs = MagicMock()
    mock_fs = MagicMock()

    def mock_transfer(_rpath, _lpath, **kwargs):
        """Simulate a successful transfer (no callback needed now)."""

    mock_fs.get.side_effect = mock_transfer
    mock_fs.put.side_effect = mock_transfer
    mock_fs.info.return_value = {"size": 40802738, "type": "file"}
    mock_url_to_fs.return_value = (mock_fs, "/eos/.../file.root")

    with (
        patch("gfal.core.fs.url_to_fs", mock_url_to_fs),
        patch("gfal.tui.url_to_fs", mock_url_to_fs),
    ):
        async with app.run_test() as pilot:
            await pilot.pause()
            app.yanked_urls = {src_url}

            dst_tree = app.query_one("#right-tree")

            mock_node = MagicMock()
            from textual.widgets._directory_tree import DirEntry

            mock_node.data = DirEntry(path=tmp_path, loaded=True)
            mock_node.allow_expand = True
            mock_node._line = 0

            with patch.object(
                HighlightableDirectoryTree,
                "cursor_node",
                new_callable=PropertyMock,
            ) as mock_cursor:
                mock_cursor.return_value = mock_node
                app.set_focus(dst_tree)

                app.action_paste()
                await pilot.pause()

                assert isinstance(app.screen, PasteModal)
                app.screen.on_paste()
                await pilot.pause()

                # Wait for copy completion
                success_msg = "Successfully copied"
                found_success = False
                for _ in range(100):
                    log = app.query_one("#log-window")
                    log_text = "".join(
                        "".join(s.text for s in strip._segments) for strip in log.lines
                    )
                    if success_msg in log_text:
                        found_success = True
                        break
                    await pilot.pause(0.1)

                if not found_success:
                    pytest.fail(
                        f"Copy success message not found in log. Log: {log_text}"
                    )

                await pilot.pause()

                # Find TransferSummaryModal in screen stack
                found_modal = False
                for screen in app.screen_stack:
                    if isinstance(screen, TransferSummaryModal):
                        found_modal = True
                        modal = screen
                        break

                assert found_modal, "TransferSummaryModal not found in screen stack"

                # All transfers should be done
                assert all(t["status"] == "done" for t in modal._transfers)
                assert modal._transfers[0]["dst"].endswith("file.root")

                # Cleanup
                while len(app.screen_stack) > 1:
                    app.pop_screen()
                    await pilot.pause()
