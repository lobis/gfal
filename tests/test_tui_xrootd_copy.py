import asyncio
from unittest.mock import MagicMock, PropertyMock, patch

import pytest

pytest.importorskip("textual")

from gfal.cli.progress import TuiProgress
from gfal.tui import GfalTui, HighlightableDirectoryTree, PasteModal


def test_tui_progress_has_branch_coro():
    """Unit test for TuiProgress.branch_coro fix."""
    tp = TuiProgress(lambda *args, **kwargs: None)
    assert hasattr(tp, "branch_coro")
    assert tp.branch_coro("test_coro") == "test_coro"


@pytest.mark.asyncio
async def test_tui_xrootd_copy_branch_coro_fix(tmp_path):
    """Verify that TuiProgress supports branch_coro, fixing XRootD copy AttributeError."""
    app = GfalTui(dst=str(tmp_path))

    # Use realistic EOS path provided by user
    src_url = "root://eospublic.cern.ch//eos/opendata/cms/Run2017E/BTagCSV/MINIAOD/UL2017_MiniAODv2-v1/260000/118EDE47-ED73-8E4E-9CEB-4C4BF9E01704.root"
    src_filename = "118EDE47-ED73-8E4E-9CEB-4C4BF9E01704.root"

    with patch("gfal.tui.url_to_fs") as mock_url_to_fs:
        mock_fs = MagicMock()

        def mock_put(lpath, rpath, callback=None, **kwargs):
            if hasattr(callback, "branch_coro"):
                callback.branch_coro(asyncio.sleep(0.001))
            # Don't call set_size immediately to test progress bar stall
            callback.absolute_update(100)
            callback.set_size(40802738)
            callback.absolute_update(200)
            callback.stop(success=True)

        mock_fs.put.side_effect = mock_put
        mock_url_to_fs.return_value = (
            mock_fs,
            "/eos/opendata/cms/Run2017E/BTagCSV/MINIAOD/UL2017_MiniAODv2-v1/260000/118EDE47-ED73-8E4E-9CEB-4C4BF9E01704.root",
        )

        async with app.run_test() as pilot:
            await pilot.pause()
            app.yanked_urls = {src_url}

            dst_tree = app.query_one("#right-tree", HighlightableDirectoryTree)

            mock_node = MagicMock()
            from textual.widgets._directory_tree import DirEntry

            mock_node.data = DirEntry(path=tmp_path, loaded=True)
            mock_node.allow_expand = True
            mock_node._line = 0

            with patch.object(
                HighlightableDirectoryTree, "cursor_node", new_callable=PropertyMock
            ) as mock_cursor:
                mock_cursor.return_value = mock_node
                app.set_focus(dst_tree)

                await pilot.press("p")
                assert isinstance(app.screen, PasteModal)

                # Press 'Paste' button
                await pilot.press("tab", "tab", "enter")

                success = False
                for _ in range(100):
                    log = app.query_one("#log-window")
                    log_text = "".join(
                        "".join(s.text for s in strip._segments) for strip in log.lines
                    )
                    if "Successfully copied" in log_text:
                        success = True
                        break
                    await asyncio.sleep(0.1)

                assert success, f"Copy did not succeed in the TUI log. Log: {log_text}"
                assert src_filename in log_text


@pytest.mark.asyncio
async def test_tui_xrootd_multi_copy_branch_coro_fix(tmp_path):
    """Verify that multi-file XRootD copy also works without AttributeErrors."""
    app = GfalTui(dst=str(tmp_path))

    src_urls = {
        "root://eospublic.cern.ch//eos/file1.root",
        "root://eospublic.cern.ch//eos/file2.root",
    }

    with patch("gfal.tui.url_to_fs") as mock_url_to_fs:
        mock_fs = MagicMock()

        def mock_put(lpath, rpath, callback=None, **kwargs):
            if hasattr(callback, "branch_coro"):
                callback.branch_coro(asyncio.sleep(0.001))
            callback.stop(success=True)

        mock_fs.put.side_effect = mock_put
        mock_url_to_fs.return_value = (mock_fs, "/eos")

        async with app.run_test() as pilot:
            await pilot.pause()
            app.yanked_urls = src_urls
            dst_tree = app.query_one("#right-tree", HighlightableDirectoryTree)

            mock_node = MagicMock()
            from textual.widgets._directory_tree import DirEntry

            mock_node.data = DirEntry(path=tmp_path, loaded=True)
            mock_node.allow_expand = True
            mock_node._line = 0

            with patch.object(
                HighlightableDirectoryTree, "cursor_node", new_callable=PropertyMock
            ) as mock_cursor:
                mock_cursor.return_value = mock_node
                app.set_focus(dst_tree)

                await pilot.press("p")
                assert isinstance(app.screen, PasteModal)

                # Press 'Paste' button
                await pilot.press("tab", "enter")

                success_count = 0
                for _ in range(100):
                    log = app.query_one("#log-window")
                    log_text = "".join(
                        "".join(s.text for s in strip._segments) for strip in log.lines
                    )
                    success_count = log_text.count("Successfully copied")
                    if success_count >= 2:
                        break
                    await asyncio.sleep(0.1)

                assert success_count == 2, (
                    f"Expected 2 successes, got {success_count}. Log: {log_text}"
                )
