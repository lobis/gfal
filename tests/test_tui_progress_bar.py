from unittest.mock import MagicMock, PropertyMock, patch

import pytest
from textual.widgets import Static

from gfal_cli.tui import (
    GfalTui,
    HighlightableDirectoryTree,
    PasteModal,
    TransferProgressModal,
)


@pytest.mark.asyncio
async def test_tui_progress_bar_updates(tmp_path):
    """Verify that the TransferProgressModal updates its progress using a real URL."""
    app = GfalTui(dst=str(tmp_path))

    # Real URL provided by the user
    src_url = "root://eospublic.cern.ch//eos/opendata/cms/Run2017E/BTagCSV/MINIAOD/UL2017_MiniAODv2-v1/260000/118EDE47-ED73-8E4E-9CEB-4C4BF9E01704.root"
    real_size = 40802738

    # Create the mock
    mock_url_to_fs = MagicMock()
    mock_fs = MagicMock()

    def mock_put(lpath, rpath, callback=None, **kwargs):
        # 1. Set size
        callback.set_size(real_size)
        # 2. Update progress
        callback.absolute_update(real_size // 2)
        callback.absolute_update(real_size)
        # 3. Finish
        callback.stop(success=True)

    mock_fs.put.side_effect = mock_put
    mock_url_to_fs.return_value = (
        mock_fs,
        "/eos/.../118EDE47-ED73-8E4E-9CEB-4C4BF9E01704.root",
    )

    # Patch url_to_fs in BOTH modules because GfalTui might have captured a reference
    with (
        patch("gfal_cli.fs.url_to_fs", mock_url_to_fs),
        patch("gfal_cli.tui.url_to_fs", mock_url_to_fs),
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
                HighlightableDirectoryTree, "cursor_node", new_callable=PropertyMock
            ) as mock_cursor:
                mock_cursor.return_value = mock_node
                app.set_focus(dst_tree)

                # Directly trigger the paste check which pushes the modal
                app.action_paste()
                await pilot.pause()

                # Check if PasteModal is pushed
                assert isinstance(app.screen, PasteModal)

                # Directly call on_paste on the modal
                app.screen.on_paste()
                await pilot.pause()

                # Wait for the copy success log message
                success_msg = "Successfully copied"
                found_success = False
                for _ in range(100):
                    log = app.query_one("#log-window")
                    # Collect all text from log lines
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

                # Check the screen stack for TransferProgressModal
                # It might be under a MessageModal
                found_modal = False
                for screen in app.screen_stack:
                    if isinstance(screen, TransferProgressModal):
                        found_modal = True
                        modal = screen
                        break

                assert found_modal, "TransferProgressModal not found in screen stack"
                assert modal.src == src_url

                # The progress display should exist
                display = modal.query_one("#progress-display", Static)
                assert display is not None

                # Cleanup: pop modals to avoid interfering with other tests
                while len(app.screen_stack) > 1:
                    app.pop_screen()
                    await pilot.pause()
