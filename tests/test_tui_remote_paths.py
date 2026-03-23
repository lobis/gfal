from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from gfal_cli.tui import GfalTui, HighlightableRemoteDirectoryTree


@pytest.mark.asyncio
async def test_tui_remote_command_logging_includes_base_url():
    """Verify that commands logged for remote files include the full SURL."""
    app = GfalTui()
    test_path = "/eos/opendata/cms/file.txt"
    # Mock url_to_fs so we don't hit the network
    with patch("gfal_cli.tui.url_to_fs") as mock_url_to_fs:
        mock_fs = MagicMock()
        # Mock ls to return our test file
        mock_fs.ls.return_value = [{"name": test_path, "type": "file"}]
        mock_fs.info.return_value = {"size": 100, "type": "file"}
        mock_url_to_fs.return_value = (mock_fs, test_path)

        async with app.run_test() as pilot:
            # Source tree (left) is remote by default
            tree = app.query_one("#left-tree", HighlightableRemoteDirectoryTree)
            app.set_focus(tree)

            # Wait for nodes to load
            for _ in range(50):
                if tree.root and tree.root.children:
                    break
                await pilot.pause(0.02)

            assert tree.root.children, "Remote tree children did not load"

            # Move cursor from root to the first file node
            await pilot.press("down")
            await pilot.pause(0.1)

            # Select the file node (though cursor_node is what action_stat uses)
            file_node = tree.root.children[0]
            tree.select_node(file_node)
            await pilot.pause()

            # Trigger Stat
            await pilot.press("s")
            await pilot.pause(0.1)

            # Check the log for the command (join all strips — wrapping may
            # split a single log entry across multiple render lines)
            log = app.query_one("#log-window")
            all_log_text = "".join(
                "".join(s.text for s in strip._segments) for strip in log.lines
            )
            # We expect gfal stat root://eospublic.cern.ch//eos/opendata/cms/file.txt
            assert (
                "gfal stat root://eospublic.cern.ch" in all_log_text
                and "file.txt" in all_log_text
            ), f"Command with base URL not found in log. Log: {all_log_text}"


@pytest.mark.asyncio
async def test_tui_root_label_is_descriptive():
    """Verify that the tree root labels clearly show the base path/URL."""
    app = GfalTui(src="root://eospublic.cern.ch/data/", dst="/tmp/local")
    async with app.run_test():
        src_tree = app.query_one("#left-tree")
        dst_tree = app.query_one("#right-tree")

        # Root should be visible
        assert src_tree.show_root is True
        assert dst_tree.show_root is True

        # Labels should be descriptive
        assert "root://eospublic.cern.ch" in str(src_tree.root.label)
        assert Path(str(dst_tree.root.label)).as_posix().endswith("/tmp/local")
