from unittest.mock import MagicMock, patch

import pytest

from gfal_cli.tui import (
    GfalTui,
    HighlightableDirectoryTree,
    HighlightableRemoteDirectoryTree,
    PasteModal,
)


@pytest.mark.asyncio
async def test_tui_yank_functionality():
    """Test that pressing 'y' yanks the selected item and highlights it."""
    app = GfalTui()
    async with app.run_test() as pilot:
        local_tree = app.query_one("#right-tree", HighlightableDirectoryTree)
        # Wait for trees to be ready and have nodes
        for _ in range(20):
            if local_tree.root and local_tree.root.children:
                break
            await pilot.pause(0.01)

        # Ensure tree is focused
        app.set_focus(local_tree)
        await pilot.press("down")
        node = local_tree.cursor_node
        assert node is not None

        path = str(node.data.path)

        # Yank it
        await pilot.press("y")

        assert path in app.yanked_urls

        # Verify highlight in label
        label = local_tree.render_label(node, MagicMock(), MagicMock())
        assert "[YANKED]" in str(label)

        # Verify other tree also has the yanked_url set
        remote_tree = app.query_one("#left-tree", HighlightableRemoteDirectoryTree)
        assert path in remote_tree.yanked_urls


@pytest.mark.asyncio
async def test_tui_paste_modal_trigger():
    """Test that pressing 'p' on a directory opens the PasteModal."""
    app = GfalTui()
    with patch("gfal_cli.tui.url_to_fs") as mock_url_to_fs:
        mock_fs = MagicMock()
        mock_fs.ls.return_value = [{"name": "file.txt", "type": "file"}]
        mock_url_to_fs.return_value = (mock_fs, "/remote")

        async with app.run_test() as pilot:
            await pilot.pause()
            # Yank something first
            app.yanked_urls = {"/tmp/source.txt"}

            # Focus remote tree and select a directory (root is a directory)
            remote_tree = app.query_one("#left-tree", HighlightableRemoteDirectoryTree)
            app.set_focus(remote_tree)

            # Wait for nodes to be available
            for _ in range(50):
                if remote_tree.root and remote_tree.root.children:
                    break
                await pilot.pause(0.01)

            # Ensure the root node is selected for pasting
            if remote_tree.root:
                remote_tree.select_node(remote_tree.root)
            await pilot.pause()

            # Press 'p'
            await pilot.press("p")

            # Check if PasteModal is active
            assert isinstance(app.screen, PasteModal)
            # PasteModal has src_urls (set), not src_url
            assert "/tmp/source.txt" in app.screen.src_urls
            assert app.screen.dst_dir == remote_tree.root.data


@pytest.mark.asyncio
async def test_tui_paste_execution():
    """Test that submitting PasteModal triggers _do_copy."""
    app = GfalTui()
    # We patch _do_copy because it runs in a background thread and we just want to verify trigger
    with (
        patch.object(GfalTui, "_do_copy") as mock_do_copy,
        patch("gfal_cli.tui.url_to_fs") as mock_url_to_fs,
    ):
        mock_fs = MagicMock()
        mock_fs.ls.return_value = [{"name": "file.txt", "type": "file"}]
        mock_url_to_fs.return_value = (mock_fs, "/remote")

        async with app.run_test() as pilot:
            await pilot.pause()
            app.yanked_urls = {"/tmp/file.txt"}
            remote_tree = app.query_one("#left-tree", HighlightableRemoteDirectoryTree)
            app.set_focus(remote_tree)

            # Wait for nodes to be available
            for _ in range(50):
                if remote_tree.root and remote_tree.root.children:
                    break
                await pilot.pause(0.01)

            # Ensure the root node is selected for pasting
            if remote_tree.root:
                remote_tree.select_node(remote_tree.root)
            await pilot.pause()

            await pilot.press("p")
            assert isinstance(app.screen, PasteModal)

            # Type a name and press enter
            # We don't need to clear exactly, just append or type
            await pilot.press("n", "e", "w", ".", "t", "x", "t", "enter")

            # Wait for any background task to be triggered
            await pilot.pause()

            # The destination path should be remote_root + /new.txt
            expected_dst = remote_tree.root.data.rstrip("/") + "/new.txt"

            # Since _do_copy is run via run_worker, we might need a bit more pause
            import asyncio

            await asyncio.sleep(0.1)

            mock_do_copy.assert_called_once()
            args, _ = mock_do_copy.call_args
            assert args[0] == "/tmp/file.txt"
            assert args[1] == expected_dst


@pytest.mark.asyncio
async def test_tui_paste_to_file_uses_parent():
    """Verify that pasting onto a file uses the parent directory as destination."""
    app = GfalTui()
    with (
        patch.object(GfalTui, "_do_copy") as mock_do_copy,
        patch("gfal_cli.tui.url_to_fs") as mock_url_to_fs,
    ):
        mock_fs = MagicMock()
        # Mock a file entry
        mock_fs.ls.return_value = [{"name": "/remote/file.txt", "type": "file"}]
        mock_url_to_fs.return_value = (mock_fs, "/remote")

        async with app.run_test() as pilot:
            await pilot.pause()
            app.yanked_urls = {"/tmp/source.txt"}

            # Focus remote tree
            tree = app.query_one("#left-tree", HighlightableRemoteDirectoryTree)
            app.set_focus(tree)

            # Wait for nodes and select the file (child of root)
            for _ in range(200):
                if tree.root and tree.root.children:
                    break
                await pilot.pause(0.05)

            file_node = tree.root.children[0]
            tree.select_node(file_node)
            await pilot.pause()

            # Trigger paste
            await pilot.press("p")
            assert isinstance(app.screen, PasteModal)

            # The destination directory in the modal should be the parent (the root URL)
            assert app.screen.dst_dir == tree.root.data

            # Submit
            await pilot.press("enter")
            await pilot.pause()

            import asyncio

            await asyncio.sleep(0.1)

            mock_do_copy.assert_called_once()
            args, _ = mock_do_copy.call_args
            # Expected destination: root_url + /source.txt
            expected_dst = tree.root.data.rstrip("/") + "/source.txt"
            assert args[1] == expected_dst


@pytest.mark.asyncio
async def test_tui_local_paste_to_file_uses_parent(tmp_path):
    """Verify that pasting onto a local file uses the parent directory as destination."""
    app = GfalTui()
    # Mock _do_copy to verify it gets the right destination
    with patch.object(GfalTui, "_do_copy") as mock_do_copy:
        async with app.run_test() as pilot:
            # Create a source file and a destination file
            src_file = tmp_path / "src.txt"
            src_file.write_text("source")
            dst_dir = tmp_path / "subdir"
            dst_dir.mkdir()
            dst_file = dst_dir / "target.txt"
            dst_file.write_text("target")

            app.yanked_urls = {str(src_file)}

            # Since DirectoryTree loads the FS, we need to wait or mock
            # Faster to just set what we need
            from types import SimpleNamespace

            from textual.widgets._directory_tree import DirEntry

            mock_target_node = SimpleNamespace(
                data=DirEntry(path=dst_file, loaded=True),
                parent=SimpleNamespace(data=DirEntry(path=dst_dir, loaded=True)),
            )
            mock_tree = SimpleNamespace(
                cursor_node=mock_target_node,
            )
            # Ensure mock_tree is identified as local (not HighlightableRemoteDirectoryTree)
            # Use isinstance check in the code: is_remote = isinstance(tree, HighlightableRemoteDirectoryTree)
            # SimpleNamespace is NOT HighlightableRemoteDirectoryTree, so it's correct.

            # Mock _get_focused_tree to return our mock_tree
            with patch.object(GfalTui, "_get_focused_tree", return_value=mock_tree):
                await pilot.press("p")
                assert isinstance(app.screen, PasteModal)
                # The destination directory in the modal should be the parent
                assert app.screen.dst_dir == str(dst_dir)

                # Submit
                await pilot.press("enter")
                await pilot.pause()

                import asyncio

                await asyncio.sleep(0.1)

                mock_do_copy.assert_called_once()
                args, _ = mock_do_copy.call_args
                # Expected destination: dst_dir / src.txt
                expected_dst = str(dst_dir / "src.txt")
                assert args[1] == expected_dst
