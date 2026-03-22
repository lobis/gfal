from __future__ import annotations

import sys
import tempfile
import threading
from contextlib import suppress
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urlparse

from rich.style import Style
from rich.text import Text
from textual import events, on
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, Vertical
from textual.reactive import reactive
from textual.screen import ModalScreen
from textual.widgets import (
    Button,
    DirectoryTree,
    Footer,
    Header,
    Input,
    Label,
    RichLog,
    Select,
    Static,
    Tree,
)
from textual.widgets._tree import TreeNode

from gfal_cli.base import CommandBase, arg, interactive
from gfal_cli.fs import compute_checksum, url_to_fs
from gfal_cli.utils import (
    human_readable_size,
    human_readable_time,
)


class HighlightableRemoteDirectoryTree(Tree):
    """A lazy-loading tree for remote filesystems with yank highlight support."""

    yanked_urls: reactive[set[str]] = reactive(set())

    def __init__(self, url: str, ssl_verify: bool = False, **kwargs):
        self.url = url
        self.ssl_verify = ssl_verify
        # Show a shorter base URL in the root label if it's very long
        label = url
        if len(url) > 50:
            parsed = urlparse(url)
            label = f"{parsed.scheme}://{parsed.netloc}/.../{Path(url).name}"
        super().__init__(label, data=url, **kwargs)

    def render_label(
        self, node: TreeNode[Any], base_style: Style, control_style: Style
    ) -> Any:
        label = super().render_label(node, base_style, control_style)
        if node.data in self.yanked_urls:
            if isinstance(label, Text):
                label.append(" [YANKED]", style="bold yellow")
            else:
                label = Text.assemble(label, " [YANKED]", style="bold yellow")
        return label

    def on_mount(self):
        # Use call_after_refresh to ensure the tree is ready
        self.call_after_refresh(self.root.expand)

    def _on_tree_node_expanded(self, event: Tree.NodeExpanded):
        node = event.node
        if not node.children:
            self.run_worker(lambda: self.load_directory(node), thread=True)

    def load_directory(self, node):
        path = node.data
        self.app.log_activity(f"Loading directory: {path}")
        try:
            fs, fs_path = url_to_fs(path, ssl_verify=self.ssl_verify)
            # Use detail=True to distinguish files and directories
            entries = fs.ls(fs_path, detail=True)

            # Get the base URL (protocol + host/authority)
            parsed = urlparse(self.url)
            base_url = f"{parsed.scheme}://{parsed.netloc}"

            def add_nodes():
                for entry in sorted(
                    entries, key=lambda e: (e["type"] != "directory", e["name"])
                ):
                    name = Path(entry["name"]).name
                    if not name:
                        continue
                    is_dir = entry["type"] == "directory"
                    # Ensure node data is a full SURL
                    full_path = entry["name"]
                    if not full_path.startswith((parsed.scheme, "http", "root")):
                        # Handle absolute/relative paths from fsspec
                        if not full_path.startswith("/"):
                            full_path = f"/{full_path}"
                        # Special case for XRootD double slash
                        if parsed.scheme == "root" and not full_path.startswith("//"):
                            full_path = f"/{full_path}"
                        full_path = f"{base_url}{full_path}"

                    node.add(name, data=full_path, allow_expand=is_dir)

                self.app.log_activity(
                    f"Loaded {len(entries)} items from {path}", level="success"
                )

            self.app.call_from_thread(add_nodes)
        except Exception as e:
            error_msg = CommandBase._format_error(e)
            self.app.log_activity(f"Failed to load {path}: {error_msg}", level="error")
            self.app.call_from_thread(
                self.app.notify, f"Error loading {path}: {error_msg}", severity="error"
            )


class HighlightableDirectoryTree(DirectoryTree):
    """A DirectoryTree that supports yank highlight."""

    yanked_urls: reactive[set[str]] = reactive(set())

    def render_label(
        self, node: TreeNode[Any], base_style: Style, control_style: Style
    ) -> Any:
        label = super().render_label(node, base_style, control_style)
        # DirectoryTree data is DirEntry (has .path)
        if (
            node.data
            and hasattr(node.data, "path")
            and str(node.data.path) in self.yanked_urls
        ):
            if isinstance(label, Text):
                label.append(" [YANKED]", style="bold yellow")
            else:
                label = Text.assemble(label, " [YANKED]", style="bold yellow")
        return label

    def on_mount(self) -> None:
        # Update root label to be more descriptive
        if self.root:
            self.root.label = f"Local: {self.path}"


class GfalTui(App):
    """A k9s-style TUI for gfal-cli."""

    TITLE = "gfal"

    ssl_verify = reactive(False)
    tpc_enabled = reactive(True)
    yanked_urls = reactive(set())
    log_file = reactive(str(Path(tempfile.gettempdir()) / "gfal-tui.log"))

    def __init__(
        self,
        log_file: Optional[str] = None,
        src: Optional[str] = None,
        dst: Optional[str] = None,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        if log_file:
            self.log_file = log_file
        self.initial_src = src or "root://eospublic.cern.ch//eos/opendata/cms/"
        self.initial_dst = dst or "./"
        self.last_checksum_algo = "ADLER32"
        self._thread_id = threading.get_ident()

    CSS = """
    Screen {
        background: #1e1e1e;
    }
    .pane {
        width: 50%;
        height: 100%;
        border: solid #333;
    }
    .pane-label {
        padding: 0 1;
        background: $primary;
        color: $text;
        text-align: center;
        width: 100%;
        text-style: bold;
    }
    Input {
        margin: 1;
    }
    Checkbox {
        margin: 1;
        width: auto;
    }
    #input-container {
        display: none;
    }
    #log-window {
        height: 10;
        border: thick $primary;
        margin: 1 2;
    }

    /* Modal styles */
    MessageModal, ChecksumResultModal, UrlInputModal {
        align: center middle;
        background: rgba(0, 0, 0, 0.5);
    }

    #modal-content {
        width: 80;
        max-height: 80%;
        border: thick $primary;
        background: $surface;
        padding: 1 2;
    }

    .modal-row {
        height: auto;
        align: center middle;
        padding: 0 1;
        margin: 1 0;
    }

    .modal-title {
        background: $primary;
        color: $text;
        text-align: center;
        padding: 1;
        margin-bottom: 1;
    }

    .modal-body {
        padding: 1;
    }

    #modal-btn-row {
        align: center middle;
        width: 100%;
        margin-top: 1;
    }
    """

    BINDINGS = [
        ("q", "quit", "Quit"),
        ("s", "stat", "Stat Info"),
        Binding("c", "checksum_request", "Checksum", show=True),
        ("r", "refresh", "Refresh"),
        ("r", "refresh", "Refresh"),
        ("x", "swap", "Swap Panes"),
        ("/", "search", "Search"),
        Binding("j", "cursor_down", "Down", show=False),
        Binding("k", "cursor_up", "Up", show=False),
        Binding("g", "cursor_top", "Top", show=False),
        Binding("G", "cursor_bottom", "Bottom", show=False),
        Binding("v", "toggle_ssl", "SSL [OFF]", show=True),
        Binding("t", "toggle_tpc", "TPC [ON]", show=True),
        Binding("y", "yank", "Yank", show=True),
        Binding("p", "paste", "Paste", show=True),
        Binding("L", "toggle_log", "Log", show=True),
        ("left", "focus_left", "Focus Left"),
        ("right", "focus_right", "Focus Right"),
        Binding("h", "focus_left", "Focus Left", show=False),
        Binding("l", "focus_right", "Focus Right", show=False),
    ]

    def compose(self) -> ComposeResult:
        yield Header()
        with Horizontal():
            with Vertical(classes="pane", id="left-pane"):
                yield Label("Source", classes="pane-header")
                # Detect if initial_src is remote or local
                if "://" in self.initial_src:
                    tree = HighlightableRemoteDirectoryTree(
                        self.initial_src,
                        id="source-tree",
                        ssl_verify=self.ssl_verify,
                    )
                else:
                    tree = HighlightableDirectoryTree(
                        self.initial_src, id="source-tree"
                    )
                tree.show_root = True
                tree.yanked_urls = self.yanked_urls
                yield tree
            with Vertical(classes="pane", id="right-pane"):
                yield Label("Destination", classes="pane-header")
                if "://" in self.initial_dst:
                    tree = HighlightableRemoteDirectoryTree(
                        self.initial_dst,
                        id="dest-tree",
                        ssl_verify=self.ssl_verify,
                    )
                else:
                    tree = HighlightableDirectoryTree(self.initial_dst, id="dest-tree")
                tree.show_root = True
                tree.yanked_urls = self.yanked_urls
                yield tree
        yield RichLog(id="log-window", auto_scroll=True, max_lines=1000)
        yield Footer()

    def on_mount(self) -> None:
        """Called when the app is mounted."""
        self.log_activity("Welcome to gfal-cli TUI", level="info")
        self._update_toggle_labels()
        # Set initial focus to Source tree (Left)
        with suppress(Exception):
            self.query_one("#source-tree").focus()

    def _update_toggle_labels(self) -> None:
        """Update the footer labels for SSL and TPC."""
        ssl_status = "ON" if self.ssl_verify else "OFF"
        tpc_status = "ON" if self.tpc_enabled else "OFF"
        self.bind("v", "toggle_ssl", description=f"SSL [{ssl_status}]")
        self.bind("t", "toggle_tpc", description=f"TPC [{tpc_status}]")
        self.refresh_bindings()
        with suppress(Exception):
            self.query_one(Footer).refresh()

    def action_search(self) -> None:
        """Open a modal to search/input a new URL for the focused pane."""
        self.push_screen(UrlInputModal())

    def on_key(self, event: events.Key) -> None:
        """Handle global keys, especially those swallowed by sub-widgets."""
        if event.key == "left" or event.key == "h":
            self.action_focus_left()
        elif event.key == "right" or event.key == "l":
            self.action_focus_right()

    def action_yank(self) -> None:
        """Yank the current URL (toggle)."""
        tree = self._get_focused_tree()
        if not tree or not tree.cursor_node or not tree.cursor_node.data:
            return

        node = tree.cursor_node
        url = (
            node.data
            if isinstance(tree, HighlightableRemoteDirectoryTree)
            else str(node.data.path)
        )

        # Toggle in the set
        new_yanked = set(self.yanked_urls)
        if url in new_yanked:
            new_yanked.remove(url)
            self.log_activity(f"Un-yanked: {url}")
        else:
            new_yanked.add(url)
            self.log_activity(f"Yanked: {url}", level="success")

        self.yanked_urls = new_yanked

        # Update trees to show highlights
        for tree_widget in self.query(
            "HighlightableDirectoryTree, HighlightableRemoteDirectoryTree"
        ):
            tree_widget.yanked_urls = self.yanked_urls
            tree_widget.refresh()

    def action_paste(self) -> None:
        """Paste the yanked files/directories to the currently selected directory."""
        if not self.yanked_urls:
            self.notify("Nothing to paste!", severity="warning")
            return

        tree = self._get_focused_tree()
        if not tree or not tree.cursor_node:
            return

        # Target must be a directory
        target_node = tree.cursor_node
        is_remote = isinstance(tree, HighlightableRemoteDirectoryTree)

        if is_remote:
            # For remote tree, allow_expand=True for directories, False for files
            if not target_node.allow_expand and target_node.parent:
                target_path = target_node.parent.data
            else:
                target_path = target_node.data
        else:
            # For local tree, node.data is DirEntry (has .is_dir and .path)
            if not target_node.data.is_dir and target_node.parent:
                # Use parent node's data (DirEntry) path
                target_path = str(target_node.parent.data.path)
            else:
                target_path = str(target_node.data.path)

        def handle_paste(confirm_data: Optional[dict]):
            if not confirm_data:
                return

            dest_dir = confirm_data.get("path")
            custom_name = confirm_data.get("name")
            self.log_activity(f"Pasting {len(self.yanked_urls)} items to {dest_dir}")

            for src_url in self.yanked_urls:
                name = Path(urlparse(src_url).path).name
                if not name:
                    name = Path(src_url).name

                # If single file and custom name provided, use it
                actual_name = (
                    custom_name
                    if (custom_name and len(self.yanked_urls) == 1)
                    else name
                )

                if "://" in dest_dir:
                    dst_url = f"{dest_dir.rstrip('/')}/{actual_name}"
                else:
                    dst_url = str(Path(dest_dir) / actual_name)

                self.run_worker(
                    lambda s=src_url, d=dst_url: self._do_copy(s, d),
                    thread=True,
                    name=f"paste_worker_{name}",
                )

        # Use PasteModal (will need update for multiple)
        # For multi-paste, we just confirm the target directory
        self.push_screen(PasteModal(self.yanked_urls, target_path), handle_paste)

    def action_quit(self) -> None:
        """Exit the application cleanly."""
        import os
        import tempfile

        # Inform the user about the log file before immediate exit
        log_path = Path(tempfile.gettempdir()) / "gfal-tui.log"
        # Since we are in the TUI, sys.stdout.write might not be visible
        # until the terminal state is restored. textual.app.App.exit()
        # handles terminal restoration.
        self.exit()

        # Use a small delay to allow textual to restore the terminal state
        # before we hammer it with os._exit(0).
        def force_exit():
            import time

            time.sleep(0.1)
            sys.stdout.write(f"\nTUI exited. Logs are available at: {log_path}\n")
            sys.stdout.flush()

            # Do not os._exit if we are running in a test environment
            if "PYTEST_CURRENT_TEST" not in os.environ:
                os._exit(0)

        import threading

        threading.Thread(target=force_exit, daemon=True).start()

    def action_focus_left(self) -> None:
        """Focus the left pane."""
        with suppress(Exception):
            self.query_one("#left-pane").query_one(Tree).focus()

    def action_focus_right(self) -> None:
        """Focus the right pane."""
        with suppress(Exception):
            self.query_one("#right-pane").query_one(Tree).focus()

    def action_cursor_up(self) -> None:
        """Move cursor up in the focused tree."""
        with suppress(Exception):
            tree = self._get_focused_tree()
            if tree:
                tree.action_cursor_up()

    def action_cursor_down(self) -> None:
        """Move cursor down in the focused tree."""
        with suppress(Exception):
            tree = self._get_focused_tree()
            if tree:
                tree.action_cursor_down()

    def action_cursor_top(self) -> None:
        """Move cursor to the top of the focused tree."""
        with suppress(Exception):
            tree = self._get_focused_tree()
            if tree:
                tree.action_scroll_home()
                # If show_root=False, the first visible node is either root (if shown)
                # or the first child. cursor_line=0 always selects the first visible line.
                tree.cursor_line = 0

    def action_cursor_bottom(self) -> None:
        """Move cursor to the bottom of the focused tree."""
        with suppress(Exception):
            tree = self._get_focused_tree()
            if tree:
                tree.action_scroll_end()

                # Recursively find the last visible node
                def get_last(node):
                    if node.is_expanded and node.children:
                        return get_last(node.children[-1])
                    return node

                last_node = get_last(tree.root)
                tree.select_node(last_node)
                tree.scroll_to_node(last_node)

    async def update_focused_pane(
        self, url: str, tree_to_update: Optional[Tree] = None
    ):
        self.log_activity(f"Updating pane to: {url} (verify={self.ssl_verify})")
        try:
            # Update the specified tree or the currently focused tree
            tree = tree_to_update or self._get_focused_tree()
            if not tree:
                return
            pane = tree.parent
            tree_id = tree.id
            await tree.remove()

            if "://" in url:
                new_tree = HighlightableRemoteDirectoryTree(
                    url, ssl_verify=self.ssl_verify, id=tree_id
                )
            else:
                new_tree = HighlightableDirectoryTree(url, id=tree_id)
            new_tree.show_root = True
            new_tree.yanked_urls = self.yanked_urls
            await pane.mount(new_tree)
        except Exception as e:
            self.log_activity(f"Error updating remote: {e}", level="error")
            self.notify(f"Error updating remote: {e}", severity="error")

    def log_activity(self, message: str, level: str = "info"):
        """Log a message to the TUI log window."""
        from datetime import datetime

        timestamp = datetime.now().strftime("%H:%M:%S")
        colors = {
            "info": "bright_blue",
            "success": "bright_green",
            "error": "bright_red",
            "warning": "bright_yellow",
            "command": "bold magenta",
        }
        color = colors.get(level, "white")

        def do_log():
            from rich.text import Text

            try:
                log_window = self.query_one("#log-window", RichLog)
                log_window.write(
                    Text.from_markup(
                        f"[{timestamp}] [{color}]{level.upper():>7}[/{color}] {message}"
                    )
                )
            except Exception:
                pass

        # Persistence to file
        with suppress(Exception), Path(self.log_file).open("a") as f:
            f.write(f"[{timestamp}] [{level.upper():>7}] {message}\n")

        if threading.get_ident() == self._thread_id:
            do_log()
        else:
            self.call_from_thread(do_log)

    def _get_node_path(self, node: Any) -> str:
        """Extract the string path/URL from a tree node."""
        if not node or not hasattr(node, "data"):
            return ""
        if node.data is None:
            return ""
        # Local DirectoryTree DirEntry
        if hasattr(node.data, "path"):
            return str(node.data.path)
        # Remote SURL string
        return str(node.data)

    def action_stat(self) -> None:
        """Fetch and log information for the selected node."""
        tree = self._get_focused_tree()
        if not tree:
            return
        node = tree.cursor_node
        path = self._get_node_path(node)
        if not path:
            return

        self.log_activity(f"gfal-stat {path}", level="command")
        self.log_activity(f"Fetching stat for: {path}")

        def get_stat():
            try:
                # Determine if it's local or remote based on the tree or path
                fs, fs_path = url_to_fs(path, ssl_verify=self.ssl_verify)
                info = fs.info(fs_path)
                msg = f"Stat Info for {path}:\n"
                for k, v in sorted(info.items()):
                    display_v = v
                    if k == "size":
                        display_v = f"{v} ({human_readable_size(v)})"
                    elif k in ["mtime", "atime", "ctime"] and isinstance(
                        v, (int, float)
                    ):
                        display_v = f"{v} ({human_readable_time(v)})"
                    msg += f"  {k}: {display_v}\n"
                self.log_activity(msg.strip())
                self.call_from_thread(
                    lambda: self.push_screen(
                        MessageModal(msg.strip(), title="Stat Info")
                    )
                )
            except Exception as e:
                error_msg = CommandBase._format_error(e)
                self.log_activity(f"Stat failed for {path}: {error_msg}", level="error")
                self.call_from_thread(
                    lambda: self.push_screen(
                        MessageModal(
                            f"Stat failed for {path}:\n{error_msg}", title="Stat Error"
                        )
                    )
                )

        self.run_worker(get_stat, thread=True)

    def _get_focused_tree(self) -> Optional[Tree]:
        """Helper to get the currently focused tree widget."""
        focused = self.focused
        if not focused:
            return self.query_one("#left-pane").query_one(Tree)

        if isinstance(focused, Tree):
            return focused

        if focused.id == "left-pane":
            return self.query_one("#left-pane").query_one(Tree)
        if focused.id == "right-pane":
            return self.query_one("#right-pane").query_one(Tree)

        return self.query_one("#left-pane").query_one(Tree)

    def action_checksum_request(self) -> None:
        """Open a modal to select checksum algorithm and calculate."""
        tree = self._get_focused_tree()
        if not tree:
            return
        node = tree.cursor_node
        path = self._get_node_path(node)
        if not path:
            return

        # Start initial calculation with last algo and show modal
        self.action_checksum(path, self.last_checksum_algo)

    def action_checksum(
        self, path: str, algo: str, modal: Optional[ChecksumResultModal] = None
    ) -> None:
        """Calculate and log checksum for the selected node."""

        def get_checksum():
            try:
                fs, fs_path = url_to_fs(path, ssl_verify=self.ssl_verify)
                result = compute_checksum(fs, fs_path, algo)
                if result:
                    # Format as hex if it's bytes
                    if isinstance(result, bytes):
                        result = result.hex()
                    msg = f"Checksum ({algo}) for {path}:\n  {result}"
                    self.log_activity(f"gfal-sum {path} {algo}", level="command")
                    self.log_activity(msg, level="success")
                    if modal:
                        modal.result = str(result)
                    else:
                        # Initial calculation: result will be set when modal mounts or manually
                        pass
                else:
                    error_msg = f"Checksum not supported for {path} ({algo})"
                    if modal:
                        modal.result = error_msg
                    self.log_activity(error_msg, level="warning")
            except Exception as e:
                error_msg = CommandBase._format_error(e)
                if modal:
                    modal.result = f"Error: {error_msg}"
                self.log_activity(
                    f"Checksum failed for {path} ({algo}): {error_msg}",
                    level="error",
                )

        if not modal:
            # First time: push the modal and start initial calc
            new_modal = ChecksumResultModal(path, algo)
            self.push_screen(new_modal)
            self.run_worker(get_checksum, thread=True)

            # We need to bridge the worker to the new modal
            # Since worker started, it will update modal.result if it gets it later
            # But the closure already has path and algo.
            # actually, better to pass the modal to the worker if we have it
            # But here we just created it.
            # We'll use a trick: pass newly created modal to the worker's parent scope?
            # Or just rely on handle_checksum to find the modal?
            # Actually, I'll update get_checksum to take modal
            def get_checksum_with_modal(m):
                try:
                    fs, fs_path = url_to_fs(path, ssl_verify=self.ssl_verify)
                    result = compute_checksum(fs, fs_path, algo)
                    # Format as hex if it's bytes
                    if isinstance(result, bytes):
                        result = result.hex()
                    if result:
                        m.result = str(result)
                        self.log_activity(f"gfal-sum {path} {algo}", level="command")
                        self.log_activity(
                            f"Checksum ({algo}) for {path}:\n  {result}",
                            level="success",
                        )
                    else:
                        m.result = f"Not supported for {algo}"
                except Exception as e:
                    error_msg = CommandBase._format_error(e)
                    m.result = f"Error: {error_msg}"

            self.run_worker(lambda: get_checksum_with_modal(new_modal), thread=True)
        else:
            # Subsequent re-calc from Select.Changed
            self.run_worker(get_checksum, thread=True)

    def action_refresh(self) -> None:
        """Refresh the selected directory node."""
        tree = self._get_focused_tree()
        if not tree:
            return
        node = tree.cursor_node
        if not node:
            return

        # Only refresh if it's a directory (remote tree handles expansion)
        path = self._get_node_path(node)
        if isinstance(tree, HighlightableRemoteDirectoryTree):
            self.log_activity(f"gfal-ls {path}", level="command")
            node.remove_children()
            tree.run_worker(lambda: tree.load_directory(node), thread=True)
            self.log_activity(f"Refreshed remote: {path}")
        else:
            # Local DirectoryTree doesn't expose easy child refresh, just reload the whole tree or wait for filesystem events
            # For now, we just log info for local
            self.log_activity(
                f"Local refresh not implemented in UI, but selection is: {node.data}"
            )

    def action_toggle_log(self) -> None:
        """Toggle the visibility of the log window."""
        log = self.query_one("#log-window")
        log.display = not log.display
        self.log_activity(
            f"Log window toggled: {'visible' if log.display else 'hidden'}"
        )

    def action_toggle_ssl(self) -> None:
        """Toggle SSL verification."""
        self.ssl_verify = not self.ssl_verify
        self.log_activity(
            f"SSL verification turned {'ON' if self.ssl_verify else 'OFF'}"
        )
        self._update_toggle_labels()
        # Refresh any remote trees to apply the new SSL setting
        for tree in self.query(HighlightableRemoteDirectoryTree):
            self.run_worker(self.update_focused_pane(tree.url, tree))

    def action_toggle_tpc(self) -> None:
        """Toggle Third Party Copy."""
        self.tpc_enabled = not self.tpc_enabled
        self.log_activity(
            f"Third Party Copy turned {'ON' if self.tpc_enabled else 'OFF'}"
        )
        self._update_toggle_labels()

    async def action_swap(self) -> None:
        """Swap the contents of the left and right panes."""
        left_pane = self.query_one("#left-pane", Vertical)
        right_pane = self.query_one("#right-pane", Vertical)

        left_tree = left_pane.query_one(Tree)
        right_tree = right_pane.query_one(Tree)

        # Explicitly await removal and mounting to ensure DOM is stable
        await left_tree.remove()
        await right_tree.remove()

        await left_pane.mount(right_tree)
        await right_pane.mount(left_tree)

        self.log_activity("Panes swapped: left and right trees exchanged")

    def _do_copy(self, src: str, dest: str, to_remote: Optional[bool] = None) -> None:
        """Perform the copy operation in a background thread.

        If to_remote is True/False, dest is assumed to be a directory base.
        If to_remote is None, dest is assumed to be the full destination path.
        """
        try:
            from pathlib import Path

            final_dest = dest
            if to_remote is not None:
                # Traditional Copy (F5/action_copy): dest is a directory, append src name
                src_name = Path(src).name
                if "://" in dest:
                    final_dest = f"{dest.rstrip('/')}/{src_name}"
                else:
                    final_dest = str(Path(dest) / src_name)
            else:
                # Paste (p/action_paste): dest is already the full destination path
                # Infer to_remote for internal logic
                is_src_remote = "://" in src
                is_dest_remote = "://" in final_dest
                if not is_src_remote and is_dest_remote:
                    to_remote = True
                elif is_src_remote and not is_dest_remote:
                    to_remote = False
                elif is_src_remote and is_dest_remote:
                    to_remote = True  # Remote to Remote (streaming fallback)
                else:
                    to_remote = False  # Local to Local

            self.log_activity(f"Starting copy: {src} -> {final_dest}")

            if to_remote:
                # Target is remote
                fs, fs_path = url_to_fs(final_dest, ssl_verify=self.ssl_verify)
                # put() handles local-to-remote and potentially remote-to-remote
                fs.put(src, fs_path, recursive=True)
            else:
                # Target is local
                if "://" in src:
                    # Remote to Local
                    fs, fs_path = url_to_fs(src, ssl_verify=self.ssl_verify)
                    fs.get(fs_path, final_dest, recursive=True)
                else:
                    # Local to Local
                    import shutil

                    if Path(src).is_dir():
                        shutil.copytree(src, final_dest, dirs_exist_ok=True)
                    else:
                        shutil.copy2(src, final_dest)

            self.log_activity(f"Successfully copied to {final_dest}", level="success")
            self.call_from_thread(
                lambda: self.push_screen(
                    MessageModal(
                        f"Copied {src}\nto {final_dest}", title="Transfer Success"
                    )
                )
            )
            # Refresh both trees to show the new content
            self.call_from_thread(self.action_refresh)
        except Exception as e:
            error_msg = CommandBase._format_error(e)
            self.log_activity(f"Copy failed: {error_msg}", level="error")
            self.call_from_thread(
                lambda: self.push_screen(
                    MessageModal(
                        f"Failed to copy {src}:\n{error_msg}", title="Transfer Error"
                    )
                )
            )

    def on_unmount(self) -> None:
        """Cancel all workers on exit."""
        self.workers.cancel_all()


def main():
    import os

    # Disable clipboard synchronization to avoid macOS system prompts on exit
    os.environ.setdefault("TEXTUAL_CLIPBOARD", "none")

    app = GfalTui()
    from contextlib import suppress

    with suppress(KeyboardInterrupt):
        app.run()


class ChecksumResultModal(ModalScreen):
    """A centered modal screen for displaying and selecting checksums."""

    BINDINGS = [("escape", "dismiss", "Close")]

    result = reactive("Calculating...")

    def __init__(self, path: str, algo: str):
        super().__init__()
        self.path = path
        self.algo = algo

    def on_mount(self) -> None:
        """Set focus to the close button when the modal is pushed."""
        self.query_one("#close-btn").focus()

    def compose(self) -> ComposeResult:
        with Vertical(id="modal-content"):
            yield Static("[bold]Checksum[/bold]", classes="modal-title")
            yield Label(f"File: {self.path}", classes="modal-body")
            with Horizontal(classes="modal-row"):
                yield Label("Algorithm: ")
                yield Select(
                    [(a, a) for a in ["ADLER32", "MD5", "SHA1", "CRC32"]],
                    value=self.algo,
                    id="algo-select",
                )
            yield Label("Value:", classes="modal-body")
            yield Static(self.result, id="checksum-value")
            with Horizontal(id="modal-btn-row"):
                yield Button("Close", variant="primary", id="close-btn")

    def watch_result(self, result: str) -> None:
        """Update the value label when result changes."""
        with suppress(Exception):
            self.query_one("#checksum-value", Static).update(result)

    @on(Select.Changed, "#algo-select")
    def on_algo_changed(self, event: Select.Changed) -> None:
        if event.value and event.value != Select.BLANK:
            new_algo = str(event.value)
            self.algo = new_algo
            self.app.last_checksum_algo = new_algo
            self.result = "Calculating..."
            self.app.action_checksum(self.path, new_algo, modal=self)

    @on(Button.Pressed, "#close-btn")
    def on_close(self) -> None:
        self.dismiss()


class MessageModal(ModalScreen):
    """A centered modal screen for displaying messages."""

    BINDINGS = [("escape", "close", "Close")]

    def __init__(self, message: str, title: str = "Message"):
        super().__init__()
        self.message = message
        self.title = title

    def compose(self) -> ComposeResult:
        with Vertical(id="modal-content"):
            yield Static(f"[bold]{self.title}[/bold]", classes="modal-title")
            yield Static(self.message, classes="modal-body")
            with Horizontal(id="modal-btn-row"):
                yield Button("Close", variant="primary", id="close-btn")

    def action_close(self) -> None:
        self.app.pop_screen()

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "close-btn":
            self.action_close()


class PasteModal(ModalScreen[Optional[dict]]):
    """A modal to confirm the destination for a paste operation."""

    BINDINGS = [("escape", "cancel", "Cancel")]

    def __init__(self, src_urls: set[str], dst_dir: str):
        super().__init__()
        self.src_urls = src_urls
        self.dst_dir = dst_dir
        self.multi = len(src_urls) > 1
        if not self.multi:
            src_url = list(src_urls)[0]
            # Use urlparse to get the last path component
            path_part = urlparse(src_url).path
            self.suggested_name = Path(path_part).name or Path(src_url).name
        else:
            self.suggested_name = ""

    def compose(self) -> ComposeResult:
        with Vertical(id="modal-content"):
            yield Static("[bold]Paste[/bold]", classes="modal-title")
            if self.multi:
                yield Label(f"Pasting {len(self.src_urls)} items")
            else:
                yield Label(f"Source: '{list(self.src_urls)[0]}'")

            yield Label(f"Destination Directory: '{self.dst_dir}'")

            if not self.multi:
                yield Label("Destination Name:")
                yield Input(value=self.suggested_name, id="dest-name-input")

            with Horizontal(id="modal-btn-row"):
                yield Button("Cancel", id="cancel-btn")
                yield Button("Paste", variant="primary", id="paste-btn")

    def action_cancel(self) -> None:
        """Dismiss the modal without performing any action."""
        self.dismiss(None)

    @on(Button.Pressed, "#paste-btn")
    def on_paste(self) -> None:
        if self.multi:
            self.dismiss({"path": self.dst_dir})
        else:
            name = self.query_one("#dest-name-input", Input).value
            if not name:
                self.app.notify("Destination name cannot be empty", severity="error")
                return
            self.dismiss({"path": self.dst_dir, "name": name})

    @on(Input.Submitted, "#dest-name-input")
    def on_submit(self) -> None:
        self.on_paste()

    @on(Button.Pressed, "#cancel-btn")
    def on_cancel(self) -> None:
        self.action_cancel()


class UrlInputModal(ModalScreen):
    """A modal screen for inputting a new remote URL."""

    BINDINGS = [("escape", "close", "Close")]

    def compose(self) -> ComposeResult:
        with Vertical(id="modal-content"):
            yield Static("[bold]Enter Remote URL[/bold]", classes="modal-title")
            yield Input(
                placeholder="root://... or https://...",
                id="modal-url-input",
            )
            with Horizontal(id="modal-btn-row"):
                yield Button("Cancel", id="cancel-btn")
                yield Button("Load", variant="primary", id="load-btn")

    def action_close(self) -> None:
        self.app.pop_screen()

    @on(Input.Submitted, "#modal-url-input")
    def handle_submit(self):
        url = self.query_one("#modal-url-input", Input).value
        if url:
            self.app.log_activity(f"gfal-ls {url}", level="command")
            self.app.run_worker(self.app.update_focused_pane(url))
        self.action_close()

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "cancel-btn":
            self.action_close()
        elif event.button.id == "load-btn":
            self.handle_submit()


class CommandTui(CommandBase):
    @interactive
    @arg("src", nargs="?", help="source path")
    @arg("dst", nargs="?", help="destination path")
    def execute_tui(self):
        """Launch the Text User Interface."""
        from gfal_cli.base import surl

        src = surl(self.params.src) if self.params.src else None
        dst = surl(self.params.dst) if self.params.dst else None

        GfalTui(src=src, dst=dst).run()
        return 0


if __name__ == "__main__":
    main()
