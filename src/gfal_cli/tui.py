from __future__ import annotations

import tempfile
import threading
from contextlib import suppress
from pathlib import Path
from typing import Any

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
    Static,
    Tree,
)
from textual.widgets._tree import TreeNode

from gfal_cli.fs import compute_checksum, url_to_fs
from gfal_cli.utils import (
    human_readable_size,
    human_readable_time,
)


class HighlightableRemoteDirectoryTree(Tree):
    """A lazy-loading tree for remote filesystems with yank highlight support."""

    yanked_url: reactive[str | None] = reactive(None)

    def __init__(self, url: str, ssl_verify: bool = False, **kwargs):
        self.url = url
        self.ssl_verify = ssl_verify
        super().__init__(url, data=url, **kwargs)

    def render_label(
        self, node: TreeNode[Any], base_style: Style, control_style: Style
    ) -> Any:
        label = super().render_label(node, base_style, control_style)
        if self.yanked_url and node.data == self.yanked_url:
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

            def add_nodes():
                for entry in sorted(
                    entries, key=lambda e: (e["type"] != "directory", e["name"])
                ):
                    name = Path(entry["name"]).name
                    if not name:
                        continue
                    is_dir = entry["type"] == "directory"
                    node.add(name, data=entry["name"], allow_expand=is_dir)
                self.app.log_activity(
                    f"Loaded {len(entries)} items from {path}", level="success"
                )

            self.app.call_from_thread(add_nodes)
        except Exception as e:
            self.app.log_activity(f"Failed to load {path}: {e}", level="error")
            self.app.call_from_thread(
                self.app.notify, f"Error loading {path}: {e}", severity="error"
            )


class HighlightableDirectoryTree(DirectoryTree):
    """A DirectoryTree that supports yank highlight."""

    yanked_url: reactive[str | None] = reactive(None)

    def render_label(
        self, node: TreeNode[Any], base_style: Style, control_style: Style
    ) -> Any:
        label = super().render_label(node, base_style, control_style)
        # DirectoryTree data is DirEntry (has .path)
        if (
            self.yanked_url
            and node.data
            and hasattr(node.data, "path")
            and str(node.data.path) == self.yanked_url
        ):
            if isinstance(label, Text):
                label.append(" [YANKED]", style="bold yellow")
            else:
                label = Text.assemble(label, " [YANKED]", style="bold yellow")
        return label


class GfalTui(App):
    """A k9s-style TUI for gfal-cli."""

    TITLE = "gfal"

    ssl_verify = reactive(False)
    tpc_enabled = reactive(True)
    yanked_url = reactive(None)
    log_file = reactive(str(Path(tempfile.gettempdir()) / "gfal-tui.log"))

    def __init__(self, log_file: str | None = None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if log_file:
            self.log_file = log_file
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
    MessageModal, UrlInputModal {
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
        ("c", "checksum", "Checksum"),
        ("r", "refresh", "Refresh"),
        ("L", "toggle_log", "Toggle Log"),
        ("f5", "copy", "Copy"),
        ("x", "swap", "Swap Panes"),
        ("/", "search", "Search"),
        ("g", "cursor_top", "Top"),
        ("G", "cursor_bottom", "Bottom"),
        Binding("v", "toggle_ssl", "SSL [OFF]", show=True),
        Binding("t", "toggle_tpc", "TPC [ON]", show=True),
        Binding("y", "yank", "Yank", show=True),
        Binding("p", "paste", "Paste", show=True),
        Binding("L", "toggle_log", "Log", show=True),
        ("left,h", "focus_left", "Focus Left"),
        ("right,l", "focus_right", "Focus Right"),
    ]

    def compose(self) -> ComposeResult:
        yield Header()
        with Horizontal():
            with Vertical(classes="pane", id="local-pane"):
                yield Label("Source (Local)", classes="pane-header")
                tree = HighlightableDirectoryTree("./", id="local-tree")
                tree.yanked_url = self.yanked_url
                yield tree
            with Vertical(classes="pane", id="remote-pane"):
                yield Label("Destination (Remote)", classes="pane-header")
                tree = HighlightableRemoteDirectoryTree(
                    "https://eospublic.cern.ch:8444/eos/opendata/cms/",
                    id="remote-tree",
                    ssl_verify=self.ssl_verify,
                )
                tree.yanked_url = self.yanked_url
                yield tree
        yield RichLog(id="log-window", auto_scroll=True, max_lines=1000)
        yield Footer()

    def on_mount(self) -> None:
        """Called when the app is mounted."""
        self.log_activity("Welcome to gfal-cli TUI", level="info")
        self._update_toggle_labels()

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
        """Open a modal to search/input a new remote URL."""
        self.push_screen(UrlInputModal())

    def on_key(self, event: events.Key) -> None:
        """Handle global keys, especially those swallowed by sub-widgets."""
        if event.key == "left" or event.key == "h":
            self.action_focus_left()
        elif event.key == "right" or event.key == "l":
            self.action_focus_right()

    def action_yank(self) -> None:
        """Yank the currently selected file/directory."""
        tree = self._get_focused_tree()
        if not tree or not tree.cursor_node:
            return

        node = tree.cursor_node
        url = (
            node.data
            if isinstance(tree, HighlightableRemoteDirectoryTree)
            else str(node.data.path)
        )
        self.yanked_url = url
        self.log_activity(f"Yanked: {url}", level="success")

        # Update trees to show highlights
        for tree_widget in self.query(
            "HighlightableDirectoryTree, HighlightableRemoteDirectoryTree"
        ):
            tree_widget.yanked_url = url
            tree_widget.refresh()

    def action_paste(self) -> None:
        """Paste the yanked file/directory to the currently selected directory."""
        if not self.yanked_url:
            self.notify(
                "Nothing yanked! Press 'y' to yank an item first.", severity="warning"
            )
            return

        tree = self._get_focused_tree()
        if not tree or not tree.cursor_node:
            return

        node = tree.cursor_node
        # For RemoteDirectoryTree, data is the full URL.
        # For DirectoryTree, data is DirEntry (has .path and .is_dir).
        target_is_dir = False
        target_path = ""

        if isinstance(tree, HighlightableRemoteDirectoryTree):
            target_path = node.data
            # We assume highlighted remote nodes that allow expand are directories
            target_is_dir = node.allow_expand
        else:
            target_path = str(node.data.path)
            target_is_dir = node.data.is_dir

        if not target_is_dir:
            self.notify("Can only paste into directories!", severity="error")
            return

        def handle_paste(dest_name: str) -> None:
            if not dest_name:
                return

            # Construct final destination URL
            # Use a local variable to avoid UnboundLocalError with the closure
            tp = target_path
            if "://" in tp:
                if not tp.endswith("/"):
                    tp += "/"
                dst_url = tp + dest_name
            else:
                dst_url = str(Path(tp) / dest_name)

            self.log_activity(f"Pasting {self.yanked_url} to {dst_url}")
            self.run_worker(
                lambda: self._do_copy(self.yanked_url, dst_url),
                thread=True,
                name="paste_worker",
            )

        self.push_screen(PasteModal(self.yanked_url, target_path), handle_paste)

    def action_focus_left(self) -> None:
        """Focus the left pane (local tree)."""
        self.query_one("#local-tree").focus()

    def action_focus_right(self) -> None:
        """Focus the right pane (remote tree)."""
        self.query_one("#remote-tree").focus()

    def action_cursor_top(self) -> None:
        """Move cursor to the top of the focused tree."""
        with suppress(Exception):
            tree = self.query_one("Tree:focus")
            target = tree.root
            if not tree.show_root and tree.root.children:
                target = tree.root.children[0]
            tree.select_node(target)
            tree.scroll_to_node(target)

    def action_cursor_bottom(self) -> None:
        """Move cursor to the bottom of the focused tree."""
        with suppress(Exception):
            tree = self.query_one("Tree:focus")

            def get_last(node):
                if node.is_expanded and node.children:
                    return get_last(node.children[-1])
                return node

            last_node = get_last(tree.root)
            tree.select_node(last_node)
            tree.scroll_to_node(last_node)

    async def update_remote(self, url: str):
        self.log_activity(f"Updating remote to: {url} (verify={self.ssl_verify})")
        try:
            # Find the tree wherever it is
            remote_tree = self.query_one("#remote-tree")
            pane = remote_tree.parent
            await remote_tree.remove()

            new_tree = HighlightableRemoteDirectoryTree(
                url, ssl_verify=self.ssl_verify, id="remote-tree"
            )
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
                self.log_activity(f"Stat failed for {path}: {e}", level="error")

        self.run_worker(get_stat, thread=True)

    def _get_focused_tree(self) -> Tree | None:
        """Helper to get the currently focused tree widget."""
        # Try to find which pane is focused, then get its tree
        focused = self.focused
        if not focused:
            # Default to left pane
            return self.query_one("#local-tree", Tree)

        # If we focused the tree directly
        if isinstance(focused, Tree):
            return focused

        # If we focused a pane container
        if focused.id == "left-pane":
            return self.query_one("#local-tree", Tree)
        if focused.id == "right-pane":
            return self.query_one("#remote-tree", Tree)

        # Fallback
        return self.query_one("#local-tree", Tree)

    def action_checksum(self) -> None:
        """Calculate and log checksum for the selected node."""
        tree = self._get_focused_tree()
        if not tree:
            return
        node = tree.cursor_node
        path = self._get_node_path(node)
        if not path:
            return

        def get_checksum():
            try:
                fs, fs_path = url_to_fs(path, ssl_verify=self.ssl_verify)
                # Try common checksum algorithms
                result = None
                algo = "ADLER32"
                for a in ["ADLER32", "MD5"]:
                    try:
                        result = compute_checksum(fs, fs_path, a)
                        if result:
                            algo = a
                            break
                    except Exception:
                        continue

                if result:
                    msg = f"Checksum ({algo}) for {path}:\n  {result}"
                    self.log_activity(f"gfal-sum {path} {algo}", level="command")
                    self.log_activity(msg, level="success")
                    self.call_from_thread(
                        lambda: self.push_screen(MessageModal(msg, title="Checksum"))
                    )
                else:
                    self.log_activity(
                        f"Checksum not supported for {path}", level="warning"
                    )
            except Exception as e:
                self.log_activity(f"Checksum failed for {path}: {e}", level="error")

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
        # If remote tree exists, we might want to refresh it or notify it.
        # For now, just log and it will apply to NEXT operations/loads.
        # To be thorough, we can trigger a refresh of the remote target.
        remote_tree = self.query_one("#remote-tree", HighlightableRemoteDirectoryTree)
        if remote_tree:
            self.run_worker(self.update_remote(remote_tree.url))

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

        left_tree = left_pane.children[1]
        right_tree = right_pane.children[1]

        # Explicitly await removal and mounting to ensure DOM is stable for tests
        await left_tree.remove()
        await right_tree.remove()

        await left_pane.mount(right_tree)
        await right_pane.mount(left_tree)

        self.log_activity("Panes swapped: Source and Destination content exchanged")

    def action_copy(self) -> None:
        """Copy the selected file from Source (Left) to Destination (Right)."""
        left_pane = self.query_one("#left-pane", Vertical)
        right_pane = self.query_one("#right-pane", Vertical)

        src_tree = left_pane.children[1]
        dest_tree = right_pane.children[1]

        # Get the focused node from the Source pane
        node = src_tree.cursor_node
        if not node or not node.data:
            self.log_activity(
                "No file selected in Source pane for copy", level="warning"
            )
            return

        is_src_local = isinstance(src_tree, DirectoryTree)
        src_path = self._get_node_path(node)
        if not src_path:
            return

        if is_src_local:
            # Source -> Remote (put)
            dest_dir = dest_tree.url
            self.log_activity(f"gfal-copy {src_path} {dest_dir}", level="command")
            self.run_worker(
                self._do_copy(src_path, dest_dir, to_remote=True), thread=True
            )
        else:
            # Remote -> Local (get)
            dest_dir = str(dest_tree.path)
            self.log_activity(f"gfal-copy {src_path} {dest_dir}", level="command")
            self.run_worker(
                self._do_copy(src_path, dest_dir, to_remote=False), thread=True
            )

    async def _do_copy(
        self, src: str, dest: str, to_remote: bool | None = None
    ) -> None:
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
            error_msg = str(e)
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


class PasteModal(ModalScreen[str]):
    """A modal to confirm the destination name for a paste operation."""

    BINDINGS = [("escape", "cancel", "Cancel")]

    def __init__(self, src_url: str, dst_dir: str):
        super().__init__()
        self.src_url = src_url
        self.dst_dir = dst_dir
        self.suggested_name = Path(src_url).name

    def compose(self) -> ComposeResult:
        with Vertical(id="modal-content"):
            yield Static("[bold]Paste[/bold]", classes="modal-title")
            yield Label(f"Source: '{self.src_url}'")
            yield Label(f"Destination Directory: '{self.dst_dir}'")
            yield Label("Destination Name:")
            yield Input(value=self.suggested_name, id="dest-name-input")
            with Horizontal(id="modal-btn-row"):
                yield Button("Cancel", id="cancel-btn")
                yield Button("Paste", variant="primary", id="paste-btn")

    def action_cancel(self) -> None:
        """Dismiss the modal without performing any action."""
        self.dismiss("")

    @on(Button.Pressed, "#paste-btn")
    @on(Input.Submitted, "#dest-name-input")
    def on_paste(self) -> None:
        name = self.query_one("#dest-name-input", Input).value
        if not name:
            self.app.notify("Destination name cannot be empty", severity="error")
            return
        self.dismiss(name)

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
            self.app.run_worker(self.app.update_remote(url))
        self.action_close()

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "cancel-btn":
            self.action_close()
        elif event.button.id == "load-btn":
            self.handle_submit()


if __name__ == "__main__":
    main()
