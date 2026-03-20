import threading
from pathlib import Path

from textual import on
from textual.app import App, ComposeResult
from textual.containers import Horizontal, Vertical
from textual.screen import ModalScreen
from textual.widgets import (
    Button,
    Checkbox,
    DirectoryTree,
    Footer,
    Header,
    Input,
    Label,
    RichLog,
    Static,
    Tree,
)

from gfal_cli.fs import url_to_fs


class RemoteDirectoryTree(Tree):
    """A lazy-loading tree for remote filesystems."""

    def __init__(self, url: str, ssl_verify: bool = False, **kwargs):
        self.url = url
        self.ssl_verify = ssl_verify
        super().__init__(url, data=url, **kwargs)

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


class GfalTui(App):
    """A k9s-style TUI for gfal-cli."""

    TITLE = "gfal"

    CSS = """
    Screen {
        background: #1e1e1e;
    }
    .pane {
        width: 50%;
        height: 100%;
        border: solid #333;
    }
    #remote-pane {
        border: solid #007acc;
    }
    Label {
        padding: 1;
        background: #333;
        width: 100%;
    }
    Input {
        margin: 1;
    }
    Checkbox {
        margin: 1;
        width: auto;
    }
    #input-container {
        height: auto;
        dock: top;
    }
    #log-window {
        height: auto;
        border: thick $primary;
        margin: 1 2;
    }

    MessageModal {
        align: center middle;
        background: rgba(0, 0, 0, 0.5);
    }

    #modal-content {
        width: 60;
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
        ("l", "toggle_log", "Toggle Log"),
        ("f5", "copy", "Copy"),
    ]

    def compose(self) -> ComposeResult:
        yield Header()
        with Horizontal(id="input-container"):
            yield Checkbox("Enable TPC", value=True, id="tpc-toggle")
            yield Checkbox("Verify SSL", value=False, id="ssl-toggle")
            yield Button("Local ⮕ Remote", id="direction-button", variant="success")
            yield Input(
                placeholder="Remote URL (e.g., root://... or https://...)",
                id="url-input",
            )
        with Horizontal():
            with Vertical(classes="pane"):
                yield Label("Local Filesystem")
                yield DirectoryTree("./", id="local-tree")
            with Vertical(classes="pane", id="remote-pane"):
                yield Label("Remote / Target")
                yield RemoteDirectoryTree(
                    "https://eospublic.cern.ch:8444/eos/opendata/cms/", id="remote-tree"
                )
        yield RichLog(id="log-window", auto_scroll=True, max_lines=1000)
        yield Footer()

    @on(Input.Submitted, "#url-input")
    async def handle_url(self, event: Input.Submitted):
        url = event.value
        if not url:
            return

        await self.update_remote(url)

    async def update_remote(self, url: str):
        ssl_verify = self.query_one("#ssl-toggle", Checkbox).value
        self.log_activity(f"Updating remote to: {url} (verify={ssl_verify})")
        try:
            # Replace the old tree with a new one
            remote_pane = self.query_one("#remote-pane", Vertical)
            await remote_pane.query("#remote-tree").remove()

            new_tree = RemoteDirectoryTree(url, ssl_verify=ssl_verify, id="remote-tree")
            await remote_pane.mount(new_tree)
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
        }
        color = colors.get(level, "white")
        log_window = self.query_one("#log-window", RichLog)

        def do_log():
            log_window.write(
                f"[{timestamp}] [{color}]{level.upper():>7}[/{color}] {message}"
            )

        if self._thread_id == threading.get_ident():
            do_log()
        else:
            self.call_from_thread(do_log)

    def action_stat(self) -> None:
        """Fetch and log information for the selected node."""
        node = self.query_one("Tree:focus").cursor_node
        if not node or not node.data:
            return

        path = node.data
        self.log_activity(f"Fetching stat for: {path}")

        def get_stat():
            try:
                # Determine if it's local or remote based on the tree or path
                ssl_verify = self.query_one("#ssl-toggle", Checkbox).value
                fs, fs_path = url_to_fs(path, ssl_verify=ssl_verify)
                info = fs.info(fs_path)
                msg = f"Stat Info for {path}:\n"
                for k, v in sorted(info.items()):
                    msg += f"  {k}: {v}\n"
                self.log_activity(msg.strip())
                self.call_from_thread(
                    lambda: self.push_screen(
                        MessageModal(msg.strip(), title="Stat Info")
                    )
                )
            except Exception as e:
                self.log_activity(f"Stat failed for {path}: {e}", level="error")

        self.run_worker(get_stat, thread=True)

    def action_checksum(self) -> None:
        """Calculate and log checksum for the selected node."""
        node = self.query_one("Tree:focus").cursor_node
        if not node or not node.data:
            return

        path = node.data
        self.log_activity(f"Calculating checksum for: {path}")

        def get_checksum():
            try:
                ssl_verify = self.query_one("#ssl-toggle", Checkbox).value
                fs, fs_path = url_to_fs(path, ssl_verify=ssl_verify)
                # Try common checksum algorithms
                result = None
                for _ in ["ADLER32", "MD5"]:
                    try:
                        # Some fsspec backends support checksum(path)
                        if hasattr(fs, "checksum"):
                            result = fs.checksum(fs_path)
                            if result:
                                break
                    except Exception:
                        continue

                if result:
                    msg = f"Checksum for {path}: {result}"
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
        tree = self.query_one("Tree:focus")
        node = tree.cursor_node
        if not node:
            return

        # Only refresh if it's a directory (remote tree handles expansion)
        if isinstance(tree, RemoteDirectoryTree):
            node.remove_children()
            tree.run_worker(lambda: tree.load_directory(node), thread=True)
            self.log_activity(f"Refreshed remote: {node.data}")
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

    @on(Button.Pressed, "#direction-button")
    def on_direction_toggle(self, event: Button.Pressed) -> None:
        """Switch the copy direction."""
        btn = event.button
        if btn.variant == "success":
            btn.label = "Remote ⮕ Local"
            btn.variant = "warning"
        else:
            btn.label = "Local ⮕ Remote"
            btn.variant = "success"
        self.log_activity(f"Copy direction changed to: {btn.label}")

    def action_copy(self) -> None:
        """Copy the selected file to the other pane."""
        direction = str(self.query_one("#direction-button").label)
        local_tree = self.query_one("#local-tree", DirectoryTree)
        remote_tree = self.query_one("#remote-tree", RemoteDirectoryTree)

        if "Local ⮕ Remote" in direction:
            # Source is Local, Destination is Remote
            node = local_tree.cursor_node
            if not node or not node.data:
                self.log_activity("No local file selected for copy", level="warning")
                return
            src_path = str(node.data.path)
            dest_dir = remote_tree.url  # Use the root of the remote tree
            self.run_worker(
                self._do_copy(src_path, dest_dir, to_remote=True), thread=True
            )
        else:
            # Source is Remote, Destination is Local
            node = remote_tree.cursor_node
            if not node or not node.data:
                self.log_activity("No remote file selected for copy", level="warning")
                return
            src_path = node.data
            dest_dir = str(local_tree.path)
            self.run_worker(
                self._do_copy(src_path, dest_dir, to_remote=False), thread=True
            )

    async def _do_copy(self, src: str, dest_dir: str, to_remote: bool) -> None:
        """Perform the copy operation in a background thread."""
        try:
            from pathlib import Path

            src_name = Path(src).name
            dest_path = f"{dest_dir.rstrip('/')}/{src_name}"
            ssl_verify = self.query_one("#ssl-toggle", Checkbox).value

            self.log_activity(f"Starting copy: {src} -> {dest_path}")

            if to_remote:
                # Local -> Remote (put)
                # We need the remote filesystem
                fs, _ = url_to_fs(dest_dir, ssl_verify=ssl_verify)
                # fsspec put() handles directories if recursive=True
                fs.put(src, dest_path, recursive=True)
            else:
                # Remote -> Local (get)
                # We need the source remote filesystem
                fs, _ = url_to_fs(src, ssl_verify=ssl_verify)
                fs.get(src, dest_path, recursive=True)

            self.log_activity(f"Successfully copied to {dest_path}", level="success")
            self.call_from_thread(
                lambda: self.push_screen(
                    MessageModal(
                        f"Copied {src}\nto {dest_path}", title="Transfer Success"
                    )
                )
            )
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


if __name__ == "__main__":
    main()
