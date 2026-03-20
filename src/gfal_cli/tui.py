from pathlib import Path

from textual import on
from textual.app import App, ComposeResult
from textual.containers import Horizontal, Vertical
from textual.widgets import (
    Checkbox,
    DirectoryTree,
    Footer,
    Header,
    Input,
    Label,
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
            self.run_worker(self.load_directory(node))

    async def load_directory(self, node):
        path = node.data
        try:
            fs, fs_path = url_to_fs(path, ssl_verify=self.ssl_verify)
            # Use detail=True to distinguish files and directories
            entries = fs.ls(fs_path, detail=True)
            for entry in sorted(
                entries, key=lambda e: (e["type"] != "directory", e["name"])
            ):
                name = Path(entry["name"]).name
                if not name:
                    continue
                is_dir = entry["type"] == "directory"
                node.add(name, data=entry["name"], allow_expand=is_dir)
        except Exception as e:
            self.app.notify(f"Error loading {path}: {e}", severity="error")


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
    """

    BINDINGS = [
        ("q", "quit", "Quit"),
        ("r", "refresh", "Refresh"),
    ]

    def compose(self) -> ComposeResult:
        yield Header()
        with Horizontal(id="input-container"):
            yield Input(
                value="https://eospublic.cern.ch:8444/eos/opendata/cms/",
                placeholder="Enter remote URL (e.g. root://...)",
                id="url-input",
            )
            yield Checkbox("Verify SSL", value=False, id="ssl-verify")
        with Horizontal():
            with Vertical(classes="pane"):
                yield Label("Local Filesystem")
                yield DirectoryTree("./", id="local-tree")
            with Vertical(classes="pane", id="remote-pane"):
                yield Label("Remote / Target")
                yield RemoteDirectoryTree(
                    "https://eospublic.cern.ch:8444/eos/opendata/cms/", id="remote-tree"
                )
        yield Footer()

    @on(Input.Submitted, "#url-input")
    async def handle_url(self, event: Input.Submitted):
        url = event.value
        if not url:
            return

        await self.update_remote(url)

    async def update_remote(self, url: str):
        ssl_verify = self.query_one("#ssl-verify", Checkbox).value
        try:
            # Replace the old tree with a new one
            remote_pane = self.query_one("#remote-pane", Vertical)
            old_tree = self.query("#remote-tree")
            if old_tree:
                await old_tree.remove()

            new_tree = RemoteDirectoryTree(url, ssl_verify=ssl_verify, id="remote-tree")
            await remote_pane.mount(new_tree)
        except Exception as e:
            self.notify(f"Error updating remote: {e}", severity="error")


def main():
    app = GfalTui()
    app.run()


if __name__ == "__main__":
    main()
