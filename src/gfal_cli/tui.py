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
    Static,
)

from gfal_cli.fs import url_to_fs


class RemoteDirectoryTree(DirectoryTree):
    """A DirectoryTree that can handle remote URLs via fsspec."""

    def __init__(self, path: str, **kwargs):
        self.url = path
        try:
            self.fs, self.fs_path = url_to_fs(path)
        except Exception:
            self.fs, self.fs_path = None, None
        super().__init__(self.fs_path or path, **kwargs)

    def load_directory(self, node):
        """Custom loader for remote paths."""
        if not self.fs:
            return super().load_directory(node)

        # Implementation detail: DirectoryTree uses path.iterdir()
        # We might need to override more if we want true remote support
        # inside the built-in widget, or implement our own Tree.
        # For the PoC, we'll try to stick to local or simple remote if possible.
        return super().load_directory(node)


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
                yield Static(
                    "Enter a URL above to browse remote paths", id="remote-placeholder"
                )
        yield Footer()

    @on(Input.Submitted, "#url-input")
    def handle_url(self, event: Input.Submitted):
        url = event.value
        if not url:
            return

        # For the PoC, we update the remote placeholder with the LS output
        # A full tree implementation would take more time.
        self.run_worker(self.update_remote(url), thread=True)

    async def update_remote(self, url: str):
        placeholder = self.query_one("#remote-placeholder", Static)
        ssl_verify = self.query_one("#ssl-verify", Checkbox).value
        try:
            placeholder.update(f"Fetching {url}... (SSL Verify: {ssl_verify})")
            fs, path = url_to_fs(url, ssl_verify=ssl_verify)
            files = fs.ls(path, detail=False)
            output = "\n".join([Path(f).name for f in files])
            placeholder.update(f"Contents of {url}:\n\n{output}")
        except Exception as e:
            placeholder.update(f"Error: {e}")


def main():
    app = GfalTui()
    app.run()


if __name__ == "__main__":
    main()
