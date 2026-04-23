"""Read-only FUSE mount command."""

from __future__ import annotations

from pathlib import Path

try:
    import rich_click as click
except ImportError:
    import click  # type: ignore[no-redef]

from gfal.cli import base
from gfal.core.api import GfalClient
from gfal.core.mount import mount_foreground


class CommandMount(base.CommandBase):
    @base.interactive
    @base.arg(
        "source",
        type=base.surl,
        help="directory URI to mount read-only",
    )
    @base.arg(
        "mountpoint",
        type=click.Path(
            exists=True,
            file_okay=False,
            dir_okay=True,
            path_type=Path,
        ),
        help="local directory where the filesystem will be mounted",
    )
    def execute_mount(self):
        """Mount a directory read-only via FUSE (Linux and macOS)."""
        client = GfalClient(**base.build_client_kwargs(self.params))
        mount_foreground(
            self.params.source,
            self.params.mountpoint.expanduser(),
            client,
        )
        return 0
