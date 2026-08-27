#!/usr/bin/env python3
"""Verify that the published wheel contains only the dependency-free CLI."""

from __future__ import annotations

import sys
from email.parser import Parser
from pathlib import Path
from zipfile import ZipFile

FORBIDDEN_MODULES = (
    "gfal/core/",
    "gfal/cli/base.py",
    "gfal/cli/commands.py",
    "gfal/cli/copy.py",
    "gfal/cli/ls.py",
    "gfal/cli/mount.py",
    "gfal/cli/progress.py",
    "gfal/cli/rm.py",
    "gfal/cli/shell.py",
    "gfal/cli/tape.py",
)


def verify(wheel: Path) -> None:
    with ZipFile(wheel) as archive:
        names = archive.namelist()
        forbidden = [
            name
            for name in names
            if any(name.startswith(prefix) for prefix in FORBIDDEN_MODULES)
        ]
        if forbidden:
            raise RuntimeError(f"legacy modules entered the wheel: {forbidden}")

        metadata_name = next(name for name in names if name.endswith("/METADATA"))
        metadata = Parser().parsestr(archive.read(metadata_name).decode("utf-8"))
        runtime = [
            value
            for value in metadata.get_all("Requires-Dist", [])
            if "extra ==" not in value
        ]
        if runtime:
            raise RuntimeError(f"unexpected Python runtime dependencies: {runtime}")


def main() -> int:
    wheels = [Path(value) for value in sys.argv[1:]]
    if not wheels:
        wheels = sorted(Path("dist").glob("*.whl"))
    if len(wheels) != 1:
        raise RuntimeError(f"expected one wheel, found {len(wheels)}")
    verify(wheels[0])
    print(f"verified dependency-free distribution: {wheels[0]}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
