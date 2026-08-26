"""Tests for dependency-free local command adapters."""

from __future__ import annotations

import errno
import subprocess
import sys

from gfal.cli.main import main


def test_local_rm_reports_deleted_file_like_legacy_gfal(tmp_path, capsys):
    target = tmp_path / "file.txt"
    target.write_text("fixture", encoding="utf-8")

    assert main(["gfal", "rm", target.as_uri()]) == 0

    assert not target.exists()
    assert capsys.readouterr() == (f"{target.as_uri()}\tDELETED\n", "")


def test_local_rm_accepts_bare_paths(tmp_path, capsys):
    target = tmp_path / "file.txt"
    target.write_text("fixture", encoding="utf-8")

    assert main(["gfal", "rm", str(target)]) == 0

    assert not target.exists()
    assert capsys.readouterr() == (f"{target}\tDELETED\n", "")


def test_local_rm_removes_directory_recursively(tmp_path, capsys):
    target = tmp_path / "tree"
    child = target / "nested" / "file.txt"
    child.parent.mkdir(parents=True)
    child.write_text("fixture", encoding="utf-8")

    assert main(["gfal", "rm", "--recursive", target.as_uri()]) == 0

    assert not target.exists()
    assert capsys.readouterr() == (
        f"{child.as_uri()}\tDELETED\n"
        f"{child.parent.as_uri()}\tRMDIR\n"
        f"{target.as_uri()}\tRMDIR\n",
        "",
    )


def test_local_rm_rejects_directory_without_recursive(tmp_path, capsys):
    target = tmp_path / "tree"
    target.mkdir()

    assert main(["gfal", "rm", target.as_uri()]) == errno.EISDIR

    assert target.is_dir()
    captured = capsys.readouterr()
    assert captured.out == ""
    assert "Is a directory" in captured.err


def test_local_rm_dry_run_preserves_tree_and_prints_plan(tmp_path, capsys):
    target = tmp_path / "tree"
    child = target / "file.txt"
    target.mkdir()
    child.write_text("fixture", encoding="utf-8")

    assert main(["gfal", "rm", "-r", "--dry-run", target.as_uri()]) == 0

    assert child.is_file()
    assert target.is_dir()
    output = capsys.readouterr().out
    assert f"{child.as_uri()}\tSKIP" in output
    assert f"{target.as_uri()}\tSKIP DIR" in output


def test_local_rm_continues_after_failure_and_returns_first_error(tmp_path, capsys):
    missing = tmp_path / "missing.txt"
    existing = tmp_path / "existing.txt"
    existing.write_text("fixture", encoding="utf-8")

    assert main(["gfal", "rm", missing.as_uri(), existing.as_uri()]) == errno.ENOENT

    assert not existing.exists()
    captured = capsys.readouterr()
    assert captured.out == (
        f"{missing.as_uri()}\tMISSING\n{existing.as_uri()}\tDELETED\n"
    )
    assert captured.err == ""


def test_local_rm_reads_operands_from_file(tmp_path, capsys):
    first = tmp_path / "first.txt"
    second = tmp_path / "second.txt"
    first.write_text("one", encoding="utf-8")
    second.write_text("two", encoding="utf-8")
    source_list = tmp_path / "remove.txt"
    source_list.write_text(
        f"\n{first.as_uri()}\n\n{second.as_uri()}\n",
        encoding="utf-8",
    )

    assert main(["gfal", "rm", "--from-file", str(source_list)]) == 0

    assert not first.exists()
    assert not second.exists()
    assert capsys.readouterr() == (
        f"{first.as_uri()}\tDELETED\n{second.as_uri()}\tDELETED\n",
        "",
    )


def test_local_rm_no_operands_does_not_require_xrdfs(tmp_path, monkeypatch, capsys):
    monkeypatch.setenv("GFAL_XRDFS", str(tmp_path / "missing-xrdfs"))

    assert main(["gfal", "rm"]) == errno.EINVAL

    captured = capsys.readouterr()
    assert captured.out == ""
    assert "No URI specified" in captured.err


def test_local_rm_does_not_import_legacy_protocol_modules(tmp_path):
    target = tmp_path / "file.txt"
    target.write_text("fixture", encoding="utf-8")
    code = (
        "import sys; from gfal.cli.main import main; "
        f"status=main(['gfal','rm',{target.as_uri()!r}]); "
        "assert status == 0; "
        "assert 'fsspec' not in sys.modules; "
        "assert 'aiohttp' not in sys.modules; "
        "assert 'XRootD' not in sys.modules"
    )

    result = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert not target.exists()
