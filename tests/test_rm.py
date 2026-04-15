"""Tests for gfal-rm — mirrors reference gfal2-util/test/functional/test_rm.py."""

import os

from helpers import run_gfal

# ---------------------------------------------------------------------------
# Single file deletion
# ---------------------------------------------------------------------------


class TestRmSingleFile:
    def test_rm_single_file(self, tmp_path):
        f = tmp_path / "file.txt"
        f.write_text("content")

        rc, out, err = run_gfal("rm", f.as_uri())

        assert rc == 0
        assert not f.exists()
        assert "DELETED" in out

    def test_rm_binary_file(self, tmp_path):
        f = tmp_path / "data.bin"
        f.write_bytes(os.urandom(1025))

        rc, out, err = run_gfal("rm", f.as_uri())

        assert rc == 0
        assert not f.exists()

    def test_rm_single_file_with_ipv6_flag(self, tmp_path):
        f = tmp_path / "file.txt"
        f.write_text("content")

        rc, out, err = run_gfal("rm", "--ipv6", f.as_uri())

        assert rc == 0
        assert not f.exists()


# ---------------------------------------------------------------------------
# Multiple files
# ---------------------------------------------------------------------------


class TestRmMultiple:
    def test_rm_multiple_files(self, tmp_path):
        """Reference: test_multiple."""
        f1 = tmp_path / "f1.txt"
        f2 = tmp_path / "f2.txt"
        f1.write_text("a")
        f2.write_text("b")

        rc, out, err = run_gfal("rm", f1.as_uri(), f2.as_uri())

        assert rc == 0
        assert not f1.exists()
        assert not f2.exists()
        assert out.count("DELETED") == 2

    def test_rm_three_files(self, tmp_path):
        files = []
        for i in range(3):
            f = tmp_path / f"f{i}.txt"
            f.write_text(f"content {i}")
            files.append(f)

        rc, out, err = run_gfal("rm", *[f.as_uri() for f in files])

        assert rc == 0
        assert all(not f.exists() for f in files)
        assert out.count("DELETED") == 3


# ---------------------------------------------------------------------------
# Nonexistent file
# ---------------------------------------------------------------------------


class TestRmNonexistent:
    def test_rm_nonexistent(self, tmp_path):
        f = tmp_path / "no_such_file.txt"

        rc, out, err = run_gfal("rm", f.as_uri())

        assert rc != 0
        assert "MISSING" in out


# ---------------------------------------------------------------------------
# Directory handling
# ---------------------------------------------------------------------------


class TestRmDirectory:
    def test_rm_directory_without_recursive(self, tmp_path):
        """Reference: test_dir_non_rec — fails with error about 'directory'."""
        d = tmp_path / "mydir"
        d.mkdir()

        rc, out, err = run_gfal("rm", d.as_uri())

        assert rc != 0
        assert d.is_dir()
        assert "directory" in err.lower()

    def test_rm_directory_recursive(self, tmp_path):
        """Reference: test_recursive."""
        d = tmp_path / "mydir"
        d.mkdir()
        (d / "file.txt").write_text("x")
        (d / "sub").mkdir()
        (d / "sub" / "nested.txt").write_text("y")

        rc, out, err = run_gfal("rm", "-r", d.as_uri())

        assert rc == 0
        assert not d.exists()
        assert "RMDIR" in out

    def test_rm_recursive_short_flag_R(self, tmp_path):
        """-R is also accepted."""
        d = tmp_path / "mydir"
        d.mkdir()
        (d / "f.txt").write_text("x")

        rc, out, err = run_gfal("rm", "-R", d.as_uri())

        assert rc == 0
        assert not d.exists()

    def test_rm_recursive_empty_dir(self, tmp_path):
        d = tmp_path / "emptydir"
        d.mkdir()

        rc, out, err = run_gfal("rm", "-r", d.as_uri())

        assert rc == 0
        assert not d.exists()
        assert "RMDIR" in out

    def test_rm_recursive_deep(self, nested_dir):
        """Remove a deeply nested directory tree."""
        rc, out, err = run_gfal("rm", "-r", nested_dir.as_uri())

        assert rc == 0
        assert not nested_dir.exists()

    def test_rm_recursive_reports_deleted_files(self, tmp_path):
        d = tmp_path / "mydir"
        d.mkdir()
        (d / "f1.txt").write_text("a")
        (d / "f2.txt").write_text("b")

        rc, out, err = run_gfal("rm", "-r", d.as_uri())

        assert rc == 0
        assert out.count("DELETED") == 2
        assert "RMDIR" in out


# ---------------------------------------------------------------------------
# Dry run
# ---------------------------------------------------------------------------


class TestRmDryRun:
    def test_dry_run_file(self, tmp_path):
        f = tmp_path / "file.txt"
        f.write_text("x")

        rc, out, err = run_gfal("rm", "--dry-run", f.as_uri())

        assert rc == 0
        assert f.exists()
        assert "SKIP" in out

    def test_dry_run_directory(self, tmp_path):
        d = tmp_path / "mydir"
        d.mkdir()
        (d / "f.txt").write_text("x")

        rc, out, err = run_gfal("rm", "-r", "--dry-run", d.as_uri())

        assert rc == 0
        assert d.exists()
        assert (d / "f.txt").exists()
        assert "SKIP DIR" in out

    def test_dry_run_multiple_files(self, tmp_path):
        f1 = tmp_path / "f1.txt"
        f2 = tmp_path / "f2.txt"
        f1.write_text("a")
        f2.write_text("b")

        rc, out, err = run_gfal("rm", "--dry-run", f1.as_uri(), f2.as_uri())

        assert rc == 0
        assert f1.exists()
        assert f2.exists()
        assert out.count("SKIP") == 2


# ---------------------------------------------------------------------------
# --from-file
# ---------------------------------------------------------------------------


class TestRmFromFile:
    def test_from_file(self, tmp_path):
        f1 = tmp_path / "f1.txt"
        f2 = tmp_path / "f2.txt"
        f1.write_text("a")
        f2.write_text("b")

        list_file = tmp_path / "to_delete.txt"
        list_file.write_text(f"{f1.as_uri()}\n{f2.as_uri()}\n")

        rc, out, err = run_gfal("rm", "--from-file", str(list_file))

        assert rc == 0
        assert not f1.exists()
        assert not f2.exists()

    def test_from_file_blank_lines_ignored(self, tmp_path):
        f = tmp_path / "f.txt"
        f.write_text("content")
        list_file = tmp_path / "list.txt"
        list_file.write_text(f"\n\n{f.as_uri()}\n\n")

        rc, out, err = run_gfal("rm", "--from-file", str(list_file))

        assert rc == 0
        assert not f.exists()

    def test_from_file_cannot_combine_with_positional(self, tmp_path):
        f = tmp_path / "f.txt"
        f.write_text("x")
        list_file = tmp_path / "list.txt"
        list_file.write_text(f.as_uri())

        rc, out, err = run_gfal("rm", "--from-file", str(list_file), f.as_uri())

        assert rc != 0


# ---------------------------------------------------------------------------
# --just-delete
# ---------------------------------------------------------------------------


class TestRmJustDelete:
    def test_just_delete(self, tmp_path):
        """--just-delete skips the stat check and deletes directly."""
        f = tmp_path / "file.txt"
        f.write_text("x")

        rc, out, err = run_gfal("rm", "--just-delete", f.as_uri())

        assert rc == 0
        assert not f.exists()
        assert "DELETED" in out

    def test_just_delete_nonexistent(self, tmp_path):
        """--just-delete on nonexistent file should still fail."""
        f = tmp_path / "no_such.txt"

        rc, out, err = run_gfal("rm", "--just-delete", f.as_uri())

        assert rc != 0


# ---------------------------------------------------------------------------
# No arguments
# ---------------------------------------------------------------------------


class TestRmNoArgs:
    def test_no_args(self):
        rc, out, err = run_gfal("rm")
        assert rc != 0


# ---------------------------------------------------------------------------
# --bulk flag
# ---------------------------------------------------------------------------


class TestRmBulk:
    def test_bulk_accepted(self, tmp_path):
        """--bulk flag is accepted; deletion still works sequentially."""
        f = tmp_path / "file.txt"
        f.write_text("x")

        rc, out, err = run_gfal("rm", "--bulk", f.as_uri())

        assert rc == 0
        assert not f.exists()

    def test_bulk_appears_in_help(self):
        rc, out, err = run_gfal("rm", "--help")
        assert rc == 0
        combined = out + err
        assert "bulk" in combined
