"""Tests for gfal-rename."""

from helpers import run_gfal

# ---------------------------------------------------------------------------
# File rename
# ---------------------------------------------------------------------------


class TestRenameFile:
    def test_rename_file(self, tmp_path):
        src = tmp_path / "old.txt"
        dst = tmp_path / "new.txt"
        src.write_text("content")

        rc, out, err = run_gfal("rename", src.as_uri(), dst.as_uri())

        assert rc == 0
        assert not src.exists()
        assert dst.read_text() == "content"

    def test_rename_preserves_content(self, tmp_path):
        import os

        data = os.urandom(1025)
        src = tmp_path / "old.bin"
        dst = tmp_path / "new.bin"
        src.write_bytes(data)

        rc, out, err = run_gfal("rename", src.as_uri(), dst.as_uri())

        assert rc == 0
        assert dst.read_bytes() == data

    def test_rename_overwrites_existing_destination(self, tmp_path):
        src = tmp_path / "src.txt"
        dst = tmp_path / "dst.txt"
        src.write_text("new")
        dst.write_text("old")

        rc, out, err = run_gfal("rename", src.as_uri(), dst.as_uri())

        assert rc == 0
        assert not src.exists()
        assert dst.read_text() == "new"


# ---------------------------------------------------------------------------
# Directory rename
# ---------------------------------------------------------------------------


class TestRenameDirectory:
    def test_rename_directory(self, tmp_path):
        src = tmp_path / "olddir"
        dst = tmp_path / "newdir"
        src.mkdir()
        (src / "file.txt").write_text("x")

        rc, out, err = run_gfal("rename", src.as_uri(), dst.as_uri())

        assert rc == 0
        assert not src.exists()
        assert dst.is_dir()
        assert (dst / "file.txt").read_text() == "x"

    def test_rename_empty_directory(self, tmp_path):
        src = tmp_path / "olddir"
        dst = tmp_path / "newdir"
        src.mkdir()

        rc, out, err = run_gfal("rename", src.as_uri(), dst.as_uri())

        assert rc == 0
        assert not src.exists()
        assert dst.is_dir()


# ---------------------------------------------------------------------------
# Error cases
# ---------------------------------------------------------------------------


class TestRenameErrors:
    def test_nonexistent_source(self, tmp_path):
        src = tmp_path / "no_such.txt"
        dst = tmp_path / "dst.txt"

        rc, out, err = run_gfal("rename", src.as_uri(), dst.as_uri())

        assert rc != 0

    def test_destination_parent_missing(self, tmp_path):
        """rename to a path whose parent doesn't exist should fail."""
        src = tmp_path / "src.txt"
        src.write_text("data")
        dst = tmp_path / "nonexistent_parent" / "dst.txt"

        rc, out, err = run_gfal("rename", src.as_uri(), dst.as_uri())

        assert rc != 0
        assert src.exists()  # source unchanged

    def test_error_message_goes_to_stderr(self, tmp_path):
        src = tmp_path / "no_such.txt"
        dst = tmp_path / "dst.txt"

        rc, out, err = run_gfal("rename", src.as_uri(), dst.as_uri())

        assert rc != 0
        assert err.strip() != ""
        assert out.strip() == ""
