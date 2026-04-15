"""Tests for gfal-save (stdin → remote file)."""

from helpers import run_gfal, run_gfal_binary

# ---------------------------------------------------------------------------
# Basic save
# ---------------------------------------------------------------------------


class TestSaveBasic:
    def test_save_text(self, tmp_path):
        f = tmp_path / "out.txt"
        content = "hello from stdin\n"

        rc, out, err = run_gfal("save", f.as_uri(), input=content)

        assert rc == 0
        assert f.read_text() == content

    def test_save_multiline(self, tmp_path):
        f = tmp_path / "out.txt"
        content = "line1\nline2\nline3\n"

        rc, out, err = run_gfal("save", f.as_uri(), input=content)

        assert rc == 0
        assert f.read_text() == content

    def test_save_empty_stdin(self, tmp_path):
        f = tmp_path / "empty.txt"

        rc, out, err = run_gfal("save", f.as_uri(), input="")

        assert rc == 0
        assert f.read_bytes() == b""

    def test_save_text_with_ipv4_flag(self, tmp_path):
        f = tmp_path / "out.txt"
        content = "hello over ipv4\n"

        rc, out, err = run_gfal("save", "-4", f.as_uri(), input=content)

        assert rc == 0
        assert f.read_text() == content


# ---------------------------------------------------------------------------
# Binary save
# ---------------------------------------------------------------------------


class TestSaveBinary:
    def test_save_binary(self, tmp_path):
        f = tmp_path / "out.bin"
        data = bytes(range(256))

        rc, stdout, stderr = run_gfal_binary("save", f.as_uri(), input_bytes=data)

        assert rc == 0
        assert f.read_bytes() == data

    def test_save_binary_with_nulls(self, tmp_path):
        f = tmp_path / "nulls.bin"
        data = b"\x00" * 512

        rc, stdout, stderr = run_gfal_binary("save", f.as_uri(), input_bytes=data)

        assert rc == 0
        assert f.read_bytes() == data


# ---------------------------------------------------------------------------
# Large input (> CHUNK_SIZE)
# ---------------------------------------------------------------------------


class TestSaveLargeInput:
    def test_large_input(self, tmp_path):
        f = tmp_path / "large.bin"
        data = b"B" * (5 * 1024 * 1024)

        rc, stdout, stderr = run_gfal_binary("save", f.as_uri(), input_bytes=data)

        assert rc == 0
        assert f.read_bytes() == data


# ---------------------------------------------------------------------------
# Overwrite
# ---------------------------------------------------------------------------


class TestSaveOverwrite:
    def test_overwrites_existing(self, tmp_path):
        f = tmp_path / "existing.txt"
        f.write_text("old content")

        rc, out, err = run_gfal("save", f.as_uri(), input="new content")

        assert rc == 0
        assert f.read_text() == "new content"

    def test_overwrites_with_smaller_content(self, tmp_path):
        f = tmp_path / "existing.txt"
        f.write_text("this is a long string of old content")

        rc, out, err = run_gfal("save", f.as_uri(), input="short")

        assert rc == 0
        assert f.read_text() == "short"


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------


class TestSaveErrors:
    def test_missing_parent_directory_fails(self, tmp_path):
        """Writing to a path whose parent doesn't exist should fail."""
        f = tmp_path / "nonexistent_parent" / "out.txt"

        rc, out, err = run_gfal("save", f.as_uri(), input="data")

        assert rc != 0

    def test_error_goes_to_stderr(self, tmp_path):
        """Error messages should go to stderr, not stdout."""
        f = tmp_path / "no_parent" / "out.txt"

        rc, out, err = run_gfal("save", f.as_uri(), input="data")

        assert rc != 0
        assert out.strip() == ""
        assert err.strip() != ""
