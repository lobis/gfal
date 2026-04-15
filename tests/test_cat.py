"""Tests for gfal-cat."""

from helpers import run_gfal, run_gfal_binary

# ---------------------------------------------------------------------------
# Basic cat
# ---------------------------------------------------------------------------


class TestCatBasic:
    def test_single_file(self, tmp_path):
        f = tmp_path / "test.txt"
        f.write_text("hello world\n")

        rc, out, err = run_gfal("cat", f.as_uri())

        assert rc == 0
        assert out == "hello world\n"

    def test_empty_file(self, tmp_path):
        f = tmp_path / "empty.txt"
        f.write_bytes(b"")

        rc, out, err = run_gfal("cat", f.as_uri())

        assert rc == 0
        assert out == ""

    def test_nonexistent(self, tmp_path):
        rc, out, err = run_gfal("cat", (tmp_path / "no_such_file.txt").as_uri())

        assert rc != 0

    def test_single_file_with_ipv6_flag(self, tmp_path):
        f = tmp_path / "test.txt"
        f.write_text("hello ipv6\n")

        rc, out, err = run_gfal("cat", "--ipv6", f.as_uri())

        assert rc == 0
        assert out == "hello ipv6\n"


# ---------------------------------------------------------------------------
# Binary content
# ---------------------------------------------------------------------------


class TestCatBinary:
    def test_binary_content_preserved(self, tmp_path):
        """Content is passed through as-is, including null bytes."""
        data = bytes(range(256))
        f = tmp_path / "binary.bin"
        f.write_bytes(data)

        rc, stdout, stderr = run_gfal_binary("cat", f.as_uri())

        assert rc == 0
        assert stdout == data

    def test_binary_all_zeros(self, tmp_path):
        data = b"\x00" * 1024
        f = tmp_path / "zeros.bin"
        f.write_bytes(data)

        rc, stdout, stderr = run_gfal_binary("cat", f.as_uri())

        assert rc == 0
        assert stdout == data


# ---------------------------------------------------------------------------
# Multiple files
# ---------------------------------------------------------------------------


class TestCatMultiple:
    def test_multiple_files_concatenated(self, tmp_path):
        f1 = tmp_path / "f1.txt"
        f2 = tmp_path / "f2.txt"
        f1.write_text("part1")
        f2.write_text("part2")

        rc, out, err = run_gfal("cat", f1.as_uri(), f2.as_uri())

        assert rc == 0
        assert out == "part1part2"

    def test_three_files(self, tmp_path):
        files = []
        for i in range(3):
            f = tmp_path / f"f{i}.txt"
            f.write_text(f"chunk{i}")
            files.append(f)

        rc, out, err = run_gfal("cat", *[f.as_uri() for f in files])

        assert rc == 0
        assert out == "chunk0chunk1chunk2"


# ---------------------------------------------------------------------------
# Large files (> CHUNK_SIZE)
# ---------------------------------------------------------------------------


class TestCatLargeFile:
    def test_large_file(self, tmp_path):
        """Content across multiple read chunks (> 4 MiB)."""
        data = b"Z" * (5 * 1024 * 1024)
        f = tmp_path / "large.bin"
        f.write_bytes(data)

        rc, stdout, stderr = run_gfal_binary("cat", f.as_uri())

        assert rc == 0
        assert stdout == data


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------


class TestCatErrors:
    def test_nonexistent_error_message(self, tmp_path):
        rc, out, err = run_gfal("cat", (tmp_path / "no_such.txt").as_uri())

        assert rc != 0
        assert "No such file" in err or "no such file" in err.lower()

    def test_multi_file_one_missing_continues(self, tmp_path):
        """Valid files before/after a missing file are still printed."""
        f1 = tmp_path / "f1.txt"
        f3 = tmp_path / "f3.txt"
        f1.write_text("first")
        f3.write_text("third")
        missing = tmp_path / "no_such.txt"

        rc, out, err = run_gfal("cat", f1.as_uri(), missing.as_uri(), f3.as_uri())

        assert rc != 0
        assert "first" in out
        assert "third" in out
        assert "no_such" in err or "No such file" in err

    def test_multi_file_all_missing(self, tmp_path):
        rc, out, err = run_gfal(
            "cat",
            (tmp_path / "a.txt").as_uri(),
            (tmp_path / "b.txt").as_uri(),
        )

        assert rc != 0
        assert out == ""

    def test_multi_file_first_missing_rest_printed(self, tmp_path):
        """Error on first file should not prevent subsequent files from printing."""
        f2 = tmp_path / "f2.txt"
        f2.write_text("present")

        rc, out, err = run_gfal("cat", (tmp_path / "missing.txt").as_uri(), f2.as_uri())

        assert rc != 0
        assert "present" in out
