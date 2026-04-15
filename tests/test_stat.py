"""Tests for gfal-stat."""

from helpers import run_gfal

# ---------------------------------------------------------------------------
# Regular files
# ---------------------------------------------------------------------------


class TestStatRegularFile:
    def test_regular_file(self, tmp_path):
        f = tmp_path / "test.txt"
        f.write_bytes(b"hello world")

        rc, out, err = run_gfal("stat", f.as_uri())

        assert rc == 0
        assert "File:" in out
        assert "Size:" in out
        assert "11" in out
        assert "Access:" in out
        assert "Modify:" in out
        assert "Change:" in out

    def test_shows_file_type(self, tmp_path):
        f = tmp_path / "test.txt"
        f.write_text("x")

        rc, out, err = run_gfal("stat", f.as_uri())

        assert rc == 0
        assert "regular file" in out

    def test_shows_size_zero(self, tmp_path):
        f = tmp_path / "empty.txt"
        f.write_bytes(b"")

        rc, out, err = run_gfal("stat", f.as_uri())

        assert rc == 0
        assert "Size: 0" in out

    def test_shows_uri_in_output(self, tmp_path):
        f = tmp_path / "test.txt"
        f.write_text("x")
        uri = f.as_uri()

        rc, out, err = run_gfal("stat", uri)

        assert rc == 0
        assert uri in out

    def test_shows_permissions(self, tmp_path):
        f = tmp_path / "test.txt"
        f.write_text("x")

        rc, out, err = run_gfal("stat", f.as_uri())

        assert rc == 0
        # Should show something like (0644/-rw-r--r--)
        assert "/" in out  # The mode string like "0644/-rw-r--r--"

    def test_various_sizes(self, tmp_path):
        for size in [1, 100, 1025, 65536]:
            f = tmp_path / f"file_{size}.bin"
            f.write_bytes(b"x" * size)

            rc, out, err = run_gfal("stat", f.as_uri())

            assert rc == 0
            assert str(size) in out

    def test_regular_file_with_ipv4_flag(self, tmp_path):
        f = tmp_path / "test.txt"
        f.write_text("x")

        rc, out, err = run_gfal("stat", "-4", f.as_uri())

        assert rc == 0
        assert "File:" in out


# ---------------------------------------------------------------------------
# Directories
# ---------------------------------------------------------------------------


class TestStatDirectory:
    def test_directory(self, tmp_path):
        rc, out, err = run_gfal("stat", tmp_path.as_uri())

        assert rc == 0
        assert "directory" in out

    def test_subdirectory(self, tmp_path):
        d = tmp_path / "sub"
        d.mkdir()

        rc, out, err = run_gfal("stat", d.as_uri())

        assert rc == 0
        assert "directory" in out


# ---------------------------------------------------------------------------
# Nonexistent
# ---------------------------------------------------------------------------


class TestStatNonexistent:
    def test_nonexistent_file(self, tmp_path):
        rc, out, err = run_gfal("stat", (tmp_path / "no_such_file").as_uri())

        assert rc != 0

    def test_nonexistent_nested(self, tmp_path):
        rc, out, err = run_gfal(
            "stat", (tmp_path / "a" / "b" / "c" / "no_such").as_uri()
        )

        assert rc != 0


# ---------------------------------------------------------------------------
# Timestamps
# ---------------------------------------------------------------------------


class TestStatTimestamps:
    def test_timestamps_present(self, tmp_path):
        f = tmp_path / "test.txt"
        f.write_text("x")

        rc, out, err = run_gfal("stat", f.as_uri())

        assert rc == 0
        # Should have Access:, Modify:, Change: timestamps
        assert out.count("Access:") == 2  # permission + timestamp
        assert "Modify:" in out
        assert "Change:" in out

    def test_timestamp_format(self, tmp_path):
        """Timestamps should be in YYYY-MM-DD HH:MM:SS format."""
        f = tmp_path / "test.txt"
        f.write_text("x")

        rc, out, err = run_gfal("stat", f.as_uri())

        assert rc == 0
        # Look for a timestamp-like pattern
        import re

        assert re.search(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}", out)


# ---------------------------------------------------------------------------
# Multiple files
# ---------------------------------------------------------------------------


class TestStatMultipleFiles:
    def test_two_files(self, tmp_path):
        a = tmp_path / "a.txt"
        b = tmp_path / "b.txt"
        a.write_bytes(b"hello")
        b.write_bytes(b"world!!")

        rc, out, err = run_gfal("stat", a.as_uri(), b.as_uri())

        assert rc == 0
        assert "a.txt" in out
        assert "b.txt" in out
        assert "5" in out  # size of a.txt
        assert "7" in out  # size of b.txt

    def test_all_nonexistent(self, tmp_path):
        rc, out, err = run_gfal(
            "stat", (tmp_path / "x").as_uri(), (tmp_path / "y").as_uri()
        )
        assert rc != 0

    def test_mixed_existing_and_nonexistent(self, tmp_path):
        a = tmp_path / "a.txt"
        a.write_text("x")

        rc, out, err = run_gfal("stat", a.as_uri(), (tmp_path / "no_such").as_uri())

        assert rc != 0
        # The existing file's output is still printed
        assert "a.txt" in out

    def test_directory_and_file(self, tmp_path):
        f = tmp_path / "file.txt"
        f.write_text("data")
        d = tmp_path / "subdir"
        d.mkdir()

        rc, out, err = run_gfal("stat", f.as_uri(), d.as_uri())

        assert rc == 0
        assert "regular file" in out
        assert "directory" in out

    def test_blank_line_separator_between_files(self, tmp_path):
        """Multiple files should be separated by a blank line."""
        a = tmp_path / "a.txt"
        b = tmp_path / "b.txt"
        a.write_text("x")
        b.write_text("y")

        rc, out, err = run_gfal("stat", a.as_uri(), b.as_uri())

        assert rc == 0
        lines = out.splitlines()
        # There must be at least one blank line between the two file blocks
        assert "" in lines

    def test_no_trailing_blank_line_single_file(self, tmp_path):
        """A single file stat should not have a leading blank line."""
        f = tmp_path / "f.txt"
        f.write_text("x")

        rc, out, err = run_gfal("stat", f.as_uri())

        assert rc == 0
        assert not out.startswith("\n")


# ---------------------------------------------------------------------------
# Verbose and flag acceptance
# ---------------------------------------------------------------------------


class TestStatVerbose:
    def test_verbose_flag_accepted(self, tmp_path):
        f = tmp_path / "test.txt"
        f.write_text("x")

        rc, out, err = run_gfal("stat", "-v", f.as_uri())

        assert rc == 0
        assert "File:" in out

    def test_no_verify_flag_accepted(self, tmp_path):
        f = tmp_path / "test.txt"
        f.write_text("x")

        rc, out, err = run_gfal("stat", "--no-verify", f.as_uri())

        assert rc == 0

    def test_stat_bare_path(self, tmp_path):
        """gfal-stat should also accept a bare filesystem path (no file://)."""
        f = tmp_path / "test.txt"
        f.write_text("x")

        rc, out, err = run_gfal("stat", str(f))

        assert rc == 0
        assert "File:" in out
