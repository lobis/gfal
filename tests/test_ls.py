"""Tests for gfal-ls — mirrors reference gfal2-util/test/functional/test_ls.py."""

import pytest

from helpers import run_gfal

# ---------------------------------------------------------------------------
# Basic listing
# ---------------------------------------------------------------------------


class TestLsBasic:
    def test_list_directory(self, tmp_path):
        (tmp_path / "alpha.txt").write_text("a")
        (tmp_path / "beta.txt").write_text("b")

        rc, out, err = run_gfal("ls", tmp_path.as_uri())

        assert rc == 0
        assert "alpha.txt" in out
        assert "beta.txt" in out

    def test_list_counts_match(self, tmp_path):
        """Output line count should equal the number of entries (ref: test_basic)."""
        (tmp_path / "f1.bin").write_bytes(b"x")
        (tmp_path / "f2.bin").write_bytes(b"y")
        sub = tmp_path / "subdir"
        sub.mkdir()

        rc, out, err = run_gfal("ls", tmp_path.as_uri())

        assert rc == 0
        lines = [ln for ln in out.splitlines() if ln.strip()]
        assert len(lines) == len(list(tmp_path.iterdir()))

    def test_list_single_file(self, tmp_path):
        f = tmp_path / "solo.txt"
        f.write_text("content")

        rc, out, err = run_gfal("ls", f.as_uri())

        assert rc == 0
        assert "solo.txt" in out

    def test_list_empty_directory(self, tmp_path):
        d = tmp_path / "empty"
        d.mkdir()

        rc, out, err = run_gfal("ls", d.as_uri())

        assert rc == 0
        assert out.strip() == ""

    def test_list_nonexistent(self, tmp_path):
        rc, out, err = run_gfal("ls", (tmp_path / "no_such_path").as_uri())
        assert rc != 0


# ---------------------------------------------------------------------------
# Long format (-l)
# ---------------------------------------------------------------------------


class TestLsLongFormat:
    def test_long_format_shows_size(self, tmp_path):
        """Reference: test_size — '-l' shows raw byte count."""
        f = tmp_path / "file.txt"
        f.write_bytes(b"x" * 1025)

        rc, out, err = run_gfal("ls", "-l", tmp_path.as_uri())

        assert rc == 0
        assert "1025" in out
        assert "file.txt" in out

    def test_long_format_human_readable(self, tmp_path):
        """Reference: test_size — '-lH' shows human-readable size."""
        f = tmp_path / "file.txt"
        f.write_bytes(b"x" * 1025)

        rc, out, err = run_gfal("ls", "-lH", tmp_path.as_uri())

        assert rc == 0
        assert "1.1K" in out

    def test_long_format_single_file(self, tmp_path):
        f = tmp_path / "solo.txt"
        f.write_bytes(b"y" * 42)

        rc, out, err = run_gfal("ls", "-l", f.as_uri())

        assert rc == 0
        assert "42" in out

    def test_long_format_shows_name(self, tmp_path):
        """Reference: test_name — long format includes the URI."""
        f = tmp_path / "myfile.txt"
        f.write_text("x")

        rc, out, err = run_gfal("ls", "-l", f.as_uri())

        assert rc == 0
        assert f.as_uri() in out

    def test_long_format_permissions_string(self, tmp_path):
        f = tmp_path / "file.txt"
        f.write_text("x")

        rc, out, err = run_gfal("ls", "-l", tmp_path.as_uri())

        assert rc == 0
        # Should contain something like "-rw-r--r--" or similar
        assert "rw" in out

    def test_long_format_invalid_file(self, tmp_path):
        """Reference: test_invalid — nonexistent file returns errno 2."""
        inv = tmp_path / "INVALID"
        rc, out, err = run_gfal("ls", "-lH", inv.as_uri())

        assert rc != 0
        assert len(out) == 0 or "No such file" in err


# ---------------------------------------------------------------------------
# -d directory flag
# ---------------------------------------------------------------------------


class TestLsDirectoryFlag:
    def test_directory_flag_shows_single_entry(self, tmp_path):
        """Reference: test_directory — '-d' gives one line, not contents."""
        (tmp_path / "child.txt").write_text("c")

        rc, out, err = run_gfal("ls", "-d", tmp_path.as_uri())

        assert rc == 0
        lines = [ln for ln in out.splitlines() if ln.strip()]
        assert len(lines) == 1
        assert "child.txt" not in out

    def test_directory_flag_long_format(self, tmp_path):
        """Reference: test_name — '-dl' includes the URI."""
        f = tmp_path / "myfile.txt"
        f.write_text("x")

        rc, out, err = run_gfal("ls", "-dl", f.as_uri())

        assert rc == 0
        assert f.as_uri() in out
        lines = [ln for ln in out.splitlines() if ln.strip()]
        assert len(lines) == 1


# ---------------------------------------------------------------------------
# Hidden files
# ---------------------------------------------------------------------------


class TestLsHiddenFiles:
    def test_hidden_files_hidden_by_default(self, hidden_dir):
        rc, out, err = run_gfal("ls", hidden_dir.as_uri())

        assert rc == 0
        assert ".hidden1" not in out
        assert ".hidden2" not in out
        assert "visible1" in out
        assert "visible2" in out

    def test_hidden_files_shown_with_all(self, hidden_dir):
        rc, out, err = run_gfal("ls", "-a", hidden_dir.as_uri())

        assert rc == 0
        assert ".hidden1" in out
        assert ".hidden2" in out
        assert "visible1" in out
        assert "visible2" in out


# ---------------------------------------------------------------------------
# Time styles
# ---------------------------------------------------------------------------


class TestLsTimeStyles:
    @pytest.mark.parametrize("style", ["full-iso", "long-iso", "iso", "locale"])
    def test_time_style(self, tmp_path, style):
        (tmp_path / "f.txt").write_text("x")

        rc, out, err = run_gfal("ls", "-l", f"--time-style={style}", tmp_path.as_uri())

        assert rc == 0
        assert "f.txt" in out

    def test_full_time(self, tmp_path):
        (tmp_path / "f.txt").write_text("x")

        rc, out, err = run_gfal("ls", "-l", "--full-time", tmp_path.as_uri())

        assert rc == 0
        assert "f.txt" in out

    def test_full_time_uses_full_iso(self, tmp_path):
        """--full-time should produce full-ISO timestamps (with seconds and offset)."""
        (tmp_path / "f.txt").write_text("x")

        rc, out, err = run_gfal("ls", "-l", "--full-time", tmp_path.as_uri())

        assert rc == 0
        # full-iso format: YYYY-MM-DD HH:MM:SS.ffffff +0000
        assert "+0000" in out


# ---------------------------------------------------------------------------
# Color output
# ---------------------------------------------------------------------------


class TestLsColor:
    def test_color_never(self, tmp_path):
        (tmp_path / "f.txt").write_text("x")

        rc, out, err = run_gfal("ls", "--color=never", tmp_path.as_uri())

        assert rc == 0
        assert "\033[" not in out

    def test_color_always(self, tmp_path):
        (tmp_path / "f.txt").write_text("x")

        rc, out, err = run_gfal("ls", "--color=always", tmp_path.as_uri())

        assert rc == 0
        assert "\033[" in out

    def test_color_always_directory(self, tmp_path):
        """Directories should be colorised differently from files."""
        sub = tmp_path / "mysubdir"
        sub.mkdir()

        rc, out, err = run_gfal("ls", "--color=always", tmp_path.as_uri())

        assert rc == 0
        assert "\033[" in out


# ---------------------------------------------------------------------------
# Human-readable sizes
# ---------------------------------------------------------------------------


class TestLsHumanReadable:
    @pytest.mark.parametrize(
        "size, expected",
        [
            (0, "0"),
            (512, "512"),
            (1024, "1.0K"),
            (1025, "1.1K"),
            (1048576, "1.0M"),
            (1073741824, "1.0G"),
        ],
    )
    def test_human_size_values(self, tmp_path, size, expected):
        """Verify -lH produces expected human-readable values."""
        f = tmp_path / "test.bin"
        f.write_bytes(b"\x00" * size)

        rc, out, err = run_gfal("ls", "-lH", f.as_uri())

        assert rc == 0
        assert expected in out


# ---------------------------------------------------------------------------
# Multiple files / subdirectories
# ---------------------------------------------------------------------------


class TestLsPopulatedDir:
    def test_list_populated_dir(self, populated_dir):
        rc, out, err = run_gfal("ls", populated_dir.as_uri())

        assert rc == 0
        assert "f1.bin" in out
        assert "f2.bin" in out
        assert "subdir" in out

    def test_list_long_populated_dir(self, populated_dir):
        rc, out, err = run_gfal("ls", "-l", populated_dir.as_uri())

        assert rc == 0
        assert "1025" in out  # file size
        lines = [ln for ln in out.splitlines() if ln.strip()]
        assert len(lines) == 3  # f1.bin, f2.bin, subdir


# ---------------------------------------------------------------------------
# Multiple URL listing
# ---------------------------------------------------------------------------


class TestLsMultipleUrls:
    def test_two_files(self, tmp_path):
        a = tmp_path / "a.txt"
        b = tmp_path / "b.txt"
        a.write_text("A")
        b.write_text("B")

        rc, out, err = run_gfal("ls", a.as_uri(), b.as_uri())

        assert rc == 0
        # Both files appear (as headers when multiple URLs)
        assert "a.txt" in out
        assert "b.txt" in out

    def test_two_directories(self, tmp_path):
        d1 = tmp_path / "dir1"
        d2 = tmp_path / "dir2"
        d1.mkdir()
        d2.mkdir()
        (d1 / "x.txt").write_text("x")
        (d2 / "y.txt").write_text("y")

        rc, out, err = run_gfal("ls", d1.as_uri(), d2.as_uri())

        assert rc == 0
        assert "x.txt" in out
        assert "y.txt" in out

    def test_header_shown_for_multiple_dirs(self, tmp_path):
        d1 = tmp_path / "dir1"
        d2 = tmp_path / "dir2"
        d1.mkdir()
        d2.mkdir()
        (d1 / "f.txt").write_text("x")
        (d2 / "g.txt").write_text("y")

        rc, out, err = run_gfal("ls", d1.as_uri(), d2.as_uri())

        assert rc == 0
        # Headers like "dir1:" and "dir2:" should appear
        assert "dir1" in out
        assert "dir2" in out

    def test_one_nonexistent_continues(self, tmp_path):
        a = tmp_path / "a.txt"
        a.write_text("A")
        missing = tmp_path / "no_such"

        rc, out, err = run_gfal("ls", a.as_uri(), missing.as_uri())

        # Should report error for missing but still list existing
        assert rc != 0
        assert "a.txt" in out

    def test_long_format_multiple(self, tmp_path):
        a = tmp_path / "alpha.txt"
        b = tmp_path / "beta.txt"
        a.write_bytes(b"hello")
        b.write_bytes(b"world!!")

        rc, out, err = run_gfal("ls", "-l", a.as_uri(), b.as_uri())

        assert rc == 0
        assert "5" in out  # size of alpha.txt
        assert "7" in out  # size of beta.txt

    def test_single_url_no_header(self, tmp_path):
        """With a single URL no 'url:\\n' header is printed before contents."""
        a = tmp_path / "solo.txt"
        a.write_text("x")

        rc, out, err = run_gfal("ls", a.as_uri())

        assert rc == 0
        lines = [ln for ln in out.splitlines() if ln.strip()]
        assert len(lines) == 1
        # A header line ends with ':'; the file URI itself does not end with ':'
        assert not lines[0].endswith(":")


# ---------------------------------------------------------------------------
# Sorted output
# ---------------------------------------------------------------------------


class TestLsReverse:
    def test_reverse_flag(self, tmp_path):
        for name in ["apple.txt", "mango.txt", "zebra.txt"]:
            (tmp_path / name).write_text(name)

        rc, out, err = run_gfal("ls", "-r", tmp_path.as_uri())

        assert rc == 0
        names = [ln.strip() for ln in out.splitlines() if ln.strip()]
        assert names == sorted(names, key=str.lower, reverse=True)

    def test_reverse_is_opposite_of_normal(self, tmp_path):
        for name in ["cat.txt", "ant.txt", "bee.txt"]:
            (tmp_path / name).write_text(name)

        _, out_normal, _ = run_gfal("ls", tmp_path.as_uri())
        _, out_reverse, _ = run_gfal("ls", "-r", tmp_path.as_uri())

        normal_names = [ln.strip() for ln in out_normal.splitlines() if ln.strip()]
        reverse_names = [ln.strip() for ln in out_reverse.splitlines() if ln.strip()]
        assert normal_names == list(reversed(reverse_names))

    def test_reverse_long_format(self, tmp_path):
        for name in ["z.txt", "a.txt"]:
            (tmp_path / name).write_text(name)

        rc, out, err = run_gfal("ls", "-lr", tmp_path.as_uri())

        assert rc == 0
        names = [ln.split()[-1] for ln in out.splitlines() if ln.strip()]
        assert names[0] == "z.txt"
        assert names[1] == "a.txt"


class TestLsSorted:
    def test_alphabetical_order(self, tmp_path):
        """Directory entries are listed in alphabetical order."""
        for name in ["zebra.txt", "apple.txt", "mango.txt"]:
            (tmp_path / name).write_text(name)

        rc, out, err = run_gfal("ls", tmp_path.as_uri())

        assert rc == 0
        names = [ln.strip() for ln in out.splitlines() if ln.strip()]
        assert names == sorted(names, key=str.lower)

    def test_sorted_with_long_format(self, tmp_path):
        for name in ["zoo.txt", "ant.txt", "bee.txt"]:
            (tmp_path / name).write_text(name)

        rc, out, err = run_gfal("ls", "-l", tmp_path.as_uri())

        assert rc == 0
        names = [ln.split()[-1] for ln in out.splitlines() if ln.strip()]
        assert names == sorted(names, key=str.lower)

    def test_case_insensitive_sort(self, tmp_path):
        """Mixed-case names sort without case sensitivity."""
        for name in ["Beta.txt", "alpha.txt", "GAMMA.txt"]:
            (tmp_path / name).write_text(name)

        rc, out, err = run_gfal("ls", tmp_path.as_uri())

        assert rc == 0
        names = [ln.strip() for ln in out.splitlines() if ln.strip()]
        assert names == sorted(names, key=str.lower)
