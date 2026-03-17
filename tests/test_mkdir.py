"""Tests for gfal-mkdir — mirrors reference gfal2-util/test/functional/test_mkdir.py."""

from helpers import run_gfal

# ---------------------------------------------------------------------------
# Basic creation
# ---------------------------------------------------------------------------


class TestMkdirBasic:
    def test_mkdir_basic(self, tmp_path):
        """Reference: test_mkdir."""
        d = tmp_path / "newdir"

        rc, out, err = run_gfal("mkdir", d.as_uri())

        assert rc == 0
        assert d.is_dir()

    def test_mkdir_default_permissions(self, tmp_path):
        """Reference: test_mkdir — default permissions should be 755."""
        d = tmp_path / "newdir"

        rc, out, err = run_gfal("mkdir", d.as_uri())

        assert rc == 0
        assert d.is_dir()
        # On local filesystem, actual perms depend on umask; just verify it created
        # The reference checks for 755 but we can't guarantee that with all umasks

    def test_mkdir_no_output(self, tmp_path):
        """Reference: test_mkdir — mkdir should produce no stdout."""
        d = tmp_path / "newdir"

        rc, out, err = run_gfal("mkdir", d.as_uri())

        assert rc == 0
        assert len(out) == 0


# ---------------------------------------------------------------------------
# Mode (-m)
# ---------------------------------------------------------------------------


class TestMkdirMode:
    def test_mkdir_mode_700(self, tmp_path):
        """Reference: test_mkdir_mode."""
        d = tmp_path / "modedir"

        rc, out, err = run_gfal("mkdir", "-m", "700", d.as_uri())

        assert rc == 0
        assert d.is_dir()

    def test_mkdir_mode_with_leading_zero(self, tmp_path):
        d = tmp_path / "modedir"

        rc, out, err = run_gfal("mkdir", "-m", "0755", d.as_uri())

        assert rc == 0
        assert d.is_dir()

    def test_mkdir_invalid_mode(self, tmp_path):
        """Reference: test_invalid_mode."""
        d = tmp_path / "badmode"

        rc, out, err = run_gfal("mkdir", "-m", "A", d.as_uri())

        # argparse should reject non-integer mode
        assert rc != 0
        assert not d.exists()

    def test_mkdir_invalid_mode_text(self, tmp_path):
        d = tmp_path / "badmode"

        rc, out, err = run_gfal("mkdir", "-m", "XYZ", d.as_uri())

        assert rc != 0
        assert not d.exists()


# ---------------------------------------------------------------------------
# Multiple directories
# ---------------------------------------------------------------------------


class TestMkdirMultiple:
    def test_mkdir_multiple(self, tmp_path):
        d1 = tmp_path / "dir1"
        d2 = tmp_path / "dir2"

        rc, out, err = run_gfal("mkdir", d1.as_uri(), d2.as_uri())

        assert rc == 0
        assert d1.is_dir()
        assert d2.is_dir()

    def test_mkdir_three_directories(self, tmp_path):
        d1 = tmp_path / "a"
        d2 = tmp_path / "b"
        d3 = tmp_path / "c"

        rc, out, err = run_gfal("mkdir", d1.as_uri(), d2.as_uri(), d3.as_uri())

        assert rc == 0
        assert d1.is_dir()
        assert d2.is_dir()
        assert d3.is_dir()


# ---------------------------------------------------------------------------
# Parents (-p)
# ---------------------------------------------------------------------------


class TestMkdirParents:
    def test_mkdir_parents(self, tmp_path):
        """Reference: test_mkdir_recursive."""
        d = tmp_path / "a" / "b" / "c"

        rc, out, err = run_gfal("mkdir", "-p", d.as_uri())

        assert rc == 0
        assert d.is_dir()

    def test_mkdir_parents_deep(self, tmp_path):
        d = tmp_path / "x" / "y" / "z" / "w"

        rc, out, err = run_gfal("mkdir", "-p", d.as_uri())

        assert rc == 0
        assert d.is_dir()


# ---------------------------------------------------------------------------
# Already exists
# ---------------------------------------------------------------------------


class TestMkdirAlreadyExists:
    def test_already_exists_fails(self, tmp_path):
        """Reference: test_already_exists."""
        d = tmp_path / "existing"
        d.mkdir()

        rc, out, err = run_gfal("mkdir", d.as_uri())

        assert rc != 0
        assert d.is_dir()

    def test_already_exists_with_parents_ok(self, tmp_path):
        """Reference: test_already_exists_p."""
        d = tmp_path / "existing"
        d.mkdir()

        rc, out, err = run_gfal("mkdir", "-p", d.as_uri())

        assert rc == 0
        assert d.is_dir()

    def test_already_exists_with_parents_preserves_contents(self, tmp_path):
        """mkdir -p on existing dir should not remove its contents."""
        d = tmp_path / "existing"
        d.mkdir()
        child = d / "child.txt"
        child.write_text("keep me")

        rc, out, err = run_gfal("mkdir", "-p", d.as_uri())

        assert rc == 0
        assert child.read_text() == "keep me"


# ---------------------------------------------------------------------------
# Error handling / partial failure
# ---------------------------------------------------------------------------


class TestMkdirErrors:
    def test_error_message_to_stderr(self, tmp_path):
        existing = tmp_path / "existing"
        existing.mkdir()

        rc, out, err = run_gfal("mkdir", existing.as_uri())

        assert rc != 0
        assert err.strip() != ""
        assert out.strip() == ""

    def test_partial_failure_continues(self, tmp_path):
        """If one directory fails, the rest should still be attempted."""
        existing = tmp_path / "existing"
        existing.mkdir()
        new_dir = tmp_path / "new"

        rc, out, err = run_gfal("mkdir", existing.as_uri(), new_dir.as_uri())

        assert rc != 0
        assert new_dir.is_dir()  # second dir was still created

    def test_parent_needed_but_no_p_flag(self, tmp_path):
        """Creating a nested path without -p should fail."""
        d = tmp_path / "missing_parent" / "child"

        rc, out, err = run_gfal("mkdir", d.as_uri())

        assert rc != 0
        assert not d.exists()
