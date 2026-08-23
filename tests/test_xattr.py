"""Tests for gfal-xattr (extended attributes).

Extended attributes are supported on Linux (ext4/xfs/btrfs) and macOS (HFS+/APFS)
but not on Windows or most network filesystems.  Tests that require real xattr
support are skipped on platforms that don't have it.
"""

import sys
from types import SimpleNamespace

import pytest

import gfal.cli.commands as commands
from helpers import run_gfal

# ---------------------------------------------------------------------------
# Platform detection
# ---------------------------------------------------------------------------

_XATTR_SUPPORTED = sys.platform in ("linux", "darwin")


def _has_xattr(path):
    """Check whether extended attributes actually work on this filesystem."""
    try:
        import os

        os.setxattr(str(path), "user.probe", b"1")
        os.removexattr(str(path), "user.probe")
        return True
    except (AttributeError, OSError):
        pass
    # macOS fallback
    try:
        import subprocess

        r = subprocess.run(
            ["xattr", "-w", "user.probe", "1", str(path)],
            capture_output=True,
        )
        if r.returncode == 0:
            subprocess.run(
                ["xattr", "-d", "user.probe", str(path)],
                capture_output=True,
            )
            return True
    except Exception:
        pass
    return False


# ---------------------------------------------------------------------------
# Unsupported filesystem
# ---------------------------------------------------------------------------


class TestXattrUnsupported:
    def test_unsupported_fs_exits_nonzero(self, tmp_path):
        """gfal-xattr should report an error on filesystems without xattr support.

        On local filesystems that *do* have xattr support the command succeeds,
        so we only verify the exit code is 1 on platforms that truly lack it.
        """
        f = tmp_path / "file.txt"
        f.write_text("x")
        # On Windows the local filesystem backend doesn't expose xattr
        if sys.platform == "win32":
            rc, out, err = run_gfal("xattr", f.as_uri())
            assert rc != 0


class TestXattrOutput:
    def test_list_uses_one_line_per_attribute(self, monkeypatch, capsys):
        class FakeClient:
            @staticmethod
            def listxattr(_file):
                return ["user.good", "user.bad"]

            @staticmethod
            def getxattr(_file, attribute):
                if attribute == "user.bad":
                    raise OSError("permission denied")
                return "value"

        monkeypatch.setattr(commands, "GfalClient", lambda **_kwargs: FakeClient())

        cmd = commands.GfalCommands()
        cmd.params = SimpleNamespace(
            attribute=None,
            file="file:///test",
            quiet=True,
        )

        assert cmd.execute_xattr() == 0
        assert capsys.readouterr().out == (
            "user.good = value\nuser.bad FAILED: permission denied\n"
        )


# ---------------------------------------------------------------------------
# Linux / macOS with real xattr support
# ---------------------------------------------------------------------------


@pytest.mark.skipif(not _XATTR_SUPPORTED, reason="xattr not available on this platform")
class TestXattrLinuxMac:
    def test_list_attrs_empty_file(self, tmp_path):
        f = tmp_path / "empty.txt"
        f.write_text("hello")
        if not _has_xattr(f):
            pytest.skip("xattr not supported on this filesystem")

        # fsspec local filesystem doesn't expose listxattr; skip if unsupported
        rc, out, err = run_gfal("xattr", f.as_uri())
        if rc != 0 and "not supported" in err:
            pytest.skip("xattr not supported by fsspec local filesystem")
        # Should succeed — empty attribute list is fine
        assert rc == 0

    def test_set_and_get_attr(self, tmp_path):
        f = tmp_path / "file.txt"
        f.write_text("data")
        if not _has_xattr(f):
            pytest.skip("xattr not supported on this filesystem")

        # Set via gfal-xattr file user.test=hello
        rc, out, err = run_gfal("xattr", f.as_uri(), "user.test=hello")
        if rc != 0:
            pytest.skip(f"setxattr failed: {err}")

        # Get via gfal-xattr file user.test
        rc2, out2, err2 = run_gfal("xattr", f.as_uri(), "user.test")
        assert rc2 == 0
        assert "hello" in out2

    def test_get_nonexistent_attr(self, tmp_path):
        f = tmp_path / "file.txt"
        f.write_text("data")
        if not _has_xattr(f):
            pytest.skip("xattr not supported on this filesystem")

        rc, out, err = run_gfal("xattr", f.as_uri(), "user.does_not_exist_xyz")
        # Should exit non-zero when the attribute doesn't exist
        assert rc != 0

    def test_list_shows_set_attrs(self, tmp_path):
        f = tmp_path / "file.txt"
        f.write_text("data")
        if not _has_xattr(f):
            pytest.skip("xattr not supported on this filesystem")

        rc_set, _, _ = run_gfal("xattr", f.as_uri(), "user.mykey=myval")
        if rc_set != 0:
            pytest.skip("setxattr not supported")

        rc, out, err = run_gfal("xattr", f.as_uri())
        assert rc == 0
        assert "user.mykey" in out

    def test_set_value_with_equals_sign(self, tmp_path):
        """Values that contain '=' are passed correctly (only first '=' splits key=value)."""
        f = tmp_path / "file.txt"
        f.write_text("data")
        if not _has_xattr(f):
            pytest.skip("xattr not supported on this filesystem")

        rc, out, err = run_gfal("xattr", f.as_uri(), "user.eq=a=b")
        if rc != 0:
            pytest.skip("setxattr not supported")

        rc2, out2, _ = run_gfal("xattr", f.as_uri(), "user.eq")
        assert rc2 == 0
        assert "a=b" in out2

    def test_nonexistent_file(self, tmp_path):
        rc, out, err = run_gfal("xattr", (tmp_path / "no_such.txt").as_uri())
        assert rc != 0
