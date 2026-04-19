"""
Tests for gfal CLI commands over SFTP (sftp://).

The ``sftp_server`` session fixture (defined in conftest.py) starts an
in-process paramiko SFTP server backed by a real temporary directory.
Tests are skipped automatically when ``paramiko`` is not installed.

Run these tests with::

    pytest tests/test_sftp.py -v

or as part of the full suite (they auto-skip when paramiko is missing).
"""

import uuid

import pytest

from helpers import run_gfal

pytestmark = [
    pytest.mark.sftp,
    pytest.mark.xdist_group("sftp"),
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _uid() -> str:
    """Return a short unique hex string suitable for filenames."""
    return uuid.uuid4().hex[:12]


def _url(sftp_server, path: str) -> str:
    """Build a full sftp:// URL for *path* (which must start with '/')."""
    return sftp_server["base_url"] + path


# ---------------------------------------------------------------------------
# gfal stat  (sftp://)
# ---------------------------------------------------------------------------


class TestSFTPStat:
    def test_stat_file(self, sftp_server):
        """gfal stat returns size and file-type for an existing file."""
        name = f"stat_{_uid()}.txt"
        (sftp_server["data_dir"] / name).write_text("stat me")

        rc, out, err = run_gfal("stat", _url(sftp_server, f"/{name}"))

        assert rc == 0, err
        assert "Size:" in out
        assert "7" in out  # len("stat me")

    def test_stat_directory(self, sftp_server):
        """gfal stat recognises directories."""
        name = f"statdir_{_uid()}"
        (sftp_server["data_dir"] / name).mkdir()

        rc, out, err = run_gfal("stat", _url(sftp_server, f"/{name}"))

        assert rc == 0, err
        assert "directory" in out

    def test_stat_nonexistent(self, sftp_server):
        """gfal stat exits non-zero for a missing path."""
        rc, _out, _err = run_gfal("stat", _url(sftp_server, f"/no_such_{_uid()}"))
        assert rc != 0


# ---------------------------------------------------------------------------
# gfal ls  (sftp://)
# ---------------------------------------------------------------------------


class TestSFTPLs:
    def test_ls_directory(self, sftp_server):
        """gfal ls lists files in a directory."""
        name = f"ls_{_uid()}.txt"
        (sftp_server["data_dir"] / name).write_text("ls test")

        rc, out, err = run_gfal("ls", _url(sftp_server, "/"))

        assert rc == 0, err
        assert name in out

    def test_ls_long_format(self, sftp_server):
        """gfal ls -l shows size and permissions."""
        name = f"lsl_{_uid()}.bin"
        (sftp_server["data_dir"] / name).write_bytes(b"x" * 42)

        rc, out, err = run_gfal("ls", "-l", _url(sftp_server, "/"))

        assert rc == 0, err
        assert "42" in out

    def test_ls_nonexistent(self, sftp_server):
        """gfal ls exits non-zero for a missing path."""
        rc, _out, _err = run_gfal("ls", _url(sftp_server, f"/no_such_{_uid()}"))
        assert rc != 0


# ---------------------------------------------------------------------------
# gfal cat  (sftp://)
# ---------------------------------------------------------------------------


class TestSFTPCat:
    def test_cat_file(self, sftp_server):
        """gfal cat prints the file content."""
        name = f"cat_{_uid()}.txt"
        (sftp_server["data_dir"] / name).write_text("hello from sftp")

        rc, out, err = run_gfal("cat", _url(sftp_server, f"/{name}"))

        assert rc == 0, err
        assert "hello from sftp" in out

    def test_cat_nonexistent(self, sftp_server):
        """gfal cat exits non-zero for a missing file."""
        rc, _out, _err = run_gfal("cat", _url(sftp_server, f"/no_such_{_uid()}.txt"))
        assert rc != 0


# ---------------------------------------------------------------------------
# gfal cp  (sftp://)
# ---------------------------------------------------------------------------


class TestSFTPCopy:
    def test_copy_local_to_sftp(self, sftp_server, tmp_path):
        """Copy a local file to the SFTP server."""
        src = tmp_path / "src.txt"
        src.write_bytes(b"local to sftp")
        dst_name = f"cp_{_uid()}.txt"

        rc, out, err = run_gfal("cp", str(src), _url(sftp_server, f"/{dst_name}"))

        assert rc == 0, err
        assert (sftp_server["data_dir"] / dst_name).read_bytes() == b"local to sftp"

    def test_copy_sftp_to_local(self, sftp_server, tmp_path):
        """Copy a file from the SFTP server to a local path."""
        name = f"dl_{_uid()}.txt"
        (sftp_server["data_dir"] / name).write_bytes(b"sftp to local")
        dst = tmp_path / "dst.txt"

        rc, out, err = run_gfal("cp", _url(sftp_server, f"/{name}"), str(dst))

        assert rc == 0, err
        assert dst.read_bytes() == b"sftp to local"

    def test_copy_sftp_to_sftp(self, sftp_server):
        """Copy a file within the SFTP server."""
        src_name = f"src_{_uid()}.txt"
        dst_name = f"dst_{_uid()}.txt"
        (sftp_server["data_dir"] / src_name).write_bytes(b"sftp to sftp")

        rc, out, err = run_gfal(
            "cp",
            _url(sftp_server, f"/{src_name}"),
            _url(sftp_server, f"/{dst_name}"),
        )

        assert rc == 0, err
        assert (sftp_server["data_dir"] / dst_name).read_bytes() == b"sftp to sftp"

    def test_copy_overwrites_with_force(self, sftp_server, tmp_path):
        """gfal cp -f overwrites an existing destination."""
        src = tmp_path / "new.txt"
        src.write_bytes(b"new content")
        dst_name = f"overwrite_{_uid()}.txt"
        (sftp_server["data_dir"] / dst_name).write_bytes(b"old content")

        rc, out, err = run_gfal("cp", "-f", str(src), _url(sftp_server, f"/{dst_name}"))

        assert rc == 0, err
        assert (sftp_server["data_dir"] / dst_name).read_bytes() == b"new content"


# ---------------------------------------------------------------------------
# gfal rm  (sftp://)
# ---------------------------------------------------------------------------


class TestSFTPRm:
    def test_rm_file(self, sftp_server):
        """gfal rm removes a file."""
        name = f"rm_{_uid()}.txt"
        (sftp_server["data_dir"] / name).write_text("bye")

        rc, _out, err = run_gfal("rm", _url(sftp_server, f"/{name}"))

        assert rc == 0, err
        assert not (sftp_server["data_dir"] / name).exists()

    def test_rm_nonexistent(self, sftp_server):
        """gfal rm exits non-zero for a missing file."""
        rc, _out, _err = run_gfal("rm", _url(sftp_server, f"/no_such_{_uid()}.txt"))
        assert rc != 0


# ---------------------------------------------------------------------------
# gfal mkdir  (sftp://)
# ---------------------------------------------------------------------------


class TestSFTPMkdir:
    def test_mkdir(self, sftp_server):
        """gfal mkdir creates a directory."""
        name = f"newdir_{_uid()}"

        rc, _out, err = run_gfal("mkdir", _url(sftp_server, f"/{name}"))

        assert rc == 0, err
        assert (sftp_server["data_dir"] / name).is_dir()


# ---------------------------------------------------------------------------
# gfal sum  (sftp://)
# ---------------------------------------------------------------------------


class TestSFTPSum:
    def test_sum_adler32(self, sftp_server):
        """gfal sum returns a hex ADLER32 checksum."""
        name = f"sum_{_uid()}.txt"
        (sftp_server["data_dir"] / name).write_bytes(b"checksum me")

        rc, out, err = run_gfal("sum", _url(sftp_server, f"/{name}"), "ADLER32")

        assert rc == 0, err
        # Output format: "<url>  <checksum>"
        parts = out.strip().split()
        assert len(parts) >= 1
        # Verify the checksum is a valid hex string
        checksum = parts[-1]
        assert all(c in "0123456789abcdef" for c in checksum.lower()), checksum
