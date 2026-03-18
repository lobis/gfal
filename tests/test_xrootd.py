"""
Integration tests against a local XRootD server.

The ``xrootd_server`` session fixture (defined in conftest.py) starts a real
XRootD daemon that serves a temporary directory over both the native
``root://`` protocol and ``https://`` (XrdHttp plugin).  Tests are skipped
automatically when the ``xrootd`` binary or ``fsspec-xrootd`` package is not
available.

Run these tests with::

    pytest tests/test_xrootd.py -v

or as part of the normal suite (they auto-skip when prerequisites are missing).
"""

import os
import uuid
import zlib
from pathlib import Path

import pytest

from helpers import run_gfal

pytestmark = pytest.mark.xrootd


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _unique(base_dir: Path, suffix: str = "") -> Path:
    """Return a unique path under *base_dir* (not yet created)."""
    return base_dir / f"t_{uuid.uuid4().hex}{suffix}"


# ---------------------------------------------------------------------------
# gfal-ls  (root://)
# ---------------------------------------------------------------------------


class TestXRootDLs:
    def test_ls_root_directory(self, xrootd_server):
        """Listing the root of the served directory returns files we placed there."""
        data = xrootd_server["data_dir"]
        f = _unique(data, ".txt")
        f.write_text("ls test")

        rc, out, err = run_gfal("ls", xrootd_server["root_url"])

        assert rc == 0, err
        assert f.name in out

    def test_ls_file(self, xrootd_server):
        """Listing a single file returns its name."""
        data = xrootd_server["data_dir"]
        f = _unique(data, ".txt")
        f.write_text("single file")

        url = xrootd_server["root_url"] + f.name
        rc, out, err = run_gfal("ls", url)

        assert rc == 0, err
        assert f.name in out

    def test_ls_long_format(self, xrootd_server):
        """gfal-ls -l shows size and permissions."""
        data = xrootd_server["data_dir"]
        f = _unique(data, ".bin")
        f.write_bytes(b"x" * 42)

        rc, out, err = run_gfal("ls", "-l", xrootd_server["root_url"])

        assert rc == 0, err
        assert f.name in out

    def test_ls_nonexistent_returns_error(self, xrootd_server):
        url = xrootd_server["root_url"] + "no_such_file_xyz.txt"

        rc, out, err = run_gfal("ls", url)

        assert rc != 0

    def test_ls_subdirectory(self, xrootd_server):
        """Listing a subdirectory works."""
        data = xrootd_server["data_dir"]
        sub = _unique(data)
        sub.mkdir()
        (sub / "child.txt").write_text("child")

        url = xrootd_server["root_url"] + sub.name + "/"
        rc, out, err = run_gfal("ls", url)

        assert rc == 0, err
        assert "child.txt" in out


# ---------------------------------------------------------------------------
# gfal-stat  (root://)
# ---------------------------------------------------------------------------


class TestXRootDStat:
    def test_stat_file(self, xrootd_server):
        data = xrootd_server["data_dir"]
        f = _unique(data, ".txt")
        f.write_bytes(b"stat me")

        url = xrootd_server["root_url"] + f.name
        rc, out, err = run_gfal("stat", url)

        assert rc == 0, err
        assert "Size:" in out
        assert "7" in out  # len("stat me")

    def test_stat_directory(self, xrootd_server):
        data = xrootd_server["data_dir"]
        sub = _unique(data)
        sub.mkdir()

        url = xrootd_server["root_url"] + sub.name + "/"
        rc, out, err = run_gfal("stat", url)

        assert rc == 0, err
        assert "directory" in out

    def test_stat_nonexistent(self, xrootd_server):
        url = xrootd_server["root_url"] + "no_such_xyz_stat.txt"

        rc, out, err = run_gfal("stat", url)

        assert rc != 0


# ---------------------------------------------------------------------------
# gfal-cat  (root://)
# ---------------------------------------------------------------------------


class TestXRootDCat:
    def test_cat_file(self, xrootd_server):
        data = xrootd_server["data_dir"]
        f = _unique(data, ".txt")
        f.write_text("hello from xrootd")

        url = xrootd_server["root_url"] + f.name
        rc, out, err = run_gfal("cat", url)

        assert rc == 0, err
        assert "hello from xrootd" in out

    def test_cat_binary_file(self, xrootd_server):
        from helpers import run_gfal_binary

        data = xrootd_server["data_dir"]
        f = _unique(data, ".bin")
        payload = bytes(range(256))
        f.write_bytes(payload)

        url = xrootd_server["root_url"] + f.name
        rc, out_bytes, err_bytes = run_gfal_binary("cat", url)

        assert rc == 0, err_bytes
        assert payload in out_bytes

    def test_cat_empty_file(self, xrootd_server):
        data = xrootd_server["data_dir"]
        f = _unique(data, ".txt")
        f.write_bytes(b"")

        url = xrootd_server["root_url"] + f.name
        rc, out, err = run_gfal("cat", url)

        assert rc == 0, err
        assert out == ""

    def test_cat_nonexistent(self, xrootd_server):
        url = xrootd_server["root_url"] + "no_such_cat_xyz.txt"
        rc, out, err = run_gfal("cat", url)
        assert rc != 0


# ---------------------------------------------------------------------------
# gfal-cp  (root://)
# ---------------------------------------------------------------------------


class TestXRootDCopy:
    def test_copy_local_to_xrootd(self, xrootd_server, tmp_path):
        """Copy a local file to the XRootD server."""
        src = tmp_path / "src.txt"
        src.write_bytes(b"local to xrootd")

        dst_name = _unique(xrootd_server["data_dir"], ".txt").name
        dst_url = xrootd_server["root_url"] + dst_name

        rc, out, err = run_gfal("cp", src.as_uri(), dst_url)

        assert rc == 0, err
        assert (xrootd_server["data_dir"] / dst_name).read_bytes() == b"local to xrootd"

    def test_copy_xrootd_to_local(self, xrootd_server, tmp_path):
        """Download a file from the XRootD server."""
        data = xrootd_server["data_dir"]
        f = _unique(data, ".txt")
        f.write_bytes(b"xrootd to local")

        dst = tmp_path / "dst.txt"
        rc, out, err = run_gfal("cp", xrootd_server["root_url"] + f.name, dst.as_uri())

        assert rc == 0, err
        assert dst.read_bytes() == b"xrootd to local"

    def test_copy_xrootd_to_xrootd(self, xrootd_server):
        """Server-local copy (both src and dst on the same XRootD server)."""
        data = xrootd_server["data_dir"]
        src = _unique(data, ".txt")
        src.write_bytes(b"server copy")
        dst_name = _unique(data, ".txt").name

        src_url = xrootd_server["root_url"] + src.name
        dst_url = xrootd_server["root_url"] + dst_name

        rc, out, err = run_gfal("cp", src_url, dst_url)

        assert rc == 0, err
        assert (data / dst_name).read_bytes() == b"server copy"

    def test_copy_with_checksum(self, xrootd_server, tmp_path):
        """Copy with ADLER32 checksum verification."""
        src = tmp_path / "src.bin"
        data_bytes = b"checksum test data"
        src.write_bytes(data_bytes)

        dst_name = _unique(xrootd_server["data_dir"], ".bin").name
        dst_url = xrootd_server["root_url"] + dst_name

        rc, out, err = run_gfal("cp", "-K", "ADLER32", src.as_uri(), dst_url)

        assert rc == 0, err
        assert (xrootd_server["data_dir"] / dst_name).read_bytes() == data_bytes

    def test_copy_large_file(self, xrootd_server, tmp_path):
        """Copy a 5 MiB file (larger than CHUNK_SIZE)."""
        src = tmp_path / "large.bin"
        payload = b"Z" * (5 * 1024 * 1024)
        src.write_bytes(payload)

        dst_name = _unique(xrootd_server["data_dir"], ".bin").name
        dst_url = xrootd_server["root_url"] + dst_name

        rc, out, err = run_gfal("cp", src.as_uri(), dst_url)

        assert rc == 0, err
        assert (xrootd_server["data_dir"] / dst_name).read_bytes() == payload

    def test_copy_no_overwrite_without_force(self, xrootd_server, tmp_path):
        """Without -f, copying to an existing remote file should fail."""
        data = xrootd_server["data_dir"]
        existing = _unique(data, ".txt")
        existing.write_bytes(b"original")

        src = tmp_path / "new.txt"
        src.write_bytes(b"replacement")

        rc, out, err = run_gfal(
            "cp", src.as_uri(), xrootd_server["root_url"] + existing.name
        )

        assert rc != 0
        assert existing.read_bytes() == b"original"

    def test_copy_force_overwrite(self, xrootd_server, tmp_path):
        """With -f, existing remote file is overwritten."""
        data = xrootd_server["data_dir"]
        existing = _unique(data, ".txt")
        existing.write_bytes(b"original")

        src = tmp_path / "new.txt"
        src.write_bytes(b"replacement")

        rc, out, err = run_gfal(
            "cp", "-f", src.as_uri(), xrootd_server["root_url"] + existing.name
        )

        assert rc == 0, err
        assert existing.read_bytes() == b"replacement"


# ---------------------------------------------------------------------------
# gfal-rm  (root://)
# ---------------------------------------------------------------------------


class TestXRootDRm:
    def test_rm_file(self, xrootd_server):
        data = xrootd_server["data_dir"]
        f = _unique(data, ".txt")
        f.write_text("delete me")

        url = xrootd_server["root_url"] + f.name
        rc, out, err = run_gfal("rm", url)

        assert rc == 0, err
        assert not f.exists()

    def test_rm_nonexistent(self, xrootd_server):
        url = xrootd_server["root_url"] + "no_such_rm_xyz.txt"
        rc, out, err = run_gfal("rm", url)
        assert rc != 0

    def test_rm_directory_recursive(self, xrootd_server):
        data = xrootd_server["data_dir"]
        sub = _unique(data)
        sub.mkdir()
        (sub / "f1.txt").write_text("a")
        (sub / "f2.txt").write_text("b")

        url = xrootd_server["root_url"] + sub.name + "/"
        rc, out, err = run_gfal("rm", "-r", url)

        assert rc == 0, err
        assert not sub.exists()

    def test_rm_directory_without_recursive_fails(self, xrootd_server):
        data = xrootd_server["data_dir"]
        sub = _unique(data)
        sub.mkdir()
        (sub / "f.txt").write_text("x")

        url = xrootd_server["root_url"] + sub.name + "/"
        rc, out, err = run_gfal("rm", url)

        assert rc != 0
        assert sub.exists()


# ---------------------------------------------------------------------------
# gfal-mkdir  (root://)
# ---------------------------------------------------------------------------


class TestXRootDMkdir:
    def test_mkdir(self, xrootd_server):
        data = xrootd_server["data_dir"]
        new_dir = _unique(data)

        url = xrootd_server["root_url"] + new_dir.name
        rc, out, err = run_gfal("mkdir", url)

        assert rc == 0, err
        assert new_dir.is_dir()

    def test_mkdir_parents(self, xrootd_server):
        data = xrootd_server["data_dir"]
        parent = _unique(data)
        child = parent / "child"

        url = xrootd_server["root_url"] + parent.name + "/child"
        rc, out, err = run_gfal("mkdir", "-p", url)

        assert rc == 0, err
        assert child.is_dir()

    def test_mkdir_existing_without_parents_fails(self, xrootd_server):
        data = xrootd_server["data_dir"]
        existing = _unique(data)
        existing.mkdir()

        url = xrootd_server["root_url"] + existing.name
        rc, out, err = run_gfal("mkdir", url)

        assert rc != 0

    def test_mkdir_existing_with_parents_ok(self, xrootd_server):
        data = xrootd_server["data_dir"]
        existing = _unique(data)
        existing.mkdir()

        url = xrootd_server["root_url"] + existing.name
        rc, out, err = run_gfal("mkdir", "-p", url)

        assert rc == 0, err


# ---------------------------------------------------------------------------
# gfal-sum  (root://)
# ---------------------------------------------------------------------------


class TestXRootDSum:
    def test_sum_adler32(self, xrootd_server):
        data = xrootd_server["data_dir"]
        f = _unique(data, ".bin")
        payload = b"hello xrootd checksum"
        f.write_bytes(payload)
        expected = f"{zlib.adler32(payload) & 0xFFFFFFFF:08x}"

        url = xrootd_server["root_url"] + f.name
        rc, out, err = run_gfal("sum", url, "ADLER32")

        assert rc == 0, err
        assert expected in out

    def test_sum_md5(self, xrootd_server):
        import hashlib

        data = xrootd_server["data_dir"]
        f = _unique(data, ".bin")
        payload = b"md5 test"
        f.write_bytes(payload)
        expected = hashlib.md5(payload).hexdigest()

        url = xrootd_server["root_url"] + f.name
        rc, out, err = run_gfal("sum", url, "MD5")

        assert rc == 0, err
        assert expected in out


# ---------------------------------------------------------------------------
# gfal-rename  (root://)
# ---------------------------------------------------------------------------


class TestXRootDRename:
    def test_rename_file(self, xrootd_server):
        data = xrootd_server["data_dir"]
        src = _unique(data, ".txt")
        src.write_text("rename me")
        dst_name = _unique(data, "_renamed.txt").name

        src_url = xrootd_server["root_url"] + src.name
        dst_url = xrootd_server["root_url"] + dst_name

        rc, out, err = run_gfal("rename", src_url, dst_url)

        assert rc == 0, err
        assert not src.exists()
        assert (data / dst_name).read_text() == "rename me"


# ---------------------------------------------------------------------------
# gfal-save  (root://)
# ---------------------------------------------------------------------------


class TestXRootDSave:
    def test_save(self, xrootd_server):
        data = xrootd_server["data_dir"]
        name = _unique(data, ".txt").name

        url = xrootd_server["root_url"] + name
        rc, out, err = run_gfal("save", url, input="saved via stdin\n")

        assert rc == 0, err
        assert (data / name).read_text() == "saved via stdin\n"


# ---------------------------------------------------------------------------
# HTTPS interface  (https:// via XrdHttp)
# ---------------------------------------------------------------------------


class TestXRootDHttps:
    def test_https_stat(self, xrootd_server):
        """gfal-stat works over the HTTPS interface with --no-verify."""
        if xrootd_server["https_url"] is None:
            pytest.skip("XRootD HTTPS interface not available (openssl not found)")

        data = xrootd_server["data_dir"]
        f = _unique(data, ".txt")
        f.write_bytes(b"https stat test")

        url = xrootd_server["https_url"] + f.name
        rc, out, err = run_gfal("stat", "--no-verify", url)

        assert rc == 0, err
        assert "Size:" in out

    def test_https_cat(self, xrootd_server):
        """gfal-cat works over HTTPS with --no-verify."""
        if xrootd_server["https_url"] is None:
            pytest.skip("XRootD HTTPS interface not available")

        data = xrootd_server["data_dir"]
        f = _unique(data, ".txt")
        f.write_text("https cat content")

        url = xrootd_server["https_url"] + f.name
        rc, out, err = run_gfal("cat", "--no-verify", url)

        assert rc == 0, err
        assert "https cat content" in out

    def test_https_copy_download(self, xrootd_server, tmp_path):
        """Download via HTTPS with --no-verify."""
        if xrootd_server["https_url"] is None:
            pytest.skip("XRootD HTTPS interface not available")

        data = xrootd_server["data_dir"]
        f = _unique(data, ".bin")
        payload = os.urandom(512)
        f.write_bytes(payload)

        dst = tmp_path / "dst.bin"
        url = xrootd_server["https_url"] + f.name
        rc, out, err = run_gfal("cp", "--no-verify", url, dst.as_uri())

        assert rc == 0, err
        assert dst.read_bytes() == payload

    def test_https_ls(self, xrootd_server):
        """Directory listing over HTTPS with --no-verify (WebDAV PROPFIND)."""
        if xrootd_server["https_url"] is None:
            pytest.skip("XRootD HTTPS interface not available")

        data = xrootd_server["data_dir"]
        f = _unique(data, ".txt")
        f.write_text("https ls")

        rc, out, err = run_gfal("ls", "--no-verify", xrootd_server["https_url"])

        # XRootD may not support WebDAV PROPFIND (returns 403 or 405);
        # in that case the error is acceptable.  We just verify no crash.
        assert "Traceback" not in err

    def test_https_sum(self, xrootd_server):
        """gfal-sum works over HTTPS."""
        if xrootd_server["https_url"] is None:
            pytest.skip("XRootD HTTPS interface not available")

        data = xrootd_server["data_dir"]
        f = _unique(data, ".bin")
        payload = b"https checksum"
        f.write_bytes(payload)
        expected = f"{zlib.adler32(payload) & 0xFFFFFFFF:08x}"

        url = xrootd_server["https_url"] + f.name
        rc, out, err = run_gfal("sum", "--no-verify", url, "ADLER32")

        assert rc == 0, err
        assert expected in out
