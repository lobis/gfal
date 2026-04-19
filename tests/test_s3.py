"""
Tests for gfal CLI commands over S3-compatible storage (s3://).

The ``s3_server`` session fixture (defined in conftest.py) starts an
in-process moto S3-compatible server.  Tests are skipped automatically
when ``moto`` or ``s3fs`` are not installed.

The fixture exposes ``AWS_ENDPOINT_URL`` via the environment so that the
gfal subprocesses automatically pick up the custom endpoint.

Run these tests with::

    pytest tests/test_s3.py -v

or as part of the full suite (they auto-skip when dependencies are missing).
"""

import uuid

import pytest

from helpers import run_gfal

boto3 = pytest.importorskip("boto3", reason="boto3 not installed")

pytestmark = [
    pytest.mark.s3,
    pytest.mark.xdist_group("s3"),
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _uid() -> str:
    """Return a short unique hex string suitable for key names."""
    return uuid.uuid4().hex[:12]


def _s3_env(s3_server: dict) -> dict:
    """Return the extra env vars needed so gfal subprocesses reach the mock S3."""
    return {
        "AWS_ENDPOINT_URL": s3_server["endpoint_url"],
        "AWS_ACCESS_KEY_ID": s3_server["key"],
        "AWS_SECRET_ACCESS_KEY": s3_server["secret"],
        "AWS_DEFAULT_REGION": s3_server["region"],
    }


def _s3_client(s3_server: dict):
    """Return a boto3 S3 client pre-configured for the test server."""
    return boto3.client(
        "s3",
        endpoint_url=s3_server["endpoint_url"],
        region_name=s3_server["region"],
    )


def _put(s3_server: dict, key: str, body: bytes) -> None:
    """Upload *body* to *key* in the test bucket."""
    _s3_client(s3_server).put_object(Bucket=s3_server["bucket"], Key=key, Body=body)


def _get(s3_server: dict, key: str) -> bytes:
    """Download and return the content of *key* from the test bucket."""
    return (
        _s3_client(s3_server)
        .get_object(Bucket=s3_server["bucket"], Key=key)["Body"]
        .read()
    )


# ---------------------------------------------------------------------------
# gfal stat  (s3://)
# ---------------------------------------------------------------------------


class TestS3Stat:
    def test_stat_file(self, s3_server):
        """gfal stat returns size and file-type for an existing S3 object."""
        key = f"stat_{_uid()}.txt"
        _put(s3_server, key, b"stat me")

        rc, out, err = run_gfal(
            "stat",
            f"{s3_server['base_url']}/{key}",
            env=_s3_env(s3_server),
        )

        assert rc == 0, err
        assert "Size:" in out
        assert "7" in out  # len("stat me")

    def test_stat_nonexistent(self, s3_server):
        """gfal stat exits non-zero for a missing object."""
        rc, _out, _err = run_gfal(
            "stat",
            f"{s3_server['base_url']}/no_such_{_uid()}",
            env=_s3_env(s3_server),
        )
        assert rc != 0


# ---------------------------------------------------------------------------
# gfal ls  (s3://)
# ---------------------------------------------------------------------------


class TestS3Ls:
    def test_ls_bucket(self, s3_server):
        """gfal ls lists objects in the bucket root."""
        key = f"ls_{_uid()}.txt"
        _put(s3_server, key, b"ls test")

        rc, out, err = run_gfal(
            "ls",
            s3_server["base_url"] + "/",
            env=_s3_env(s3_server),
        )

        assert rc == 0, err
        assert key in out

    def test_ls_prefix(self, s3_server):
        """gfal ls with a prefix-style 'directory' lists matching keys."""
        prefix = f"pfx_{_uid()}"
        key1 = f"{prefix}/file1.txt"
        key2 = f"{prefix}/file2.txt"
        _put(s3_server, key1, b"a")
        _put(s3_server, key2, b"b")

        rc, out, err = run_gfal(
            "ls",
            f"{s3_server['base_url']}/{prefix}/",
            env=_s3_env(s3_server),
        )

        assert rc == 0, err
        assert "file1.txt" in out
        assert "file2.txt" in out

    def test_ls_single_file(self, s3_server):
        """gfal ls on a single S3 object prints that object, like Unix ls."""
        key = f"single_{_uid()}.txt"
        _put(s3_server, key, b"single file")

        rc, out, err = run_gfal(
            "ls",
            f"{s3_server['base_url']}/{key}",
            env=_s3_env(s3_server),
        )

        assert rc == 0, err
        assert key in out


# ---------------------------------------------------------------------------
# gfal cat  (s3://)
# ---------------------------------------------------------------------------


class TestS3Cat:
    def test_cat_file(self, s3_server):
        """gfal cat prints an S3 object's content."""
        key = f"cat_{_uid()}.txt"
        _put(s3_server, key, b"hello from s3")

        rc, out, err = run_gfal(
            "cat",
            f"{s3_server['base_url']}/{key}",
            env=_s3_env(s3_server),
        )

        assert rc == 0, err
        assert "hello from s3" in out

    def test_cat_nonexistent(self, s3_server):
        """gfal cat exits non-zero for a missing object."""
        rc, _out, _err = run_gfal(
            "cat",
            f"{s3_server['base_url']}/no_such_{_uid()}.txt",
            env=_s3_env(s3_server),
        )
        assert rc != 0


# ---------------------------------------------------------------------------
# gfal cp  (s3://)
# ---------------------------------------------------------------------------


class TestS3Copy:
    def test_copy_local_to_s3(self, s3_server, tmp_path):
        """Copy a local file to S3."""
        src = tmp_path / "src.txt"
        src.write_bytes(b"local to s3")
        key = f"upload_{_uid()}.txt"

        rc, _out, err = run_gfal(
            "cp",
            str(src),
            f"{s3_server['base_url']}/{key}",
            env=_s3_env(s3_server),
        )

        assert rc == 0, err
        assert _get(s3_server, key) == b"local to s3"

    def test_copy_s3_to_local(self, s3_server, tmp_path):
        """Copy a file from S3 to a local path."""
        key = f"dl_{_uid()}.txt"
        _put(s3_server, key, b"s3 to local")
        dst = tmp_path / "dst.txt"

        rc, _out, err = run_gfal(
            "cp",
            f"{s3_server['base_url']}/{key}",
            str(dst),
            env=_s3_env(s3_server),
        )

        assert rc == 0, err
        assert dst.read_bytes() == b"s3 to local"

    def test_copy_s3_to_s3(self, s3_server):
        """Copy an object within S3 (server-side streaming copy)."""
        src_key = f"src_{_uid()}.txt"
        dst_key = f"dst_{_uid()}.txt"
        _put(s3_server, src_key, b"s3 to s3")

        rc, _out, err = run_gfal(
            "cp",
            f"{s3_server['base_url']}/{src_key}",
            f"{s3_server['base_url']}/{dst_key}",
            env=_s3_env(s3_server),
        )

        assert rc == 0, err
        assert _get(s3_server, dst_key) == b"s3 to s3"

    def test_copy_with_checksum(self, s3_server, tmp_path):
        """gfal cp -K ADLER32 verifies the checksum after uploading."""

        payload = b"checksum verify content"
        src = tmp_path / "cksum.txt"
        src.write_bytes(payload)
        key = f"cksum_{_uid()}.txt"

        rc, _out, err = run_gfal(
            "cp",
            "-K",
            "ADLER32",
            str(src),
            f"{s3_server['base_url']}/{key}",
            env=_s3_env(s3_server),
        )

        assert rc == 0, err
        assert _get(s3_server, key) == payload


# ---------------------------------------------------------------------------
# gfal rm  (s3://)
# ---------------------------------------------------------------------------


class TestS3Rm:
    def test_rm_object(self, s3_server):
        """gfal rm deletes an S3 object."""
        key = f"rm_{_uid()}.txt"
        _put(s3_server, key, b"delete me")

        rc, _out, err = run_gfal(
            "rm",
            f"{s3_server['base_url']}/{key}",
            env=_s3_env(s3_server),
        )

        assert rc == 0, err
        # Verify the object is gone
        s3 = _s3_client(s3_server)
        resp = s3.list_objects_v2(Bucket=s3_server["bucket"], Prefix=key)
        assert len(resp.get("Contents", [])) == 0


# ---------------------------------------------------------------------------
# gfal sum  (s3://)
# ---------------------------------------------------------------------------


class TestS3Sum:
    def test_sum_md5(self, s3_server):
        """gfal sum computes the correct MD5 for an S3 object."""
        import hashlib

        payload = b"checksum test content"
        key = f"sum_{_uid()}.txt"
        _put(s3_server, key, payload)
        expected = hashlib.md5(payload).hexdigest()

        rc, out, err = run_gfal(
            "sum",
            f"{s3_server['base_url']}/{key}",
            "MD5",
            env=_s3_env(s3_server),
        )

        assert rc == 0, err
        assert expected in out

    def test_sum_adler32(self, s3_server):
        """gfal sum computes the correct ADLER32 for an S3 object."""
        import zlib

        payload = b"adler32 test"
        key = f"sum_adler_{_uid()}.txt"
        _put(s3_server, key, payload)
        expected = format(zlib.adler32(payload) & 0xFFFFFFFF, "08x")

        rc, out, err = run_gfal(
            "sum",
            f"{s3_server['base_url']}/{key}",
            "ADLER32",
            env=_s3_env(s3_server),
        )

        assert rc == 0, err
        assert expected in out
