"""Additional direct tests for fs.py: checksum algorithms, ssl helpers."""

import pytest

from gfal.core.fs import compute_checksum, get_ssl_context

# ---------------------------------------------------------------------------
# get_ssl_context
# ---------------------------------------------------------------------------


class TestGetSslContext:
    def test_verified_returns_ssl_context(self):
        ctx = get_ssl_context(verify=True)
        import ssl

        assert isinstance(ctx, ssl.SSLContext)

    def test_no_verify_returns_ssl_context(self):
        ctx = get_ssl_context(verify=False)
        import ssl

        assert isinstance(ctx, ssl.SSLContext)


# ---------------------------------------------------------------------------
# compute_checksum: various algorithms on local files
# ---------------------------------------------------------------------------


class TestComputeChecksumAlgorithms:
    def _get_fso_and_path(self, tmp_path, data):
        import fsspec

        f = tmp_path / "test.bin"
        f.write_bytes(data)
        fso = fsspec.filesystem("file")
        return fso, str(f)

    def test_crc32(self, tmp_path):
        import zlib

        data = b"hello world"
        fso, path = self._get_fso_and_path(tmp_path, data)
        result = compute_checksum(fso, path, "CRC32")
        expected = f"{zlib.crc32(data) & 0xFFFFFFFF:08x}"
        assert result == expected

    def test_crc32c(self, tmp_path):
        data = b"hello world"
        fso, path = self._get_fso_and_path(tmp_path, data)
        result = compute_checksum(fso, path, "CRC32C")
        # Just verify it returns a hex string of length 8
        assert len(result) == 8
        int(result, 16)  # should not raise

    def test_sha1(self, tmp_path):
        import hashlib

        data = b"hello world"
        fso, path = self._get_fso_and_path(tmp_path, data)
        result = compute_checksum(fso, path, "SHA1")
        expected = hashlib.sha1(data).hexdigest()
        assert result == expected

    def test_sha256(self, tmp_path):
        import hashlib

        data = b"hello world"
        fso, path = self._get_fso_and_path(tmp_path, data)
        result = compute_checksum(fso, path, "SHA256")
        expected = hashlib.sha256(data).hexdigest()
        assert result == expected

    def test_unsupported_algorithm_raises(self, tmp_path):
        fso, path = self._get_fso_and_path(tmp_path, b"x")
        with pytest.raises(ValueError, match="unsupported"):
            compute_checksum(fso, path, "INVALID_ALG_XYZ")

    def test_sha256_lowercase_alias(self, tmp_path):
        import hashlib

        data = b"test data"
        fso, path = self._get_fso_and_path(tmp_path, data)
        result = compute_checksum(fso, path, "sha256")
        expected = hashlib.sha256(data).hexdigest()
        assert result == expected

    def test_adler32_large_file(self, tmp_path):
        import zlib

        # Test that chunked reading works correctly for a larger file
        data = b"x" * (5 * 1024 * 1024)  # 5 MiB > CHUNK_SIZE (4 MiB)
        fso, path = self._get_fso_and_path(tmp_path, data)
        result = compute_checksum(fso, path, "ADLER32")
        expected = f"{zlib.adler32(data) & 0xFFFFFFFF:08x}"
        assert result == expected


# ---------------------------------------------------------------------------
# _crc32c_pure (internal function tested via compute_checksum)
# ---------------------------------------------------------------------------


class TestCrc32cPure:
    def test_deterministic(self, tmp_path):
        import fsspec

        data = b"hello world"
        f = tmp_path / "test.bin"
        f.write_bytes(data)
        fso = fsspec.filesystem("file")

        from gfal.core.fs import _crc32c_pure

        result1 = _crc32c_pure(fso, str(f))
        result2 = _crc32c_pure(fso, str(f))
        assert result1 == result2

    def test_different_data_different_result(self, tmp_path):
        import fsspec

        f1 = tmp_path / "a.bin"
        f2 = tmp_path / "b.bin"
        f1.write_bytes(b"aaaaaa")
        f2.write_bytes(b"bbbbbb")
        fso = fsspec.filesystem("file")

        from gfal.core.fs import _crc32c_pure

        r1 = _crc32c_pure(fso, str(f1))
        r2 = _crc32c_pure(fso, str(f2))
        assert r1 != r2
