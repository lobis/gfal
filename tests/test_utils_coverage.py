"""Additional unit tests for utils.py to increase code coverage."""

from __future__ import annotations

import stat

from gfal.core.utils import (
    file_mode_str,
    file_type_str,
    human_readable_size,
    human_readable_time,
)


class TestHumanReadableSize:
    def test_zero_bytes(self):
        assert human_readable_size(0) == "0.00 B"

    def test_negative(self):
        assert human_readable_size(-1) == "0 B"

    def test_bytes(self):
        assert human_readable_size(512) == "512.00 B"

    def test_kilobytes(self):
        result = human_readable_size(2048)
        assert "kB" in result

    def test_megabytes(self):
        result = human_readable_size(5 * 1024 * 1024)
        assert "MB" in result

    def test_gigabytes(self):
        result = human_readable_size(2 * 1024 * 1024 * 1024)
        assert "GB" in result

    def test_terabytes(self):
        result = human_readable_size(3 * 1024**4)
        assert "TB" in result


class TestHumanReadableTime:
    def test_valid_timestamp(self):
        result = human_readable_time(1700000000)
        assert "2023" in result
        assert "UTC" in result

    def test_invalid_timestamp(self):
        result = human_readable_time("not a timestamp")
        assert result == "not a timestamp"


class TestFileTypeStr:
    def test_regular_file(self):
        assert file_type_str(stat.S_IFREG) == "regular file"

    def test_directory(self):
        assert file_type_str(stat.S_IFDIR) == "directory"

    def test_symbolic_link(self):
        assert file_type_str(stat.S_IFLNK) == "symbolic link"

    def test_unknown(self):
        assert file_type_str(0) == "unknown"


class TestFileModeStr:
    def test_regular_file(self):
        result = file_mode_str(stat.S_IFREG | 0o644)
        assert result.startswith("-")
        assert "rw-" in result

    def test_directory(self):
        result = file_mode_str(stat.S_IFDIR | 0o755)
        assert result.startswith("d")

    def test_block_device(self):
        result = file_mode_str(stat.S_IFBLK | 0o660)
        assert result.startswith("b")

    def test_char_device(self):
        result = file_mode_str(stat.S_IFCHR | 0o666)
        assert result.startswith("c")

    def test_fifo(self):
        result = file_mode_str(stat.S_IFIFO | 0o644)
        assert result.startswith("f")

    def test_socket(self):
        result = file_mode_str(stat.S_IFSOCK | 0o755)
        assert result.startswith("s")
