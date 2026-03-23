"""Unit tests for gfal_cli.utils helper functions."""

import stat

import pytest

from gfal.core.utils import file_mode_str, file_type_str

# ---------------------------------------------------------------------------
# file_type_str
# ---------------------------------------------------------------------------


class TestFileTypeStr:
    def test_regular_file(self):
        assert file_type_str(stat.S_IFREG) == "regular file"

    def test_directory(self):
        assert file_type_str(stat.S_IFDIR) == "directory"

    def test_symlink(self):
        assert file_type_str(stat.S_IFLNK) == "symbolic link"

    def test_block_device(self):
        assert file_type_str(stat.S_IFBLK) == "block device"

    def test_char_device(self):
        assert file_type_str(stat.S_IFCHR) == "character device"

    def test_fifo(self):
        assert file_type_str(stat.S_IFIFO) == "fifo"

    def test_socket(self):
        assert file_type_str(stat.S_IFSOCK) == "socket"

    def test_unknown(self):
        assert file_type_str(0) == "unknown"

    def test_with_permission_bits_stripped(self):
        """S_IFMT should isolate the type even when permission bits are set."""
        assert file_type_str(stat.S_IFMT(stat.S_IFREG | 0o755)) == "regular file"


# ---------------------------------------------------------------------------
# file_mode_str
# ---------------------------------------------------------------------------


class TestFileModeStr:
    # Standard permission combos
    def test_regular_644(self):
        assert file_mode_str(stat.S_IFREG | 0o644) == "-rw-r--r--"

    def test_regular_755(self):
        assert file_mode_str(stat.S_IFREG | 0o755) == "-rwxr-xr-x"

    def test_regular_600(self):
        assert file_mode_str(stat.S_IFREG | 0o600) == "-rw-------"

    def test_regular_000(self):
        assert file_mode_str(stat.S_IFREG | 0o000) == "----------"

    def test_regular_777(self):
        assert file_mode_str(stat.S_IFREG | 0o777) == "-rwxrwxrwx"

    def test_regular_444(self):
        assert file_mode_str(stat.S_IFREG | 0o444) == "-r--r--r--"

    def test_regular_222(self):
        assert file_mode_str(stat.S_IFREG | 0o222) == "--w--w--w-"

    def test_regular_111(self):
        assert file_mode_str(stat.S_IFREG | 0o111) == "---x--x--x"

    # Directory permissions
    def test_directory_755(self):
        assert file_mode_str(stat.S_IFDIR | 0o755) == "drwxr-xr-x"

    def test_directory_700(self):
        assert file_mode_str(stat.S_IFDIR | 0o700) == "drwx------"

    def test_directory_777(self):
        assert file_mode_str(stat.S_IFDIR | 0o777) == "drwxrwxrwx"

    # Type prefixes
    def test_prefix_block_device(self):
        assert file_mode_str(stat.S_IFBLK | 0o660).startswith("b")

    def test_prefix_char_device(self):
        assert file_mode_str(stat.S_IFCHR | 0o660).startswith("c")

    def test_prefix_fifo(self):
        assert file_mode_str(stat.S_IFIFO | 0o660).startswith("f")

    def test_prefix_socket(self):
        assert file_mode_str(stat.S_IFSOCK | 0o660).startswith("s")

    def test_prefix_symlink_fallback(self):
        """Symlinks aren't handled specially, should get '-' prefix."""
        result = file_mode_str(stat.S_IFLNK | 0o777)
        assert result.startswith("-") or result.startswith("l")

    # General properties
    def test_length_always_ten(self):
        for mode in (0o644, 0o755, 0o700, 0o777, 0o000, 0o444, 0o222, 0o111):
            assert len(file_mode_str(stat.S_IFREG | mode)) == 10

    @pytest.mark.parametrize(
        "mode",
        [
            stat.S_IFREG | 0o644,
            stat.S_IFDIR | 0o755,
            stat.S_IFBLK | 0o660,
            stat.S_IFCHR | 0o660,
            stat.S_IFIFO | 0o644,
            stat.S_IFSOCK | 0o755,
        ],
    )
    def test_output_is_ten_chars(self, mode):
        assert len(file_mode_str(mode)) == 10
