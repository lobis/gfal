"""Unit tests for the read-only FUSE mount adapter and CLI command."""

import errno
import os
import stat
from pathlib import PurePosixPath
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from gfal.cli.mount import CommandMount
from gfal.core import mount as mount_module
from gfal.core.mount import FuseOSError, ReadOnlyFuseOperations


class _FakeStat:
    def __init__(self, *, mode, size=0, name="", mtime=0.0):
        self.st_mode = mode
        self.st_size = size
        self.st_uid = 0
        self.st_gid = 0
        self.st_nlink = 1
        self.st_mtime = mtime
        self.st_atime = mtime
        self.st_ctime = mtime
        self.info = {"name": name}

    def is_dir(self):
        return stat.S_ISDIR(self.st_mode)

    def is_file(self):
        return stat.S_ISREG(self.st_mode)


def _make_mount_cmd():
    cmd = CommandMount()
    cmd.prog = "gfal mount"
    return cmd


def _default_params(**kwargs):
    defaults = {
        "cert": None,
        "key": None,
        "timeout": 1800,
        "ssl_verify": True,
        "verbose": 0,
        "log_file": None,
        "ipv4_only": False,
        "ipv6_only": False,
    }
    defaults.update(kwargs)
    return SimpleNamespace(**defaults)


class TestReadOnlyFuseOperations:
    def test_ensure_mount_supported_allows_macos(self):
        with (
            patch.object(mount_module.sys, "platform", "darwin"),
            patch.object(mount_module, "FUSE", object()),
        ):
            mount_module.ensure_mount_supported()

    def test_ensure_mount_supported_rejects_unsupported_platform(self):
        with (
            patch.object(mount_module.sys, "platform", "win32"),
            pytest.raises(OSError) as excinfo,
        ):
            mount_module.ensure_mount_supported()

        assert excinfo.value.errno == errno.EOPNOTSUPP

    def test_url_for_path_quotes_segments(self):
        client = MagicMock()
        client.stat.return_value = _FakeStat(mode=stat.S_IFDIR | 0o755, name="/")
        ops = ReadOnlyFuseOperations(
            "https://example.org/base dir/",
            client,
        )

        result = ops._url_for_path("/nested/file name.txt")

        assert result == "https://example.org/base dir/nested/file%20name.txt"

    def test_url_for_root_xrootd_mount_preserves_double_slash(self):
        client = MagicMock()
        client.stat.return_value = _FakeStat(mode=stat.S_IFDIR | 0o755, name="/")
        ops = ReadOnlyFuseOperations("root://example.org//", client)

        result = ops._url_for_path("/child")

        assert result == "root://example.org//child"

    def test_getattr_for_regular_file(self):
        client = MagicMock()
        client.stat.side_effect = [
            _FakeStat(mode=stat.S_IFDIR | 0o755, name="/"),
            _FakeStat(
                mode=stat.S_IFREG | 0o644,
                size=12,
                name="/remote/file.txt",
                mtime=123.0,
            ),
        ]
        ops = ReadOnlyFuseOperations("file:///virtual/source", client)

        attrs = ops.getattr("/file.txt")

        assert attrs["st_size"] == 12
        assert stat.S_ISREG(attrs["st_mode"])
        assert attrs["st_mtime"] == 123.0
        assert attrs["st_ino"] > 0

    def test_readdir_decodes_basenames(self):
        client = MagicMock()
        client.stat.return_value = _FakeStat(mode=stat.S_IFDIR | 0o755, name="/")
        client.ls.return_value = [
            _FakeStat(mode=stat.S_IFDIR | 0o755, name="/remote/source/subdir"),
            _FakeStat(
                mode=stat.S_IFREG | 0o644,
                name="https://example.org/base/file%20name.txt",
            ),
        ]
        ops = ReadOnlyFuseOperations("file:///virtual/source", client)

        entries = list(ops.readdir("/", 0))

        assert entries == [".", "..", "subdir", "file name.txt"]

    def test_open_rejects_write_flags(self):
        client = MagicMock()
        client.stat.return_value = _FakeStat(mode=stat.S_IFDIR | 0o755, name="/")
        ops = ReadOnlyFuseOperations("file:///virtual/source", client)

        with pytest.raises(FuseOSError) as excinfo:
            ops.open("/file.txt", os.O_WRONLY)

        assert excinfo.value.errno == errno.EROFS

    def test_open_read_and_release(self):
        handle = MagicMock()
        handle.read.return_value = b"hello"
        client = MagicMock()
        client.stat.side_effect = [
            _FakeStat(mode=stat.S_IFDIR | 0o755, name="/"),
            _FakeStat(mode=stat.S_IFREG | 0o644, name="/remote/source/file.txt"),
        ]
        client.open.return_value = handle
        ops = ReadOnlyFuseOperations("file:///virtual/source", client)

        fh = ops.open("/file.txt", 0)
        data = ops.read("/file.txt", 5, 7, fh)
        rc = ops.release("/file.txt", fh)

        assert data == b"hello"
        assert rc == 0
        handle.seek.assert_called_once_with(7)
        handle.close.assert_called_once()

    def test_read_seek_failure_raises_fuse_error(self):
        handle = MagicMock()
        handle.seek.side_effect = OSError(errno.EIO, "seek failed")
        client = MagicMock()
        client.stat.return_value = _FakeStat(mode=stat.S_IFDIR | 0o755, name="/")
        ops = ReadOnlyFuseOperations("file:///virtual/source", client)
        ops._handles[1] = handle

        with pytest.raises(FuseOSError) as excinfo:
            ops.read("/file.txt", 5, 7, 1)

        assert excinfo.value.errno == errno.EIO

    def test_access_checks_read_only(self):
        client = MagicMock()
        client.stat.return_value = _FakeStat(mode=stat.S_IFDIR | 0o755, name="/")
        ops = ReadOnlyFuseOperations("file:///virtual/source", client)

        with pytest.raises(FuseOSError) as excinfo:
            ops.access("/", 2)

        assert excinfo.value.errno == errno.EROFS

    def test_mount_foreground_uses_fskit_alias_on_macos(self, tmp_path):
        mountpoint = tmp_path / "mnt"
        mountpoint.mkdir()
        client = MagicMock()
        client.stat.return_value = _FakeStat(mode=stat.S_IFDIR | 0o755, name="/")

        with (
            patch.object(mount_module.sys, "platform", "darwin"),
            patch.object(mount_module, "FUSE") as mock_fuse,
        ):
            mount_module.mount_foreground("file:///virtual/source", mountpoint, client)

        assert mock_fuse.call_args.kwargs["backend"] == "fskit"
        assert mock_fuse.call_args.kwargs["volname"] == "gfal"
        fuse_path = PurePosixPath(mock_fuse.call_args.args[1])
        assert fuse_path.parts[:3] == ("/", "Volumes", "Macintosh HD")


class TestExecuteMount:
    def test_mount_forwards_to_core_mount(self, tmp_path):
        mountpoint = tmp_path / "mnt"
        mountpoint.mkdir()
        cmd = _make_mount_cmd()
        cmd.params = _default_params(
            source="file:///virtual/source",
            mountpoint=mountpoint,
        )

        with (
            patch("gfal.cli.mount.GfalClient") as mock_client_cls,
            patch("gfal.cli.mount.mount_foreground") as mock_mount,
        ):
            rc = cmd.execute_mount()

        assert rc == 0
        mock_client_cls.assert_called_once()
        mock_mount.assert_called_once_with(
            "file:///virtual/source",
            mountpoint.expanduser(),
            mock_client_cls.return_value,
        )
