"""Linux FUSE integration tests for ``gfal mount``."""

import errno
import hashlib
import socket

import pytest

from conftest import require_test_prereq
from helpers import fuse_available, mounted_gfal

pytestmark = [pytest.mark.mount]

_EOSPUBLIC_HOST = "eospublic.cern.ch"
_EOSPUBLIC_PORT = 443
_EOSPUBLIC_DIR = (
    "https://eospublic.cern.ch/eos/opendata/phenix/emcal-finding-pi0s-and-photons/"
)
_EOSPUBLIC_FILE = "single_cluster_r5.C"
_EOSPUBLIC_MD5 = "93f402e24c6f870470e1c5fcc5400e25"


def _tcp_reachable(host: str, port: int, timeout: float = 5.0) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


@pytest.fixture(scope="module", autouse=True)
def _require_fuse_support():
    require_test_prereq(fuse_available(), "Linux FUSE support is not available")


class TestLocalMount:
    def test_mount_lists_and_reads_local_directory(self, tmp_path):
        source = tmp_path / "source"
        source.mkdir()
        (source / "hello.txt").write_text("hello via mount\n")
        subdir = source / "subdir"
        subdir.mkdir()
        mountpoint = tmp_path / "mnt"
        mountpoint.mkdir()

        with mounted_gfal(source.as_uri(), mountpoint):
            assert sorted(path.name for path in mountpoint.iterdir()) == [
                "hello.txt",
                "subdir",
            ]
            assert (mountpoint / "hello.txt").read_text() == "hello via mount\n"

    def test_mount_is_read_only(self, tmp_path):
        source = tmp_path / "source"
        source.mkdir()
        (source / "hello.txt").write_text("hello\n")
        mountpoint = tmp_path / "mnt"
        mountpoint.mkdir()

        with (
            mounted_gfal(source.as_uri(), mountpoint),
            pytest.raises(OSError) as excinfo,
        ):
            (mountpoint / "created.txt").write_text("nope\n")

        assert excinfo.value.errno in {errno.EROFS, errno.EACCES}


@pytest.mark.integration
@pytest.mark.network
class TestEosPublicMount:
    @pytest.fixture(scope="class", autouse=True)
    def _require_eospublic(self):
        require_test_prereq(
            _tcp_reachable(_EOSPUBLIC_HOST, _EOSPUBLIC_PORT),
            f"{_EOSPUBLIC_HOST}:{_EOSPUBLIC_PORT} not reachable",
        )

    def test_mount_reads_eospublic_directory(self, tmp_path):
        mountpoint = tmp_path / "mnt"
        mountpoint.mkdir()

        with mounted_gfal(_EOSPUBLIC_DIR, mountpoint, timeout=60):
            entries = {path.name for path in mountpoint.iterdir()}
            assert _EOSPUBLIC_FILE in entries
            data = (mountpoint / _EOSPUBLIC_FILE).read_bytes()

        assert hashlib.md5(data).hexdigest() == _EOSPUBLIC_MD5
