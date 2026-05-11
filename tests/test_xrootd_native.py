from types import SimpleNamespace
from urllib.parse import urlparse

import pytest

from gfal.core.xrootd_native import XRootDNativeFileSystem


class FakeStatus:
    def __init__(self, ok=True, message="OK"):
        self.ok = ok
        self.message = message


class FakeStat:
    def __init__(self, size=0, flags=0, modtime=1234):
        self.size = size
        self.flags = flags
        self.modtime = modtime


class FakeEntry:
    def __init__(self, name, statinfo):
        self.name = name
        self.statinfo = statinfo


class FakeURL:
    def __init__(self, url):
        parsed = urlparse(url)
        self.protocol = parsed.scheme
        default_port = "1094" if parsed.scheme in {"root", "xroot"} else "443"
        port = parsed.port or default_port
        self.hostid = f"{parsed.hostname}:{port}"
        path = parsed.path
        if path.startswith("//"):
            path = path[1:]
        self.path = path
        self.path_with_params = path
        if parsed.query:
            self.path_with_params += f"?{parsed.query}"

    def is_valid(self):
        return True


class FakeFileSystemClient:
    def __init__(self, url):
        self.url = SimpleNamespace(is_valid=lambda: True)
        self.base_url = url
        self.calls = []
        self.files = {"/eos/file": bytearray(b"hello native xrootd")}
        self.dirs = {"/eos"}

    def stat(self, path, timeout=0):
        self.calls.append(("stat", path, timeout))
        if path in self.dirs:
            return FakeStatus(), FakeStat(size=0, flags=0b111, modtime=99)
        if path in self.files:
            return FakeStatus(), FakeStat(size=len(self.files[path]), flags=0b110)
        return FakeStatus(False, "No such file or directory"), None

    def dirlist(self, path, flags, timeout=0):
        self.calls.append(("dirlist", path, flags, timeout))
        if path != "/eos":
            return FakeStatus(False, "Not a directory"), None
        return (
            FakeStatus(),
            [
                FakeEntry(
                    "file", FakeStat(size=len(self.files["/eos/file"]), flags=0b110)
                )
            ],
        )

    def mkdir(self, path, flags=None, timeout=0):
        self.calls.append(("mkdir", path, flags, timeout))
        self.dirs.add(path)
        return FakeStatus(), None

    def rm(self, path, timeout=0):
        self.calls.append(("rm", path, timeout))
        self.files.pop(path, None)
        return FakeStatus(), None

    def rmdir(self, path, timeout=0):
        self.calls.append(("rmdir", path, timeout))
        self.dirs.discard(path)
        return FakeStatus(), None

    def mv(self, src, dst, timeout=0):
        self.calls.append(("mv", src, dst, timeout))
        self.files[dst] = self.files.pop(src)
        return FakeStatus(), None

    def chmod(self, path, mode, timeout=0):
        self.calls.append(("chmod", path, mode, timeout))
        return FakeStatus(), None

    def query(self, code, path, timeout=0):
        self.calls.append(("query", code, path, timeout))
        return FakeStatus(), b"adler32 00000627"


class FakeFile:
    def __init__(self):
        self.client = None
        self.path = None

    def open(self, url, flags, timeout=0):
        path = FakeURL(url).path_with_params
        self.client = FakeClient.last_filesystem
        self.path = path
        self.client.files.setdefault(path, bytearray())
        return FakeStatus(), None

    def stat(self, timeout=0):
        data = self.client.files[self.path]
        return FakeStatus(), FakeStat(size=len(data), flags=0b110)

    def read(self, offset, length, timeout=0):
        data = self.client.files[self.path]
        return FakeStatus(), bytes(data[offset : offset + length])

    def write(self, data, offset, length, timeout=0):
        current = self.client.files[self.path]
        end = offset + length
        if len(current) < end:
            current.extend(b"\x00" * (end - len(current)))
        current[offset:end] = data[:length]
        return FakeStatus(), length

    def close(self, timeout=0):
        return FakeStatus(), None


class FakeClient:
    last_filesystem = None

    URL = FakeURL
    File = FakeFile

    @staticmethod
    def FileSystem(url):
        FakeClient.last_filesystem = FakeFileSystemClient(url)
        return FakeClient.last_filesystem


class FakeFlags:
    STAT = 1
    MAKEPATH = 2
    READ = 3
    DELETE = 4
    UPDATE = 5
    CHECKSUM = 6
    IS_DIR = 0b001
    IS_READABLE = 0b010
    IS_WRITABLE = 0b100


def fake_loader():
    return FakeClient, FakeFlags, FakeFlags, FakeFlags, FakeFlags, FakeFlags


def test_from_url_preserves_https_absolute_path_and_timeout(monkeypatch):
    monkeypatch.setattr("gfal.core.xrootd_native._load_xrootd_client", fake_loader)

    fso, path = XRootDNativeFileSystem.from_url(
        "https://example.com//eos/file?eos.app=test",
        {"timeout": 17},
    )

    assert fso.base_url == "https://example.com:443"
    assert fso.timeout == 17
    assert path == "/eos/file?eos.app=test"


def test_info_and_ls_return_fsspec_shaped_dicts(monkeypatch):
    monkeypatch.setattr("gfal.core.xrootd_native._load_xrootd_client", fake_loader)
    fso, path = XRootDNativeFileSystem.from_url("root://example.com//eos")

    info = fso.info(path)
    listing = fso.ls(path, detail=True)

    assert info["type"] == "directory"
    assert info["mtime"] == 99
    assert listing[0]["name"] == "/eos/file"
    assert listing[0]["type"] == "file"
    assert listing[0]["size"] == len(b"hello native xrootd")


def test_probe_support_allows_missing_paths(monkeypatch):
    monkeypatch.setattr("gfal.core.xrootd_native._load_xrootd_client", fake_loader)
    fso, _ = XRootDNativeFileSystem.from_url("root://example.com//eos/missing")

    fso.probe_support("/eos/missing")

    assert FakeClient.last_filesystem.calls[-1] == ("stat", "/eos/missing", 0)


def test_probe_support_raises_for_unsupported_protocol(monkeypatch):
    monkeypatch.setattr("gfal.core.xrootd_native._load_xrootd_client", fake_loader)
    fso, _ = XRootDNativeFileSystem.from_url("https://example.com//eos/file")
    fso._myclient.stat = lambda path, timeout=0: (
        FakeStatus(False, "[ERROR] Operation not supported"),
        None,
    )

    with pytest.raises(OSError, match="Operation not supported"):
        fso.probe_support("/eos/file")


def test_open_read_and_write_use_native_file(monkeypatch):
    monkeypatch.setattr("gfal.core.xrootd_native._load_xrootd_client", fake_loader)
    fso, _ = XRootDNativeFileSystem.from_url("root://example.com//eos/file")

    with fso.open("/eos/file", "rb") as handle:
        assert handle.read(5) == b"hello"

    with fso.open("/eos/new-file", "wb") as handle:
        assert handle.write(b"new payload") == len(b"new payload")

    assert FakeClient.last_filesystem.files["/eos/new-file"] == bytearray(
        b"new payload"
    )


def test_checksum_returns_native_algorithm_and_value(monkeypatch):
    monkeypatch.setattr("gfal.core.xrootd_native._load_xrootd_client", fake_loader)
    fso, _ = XRootDNativeFileSystem.from_url("root://example.com//eos/file")

    assert fso.checksum("/eos/file", "ADLER32") == ("adler32", "00000627")
