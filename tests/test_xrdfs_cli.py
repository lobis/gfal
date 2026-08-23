"""Tests for the dependency-free xrdfs-backed ``gfal`` command."""

from __future__ import annotations

import errno
import json
import os
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

import pytest

import gfal.cli.xrdfs as xrdfs_cli
import gfal.xrdfs as xrdfs
from gfal.cli.main import main
from gfal.cli.xrdfs import dispatch, supports_url
from gfal.xrdfs import XrdfsResult

_URL = "root://storage.example//data/file"

_FAKE_XRDFS = r"""#!/usr/bin/env python3
import json
import os
import signal
import sys
import time
from pathlib import Path

arguments = sys.argv[1:]
record_path = os.environ.get("FAKE_XRDFS_RECORD")
if record_path:
    names = (
        "XRD_REQUESTTIMEOUT",
        "XRD_NETWORKSTACK",
        "XRD_APPNAME",
        "XRD_LOGLEVEL",
        "XRD_LOGFILE",
        "X509_USER_CERT",
        "X509_USER_KEY",
        "X509_USER_PROXY",
        "XRD_HTTPCLIENTCERTFILE",
        "XRD_HTTPCLIENTKEYFILE",
    )
    record = {
        "argv": arguments,
        "env": {name: os.environ.get(name) for name in names},
    }
    with Path(record_path).open("a", encoding="utf-8") as stream:
        stream.write(json.dumps(record, separators=(",", ":")) + "\n")

if arguments == ["--help"]:
    time.sleep(float(os.environ.get("FAKE_XRDFS_HELP_SLEEP", "0")))
    default_help = (
        "command-first batch\n"
        "--json print one JSON object per entry\n"
        "stat [--json] [-q query] [--] <path>...\n"
        "xattr <path> [attribute]\n"
    )
    sys.stdout.write(os.environ.get("FAKE_XRDFS_HELP", default_help))
    raise SystemExit(0)

if any("slow" in argument for argument in arguments):
    time.sleep(float(os.environ.get("FAKE_XRDFS_SLEEP", "5")))

if any("missing" in argument for argument in arguments):
    sys.stderr.write("[ERROR] Error response: No such file or directory\n")
    raise SystemExit(54)

if arguments and arguments[0] == "cat":
    payload_size = int(os.environ.get("FAKE_XRDFS_CAT_SIZE", "0"))
    if payload_size:
        signal.signal(signal.SIGPIPE, signal.SIG_DFL)
        chunk = b"x" * 65536
        while payload_size > 0:
            written = os.write(1, chunk[:payload_size])
            payload_size -= written
        raise SystemExit(0)
    url = arguments[-1]
    if "one" in url:
        payload = bytes.fromhex("00ff")
    elif "two" in url:
        payload = b"AB"
    else:
        payload = bytes.fromhex(os.environ.get("FAKE_XRDFS_CAT_HEX", ""))
    os.write(1, payload)
    raise SystemExit(int(os.environ.get("FAKE_XRDFS_RETURN_CODE", "0")))

configured = os.environ.get("FAKE_XRDFS_STDOUT")
configured_stderr = os.environ.get("FAKE_XRDFS_STDERR")
if configured_stderr is not None:
    sys.stderr.write(configured_stderr)
if configured is not None:
    sys.stdout.write(configured)
elif arguments and arguments[0] == "sum":
    sys.stdout.write("adler32 deadbeef\n")
elif arguments and arguments[0] == "xattr":
    if "set" not in arguments:
        sys.stdout.write("fixture-value\n")
elif arguments and arguments[0] == "stat":
    sys.stdout.write(json.dumps({
        "path": "/data/file",
        "type": "file",
        "size": 5,
        "mtime": 1700000000,
        "atime": 1699999999,
        "ctime": 1700000001,
        "flags": 0,
        "flag_names": ["IsReadable"],
        "extended": True,
        "mode": "0640",
        "permissions": "rw-r-----",
        "owner": "123",
        "group": "456",
        "checksum": None,
        "xattrs": [],
    }, separators=(",", ":")) + "\n")

raise SystemExit(int(os.environ.get("FAKE_XRDFS_RETURN_CODE", "0")))
"""


@pytest.fixture
def fake_xrdfs(tmp_path, monkeypatch):
    executable = tmp_path / "xrdfs"
    executable.write_text(_FAKE_XRDFS, encoding="utf-8")
    executable.chmod(0o755)
    record = tmp_path / "record.jsonl"
    monkeypatch.setenv("GFAL_XRDFS", str(executable))
    monkeypatch.setenv("FAKE_XRDFS_RECORD", str(record))
    monkeypatch.delenv("EOSAUTHZ", raising=False)
    monkeypatch.delenv("GFAL_AUTHZ_TOKEN", raising=False)
    return record


def _records(path: Path):
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]


def _command_records(path: Path):
    return [record for record in _records(path) if record["argv"] != ["--help"]]


def _metadata_record(**overrides):
    record = {
        "path": "/data/file",
        "type": "file",
        "size": 1,
        "mtime": 1700000000,
        "atime": 1699999999,
        "ctime": 1700000001,
        "flags": 0,
        "flag_names": ["IsReadable"],
        "extended": True,
        "mode": "0644",
        "permissions": "rw-r--r--",
        "owner": "0",
        "group": "0",
        "checksum": None,
        "xattrs": [],
    }
    record.update(overrides)
    return record


def _aggregate_process(*arguments, stdout=subprocess.PIPE):
    script = (
        "import sys; from gfal.cli.main import main; "
        "sys.argv=['gfal', *sys.argv[1:]]; raise SystemExit(main())"
    )
    return subprocess.Popen(
        [sys.executable, "-c", script, *arguments],
        stdout=stdout,
        stderr=subprocess.PIPE,
        env={**os.environ, "PYTHONUTF8": "1"},
    )


@pytest.mark.parametrize(
    "scheme",
    ("root", "roots", "xroot", "xroots", "http", "https", "dav", "davs"),
)
def test_supported_remote_url_schemes(scheme):
    assert supports_url(f"{scheme}://storage.example/path")


@pytest.mark.parametrize("value", ("/local/path", "file:///local/path", "root:///path"))
def test_nonremote_or_incomplete_urls_use_the_transition_fallback(value):
    assert not supports_url(value)


def test_import_does_not_load_previous_python_backends():
    code = (
        "import sys; from gfal.cli.main import main; "
        "\ntry: main(['gfal', 'stat', '--help'])\n"
        "except SystemExit as error: assert error.code == 0\n"
        "assert 'fsspec' not in sys.modules; "
        "assert 'aiohttp' not in sys.modules; "
        "assert 'XRootD' not in sys.modules"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize("option", ["--help", "--version"])
def test_local_information_does_not_require_xrdfs(option, tmp_path, monkeypatch):
    monkeypatch.setenv("GFAL_XRDFS", str(tmp_path / "missing-xrdfs"))
    with pytest.raises(SystemExit) as caught:
        dispatch("stat", [option], prog="gfal stat")
    assert caught.value.code == 0


def test_missing_xrdfs_is_clear(tmp_path, monkeypatch, capsys):
    missing = tmp_path / "missing-xrdfs"
    monkeypatch.setenv("GFAL_XRDFS", str(missing))
    assert dispatch("stat", [_URL], prog="gfal stat") == 127
    assert "install xrootd-client" in capsys.readouterr().err


def test_incompatible_xrdfs_fails_before_remote_operation(
    fake_xrdfs, monkeypatch, capsys
):
    monkeypatch.setenv("FAKE_XRDFS_HELP", "legacy server-first help only\n")
    assert dispatch("stat", [_URL], prog="gfal stat") == 69
    assert "incompatible xrdfs" in capsys.readouterr().err
    assert _command_records(fake_xrdfs) == []


def test_generic_json_help_does_not_satisfy_metadata_contract(
    fake_xrdfs, monkeypatch, capsys
):
    monkeypatch.setenv(
        "FAKE_XRDFS_HELP",
        "command-first batch\n--json\nxattr <path> [attribute]\n",
    )
    assert dispatch("stat", [_URL], prog="gfal stat") == 69
    assert "incompatible xrdfs" in capsys.readouterr().err
    assert _command_records(fake_xrdfs) == []


def test_rejects_local_urls_before_running_xrdfs(fake_xrdfs):
    with pytest.raises(SystemExit) as caught:
        dispatch("stat", ["file:///tmp/file"], prog="gfal stat")
    assert caught.value.code == 2
    assert _records(fake_xrdfs) == []


def test_stat_formats_json_and_maps_common_environment(fake_xrdfs, monkeypatch, capsys):
    monkeypatch.setattr(os, "getuid", lambda: 123)
    monkeypatch.setattr(os, "getgid", lambda: 456)
    result = dispatch(
        "stat",
        [
            "-vv",
            "-t",
            "17",
            "-E",
            "/tmp/proxy.pem",
            "--key",
            "/tmp/key.pem",
            "-4",
            "-C",
            "test-client/1.0",
            "--log-file",
            "/tmp/xrd.log",
            _URL,
        ],
        prog="gfal stat",
    )
    captured = capsys.readouterr()
    assert result == 0
    assert f"  File: '{_URL}'" in captured.out
    assert "  Size: 5\tregular file" in captured.out
    assert "Access: (0400/-r--------)\tUid: 123\tGid: 456" in captured.out
    assert (
        datetime.fromtimestamp(1700000000).strftime("%Y-%m-%d %H:%M:%S") in captured.out
    )

    [record] = _command_records(fake_xrdfs)
    assert record["argv"] == ["stat", "--json", _URL]
    assert record["env"] == {
        "XRD_REQUESTTIMEOUT": "17",
        "XRD_NETWORKSTACK": "IPv4",
        "XRD_APPNAME": "test-client/1.0",
        "XRD_LOGLEVEL": "Info",
        "XRD_LOGFILE": "/tmp/xrd.log",
        "X509_USER_CERT": "/tmp/proxy.pem",
        "X509_USER_KEY": "/tmp/key.pem",
        "X509_USER_PROXY": None,
        "XRD_HTTPCLIENTCERTFILE": "/tmp/proxy.pem",
        "XRD_HTTPCLIENTKEYFILE": "/tmp/key.pem",
    }


@pytest.mark.parametrize("scheme", ("http", "https", "dav", "davs"))
def test_stat_uses_legacy_defaults_for_unextended_webdav_metadata(
    fake_xrdfs, monkeypatch, capsys, scheme
):
    record = _metadata_record(
        extended=False,
        mode=None,
        permissions=None,
        owner=None,
        group=None,
        atime=0,
        ctime=0,
    )
    monkeypatch.setenv("FAKE_XRDFS_STDOUT", json.dumps(record) + "\n")
    url = f"{scheme}://storage.example/data/file"

    assert dispatch("stat", [url], prog="gfal stat") == 0
    output = capsys.readouterr().out
    assert "Access: (0777/-rwxrwxrwx)\tUid: 0\tGid: 0" in output
    epoch = datetime.fromtimestamp(0).strftime("%Y-%m-%d %H:%M:%S.%f")
    assert f"Access: {epoch}" in output
    mtime = datetime.fromtimestamp(1700000000).strftime("%Y-%m-%d %H:%M:%S.%f")
    assert f"Modify: {mtime}" in output
    assert f"Change: {mtime}" in output


def test_ls_uses_legacy_defaults_for_unextended_webdav_metadata(
    fake_xrdfs, monkeypatch, capsys
):
    record = _metadata_record(
        extended=False,
        mode=None,
        permissions=None,
        owner=None,
        group=None,
        atime=0,
        ctime=0,
    )
    monkeypatch.setenv("FAKE_XRDFS_STDOUT", json.dumps(record) + "\n")
    url = "https://storage.example/data/file"

    assert dispatch("ls", ["-ld", url], prog="gfal ls") == 0
    assert capsys.readouterr().out.startswith("-rwxrwxrwx   0 0     0     ")


def test_ls_preserves_explicit_extended_webdav_metadata(
    fake_xrdfs, monkeypatch, capsys
):
    record = _metadata_record(nlink=2)
    monkeypatch.setenv("FAKE_XRDFS_STDOUT", json.dumps(record) + "\n")
    url = "https://storage.example/data/file"

    assert dispatch("ls", ["-ld", url], prog="gfal ls") == 0
    assert capsys.readouterr().out.startswith("-rw-r--r--   2 0     0     ")


def test_ls_formats_legacy_long_output_and_filters_hidden(
    fake_xrdfs, monkeypatch, capsys
):
    timestamp = 1700000000
    entries = [
        _metadata_record(
            path="/data/.hidden",
            mtime=timestamp,
            xattrs=[{"name": "user.status", "value": "ONLINE"}],
        ),
        _metadata_record(
            path="/data/visible",
            size=1025,
            mtime=timestamp,
            mode="0750",
            permissions="rwxr-x---",
            owner="42",
            group="43",
            nlink=2,
            xattrs=[{"name": "user.status", "value": "ONLINE"}],
        ),
    ]
    monkeypatch.setenv(
        "FAKE_XRDFS_STDOUT",
        "".join(json.dumps(entry) + "\n" for entry in entries),
    )

    directory = "root://storage.example//data/"
    result = dispatch(
        "ls",
        [
            "-lH",
            "--time-style",
            "long-iso",
            "--xattr",
            "user.status",
            directory,
        ],
        prog="gfal ls",
    )
    captured = capsys.readouterr()
    assert result == 0
    assert ".hidden" not in captured.out
    assert "-r--r--r--   0 0     0" in captured.out
    assert "1.1K" in captured.out
    assert "visible\tONLINE" in captured.out
    assert datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M") in captured.out

    [record] = _command_records(fake_xrdfs)
    assert record["argv"] == [
        "ls",
        "--json",
        "--xattr",
        "user.status",
        directory,
    ]


def test_ls_target_uses_legacy_root_stat_projection(fake_xrdfs, monkeypatch, capsys):
    record = _metadata_record(owner="named-user", group="named-group", nlink=9)
    monkeypatch.setenv("FAKE_XRDFS_STDOUT", json.dumps(record) + "\n")
    monkeypatch.setattr(os, "getuid", lambda: 123)
    monkeypatch.setattr(os, "getgid", lambda: 456)

    assert dispatch("ls", ["-ld", _URL], prog="gfal ls") == 0
    assert capsys.readouterr().out.startswith("-r--------   1 123   456")


def test_stat_uses_legacy_root_directory_permissions(fake_xrdfs, monkeypatch, capsys):
    record = _metadata_record(
        type="directory",
        mode="0755",
        permissions="rwxr-xr-x",
        flag_names=["XBitSet", "IsDir", "IsReadable"],
    )
    monkeypatch.setenv("FAKE_XRDFS_STDOUT", json.dumps(record) + "\n")
    monkeypatch.setattr(os, "getuid", lambda: 123)
    monkeypatch.setattr(os, "getgid", lambda: 456)

    directory = "root://storage.example//data"
    assert dispatch("stat", [directory], prog="gfal stat") == 0
    assert "Access: (0500/dr-x------)\tUid: 123\tGid: 456" in capsys.readouterr().out


def test_ls_file_and_directory_option_print_original_url(
    fake_xrdfs, monkeypatch, capsys
):
    record = _metadata_record()
    monkeypatch.setenv("FAKE_XRDFS_STDOUT", json.dumps(record) + "\n")
    assert dispatch("ls", ["-d", _URL], prog="gfal ls") == 0
    assert capsys.readouterr().out == f"{_URL}\n"


@pytest.mark.parametrize(
    ("colors", "color"),
    [("", "037"), ("no=35:*.txt=31:fi=32", "35")],
)
def test_ls_short_color_uses_legacy_no_class(
    fake_xrdfs, monkeypatch, capsys, colors, color
):
    record = _metadata_record(path="/data/file.txt")
    url = "root://storage.example//data/file.txt"
    monkeypatch.setenv("FAKE_XRDFS_STDOUT", json.dumps(record) + "\n")
    monkeypatch.setenv("LS_COLORS", colors)
    assert dispatch("ls", ["--color", "always", url], prog="gfal ls") == 0
    assert capsys.readouterr().out == f"\033[{color}m{url}\033[0m\n"


@pytest.mark.parametrize(
    ("overrides", "colors", "color"),
    [
        ({}, "fi=32", "037"),
        ({"type": "directory", "mode": "0755"}, "di=34", "34"),
        ({"mode": "0755", "flag_names": ["IsReadable", "XBitSet"]}, "ex=33", "33"),
    ],
)
def test_ls_long_color_uses_legacy_mode_classes(
    fake_xrdfs, monkeypatch, capsys, overrides, colors, color
):
    record = _metadata_record(path="/data/item", **overrides)
    url = "root://storage.example//data/item"
    monkeypatch.setenv("FAKE_XRDFS_STDOUT", json.dumps(record) + "\n")
    monkeypatch.setenv("LS_COLORS", colors)
    assert dispatch("ls", ["-l", "--color", "always", url], prog="gfal ls") == 0
    assert f"\033[{color}m{url}\033[0m" in capsys.readouterr().out


def test_cat_is_binary_safe_and_runs_each_url_sequentially(fake_xrdfs, capfdbinary):
    one = "root://one.example//one"
    two = "https://two.example/two"
    assert dispatch("cat", ["-b", one, two], prog="gfal cat") == 0
    assert capfdbinary.readouterr().out == b"\x00\xffAB"
    assert [record["argv"] for record in _command_records(fake_xrdfs)] == [
        ["cat", "--bytes", one],
        ["cat", "--bytes", two],
    ]


def test_cat_accepts_compatibility_option_after_url(fake_xrdfs, capfdbinary):
    assert dispatch("cat", [_URL, "-b"], prog="gfal cat") == 0
    assert capfdbinary.readouterr().out == b""
    [record] = _command_records(fake_xrdfs)
    assert record["argv"] == ["cat", "--bytes", _URL]


def test_cat_stops_at_first_failure(fake_xrdfs, capfdbinary):
    one = "root://one.example//one"
    missing = "root://one.example//missing"
    two = "root://one.example//two"
    assert dispatch("cat", [one, missing, two], prog="gfal cat") == 2
    captured = capfdbinary.readouterr()
    assert captured.out == b"\x00\xff"
    assert b"gfal cat error: 2" in captured.err
    assert [record["argv"] for record in _command_records(fake_xrdfs)] == [
        ["cat", one],
        ["cat", missing],
    ]


def test_cat_enforces_whole_command_timeout(fake_xrdfs, monkeypatch, capsys):
    monkeypatch.setenv("FAKE_XRDFS_SLEEP", "5")
    started = time.monotonic()
    result = dispatch(
        "cat",
        ["-t", "1", "root://storage.example//slow"],
        prog="gfal cat",
    )
    assert result == 110
    assert time.monotonic() - started < 4
    assert "Command timed out after 1 seconds!" in capsys.readouterr().err


def test_interrupted_child_preserves_legacy_status_and_message(
    fake_xrdfs, monkeypatch, capsys
):
    interrupted = XrdfsResult(errno.EINTR, b"", b"")
    monkeypatch.setattr(xrdfs_cli, "run_xrdfs", lambda *args, **kwargs: interrupted)

    assert dispatch("sum", [_URL, "ADLER32"], prog="gfal sum") == errno.EINTR
    assert capsys.readouterr().err == "Caught keyboard interrupt. Canceling..."


def test_interrupted_capability_probe_preserves_legacy_status_and_message(
    fake_xrdfs, monkeypatch, capsys
):
    interrupted = XrdfsResult(errno.EINTR, b"", b"")
    monkeypatch.setattr(xrdfs, "run_xrdfs", lambda *args, **kwargs: interrupted)

    assert dispatch("sum", [_URL, "ADLER32"], prog="gfal sum") == errno.EINTR
    assert capsys.readouterr().err == "Caught keyboard interrupt. Canceling..."
    assert _command_records(fake_xrdfs) == []


def test_host_down_diagnostic_uses_legacy_linux_status(fake_xrdfs, monkeypatch, capsys):
    monkeypatch.setenv("FAKE_XRDFS_RETURN_CODE", "50")
    monkeypatch.setenv("FAKE_XRDFS_STDERR", "[ERROR] Host is down\n")

    assert dispatch("sum", [_URL, "ADLER32"], prog="gfal sum") == 112
    assert "gfal sum error: 112" in capsys.readouterr().err


def test_checksum_mismatch_uses_legacy_linux_status(fake_xrdfs, monkeypatch, capsys):
    monkeypatch.setenv("FAKE_XRDFS_RETURN_CODE", "50")
    monkeypatch.setenv(
        "FAKE_XRDFS_STDERR",
        "[ERROR] CheckSum error: Checksum response used adler32 instead of md5\n",
    )

    assert dispatch("sum", [_URL, "MD5"], prog="gfal sum") == 115
    assert "gfal sum error: 115" in capsys.readouterr().err


def test_unsupported_xattr_uses_legacy_linux_status(fake_xrdfs, monkeypatch, capsys):
    monkeypatch.setenv("FAKE_XRDFS_RETURN_CODE", "50")
    monkeypatch.setenv(
        "FAKE_XRDFS_STDERR",
        "[ERROR] Server responded with an error: [3013] Unable to fsctl: "
        "Operation not supported; /data/file\n",
    )

    assert dispatch("xattr", [_URL, "xroot.xattr"], prog="gfal xattr") == 95
    assert "gfal xattr error: 95" in capsys.readouterr().err


def test_timeout_includes_capability_probe(fake_xrdfs, monkeypatch, capsys):
    monkeypatch.setenv("FAKE_XRDFS_HELP_SLEEP", "5")
    started = time.monotonic()
    assert dispatch("stat", ["-t", "1", _URL], prog="gfal stat") == 110
    assert time.monotonic() - started < 4
    assert "Command timed out after 1 seconds!" in capsys.readouterr().err
    assert _command_records(fake_xrdfs) == []


def test_sum_rewrites_native_output(fake_xrdfs, capsys):
    assert dispatch("sum", [_URL, "ADLER32"], prog="gfal sum") == 0
    assert capsys.readouterr().out == f"{_URL} deadbeef\n"
    [record] = _command_records(fake_xrdfs)
    assert record["argv"] == ["sum", _URL, "ADLER32"]


@pytest.mark.parametrize(
    ("command", "arguments"),
    [
        ("stat", []),
        ("ls", ["-d"]),
        ("sum", ["ADLER32"]),
    ],
)
def test_url_labels_redact_authz_tokens(
    fake_xrdfs, monkeypatch, capsys, command, arguments
):
    token_url = f"{_URL}?authz=zteos64:do-not-print&eos.app=gfal"
    if command == "ls":
        monkeypatch.setenv("FAKE_XRDFS_STDOUT", json.dumps(_metadata_record()) + "\n")
    if command == "sum":
        invocation = [token_url, *arguments]
    else:
        invocation = [*arguments, token_url]
    assert dispatch(command, invocation, prog=f"gfal {command}") == 0
    captured = capsys.readouterr()
    assert "zteos64:do-not-print" not in captured.out
    assert "authz=<redacted>" in captured.out


def test_native_diagnostics_redact_authz_tokens(fake_xrdfs, monkeypatch, capsys):
    token_url = f"{_URL}?authz=zteos64:do-not-print"
    monkeypatch.setenv("FAKE_XRDFS_STDERR", f"notice for {token_url}\n")
    assert dispatch("sum", [token_url, "ADLER32"], prog="gfal sum") == 0
    captured = capsys.readouterr()
    assert "zteos64:do-not-print" not in captured.err
    assert "authz=<redacted>" in captured.err


def test_failure_diagnostics_redact_authz_tokens(fake_xrdfs, monkeypatch, capsys):
    token_url = f"{_URL}?authz=zteos64:do-not-print"
    monkeypatch.setenv("FAKE_XRDFS_RETURN_CODE", "1")
    monkeypatch.setenv("FAKE_XRDFS_STDERR", f"failed {token_url}\n")
    assert dispatch("sum", [token_url, "ADLER32"], prog="gfal sum") == 1
    captured = capsys.readouterr()
    assert "zteos64:do-not-print" not in captured.err
    assert "authz=<redacted>" in captured.err


def test_error_mapping_ignores_unrelated_native_warnings(
    fake_xrdfs, monkeypatch, capsys
):
    monkeypatch.setenv("FAKE_XRDFS_RETURN_CODE", "50")
    monkeypatch.setenv(
        "FAKE_XRDFS_STDERR",
        "Plugin No such file or directory loading optional plugin\n"
        "[ERROR] Permission denied\n",
    )

    assert dispatch("sum", [_URL, "ADLER32"], prog="gfal sum") == errno.EACCES
    assert "Permission denied" in capsys.readouterr().err


def test_data_payloads_are_not_redacted(fake_xrdfs, monkeypatch, capsys):
    monkeypatch.setenv("FAKE_XRDFS_STDOUT", "authz=payload-value\n")
    assert dispatch("xattr", [_URL, "user.test"], prog="gfal xattr") == 0
    assert capsys.readouterr().out == "authz=payload-value\n"


def test_malformed_url_error_redacts_authz(fake_xrdfs, capsys):
    malformed = "root://[broken/path?authz=do-not-print"
    with pytest.raises(SystemExit) as caught:
        dispatch("stat", [malformed], prog="gfal stat")
    assert caught.value.code == 2
    captured = capsys.readouterr()
    assert "do-not-print" not in captured.err
    assert "authz=<redacted>" in captured.err


@pytest.mark.parametrize(
    ("attribute", "expected"),
    [
        (None, ["xattr", _URL]),
        ("user.test", ["xattr", _URL, "--", "user.test"]),
        ("list", ["xattr", _URL, "--", "list"]),
        ("user.test=a=b", ["xattr", _URL, "set", "user.test=a=b"]),
    ],
)
def test_xattr_argument_translation(fake_xrdfs, capsys, attribute, expected):
    arguments = [_URL] if attribute is None else [_URL, attribute]
    assert dispatch("xattr", arguments, prog="gfal xattr") == 0
    output = capsys.readouterr().out
    assert output == ("" if attribute and "=" in attribute else "fixture-value\n")
    [record] = _command_records(fake_xrdfs)
    assert record["argv"] == expected


@pytest.mark.parametrize("attribute", ["=value", "key="])
def test_xattr_empty_assignment_is_legacy_noop(fake_xrdfs, attribute, capsys):
    assert dispatch("xattr", [_URL, attribute], prog="gfal xattr") == 0
    assert capsys.readouterr().out == ""
    assert _command_records(fake_xrdfs) == []


def test_xattr_accepts_dash_prefixed_attribute_after_delimiter(fake_xrdfs, capsys):
    assert dispatch("xattr", [_URL, "--", "-user.test"], prog="gfal xattr") == 0
    assert capsys.readouterr().out == "fixture-value\n"
    [record] = _command_records(fake_xrdfs)
    assert record["argv"] == ["xattr", _URL, "--", "-user.test"]


def test_nonpositive_timeout_is_unlimited_and_not_exported(
    fake_xrdfs, monkeypatch, capsys
):
    monkeypatch.setenv("XRD_REQUESTTIMEOUT", "7")
    assert dispatch("stat", ["-t", "0", _URL], prog="gfal stat") == 0
    capsys.readouterr()
    [record] = _command_records(fake_xrdfs)
    assert record["env"]["XRD_REQUESTTIMEOUT"] is None


def test_ls_xattr_without_long_is_not_queried(fake_xrdfs, capsys):
    assert dispatch("ls", ["--xattr", "user.test", _URL], prog="gfal ls") == 0
    capsys.readouterr()
    [record] = _command_records(fake_xrdfs)
    assert record["argv"] == ["ls", "--json", _URL]


def test_metadata_output_maps_broken_pipe_to_legacy_status(fake_xrdfs, monkeypatch):
    class BrokenStream:
        def write(self, _value):
            raise BrokenPipeError

        def flush(self):
            raise BrokenPipeError

    monkeypatch.setattr(sys, "stdout", BrokenStream())
    assert dispatch("stat", [_URL], prog="gfal stat") == 255


def test_invalid_metadata_schema_returns_protocol_error(
    fake_xrdfs, monkeypatch, capsys
):
    monkeypatch.setenv("FAKE_XRDFS_STDOUT", '{"path":"/data/file","size":null}\n')
    assert dispatch("stat", [_URL], prog="gfal stat") == getattr(errno, "EPROTO", 1)
    captured = capsys.readouterr()
    assert "invalid JSON from xrdfs" in captured.err
    assert "Traceback" not in captured.err


def test_xrdfs_not_found_maps_to_enoent(fake_xrdfs, capsys):
    missing = "root://storage.example//missing"
    assert dispatch("stat", [missing], prog="gfal stat") == errno.ENOENT
    assert "gfal stat error: 2" in capsys.readouterr().err


@pytest.mark.parametrize(
    ("command", "arguments", "expected"),
    [
        ("ls", [_URL], ["ls", "--json", _URL]),
        ("cat", [_URL], ["cat", _URL]),
        ("stat", [_URL], ["stat", "--json", _URL]),
        ("sum", [_URL, "ADLER32"], ["sum", _URL, "ADLER32"]),
        ("xattr", [_URL, "user.test"], ["xattr", _URL, "--", "user.test"]),
    ],
)
def test_aggregate_cli_routes_all_migrated_commands(
    fake_xrdfs, capsys, command, arguments, expected
):
    assert main(["gfal", command, *arguments]) == 0
    capsys.readouterr()
    [record] = _command_records(fake_xrdfs)
    assert record["argv"] == expected


def test_installed_entrypoint_target_runs_the_public_router(fake_xrdfs):
    process = _aggregate_process("stat", _URL)
    stdout, stderr = process.communicate(timeout=10)
    assert process.returncode == 0, stderr.decode("utf-8", errors="replace")
    assert f"  File: '{_URL}'".encode() in stdout
    [record] = _command_records(fake_xrdfs)
    assert record["argv"] == ["stat", "--json", _URL]


@pytest.mark.skipif(sys.platform == "win32", reason="SIGPIPE is POSIX-specific")
def test_cat_native_sigpipe_returns_legacy_status(fake_xrdfs, monkeypatch):
    monkeypatch.setenv("FAKE_XRDFS_CAT_SIZE", str(16 * 1024 * 1024))
    process = _aggregate_process("cat", _URL)
    assert process.stdout is not None
    assert process.stderr is not None
    try:
        assert process.stdout.read(1) == b"x"
        process.stdout.close()
        stderr = process.stderr.read()
        assert process.wait(timeout=10) == 255
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=5)
    assert b"Traceback" not in stderr
    assert b"BrokenPipeError" not in stderr


@pytest.mark.skipif(sys.platform == "win32", reason="pipe status is POSIX-specific")
@pytest.mark.parametrize(
    "arguments",
    [
        ("stat", _URL),
        ("sum", _URL, "ADLER32"),
    ],
)
def test_buffered_output_broken_pipe_returns_legacy_status(fake_xrdfs, arguments):
    process = _aggregate_process(*arguments)
    assert process.stdout is not None
    assert process.stderr is not None
    try:
        process.stdout.close()
        stderr = process.stderr.read()
        assert process.wait(timeout=10) == 255
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=5)
    assert b"Exception ignored" not in stderr
    assert b"BrokenPipeError" not in stderr
    assert b"Traceback" not in stderr


@pytest.mark.parametrize("option", ["--help", "--version"])
def test_aggregate_local_information_does_not_require_xrdfs(
    option, tmp_path, monkeypatch
):
    monkeypatch.setenv("GFAL_XRDFS", str(tmp_path / "missing-xrdfs"))
    with pytest.raises(SystemExit) as caught:
        main(["gfal", option])
    assert caught.value.code == 0


def test_aggregate_no_arguments_prints_help(capsys):
    with pytest.raises(SystemExit) as caught:
        main(["gfal"])
    assert caught.value.code == 0
    output = capsys.readouterr().out
    for command in ("ls", "cat", "stat", "sum", "xattr"):
        assert command in output


@pytest.mark.parametrize(
    "arguments",
    [
        ["gfal", "stat", "file:///tmp/local"],
        ["gfal", "stat", "--no-verify", "https://storage.example/file"],
        ["gfal", "ls", "--sort=size", _URL],
        ["gfal", "ls", "-lS", _URL],
        ["gfal", "cat", "-bq", _URL],
        ["gfal", "cat", _URL, "file:///tmp/local"],
        ["gfal", "stat", "--key", "root://keys.example/key", "file:///tmp/a"],
    ],
)
def test_aggregate_preserves_transitional_legacy_paths(monkeypatch, arguments):
    import gfal.cli.shell as legacy_shell

    calls = []

    def fake_main(argv):
        calls.append(argv)
        return 73

    monkeypatch.setattr(legacy_shell, "main", fake_main)
    assert main(arguments) == 73
    assert calls == [arguments]


@pytest.mark.parametrize("name", ["EOSAUTHZ", "GFAL_AUTHZ_TOKEN"])
def test_token_environment_preserves_legacy_backend(monkeypatch, name):
    import gfal.cli.shell as legacy_shell

    calls = []
    monkeypatch.setenv(name, "fixture-token")
    monkeypatch.setattr(legacy_shell, "main", lambda argv: calls.append(argv) or 73)
    arguments = ["gfal", "stat", "root://eos.example//data/file"]
    assert main(arguments) == 73
    assert calls == [arguments]


def test_router_accepts_long_ip_alias(fake_xrdfs, capsys):
    assert main(["gfal", "stat", "--ipv4", _URL]) == 0
    capsys.readouterr()
    [record] = _command_records(fake_xrdfs)
    assert record["argv"] == ["stat", "--json", _URL]
    assert record["env"]["XRD_NETWORKSTACK"] == "IPv4"


def test_router_treats_dash_prefixed_xattr_as_an_operand(fake_xrdfs, capsys):
    assert main(["gfal", "xattr", _URL, "--", "--no-verify"]) == 0
    assert capsys.readouterr().out == "fixture-value\n"
    [record] = _command_records(fake_xrdfs)
    assert record["argv"] == ["xattr", _URL, "--", "--no-verify"]


def test_multiple_remote_stat_operands_use_xrdfs(fake_xrdfs, capsys):
    other = "https://other.example/data/file"
    assert main(["gfal", "stat", _URL, other]) == 0
    captured = capsys.readouterr()
    assert f"  File: '{_URL}'" in captured.out
    assert f"  File: '{other}'" in captured.out
    assert [record["argv"] for record in _command_records(fake_xrdfs)] == [
        ["stat", "--json", _URL],
        ["stat", "--json", other],
    ]


def test_distribution_installs_only_aggregate_command():
    pyproject = (Path(__file__).resolve().parents[1] / "pyproject.toml").read_text(
        encoding="utf-8"
    )
    scripts = pyproject.split("[project.scripts]", 1)[1].split("[", 1)[0]
    assert scripts.strip() == 'gfal = "gfal.cli.main:main"'
    assert "gfal-" not in scripts
