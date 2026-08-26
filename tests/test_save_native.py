"""Tests for the dependency-free ``gfal save`` adapter.

The observable behavior asserted here comes from ``gfal-save`` in
gfal2-utils, not from the transitional Python implementation in this repo.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from helpers import run_gfal_router

_REMOTE = "root://storage.example//data/output.bin"
_HELP = """Usage: xrdcp [options] source destination
using a dash (-) for <src> uses stdin
--force
--nopbar
--silent
"""
_FAKE_XRDCP = f"""#!/usr/bin/env python3
import json
import os
import sys
import time

if sys.argv[1:] == ["--help"]:
    sys.stdout.write(os.environ.get("FAKE_XRDCP_HELP", {_HELP!r}))
    raise SystemExit(int(os.environ.get("FAKE_XRDCP_HELP_STATUS", "0")))

delay = float(os.environ.get("FAKE_XRDCP_DELAY", "0"))
if delay:
    time.sleep(delay)
payload = sys.stdin.buffer.read()
record = {{
    "argv": sys.argv[1:],
    "payload": payload.hex(),
    "environment": {{
        name: os.environ.get(name)
        for name in (
            "X509_USER_CERT",
            "X509_USER_KEY",
            "X509_USER_PROXY",
            "XRD_HTTPCLIENTCERTFILE",
            "XRD_HTTPCLIENTKEYFILE",
            "XRD_NETWORKSTACK",
            "XRD_REQUESTTIMEOUT",
        )
    }},
}}
with open(os.environ["FAKE_XRDCP_RECORD"], "a", encoding="utf-8") as stream:
    stream.write(json.dumps(record) + "\\n")
sys.stdout.write(os.environ.get("FAKE_XRDCP_STDOUT", ""))
sys.stderr.write(os.environ.get("FAKE_XRDCP_STDERR", ""))
raise SystemExit(int(os.environ.get("FAKE_XRDCP_STATUS", "0")))
"""


@pytest.fixture
def fake_xrdcp(tmp_path, monkeypatch):
    executable = tmp_path / "xrdcp"
    executable.write_text(_FAKE_XRDCP, encoding="utf-8")
    executable.chmod(0o755)
    record = tmp_path / "xrdcp-record.jsonl"
    monkeypatch.setenv("GFAL_XRDCP", str(executable))
    monkeypatch.setenv("FAKE_XRDCP_RECORD", str(record))
    return record


def _records(path: Path):
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text().splitlines()]


def _run_router_binary(*arguments, input_bytes=b"", env=None):
    code = (
        "import sys; from gfal.cli.main import main; "
        "raise SystemExit(main(['gfal', *sys.argv[1:]]))"
    )
    environment = {**os.environ, "PYTHONUTF8": "1"}
    if env:
        environment.update(env)
    result = subprocess.run(
        [sys.executable, "-c", code, *arguments],
        input=input_bytes,
        capture_output=True,
        env=environment,
        timeout=10,
        check=False,
    )
    return result.returncode, result.stdout, result.stderr


@pytest.mark.parametrize("destination_kind", ("uri", "path"))
def test_local_save_matches_legacy_silent_overwrite(tmp_path, destination_kind):
    destination = tmp_path / "output.bin"
    destination.write_bytes(b"old content that must be truncated")
    operand = destination.as_uri() if destination_kind == "uri" else str(destination)

    status, stdout, stderr = _run_router_binary(
        "save", operand, input_bytes=b"new\x00bytes"
    )

    assert status == 0
    assert stdout == b""
    assert stderr == b""
    assert destination.read_bytes() == b"new\x00bytes"


def test_local_save_accepts_empty_stdin(tmp_path):
    destination = tmp_path / "empty.bin"

    status, stdout, stderr = _run_router_binary("save", destination.as_uri())

    assert (status, stdout, stderr) == (0, b"", b"")
    assert destination.read_bytes() == b""


def test_local_save_missing_parent_matches_legacy_error(tmp_path):
    destination = tmp_path / "missing" / "output.bin"

    status, stdout, stderr = _run_router_binary(
        "save", destination.as_uri(), input_bytes=b"data"
    )

    assert status == 2
    assert stdout == b""
    assert stderr.decode() == (
        "gfal save error: 2 (No such file or directory) - errno reported by "
        "local system call No such file or directory\n"
    )


def test_remote_save_streams_binary_stdin_through_xrdcp(fake_xrdcp):
    payload = bytes(range(256)) + b"\x00\xff"

    status, stdout, stderr = _run_router_binary("save", _REMOTE, input_bytes=payload)

    assert (status, stdout, stderr) == (0, b"", b"")
    assert _records(fake_xrdcp) == [
        {
            "argv": ["--force", "--nopbar", "--silent", "-", _REMOTE],
            "payload": payload.hex(),
            "environment": {
                "X509_USER_CERT": None,
                "X509_USER_KEY": None,
                "X509_USER_PROXY": os.environ.get("X509_USER_PROXY"),
                "XRD_HTTPCLIENTCERTFILE": None,
                "XRD_HTTPCLIENTKEYFILE": None,
                "XRD_NETWORKSTACK": None,
                "XRD_REQUESTTIMEOUT": "1800",
            },
        }
    ]


def test_remote_save_maps_common_credentials_and_network(fake_xrdcp):
    status, stdout, stderr = run_gfal_router(
        "save",
        "-t",
        "17",
        "-E",
        "/tmp/proxy.pem",
        "--key",
        "/tmp/key.pem",
        "--ipv4",
        _REMOTE,
        input="payload",
    )

    assert (status, stdout, stderr) == (0, "", "")
    [record] = _records(fake_xrdcp)
    assert record["environment"] == {
        "X509_USER_CERT": "/tmp/proxy.pem",
        "X509_USER_KEY": "/tmp/key.pem",
        "X509_USER_PROXY": None,
        "XRD_HTTPCLIENTCERTFILE": "/tmp/proxy.pem",
        "XRD_HTTPCLIENTKEYFILE": "/tmp/key.pem",
        "XRD_NETWORKSTACK": "IPv4",
        "XRD_REQUESTTIMEOUT": "17",
    }


@pytest.mark.parametrize("scheme", ("https", "davs", "roots"))
def test_remote_save_accepts_xrootd_client_schemes(fake_xrdcp, scheme):
    destination = f"{scheme}://storage.example/data/output"

    status, stdout, stderr = run_gfal_router("save", destination, input="data")

    assert (status, stdout, stderr) == (0, "", "")
    assert _records(fake_xrdcp)[0]["argv"][-1] == destination


def test_save_rejects_unsupported_protocol_without_old_backend(fake_xrdcp):
    status, stdout, stderr = run_gfal_router(
        "save", "s3://storage.example/bucket/output", input="data"
    )

    assert status == 93
    assert stdout == ""
    assert stderr == (
        "gfal save error: 93 (Protocol not supported) - Protocol not supported "
        "or path/url invalid: s3://storage.example/bucket/output\n"
    )
    assert _records(fake_xrdcp) == []


def test_remote_save_reports_missing_xrdcp(tmp_path):
    status, stdout, stderr = run_gfal_router(
        "save",
        _REMOTE,
        input="data",
        env={"GFAL_XRDCP": str(tmp_path / "missing-xrdcp")},
    )

    assert status == 127
    assert stdout == ""
    assert "install xrootd-client" in stderr


def test_remote_save_rejects_incompatible_xrdcp(fake_xrdcp, monkeypatch):
    monkeypatch.setenv("FAKE_XRDCP_HELP", "old xrdcp help\n")

    status, stdout, stderr = run_gfal_router("save", _REMOTE, input="data")

    assert status == 69
    assert stdout == ""
    assert "incompatible xrdcp" in stderr
    assert _records(fake_xrdcp) == []


def test_save_help_does_not_require_xrdcp_or_old_backend(tmp_path):
    code = (
        "import sys; from gfal.cli.main import main; "
        "\ntry: main(['gfal', 'save', '--help'])\n"
        "except SystemExit as error: assert error.code == 0\n"
        "assert 'fsspec' not in sys.modules; "
        "assert 'aiohttp' not in sys.modules; "
        "assert 'XRootD' not in sys.modules"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
        env={**os.environ, "GFAL_XRDCP": str(tmp_path / "missing-xrdcp")},
        check=False,
    )

    assert result.returncode == 0, result.stderr
