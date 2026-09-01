"""Tests for the dependency-free ``gfal cp`` adapter."""

from __future__ import annotations

import pytest

import gfal.cli.cp as cp_cli
from gfal.xrdfs import GFAL_ENOTSUP, XrdfsResult

_SOURCE = "root://source.example//data/input"
_DESTINATION = "https://destination.example/data/output"


@pytest.fixture
def xrdcp_calls(monkeypatch):
    calls = []
    monkeypatch.setattr(
        cp_cli,
        "prepare_xrdcp",
        lambda *_args, **_kwargs: ("/usr/bin/xrdcp", 0),
    )

    def fake_run(executable, arguments, **kwargs):
        calls.append((executable, list(arguments), kwargs))
        return XrdfsResult(returncode=0, stdout=b"", stderr=b"")

    monkeypatch.setattr(cp_cli, "run_xrdcp", fake_run)
    return calls


def test_cp_maps_supported_gfal2_options_to_xrdcp(xrdcp_calls):
    result = cp_cli.dispatch_cp([
        "--force",
        "--recursive",
        "--nbstreams",
        "4",
        "--checksum",
        "ADLER32:deadbeef",
        "--authz-token",
        "secret-token",
        _SOURCE,
        _DESTINATION,
    ])

    assert result == 0
    [(_executable, arguments, kwargs)] = xrdcp_calls
    assert arguments == [
        "--nopbar",
        "--force",
        "--recursive",
        "--streams",
        "4",
        "--cksum",
        "adler32:deadbeef",
        "--rm-bad-cksum",
        _SOURCE,
        _DESTINATION,
    ]
    assert kwargs["environ"]["BEARER_TOKEN"] == "secret-token"


@pytest.mark.parametrize(
    "arguments",
    [
        ["-s", "source-token", _SOURCE, _DESTINATION],
        ["-S", "destination-token", _SOURCE, _DESTINATION],
        ["--copy-mode", "push", _SOURCE, _DESTINATION],
        ["--checksum-mode", "source", _SOURCE, _DESTINATION],
        ["--just-copy", _SOURCE, _DESTINATION],
        ["--no-delegation", _SOURCE, _DESTINATION],
        ["--scitag", "65", _SOURCE, _DESTINATION],
        ["--tcp-buffersize", "4096", _SOURCE, _DESTINATION],
        ["--evict", _SOURCE, _DESTINATION],
        ["--no-verify", _SOURCE, _DESTINATION],
    ],
)
def test_cp_rejects_options_that_cannot_be_preserved(arguments, capsys):
    assert cp_cli.dispatch_cp(arguments) == GFAL_ENOTSUP
    assert "not implemented" in capsys.readouterr().err


def test_cp_from_file_and_limit_build_independent_jobs(xrdcp_calls, tmp_path):
    sources = tmp_path / "sources.txt"
    sources.write_text(f"{_SOURCE}\nroot://source.example//data/second\n")

    assert (
        cp_cli.dispatch_cp(["--from-file", str(sources), "--limit", "1", _DESTINATION])
        == 0
    )
    assert len(xrdcp_calls) == 1
    assert xrdcp_calls[0][1][-2:] == [
        _SOURCE,
        f"{_DESTINATION}/input",
    ]


@pytest.mark.parametrize("value", ("0", "-1"))
def test_cp_limit_must_be_positive(value):
    with pytest.raises(SystemExit) as caught:
        cp_cli.dispatch_cp(["--limit", value, _SOURCE, _DESTINATION])
    assert caught.value.code == 2
