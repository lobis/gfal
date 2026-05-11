"""Integration tests against the FNAL dCache dteam WebDAV endpoint.

These tests use the CERN dteam service-account proxy created by CI.  They write
only below ``/dcache/dteam/gfal/ci`` and clean up all created paths afterwards.
"""

from __future__ import annotations

import hashlib
import os
import socket
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import pytest

from conftest import CI, require_test_prereq
from helpers import run_gfal

pytestmark = [pytest.mark.integration, pytest.mark.network]

_DCACHE_HOST = "cmsdcadisk.fnal.gov"
_DCACHE_PORT = 2880
_DCACHE_BASE = f"https://{_DCACHE_HOST}:{_DCACHE_PORT}/dcache/dteam/gfal/ci"
_RUN_DIR_PREFIX = "pytest-run-"
_RUN_DIR_TIMESTAMP_FORMAT = "%Y%m%dT%H%M%SZ"
_STALE_RUN_DIR_AGE = timedelta(days=1)


@pytest.fixture(autouse=True)
def _plain_gfal_output(monkeypatch):
    monkeypatch.setenv("GFAL_CLI_GFAL2", "1")


def _find_proxy() -> Optional[str]:
    proxy = os.environ.get("X509_USER_PROXY", "")
    if proxy and Path(proxy).is_file():
        return proxy
    try:
        default = Path(f"/tmp/x509up_u{os.getuid()}")
    except AttributeError:
        return None
    return str(default) if default.is_file() else None


def _dcache_reachable() -> bool:
    try:
        with socket.create_connection((_DCACHE_HOST, _DCACHE_PORT), timeout=5):
            return True
    except OSError:
        return False


requires_dcache = pytest.mark.skipif(
    not _dcache_reachable() and not CI,
    reason=f"{_DCACHE_HOST}:{_DCACHE_PORT} not reachable",
)

requires_proxy = pytest.mark.skipif(
    _find_proxy() is None and not CI,
    reason="No X.509 proxy found (set X509_USER_PROXY or run voms-proxy-init)",
)


@pytest.fixture(scope="session")
def proxy_cert() -> str:
    path = _find_proxy()
    require_test_prereq(
        path is not None,
        "No X.509 proxy found (set X509_USER_PROXY or provision the CI proxy)",
    )
    return path


def _run(cmd: str, proxy_cert: str, *args: str, **kwargs):
    return run_gfal(
        cmd,
        "-E",
        proxy_cert,
        "--key",
        proxy_cert,
        "--no-verify",
        *args,
        **kwargs,
    )


def _run_dir_name() -> str:
    timestamp = datetime.now(timezone.utc).strftime(_RUN_DIR_TIMESTAMP_FORMAT)
    github_run_id = os.environ.get("GITHUB_RUN_ID")
    github_attempt = os.environ.get("GITHUB_RUN_ATTEMPT", "0")
    xdist_worker = os.environ.get("PYTEST_XDIST_WORKER", "gw0")
    suffix = uuid.uuid4().hex[:10]
    token = (
        f"gha-{github_run_id}-{github_attempt}-{xdist_worker}-{suffix}"
        if github_run_id
        else f"local-{suffix}"
    )
    return f"{_RUN_DIR_PREFIX}{timestamp}-{token}"


def _created_at_from_run_dir_name(name: str) -> Optional[datetime]:
    if not name.startswith(_RUN_DIR_PREFIX):
        return None
    timestamp = name.removeprefix(_RUN_DIR_PREFIX).split("-", maxsplit=1)[0]
    try:
        created_at = datetime.strptime(timestamp, _RUN_DIR_TIMESTAMP_FORMAT)
    except ValueError:
        return None
    return created_at.replace(tzinfo=timezone.utc)


def _child_names(ls_output: str) -> list[str]:
    names = []
    for line in ls_output.splitlines():
        text = line.strip().rstrip("/")
        if not text:
            continue
        name = text.split()[-1].rstrip("/").rsplit("/", maxsplit=1)[-1]
        names.append(name)
    return names


def _cleanup_stale_run_dirs(proxy_cert: str) -> None:
    rc, out, _err = _run("ls", proxy_cert, _DCACHE_BASE)
    if rc != 0:
        return

    cutoff = datetime.now(timezone.utc) - _STALE_RUN_DIR_AGE
    for name in _child_names(out):
        created_at = _created_at_from_run_dir_name(name)
        if created_at is None or created_at >= cutoff:
            continue
        _run("rm", proxy_cert, "-r", f"{_DCACHE_BASE}/{name}")


@pytest.fixture(scope="session")
def dcache_run_dir(proxy_cert):
    rc, out, err = _run("mkdir", proxy_cert, "-p", _DCACHE_BASE)
    require_test_prereq(
        rc == 0, f"Could not create dCache CI root {_DCACHE_BASE}: {err or out}"
    )

    _cleanup_stale_run_dirs(proxy_cert)

    url = f"{_DCACHE_BASE}/{_run_dir_name()}"
    rc, out, err = _run("mkdir", proxy_cert, url)
    require_test_prereq(
        rc == 0, f"Could not create dCache CI run dir {url}: {err or out}"
    )

    try:
        yield url
    finally:
        _run("rm", proxy_cert, "-r", url)


@pytest.fixture
def dcache_dir(proxy_cert, dcache_run_dir):
    name = f"case-{uuid.uuid4().hex[:10]}"
    url = f"{dcache_run_dir}/{name}"
    created: list[str] = []

    rc, out, err = _run("mkdir", proxy_cert, url)
    require_test_prereq(
        rc == 0, f"Could not create dCache scratch dir {url}: {err or out}"
    )

    def child(name: str) -> str:
        target = f"{url}/{name}"
        created.append(target)
        return target

    yield url, child

    for target in reversed(created):
        _run("rm", proxy_cert, target)
    _run("rm", proxy_cert, "-r", url)


@requires_dcache
@requires_proxy
def test_dcache_upload_download_and_stat(proxy_cert, dcache_dir, tmp_path):
    dcache_url, child = dcache_dir
    payload = b"fnal dcache integration payload\n" * 4
    src = tmp_path / "payload.bin"
    dst = tmp_path / "roundtrip.bin"
    src.write_bytes(payload)
    remote = child("payload.bin")

    rc, out, err = _run("cp", proxy_cert, src.as_uri(), remote)
    assert rc == 0, err or out

    rc, out, err = _run("stat", proxy_cert, remote)
    assert rc == 0, err or out
    assert str(len(payload)) in out
    assert "File:" in out

    rc, out, err = _run("cp", proxy_cert, remote, dst.as_uri())
    assert rc == 0, err or out
    assert dst.read_bytes() == payload

    rc, out, err = _run("ls", proxy_cert, dcache_url)
    assert rc == 0, err or out
    assert "payload.bin" in out


@requires_dcache
@requires_proxy
def test_dcache_save_cat_and_sum(proxy_cert, dcache_dir):
    _dcache_url, child = dcache_dir
    payload = b"saved through stdin\n"
    remote = child("saved.txt")

    rc, out, err = _run("save", proxy_cert, remote, input=payload.decode())
    assert rc == 0, err or out

    rc, out, err = _run("cat", proxy_cert, remote)
    assert rc == 0, err
    assert out.encode() == payload

    rc, out, err = _run("sum", proxy_cert, remote, "MD5")
    assert rc == 0, err or out
    assert hashlib.md5(payload).hexdigest() in out


@requires_dcache
@requires_proxy
def test_dcache_rename_and_remove(proxy_cert, dcache_dir, tmp_path):
    _dcache_url, child = dcache_dir
    src = tmp_path / "rename-source.txt"
    src.write_text("rename me\n")
    original = child("original.txt")
    renamed = child("renamed.txt")

    rc, out, err = _run("cp", proxy_cert, src.as_uri(), original)
    assert rc == 0, err or out

    rc, out, err = _run("rename", proxy_cert, original, renamed)
    assert rc == 0, err or out

    rc, out, err = _run("cat", proxy_cert, renamed)
    assert rc == 0, err
    assert out == "rename me\n"

    rc, out, err = _run("rm", proxy_cert, renamed)
    assert rc == 0, err or out
