"""Generic integration contract for live EOS / dCache style deployments."""

from __future__ import annotations

import uuid
from pathlib import Path

import pytest

from conftest import require_test_prereq
from deployment_env import (
    deployment_skip_reason,
    join_remote,
    load_deployment_config,
    run_deployment_gfal,
)

pytestmark = [pytest.mark.integration]


@pytest.fixture(scope="session")
def deployment():
    config = load_deployment_config()
    require_test_prereq(config is not None, deployment_skip_reason())
    return config


def _cleanup_remote(deployment, *urls: str) -> None:
    for url in urls:
        if url:
            run_deployment_gfal(deployment, "rm", "-r", url)


def _seed_remote(deployment, tmp_path: Path, target_url: str, data: bytes) -> None:
    seed = tmp_path / f"seed-{uuid.uuid4().hex}.bin"
    seed.write_bytes(data)
    rc, out, err = run_deployment_gfal(deployment, "cp", seed.as_uri(), target_url)
    assert rc == 0, err or out


def _materialize_source(deployment, tmp_path: Path, kind: str, data: bytes):
    name = f"src-{uuid.uuid4().hex}.bin"
    if kind == "local":
        src = tmp_path / name
        src.write_bytes(data)
        return src.as_uri(), []
    if kind == "http":
        target = join_remote(deployment.http_writable_base, name)
        _seed_remote(deployment, tmp_path, target, data)
        return target, [target]
    if kind == "root":
        target = join_remote(deployment.root_writable_base, name)
        _seed_remote(deployment, tmp_path, target, data)
        return target, [target]
    raise ValueError(f"Unsupported source kind: {kind}")


def _destination_url(deployment, tmp_path: Path, kind: str):
    name = f"dst-{uuid.uuid4().hex}.bin"
    if kind == "local":
        return (tmp_path / name).as_uri(), []
    if kind == "http":
        target = join_remote(deployment.http_writable_base, name)
        return target, [target]
    if kind == "root":
        target = join_remote(deployment.root_writable_base, name)
        return target, [target]
    raise ValueError(f"Unsupported destination kind: {kind}")


def _assert_remote_bytes(deployment, tmp_path: Path, url: str, expected: bytes) -> None:
    back = tmp_path / f"roundtrip-{uuid.uuid4().hex}.bin"
    rc, out, err = run_deployment_gfal(deployment, "cp", url, back.as_uri())
    assert rc == 0, err or out
    assert back.read_bytes() == expected


def test_deployment_contract_has_a_remote_endpoint(deployment):
    assert deployment.has_http or deployment.has_root


@pytest.mark.parametrize("src_kind,dst_kind", [("local", "http"), ("http", "local")])
def test_http_copy_roundtrip(deployment, tmp_path, src_kind, dst_kind):
    if not deployment.has_http:
        pytest.skip(f"{deployment.name} does not expose an HTTPS/WebDAV endpoint")

    payload = b"http deployment roundtrip\n" * 8
    src_url, src_cleanup = _materialize_source(deployment, tmp_path, src_kind, payload)
    dst_url, dst_cleanup = _destination_url(deployment, tmp_path, dst_kind)

    try:
        rc, out, err = run_deployment_gfal(deployment, "cp", src_url, dst_url)
        assert rc == 0, err or out

        if dst_kind == "local":
            assert Path(dst_url.removeprefix("file://")).read_bytes() == payload
        else:
            _assert_remote_bytes(deployment, tmp_path, dst_url, payload)
    finally:
        _cleanup_remote(deployment, *(src_cleanup + dst_cleanup))


@pytest.mark.parametrize("src_kind,dst_kind", [("local", "root"), ("root", "local")])
def test_xrootd_copy_roundtrip(deployment, tmp_path, src_kind, dst_kind):
    if not deployment.has_root:
        pytest.skip(f"{deployment.name} does not expose an XRootD endpoint")

    payload = b"xrootd deployment roundtrip\n" * 8
    src_url, src_cleanup = _materialize_source(deployment, tmp_path, src_kind, payload)
    dst_url, dst_cleanup = _destination_url(deployment, tmp_path, dst_kind)

    try:
        rc, out, err = run_deployment_gfal(deployment, "cp", src_url, dst_url)
        assert rc == 0, err or out

        if dst_kind == "local":
            assert Path(dst_url.removeprefix("file://")).read_bytes() == payload
        else:
            _assert_remote_bytes(deployment, tmp_path, dst_url, payload)
    finally:
        _cleanup_remote(deployment, *(src_cleanup + dst_cleanup))


@pytest.mark.parametrize("src_kind,dst_kind", [("http", "root"), ("root", "http")])
def test_http_root_bridge_copy(deployment, tmp_path, src_kind, dst_kind):
    if not (deployment.has_http and deployment.has_root):
        pytest.skip(f"{deployment.name} does not expose both HTTP and XRootD endpoints")

    payload = f"{src_kind} to {dst_kind} bridge\n".encode() * 8
    src_url, src_cleanup = _materialize_source(deployment, tmp_path, src_kind, payload)
    dst_url, dst_cleanup = _destination_url(deployment, tmp_path, dst_kind)

    try:
        rc, out, err = run_deployment_gfal(deployment, "cp", src_url, dst_url)
        assert rc == 0, err or out
        if dst_kind == "local":
            assert Path(dst_url.removeprefix("file://")).read_bytes() == payload
        else:
            _assert_remote_bytes(deployment, tmp_path, dst_url, payload)
    finally:
        _cleanup_remote(deployment, *(src_cleanup + dst_cleanup))


@pytest.mark.parametrize(
    "kind",
    ["http", "root"],
)
def test_remote_listing_sees_uploaded_file(deployment, tmp_path, kind):
    if kind == "http" and not deployment.has_http:
        pytest.skip(f"{deployment.name} does not expose an HTTPS/WebDAV endpoint")
    if kind == "root" and not deployment.has_root:
        pytest.skip(f"{deployment.name} does not expose an XRootD endpoint")
    if not deployment.supports_listing:
        pytest.skip(f"{deployment.name} deployment contract disables directory listing")

    payload = b"listing check\n"
    target_base = (
        deployment.http_writable_base
        if kind == "http"
        else deployment.root_writable_base
    )
    target = join_remote(target_base, f"listing-{uuid.uuid4().hex}.txt")

    try:
        _seed_remote(deployment, tmp_path, target, payload)
        rc, out, err = run_deployment_gfal(deployment, "ls", target_base)
        assert rc == 0, err or out
        assert Path(target).name in out
    finally:
        _cleanup_remote(deployment, target)


@pytest.mark.parametrize(
    "kind,error_markers",
    [
        ("http", ("Permission denied", "403", "access denied")),
        ("root", ("Permission denied", "3010", "access denied")),
    ],
)
def test_permission_denied_paths_reject_writes(
    deployment, tmp_path, kind, error_markers
):
    denied_base = (
        deployment.http_denied_base if kind == "http" else deployment.root_denied_base
    )
    if kind == "http" and not denied_base:
        pytest.skip(f"{deployment.name} does not define an HTTPS denied path")
    if kind == "root" and not denied_base:
        pytest.skip(f"{deployment.name} does not define an XRootD denied path")

    src = tmp_path / "denied.bin"
    src.write_bytes(b"denied\n")
    target = join_remote(denied_base, f"denied-{uuid.uuid4().hex}.bin")

    rc, _out, err = run_deployment_gfal(deployment, "cp", src.as_uri(), target)

    assert rc != 0
    assert any(marker.lower() in err.lower() for marker in error_markers)
