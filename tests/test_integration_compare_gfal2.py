"""Comparison integration tests: legacy gfal2-utils vs new gfal CLI.

These tests always run through the Alma Linux Docker image for public read-only
comparisons so both command families execute in the same environment with CERN
CAs, XRootD support, and legacy gfal2 utilities available.

For writable EOSPilot HTTPS operations, the new CLI follows the same native
`run_gfal(..., -E proxy, --no-verify, ...)` path already proven by the main
integration suite, while legacy gfal2-utils still run inside Docker.
"""

from typing import Optional
from uuid import uuid4

import pytest

from helpers import docker_available, run_gfal, run_gfal2_docker, run_gfal_docker
from test_integration_eospilot import _PILOT_BASE, _PUBSRC, _find_proxy

pytestmark = [pytest.mark.integration, pytest.mark.network]


_LEGACY_PROBE_CACHE: Optional[tuple[bool, str]] = None


def _legacy_gfal2_probe() -> tuple[bool, str]:
    global _LEGACY_PROBE_CACHE
    if _LEGACY_PROBE_CACHE is not None:
        return _LEGACY_PROBE_CACHE
    rc, out, err = run_gfal2_docker("stat", _PUBSRC)
    if rc == 0:
        _LEGACY_PROBE_CACHE = (True, "")
    else:
        msg = (err or out or "legacy gfal2 unavailable").strip()
        _LEGACY_PROBE_CACHE = (False, msg)
    return _LEGACY_PROBE_CACHE


def _xfail_if_legacy_unusable():
    ok, reason = _legacy_gfal2_probe()
    if not ok:
        pytest.xfail(f"Legacy gfal2-utils unusable in current Alma image: {reason}")


def _skip_if_known_public_http_flake(rc: int, err: str):
    lowered = (err or "").lower()
    if rc != 0 and (
        "host is down" in lowered
        or "could not read status line" in lowered
        or "connection reset by peer" in lowered
        or "result (neon)" in lowered
    ):
        pytest.skip(f"Known flaky public HTTP transport failure in CI: {err}")


def _unique_pilot_path(stem: str) -> str:
    return f"{_PILOT_BASE}/{stem}-{uuid4().hex}"


requires_docker = pytest.mark.skipif(
    not docker_available(), reason="Docker image xrootd-cern-test not available"
)
requires_proxy = pytest.mark.skipif(
    _find_proxy() is None,
    reason="No X.509 proxy found (set X509_USER_PROXY or run voms-proxy-init)",
)


@requires_docker
class TestLegacyGfal2Runtime:
    def test_legacy_probe_reports_known_alma_runtime_issue(self):
        ok, reason = _legacy_gfal2_probe()
        if ok:
            pytest.skip("Legacy gfal2-utils are usable in this image")
        lowered = reason.lower()
        if "gfal-stat: command not found" in lowered:
            pytest.skip("Legacy gfal2-utils are not installed in this Docker image")
        assert "initialization of gfal2 raised unreported exception" in lowered
        assert "boost.python.enum" in lowered


@requires_docker
class TestCompareEosPublic:
    def test_ls_root_xrootd_matches_legacy(self):
        _xfail_if_legacy_unusable()
        target = "root://eospublic.cern.ch//eos/opendata/phenix/"

        rc_new, out_new, err_new = run_gfal_docker("ls", target)
        rc_old, out_old, err_old = run_gfal2_docker("ls", target)

        assert rc_new == 0, err_new
        assert rc_old == 0, err_old
        assert "emcal-finding-pi0s-and-photons" in out_new
        assert "emcal-finding-pi0s-and-photons" in out_old

    def test_stat_http_matches_legacy_size(self):
        _xfail_if_legacy_unusable()
        rc_new, out_new, err_new = run_gfal_docker("stat", _PUBSRC)
        rc_old, out_old, err_old = run_gfal2_docker("stat", _PUBSRC)

        assert rc_new == 0, err_new
        assert rc_old == 0, err_old
        assert "2184" in out_new
        assert "2184" in out_old

    def test_cat_http_matches_legacy_bytes(self):
        _xfail_if_legacy_unusable()
        rc_new, out_new, err_new = run_gfal_docker("cat", _PUBSRC)
        rc_old, out_old, err_old = run_gfal2_docker("cat", _PUBSRC)

        _skip_if_known_public_http_flake(rc_new, err_new)
        _skip_if_known_public_http_flake(rc_old, err_old)
        assert rc_new == 0, err_new
        assert rc_old == 0, err_old
        assert out_new.encode() == out_old.encode()

    @pytest.mark.parametrize("algorithm", ["ADLER32"])
    def test_sum_http_matches_legacy_checksum(self, algorithm):
        _xfail_if_legacy_unusable()
        rc_new, out_new, err_new = run_gfal_docker("sum", _PUBSRC, algorithm)
        rc_old, out_old, err_old = run_gfal2_docker("sum", _PUBSRC, algorithm)

        assert rc_new == 0, err_new
        assert rc_old == 0, err_old
        assert (
            out_new.strip().split()[-1].lower() == out_old.strip().split()[-1].lower()
        )


@requires_docker
@requires_proxy
class TestCompareEosPilot:
    def test_mkdir_and_ls_match_legacy(self):
        _xfail_if_legacy_unusable()
        proxy = _find_proxy()
        target = _unique_pilot_path("compare-gfal2-utils-dir")

        try:
            rc_new, out_new, err_new = run_gfal(
                "mkdir", "-E", proxy, "--no-verify", "-p", target
            )
            rc_old, out_old, err_old = run_gfal2_docker(
                "mkdir", "-p", target, proxy_cert=proxy
            )

            assert rc_new == 0, err_new
            assert rc_old == 0, err_old

            rc_new, out_new, err_new = run_gfal(
                "ls", "-E", proxy, "--no-verify", f"{_PILOT_BASE}/"
            )
            rc_old, out_old, err_old = run_gfal2_docker(
                "ls", f"{_PILOT_BASE}/", proxy_cert=proxy
            )

            assert rc_new == 0, err_new
            assert rc_old == 0, err_old
            assert "compare-gfal2-utils" in out_new
            assert "compare-gfal2-utils" in out_old
        finally:
            run_gfal("rm", "-E", proxy, "--no-verify", "-r", target)

    def test_copy_and_stat_match_legacy(self):
        _xfail_if_legacy_unusable()
        proxy = _find_proxy()
        payload = "compare old and new gfal\n"
        remote = _unique_pilot_path("compare-gfal2-utils-copy.bin")

        try:
            rc_new, out_new, err_new = run_gfal(
                "save", "-E", proxy, "--no-verify", remote, input=payload
            )
            assert rc_new == 0, err_new

            rc_old, out_old, err_old = run_gfal2_docker(
                "stat", remote, proxy_cert=proxy
            )
            rc_new2, out_new2, err_new2 = run_gfal(
                "stat", "-E", proxy, "--no-verify", remote
            )

            assert rc_old == 0, err_old
            assert rc_new2 == 0, err_new2
            assert str(len(payload.encode())) in out_old
            assert str(len(payload.encode())) in out_new2
        finally:
            run_gfal("rm", "-E", proxy, "--no-verify", remote)

    def test_save_and_cat_match_legacy(self):
        _xfail_if_legacy_unusable()
        proxy = _find_proxy()
        remote = _unique_pilot_path("compare-gfal2-utils-save.txt")
        payload = "compare via save\nline two\n"

        try:
            rc_new, out_new, err_new = run_gfal(
                "save", "-E", proxy, "--no-verify", remote, input=payload
            )
            assert rc_new == 0, err_new

            rc_new2, out_new2, err_new2 = run_gfal(
                "cat", "-E", proxy, "--no-verify", remote
            )
            rc_old, out_old, err_old = run_gfal2_docker("cat", remote, proxy_cert=proxy)

            assert rc_new2 == 0, err_new2
            assert rc_old == 0, err_old
            assert out_new2 == payload
            assert out_old == payload
        finally:
            run_gfal("rm", "-E", proxy, "--no-verify", remote)

    def test_rename_and_rm_match_legacy(self):
        _xfail_if_legacy_unusable()
        proxy = _find_proxy()
        source = _unique_pilot_path("compare-gfal2-utils-rename-src.txt")
        dest = _unique_pilot_path("compare-gfal2-utils-rename-dst.txt")
        payload = "rename me\n"

        rc_new, out_new, err_new = run_gfal(
            "save", "-E", proxy, "--no-verify", source, input=payload
        )
        assert rc_new == 0, err_new

        try:
            rc_new2, out_new2, err_new2 = run_gfal(
                "rename", "-E", proxy, "--no-verify", source, dest
            )
            assert rc_new2 == 0, err_new2

            rc_old, out_old, err_old = run_gfal2_docker("stat", dest, proxy_cert=proxy)
            assert rc_old == 0, err_old

            rc_new3, out_new3, err_new3 = run_gfal(
                "rm", "-E", proxy, "--no-verify", dest
            )
            assert rc_new3 == 0, err_new3

            rc_old2, out_old2, err_old2 = run_gfal2_docker(
                "stat", dest, proxy_cert=proxy
            )
            assert rc_old2 != 0
        finally:
            run_gfal("rm", "-E", proxy, "--no-verify", source)
            run_gfal("rm", "-E", proxy, "--no-verify", dest)
