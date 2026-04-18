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

from conftest import CI, require_test_prereq
from helpers import (
    _docker_run_command,
    docker_available,
    run_gfal,
    run_gfal2_docker,
    run_gfal_docker,
)
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
        require_test_prereq(False, f"Known flaky public HTTP transport failure: {err}")


def _unique_pilot_path(stem: str) -> str:
    return f"{_PILOT_BASE}/{stem}-{uuid4().hex}"


def _copy_preserves_mtime_in_docker(command: str) -> tuple[int, bool, str]:
    uid = uuid4().hex[:12]
    src_path = f"/tmp/gfal-copy-src-{uid}"
    dst_path = f"/tmp/gfal-copy-dst-{uid}"
    # Replace placeholder paths in the command with unique paths
    resolved_command = command.replace(
        "file:///tmp/gfal-copy-src", f"file://{src_path}"
    ).replace("file:///tmp/gfal-copy-dst", f"file://{dst_path}")
    script = f"""
set -e
rm -rf /var/tmp/gfal-src
rm -f {src_path} {dst_path}
printf 'mtime test\\n' >{src_path}
touch -t 200001010000 {src_path}
{resolved_command}
python3 - <<'PY'
from pathlib import Path

src = Path('{src_path}')
dst = Path('{dst_path}')
print(int(src.stat().st_mtime) == int(dst.stat().st_mtime))
PY
rm -f {src_path} {dst_path}
"""
    rc, out, err = _docker_run_command(script)
    preserved = (
        out.strip().splitlines()[-1] == "True" if rc == 0 and out.strip() else False
    )
    return rc, preserved, err or out


def _copy_existing_dst_error_in_docker(command: str) -> tuple[int, str]:
    uid = uuid4().hex[:12]
    src_path = f"/tmp/gfal-copy-src-{uid}"
    dst_path = f"/tmp/gfal-copy-dst-{uid}"
    out_file = f"/tmp/copy-{uid}.out"
    err_file = f"/tmp/copy-{uid}.err"
    # Replace placeholder paths in the command with unique paths
    resolved_command = command.replace(
        "file:///tmp/gfal-copy-src", f"file://{src_path}"
    ).replace("file:///tmp/gfal-copy-dst", f"file://{dst_path}")
    script = f"""
set -e
rm -f {src_path} {dst_path}
printf 'src\\n' >{src_path}
printf 'dst\\n' >{dst_path}
set +e
{resolved_command} >{out_file} 2>{err_file}
rc=$?
set -e
printf '%s\\n' "$rc"
cat {err_file}
rm -f {src_path} {dst_path} {out_file} {err_file}
"""
    rc, out, err = _docker_run_command(script)
    assert rc == 0, err or out
    lines = out.splitlines()
    return int(lines[0]), "\n".join(lines[1:])


requires_docker = pytest.mark.skipif(
    not docker_available() and not CI,
    reason="Docker image xrootd-cern-test not available",
)
requires_proxy = pytest.mark.skipif(
    _find_proxy() is None and not CI,
    reason="No X.509 proxy found (set X509_USER_PROXY or run voms-proxy-init)",
)


@requires_docker
class TestLegacyGfal2Runtime:
    def test_legacy_probe_reports_usable_runtime(self):
        ok, reason = _legacy_gfal2_probe()
        if not ok and "gfal-stat: command not found" in reason.lower():
            require_test_prereq(
                False, "Legacy gfal2-utils are not installed in this Docker image"
            )
        assert ok, reason

    def test_copy_preserves_mtime_by_default(self):
        """New gfal cp preserves mtime by default (--preserve-times defaults on)."""
        new_cmd = (
            "cp -r /repo /var/tmp/gfal-src && "
            "python3 -m pip install -q --no-deps /var/tmp/gfal-src > /dev/null 2>&1 && "
            "gfal cp file:///tmp/gfal-copy-src file:///tmp/gfal-copy-dst"
        )
        rc_new, new_preserved, err_new = _copy_preserves_mtime_in_docker(new_cmd)
        assert rc_new == 0, err_new
        assert new_preserved

    def test_legacy_copy_does_not_preserve_mtime(self):
        """Legacy gfal2-util gfal-copy does NOT preserve mtime by default."""
        _xfail_if_legacy_unusable()
        rc_old, old_preserved, err_old = _copy_preserves_mtime_in_docker(
            "GFAL_PYTHONBIN=/usr/bin/python3 "
            "gfal-copy file:///tmp/gfal-copy-src file:///tmp/gfal-copy-dst"
        )
        if rc_old != 0 and "gfal-copy: command not found" in err_old.lower():
            pytest.xfail("Legacy gfal2-utils are not installed in this Docker image")
        assert rc_old == 0, err_old
        assert not old_preserved

    def test_copy_existing_dst_behavior(self):
        """gfal cp without -f to an existing dst returns EEXIST (17).

        The new CLI default is None (no compare), so it errors with EEXIST when
        the destination already exists — same behaviour as legacy gfal2-utils.
        """
        rc_new, err_new = _copy_existing_dst_error_in_docker(
            "cp -r /repo /var/tmp/gfal-src && "
            "python3 -m pip install -q --no-deps /var/tmp/gfal-src > /dev/null 2>&1 && "
            "gfal cp file:///tmp/gfal-copy-src file:///tmp/gfal-copy-dst"
        )
        # New CLI: default compare=None → EEXIST (17)
        assert rc_new == 17, (
            f"expected 17 (EEXIST, no compare set), got {rc_new}: {err_new}"
        )
        assert "exists and overwrite is not set" in " ".join(err_new.split())

        _xfail_if_legacy_unusable()
        rc_old, err_old = _copy_existing_dst_error_in_docker(
            "GFAL_PYTHONBIN=/usr/bin/python3 "
            "gfal-copy file:///tmp/gfal-copy-src file:///tmp/gfal-copy-dst"
        )
        # Legacy also returns EEXIST
        assert rc_old == 17
        assert "exists and overwrite is not set" in " ".join(err_old.split())


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
class TestCompareExitCodesEosPublic:
    """Compare exact exit codes for error scenarios (not just rc != 0)."""

    def test_stat_nonexistent_exit_code_matches_legacy(self):
        """gfal stat on a missing path must return ENOENT (2) like gfal2."""
        _xfail_if_legacy_unusable()
        missing = (
            "root://eospublic.cern.ch//eos/opendata/phenix/does_not_exist_gfal_test"
        )

        rc_new, out_new, err_new = run_gfal_docker("stat", missing)
        rc_old, out_old, err_old = run_gfal2_docker("stat", missing)

        assert rc_new == 2, f"expected ENOENT(2), got {rc_new}: {err_new}"
        assert rc_old == 2, f"legacy returned {rc_old}: {err_old}"

    def test_ls_nonexistent_exit_code_matches_legacy(self):
        """gfal ls on a missing path must return ENOENT (2) like gfal2."""
        _xfail_if_legacy_unusable()
        missing = "root://eospublic.cern.ch//eos/opendata/phenix/no_such_dir_gfal_test/"

        rc_new, out_new, err_new = run_gfal_docker("ls", missing)
        rc_old, out_old, err_old = run_gfal2_docker("ls", missing)

        assert rc_new == 2, f"expected ENOENT(2), got {rc_new}: {err_new}"
        assert rc_old == 2, f"legacy returned {rc_old}: {err_old}"

    def test_stat_http_nonexistent_exit_code(self):
        """gfal stat on a non-existent HTTPS file must return ENOENT (2)."""
        _xfail_if_legacy_unusable()
        missing = "https://eospublic.cern.ch//eos/opendata/phenix/does_not_exist_gfal_test.txt"

        rc_new, out_new, err_new = run_gfal_docker("stat", missing)
        rc_old, out_old, err_old = run_gfal2_docker("stat", missing)

        _skip_if_known_public_http_flake(rc_new, err_new)
        _skip_if_known_public_http_flake(rc_old, err_old)
        assert rc_new == 2, f"expected ENOENT(2), got {rc_new}: {err_new}"
        assert rc_old == 2, f"legacy returned {rc_old}: {err_old}"

    def test_cat_nonexistent_exit_code_matches_legacy(self):
        """gfal cat on a missing file must return ENOENT (2) like gfal2."""
        _xfail_if_legacy_unusable()
        missing = "https://eospublic.cern.ch//eos/opendata/phenix/does_not_exist_gfal_test.txt"

        rc_new, out_new, err_new = run_gfal_docker("cat", missing)
        rc_old, out_old, err_old = run_gfal2_docker("cat", missing)

        _skip_if_known_public_http_flake(rc_new, err_new)
        _skip_if_known_public_http_flake(rc_old, err_old)
        assert rc_new == 2, f"expected ENOENT(2), got {rc_new}: {err_new}"
        assert rc_old == 2, f"legacy returned {rc_old}: {err_old}"


@requires_docker
@requires_proxy
class TestCompareExitCodesEosPilot:
    """Compare exact exit codes for writable-pilot error scenarios."""

    def test_stat_nonexistent_exit_code_matches_legacy(self):
        """gfal stat on a missing pilot path must return ENOENT (2) like gfal2."""
        _xfail_if_legacy_unusable()
        proxy = _find_proxy()
        missing = f"{_PILOT_BASE}/does_not_exist_gfal_exitcode_test_{__import__('uuid').uuid4().hex}.txt"

        rc_new, out_new, err_new = run_gfal("stat", "-E", proxy, "--no-verify", missing)
        rc_old, out_old, err_old = run_gfal2_docker("stat", missing, proxy_cert=proxy)

        assert rc_new == 2, f"expected ENOENT(2), got {rc_new}: {err_new}"
        assert rc_old == 2, f"legacy returned {rc_old}: {err_old}"

    def test_ls_nonexistent_exit_code_matches_legacy(self):
        """gfal ls on a missing pilot path must return ENOENT (2) like gfal2."""
        _xfail_if_legacy_unusable()
        proxy = _find_proxy()
        missing = f"{_PILOT_BASE}/does_not_exist_dir_gfal_exitcode_test_{__import__('uuid').uuid4().hex}/"

        rc_new, out_new, err_new = run_gfal("ls", "-E", proxy, "--no-verify", missing)
        rc_old, out_old, err_old = run_gfal2_docker("ls", missing, proxy_cert=proxy)

        assert rc_new == 2, f"expected ENOENT(2), got {rc_new}: {err_new}"
        assert rc_old == 2, f"legacy returned {rc_old}: {err_old}"

    def test_rm_nonexistent_exit_code_matches_legacy(self):
        """gfal rm on a missing file must return ENOENT (2) like gfal2."""
        _xfail_if_legacy_unusable()
        proxy = _find_proxy()
        missing = f"{_PILOT_BASE}/does_not_exist_rm_gfal_exitcode_test_{__import__('uuid').uuid4().hex}.txt"

        rc_new, out_new, err_new = run_gfal("rm", "-E", proxy, "--no-verify", missing)
        rc_old, out_old, err_old = run_gfal2_docker("rm", missing, proxy_cert=proxy)

        assert rc_new == 2, f"expected ENOENT(2), got {rc_new}: {err_new}"
        assert rc_old == 2, f"legacy returned {rc_old}: {err_old}"

    def test_mkdir_existing_exit_code_matches_legacy(self):
        """gfal mkdir on an existing dir (without -p) must return EEXIST (17) like gfal2."""
        _xfail_if_legacy_unusable()
        proxy = _find_proxy()
        target = _unique_pilot_path("compare-exitcode-mkdir-existing")

        try:
            rc, out, err = run_gfal("mkdir", "-E", proxy, "--no-verify", target)
            assert rc == 0, err

            rc_new, out_new, err_new = run_gfal(
                "mkdir", "-E", proxy, "--no-verify", target
            )
            rc_old, out_old, err_old = run_gfal2_docker(
                "mkdir", target, proxy_cert=proxy
            )

            assert rc_new == 17, f"expected EEXIST(17), got {rc_new}: {err_new}"
            assert rc_old == 17, f"legacy returned {rc_old}: {err_old}"
        finally:
            run_gfal("rm", "-E", proxy, "--no-verify", "-r", target)

    def test_cp_permission_denied_exit_code_matches_legacy(self):
        """gfal cp to a denied path must return EACCES (13); legacy gfal2 may return 1 or 13."""
        _xfail_if_legacy_unusable()
        from test_integration_eospilot import _PILOT_NO_ACCESS

        proxy = _find_proxy()
        payload = "permission denied test\n"
        src = _unique_pilot_path("compare-exitcode-cp-perm-src.txt")
        dst = f"{_PILOT_NO_ACCESS}/denied_gfal_exitcode_test.txt"

        try:
            rc, out, err = run_gfal(
                "save", "-E", proxy, "--no-verify", src, input=payload
            )
            assert rc == 0, err

            rc_new, out_new, err_new = run_gfal(
                "cp", "-E", proxy, "--no-verify", src, dst
            )
            rc_old, out_old, err_old = run_gfal2_docker(
                "copy", src, dst, proxy_cert=proxy
            )

            assert rc_new == 13, f"expected EACCES(13), got {rc_new}: {err_new}"
            # Legacy gfal2 exit code is environment-dependent: returns 1 when the
            # HTTP 403 response propagates as "Operation not permitted" (EPERM, 1)
            # rather than EACCES (13) depending on the server/gfal2 version.
            assert rc_old in (1, 13), (
                f"legacy returned unexpected code {rc_old}: {err_old}"
            )
        finally:
            run_gfal("rm", "-E", proxy, "--no-verify", src)

    def test_copy_existing_dst_behavior(self):
        """gfal cp without -f to an existing dst returns EEXIST (17).

        The new CLI default is None (no compare), so it errors with EEXIST when
        the destination already exists — same behaviour as legacy gfal2-utils.
        """
        _xfail_if_legacy_unusable()
        proxy = _find_proxy()
        payload = "overwrite test\n"
        dst = _unique_pilot_path("compare-exitcode-cp-overwrite.txt")

        try:
            rc, out, err = run_gfal(
                "save", "-E", proxy, "--no-verify", dst, input=payload
            )
            assert rc == 0, err

            # save always overwrites (PUT), so test cp instead
            src_local = _unique_pilot_path("compare-exitcode-cp-overwrite-src.txt")
            try:
                run_gfal("save", "-E", proxy, "--no-verify", src_local, input=payload)

                rc_new2, out_new2, err_new2 = run_gfal(
                    "cp", "-E", proxy, "--no-verify", src_local, dst
                )
                rc_old, out_old, err_old = run_gfal2_docker(
                    "copy", src_local, dst, proxy_cert=proxy
                )

                # New CLI: default compare=None → EEXIST (17)
                assert rc_new2 == 17, (
                    f"expected 17 (EEXIST, no compare set), got {rc_new2}: {err_new2}"
                )
                # Legacy also returns EEXIST
                assert rc_old == 17, f"legacy returned {rc_old}: {err_old}"
            finally:
                run_gfal("rm", "-E", proxy, "--no-verify", src_local)
        finally:
            run_gfal("rm", "-E", proxy, "--no-verify", dst)


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

    def test_save_and_rm_with_env_proxy_matches_legacy(self):
        """Guard the env-proxy path used by client-backed commands like rm."""
        _xfail_if_legacy_unusable()
        proxy = _find_proxy()
        remote = _unique_pilot_path("compare-gfal2-utils-env-proxy-rm.txt")
        payload = "remove me via env proxy\n"

        rc_new, out_new, err_new = run_gfal_docker(
            "save", remote, proxy_cert=proxy, input=payload
        )
        assert rc_new == 0, err_new

        try:
            rc_old_stat, out_old_stat, err_old_stat = run_gfal2_docker(
                "stat", remote, proxy_cert=proxy
            )
            assert rc_old_stat == 0, err_old_stat

            rc_new_rm, out_new_rm, err_new_rm = run_gfal_docker(
                "rm", remote, proxy_cert=proxy
            )
            assert rc_new_rm == 0, err_new_rm

            rc_old_stat2, out_old_stat2, err_old_stat2 = run_gfal2_docker(
                "stat", remote, proxy_cert=proxy
            )
            assert rc_old_stat2 != 0
        finally:
            run_gfal_docker("rm", remote, proxy_cert=proxy)
