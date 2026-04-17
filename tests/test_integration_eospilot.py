"""
Integration tests against eospilot.cern.ch (writable CERN EOS pilot instance).

These tests exercise real HTTP(S) endpoints with write access.  They are
marked ``integration`` and are **not** run by plain ``pytest tests/``; pass
``-m integration`` to include them.

Requirements
------------
- Network access to eospilot.cern.ch:443
- A valid X.509 proxy certificate (``X509_USER_PROXY`` env var, or auto-
  detected at ``/tmp/x509up_u<uid>``).
- ``--no-verify`` is used throughout because eospilot uses the CERN Root CA
  which is not trusted by default on most CI systems.

Paths
-----
- ``/eos/pilot/opstest/dteam/python3-gfal/tmp/``
    Writable by the service account (robot cert).  Files older than 24 h are
    cleaned up automatically; tests also clean up after themselves.
- ``/eos/pilot/opstest/dteam/python3-gfal/dteam-has-no-permissions-here/``
    Explicitly denied for the service account.  Used to test error handling.

Known stable public source file
---------------------------------
  https://eospublic.cern.ch//eos/opendata/phenix/emcal-finding-pi0s-and-photons/single_cluster_r5.C
  size    : 2184 bytes
  MD5     : 93f402e24c6f870470e1c5fcc5400e25
  ADLER32 : 335e754f
"""

import errno
import hashlib
import os
import re
import socket
import subprocess
import sys
import textwrap
import time
import uuid
import zlib
from pathlib import Path

import pytest

from conftest import CI, require_test_prereq
from helpers import (
    _docker_run_command,
    _subprocess_env,
    docker_available,
    run_gfal,
    run_gfal_docker,
)

pytestmark = [pytest.mark.integration, pytest.mark.network]

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_PILOT_BASE = "https://eospilot.cern.ch//eos/pilot/opstest/dteam/python3-gfal/tmp"
_PILOT_ROOT_BASE = "root://eospilot.cern.ch//eos/pilot/opstest/dteam/python3-gfal/tmp"
_PILOT_NO_ACCESS = "https://eospilot.cern.ch//eos/pilot/opstest/dteam/python3-gfal/dteam-has-no-permissions-here"
_PUBSRC = (
    "https://eospublic.cern.ch//eos/opendata/phenix/"
    "emcal-finding-pi0s-and-photons/single_cluster_r5.C"
)
_PUBSRC_SIZE = 2184
_PUBSRC_MD5 = "93f402e24c6f870470e1c5fcc5400e25"
_PUBSRC_ADLER32 = "335e754f"

# ---------------------------------------------------------------------------
# Proxy detection
# ---------------------------------------------------------------------------


def _find_proxy():
    """Return path to X.509 proxy cert, or None if not found."""
    proxy = os.environ.get("X509_USER_PROXY", "")
    if proxy and Path(proxy).is_file():
        return proxy
    # Auto-detect the standard voms-proxy-init location
    try:
        uid = os.getuid()
    except AttributeError:
        # Windows — no getuid
        return None
    default = Path(f"/tmp/x509up_u{uid}")
    if default.is_file():
        return str(default)
    return None


# ---------------------------------------------------------------------------
# Skip markers
# ---------------------------------------------------------------------------


def _eospilot_reachable():
    try:
        with socket.create_connection(("eospilot.cern.ch", 443), timeout=5):
            return True
    except OSError:
        return False


requires_eospilot = pytest.mark.skipif(
    not _eospilot_reachable() and not CI,
    reason="eospilot.cern.ch:443 not reachable",
)

requires_proxy = pytest.mark.skipif(
    _find_proxy() is None and not CI,
    reason="No X.509 proxy found (set X509_USER_PROXY or run voms-proxy-init)",
)

requires_docker = pytest.mark.skipif(
    not docker_available() and not CI,
    reason="Docker image xrootd-cern-test not available",
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def proxy_cert():
    """Return the path to the X.509 proxy certificate."""
    path = _find_proxy()
    require_test_prereq(
        path is not None,
        "No X.509 proxy found (set X509_USER_PROXY or provision the CI proxy)",
    )
    return path


@pytest.fixture
def pilot_dir(proxy_cert):
    """Create a unique scratch directory on eospilot, yield URL, then clean up."""
    name = f"pytest-{uuid.uuid4().hex[:8]}"
    url = f"{_PILOT_BASE}/{name}"
    rc, out, err = run_gfal("mkdir", "-E", proxy_cert, "--no-verify", url)
    require_test_prereq(rc == 0, f"Could not create pilot_dir {url}: {err.strip()}")
    yield url
    # Cleanup — ignore errors
    run_gfal("rm", "-r", "-E", proxy_cert, "--no-verify", url)


# ---------------------------------------------------------------------------
# Convenience helper
# ---------------------------------------------------------------------------


def _run(cmd, proxy_cert, *args, timeout=None):
    """Call run_gfal with the proxy cert and --no-verify flags pre-filled."""
    kwargs = {}
    if timeout is not None:
        kwargs["timeout"] = timeout
    return run_gfal(cmd, "-E", proxy_cert, "--no-verify", *args, **kwargs)


def _run_tty_gfal(cmd, *args, timeout=180, env=None):
    """Run ``gfal`` attached to a PTY so Rich progress output is emitted."""
    require_test_prereq(sys.platform != "win32", "PTY-based timing test needs POSIX")
    import pty  # noqa: PLC0415

    script = (
        f"import sys; sys.argv=['gfal', '{cmd}']+sys.argv[1:];"
        "from gfal.cli.shell import main; main()"
    )
    subprocess_env = _subprocess_env()
    subprocess_env["GFAL_CLI_GFAL2"] = "0"
    subprocess_env.setdefault("TERM", "xterm")
    subprocess_env.setdefault("NO_COLOR", "1")
    subprocess_env.setdefault("COLUMNS", "180")
    if env is not None:
        subprocess_env.update(env)

    master_fd, slave_fd = pty.openpty()
    started = time.monotonic()
    proc = None
    chunks = []
    try:
        proc = subprocess.Popen(
            [sys.executable, "-c", script, *[str(arg) for arg in args]],
            stdin=subprocess.DEVNULL,
            stdout=slave_fd,
            stderr=slave_fd,
            env=subprocess_env,
        )
        os.close(slave_fd)
        slave_fd = None
        deadline = started + timeout

        while True:
            if time.monotonic() > deadline:
                proc.kill()
                raise AssertionError(
                    f"PTY gfal helper timed out after {timeout}s: {cmd} {' '.join(map(str, args))}"
                )
            try:
                chunk = os.read(master_fd, 65536)
                if not chunk:
                    break
                chunks.append(chunk)
            except OSError as exc:
                if exc.errno == errno.EIO:
                    break
                raise
            if proc.poll() is not None:
                continue

        rc = proc.wait(timeout=5)
    finally:
        if slave_fd is not None:
            os.close(slave_fd)
        os.close(master_fd)
        if proc is not None and proc.poll() is None:
            proc.kill()

    elapsed = time.monotonic() - started
    output = b"".join(chunks).decode("utf-8", errors="replace")
    return rc, output, elapsed


_ANSI_ESCAPE_RE = re.compile(r"\x1b\[[0-9;?]*[ -/]*[@-~]")
_ELAPSED_RE = re.compile(r"\b(\d+):(\d{2}):(\d{2})\b")


def _strip_ansi(text):
    return _ANSI_ESCAPE_RE.sub("", text)


def _extract_progress_elapsed_seconds(output, *, mode):
    cleaned = _strip_ansi(output).replace("\r", "\n")
    lines = [
        line
        for line in cleaned.splitlines()
        if f"({mode})" in line and "[DONE]" in line
    ]
    assert lines, f"No completed {mode} progress line found in output:\n{cleaned}"
    match = _ELAPSED_RE.search(lines[-1])
    assert match is not None, f"No elapsed time found in line: {lines[-1]!r}"
    hours, minutes, seconds = (int(part) for part in match.groups())
    return (hours * 3600) + (minutes * 60) + seconds


def _run_repo_gfal_docker_script(shell_script, proxy_cert):
    """Run a shell script in the Docker image after installing the current repo copy."""
    setup = (
        "cp -r /repo /tmp/gfal-src && "
        "python3 -m pip install -q --no-deps /tmp/gfal-src > /dev/null 2>&1 && "
    )
    return _docker_run_command(f"{setup}{shell_script}", proxy_cert=proxy_cert)


def _preserve_times_url(kind, name):
    if kind == "local":
        return f"file:///tmp/{name}"
    if kind == "https":
        return f"{_PILOT_BASE}/{name}"
    if kind == "root":
        return f"{_PILOT_ROOT_BASE}/{name}"
    raise ValueError(f"Unknown preserve-times endpoint kind: {kind}")


def _preserve_times_setup(kind, url):
    if kind == "local":
        return ""
    if kind == "https":
        return textwrap.dedent(
            f"""
            python3 - <<'PY'
            import ssl
            import urllib.error
            import urllib.request
            proxy = "/tmp/x509proxy"
            ctx = ssl.create_default_context()
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
            ctx.load_cert_chain(proxy, proxy)
            data = b"preserve-times\\n"
            url = "{url}?eos.mtime=946684800"
            for _ in range(5):
                req = urllib.request.Request(url, data=data, method="PUT")
                try:
                    with urllib.request.urlopen(req, context=ctx, timeout=20) as resp:
                        if resp.status not in (200, 201, 204):
                            raise SystemExit(f"https source setup failed: {{resp.status}}")
                        break
                except urllib.error.HTTPError as exc:
                    if exc.code in (307, 308):
                        url = exc.headers.get("Location", url)
                        continue
                    raise
            else:
                raise SystemExit("Too many redirects setting up https source")
            PY
            """
        )
    if kind == "root":
        return f"xrdcp -f /tmp/src.txt '{url}?eos.mtime=946684800'\n"
    raise ValueError(f"Unknown preserve-times endpoint kind: {kind}")


def _preserve_times_verify(kind, url):
    if kind == "local":
        return textwrap.dedent(
            f"""
            python3 - <<'PY'
            from pathlib import Path
            from urllib.parse import urlparse

            path = Path(urlparse("{url}").path)
            ts = int(path.stat().st_mtime)
            if ts != 946684800:
                raise SystemExit(f"local mtime mismatch for {{path}}: {{ts}}")
            PY
            """
        )
    return textwrap.dedent(
        f"""
        gfal stat -t 20 --no-verify '{url}' >/tmp/stat.out 2>/tmp/stat.err
        grep -q '2000-01-01' /tmp/stat.out
        """
    )


def _preserve_times_cleanup(kind, url):
    if kind == "local":
        return ""
    return f"gfal rm -t 20 --no-verify '{url}' >/dev/null 2>/dev/null || true\n"


def _batch_copy_payloads():
    """Return ten deterministic file payloads covering varied local-file cases."""
    return [
        ("batch_00_empty.bin", b""),
        ("batch_01_one_byte.bin", b"x"),
        ("batch_02_text.txt", b"hello from gfal\n"),
        ("batch_03_lines.txt", b"line one\nline two\nline three\n"),
        ("batch_04_binary.bin", bytes(range(32))),
        ("batch_05_nulls.bin", b"\x00\x01\x00\x02" * 32),
        ("batch_06_chunk_edge.bin", b"A" * 4097),
        ("batch_07_medium.bin", b"medium-payload-" * 4096),
        ("batch_08_ascii.txt", b"The quick brown fox jumps over the lazy dog.\n" * 16),
        ("batch_09_repeated.bin", bytes(range(256)) * 32),
    ]


def _adler32_hex(data):
    """Return lowercase ADLER32 in the same hex form gfal prints."""
    return f"{zlib.adler32(data) & 0xFFFFFFFF:08x}"


def _write_batch_sources(tmp_path):
    """Create the deterministic 10-file batch locally and return metadata."""
    records = []
    for name, data in _batch_copy_payloads():
        path = tmp_path / name
        path.write_bytes(data)
        records.append(
            {
                "name": name,
                "path": path,
                "uri": path.as_uri(),
                "data": data,
                "adler32": _adler32_hex(data),
            }
        )
    return records


def _write_sources_file(tmp_path, records):
    """Write a --from-file source list for the provided local records."""
    sources = tmp_path / "sources.txt"
    sources.write_text("\n".join(record["uri"] for record in records) + "\n")
    return sources


def _verify_remote_batch(proxy_cert, pilot_dir, records):
    """Verify every remote file exists and matches the local ADLER32 checksum."""
    for record in records:
        remote = f"{pilot_dir}/{record['name']}"
        rc, out, err = _run("sum", proxy_cert, remote, "ADLER32")
        assert rc == 0, f"checksum failed for {record['name']}: {err}"
        assert record["adler32"] in out.lower(), out


# ---------------------------------------------------------------------------
# TestEosPilotStreamingCopy
# ---------------------------------------------------------------------------


@requires_eospilot
@requires_proxy
class TestEosPilotStreamingCopy:
    def test_http_to_http_copy_from_public_uses_default_gfal2_mode(
        self, proxy_cert, pilot_dir
    ):
        """Default public->pilot HTTPS copy should prefer the gfal2-compatible mode."""
        dst = f"{pilot_dir}/default_mode_from_public.C"
        rc, out, err = _run("cp", proxy_cert, _PUBSRC, dst)
        assert rc == 0, err
        assert "(TPC pull)" in out or "(streamed)" in out

    def test_copy_small_file(self, proxy_cert, pilot_dir, tmp_path):
        """Download from eospublic and re-upload to pilot dir."""
        local = tmp_path / "src.C"
        rc, out, err = _run("cp", proxy_cert, "--no-verify", _PUBSRC, local.as_uri())
        assert rc == 0, err
        assert local.stat().st_size == _PUBSRC_SIZE

        dst = f"{pilot_dir}/copy_small.C"
        rc, out, err = _run("cp", proxy_cert, local.as_uri(), dst)
        assert rc == 0, err

    def test_copy_preserves_content(self, proxy_cert, pilot_dir, tmp_path):
        """Upload then download and verify bytes match."""
        data = b"eospilot content verify " * 50
        src = tmp_path / "verify_src.bin"
        src.write_bytes(data)

        dst = f"{pilot_dir}/verify.bin"
        rc, out, err = _run("cp", proxy_cert, src.as_uri(), dst)
        assert rc == 0, err

        downloaded = tmp_path / "verify_dst.bin"
        rc, out, err = _run("cp", proxy_cert, dst, downloaded.as_uri())
        assert rc == 0, err
        assert (
            hashlib.md5(downloaded.read_bytes()).hexdigest()
            == hashlib.md5(data).hexdigest()
        )

    def test_copy_with_checksum_md5(self, proxy_cert, pilot_dir, tmp_path):
        """Copy with -K MD5 checksum verification."""
        data = b"checksum test eospilot md5"
        src = tmp_path / "chksum_md5.bin"
        src.write_bytes(data)

        dst = f"{pilot_dir}/chksum_md5.bin"
        rc, out, err = _run("cp", proxy_cert, "-K", "MD5", src.as_uri(), dst)
        assert rc == 0, err

    def test_copy_with_checksum_adler32(self, proxy_cert, pilot_dir, tmp_path):
        """Copy with -K ADLER32 checksum verification."""
        data = b"checksum test eospilot adler32"
        src = tmp_path / "chksum_adler32.bin"
        src.write_bytes(data)

        dst = f"{pilot_dir}/chksum_adler32.bin"
        rc, out, err = _run("cp", proxy_cert, "-K", "ADLER32", src.as_uri(), dst)
        assert rc == 0, err

    def test_copy_empty_file(self, proxy_cert, pilot_dir, tmp_path):
        """Upload and download an empty file."""
        src = tmp_path / "empty.bin"
        src.write_bytes(b"")

        dst = f"{pilot_dir}/empty.bin"
        rc, out, err = _run("cp", proxy_cert, src.as_uri(), dst)
        assert rc == 0, err

        downloaded = tmp_path / "empty_dl.bin"
        rc, out, err = _run("cp", proxy_cert, dst, downloaded.as_uri())
        assert rc == 0, err
        assert downloaded.read_bytes() == b""

    def test_copy_large_file(self, proxy_cert, pilot_dir, tmp_path):
        """Upload a file larger than 4 MiB (crosses chunk boundary)."""
        data = b"X" * (5 * 1024 * 1024)
        src = tmp_path / "large.bin"
        src.write_bytes(data)

        dst = f"{pilot_dir}/large.bin"
        rc, out, err = _run("cp", proxy_cert, src.as_uri(), dst, timeout=90)
        assert rc == 0, err

        rc, out, err = _run("stat", proxy_cert, dst)
        assert rc == 0, err
        assert str(len(data)) in out

    def test_streamed_copy_wall_time_tracks_reported_progress(
        self, proxy_cert, pilot_dir, tmp_path
    ):
        """Wall time for streamed uploads should stay close to the reported elapsed time."""
        src = tmp_path / "stream_timing.bin"
        with src.open("wb") as handle:
            handle.truncate(512 * 1024 * 1024)
        dst = f"{pilot_dir}/stream_timing.bin"

        rc, out, elapsed = _run_tty_gfal(
            "cp",
            "-E",
            proxy_cert,
            "--no-verify",
            "--copy-mode",
            "streamed",
            src.as_uri(),
            dst,
            timeout=180,
        )

        assert rc == 0, out
        reported = _extract_progress_elapsed_seconds(out, mode="streamed")
        assert abs(elapsed - reported) <= max(4.0, reported * 0.5), (
            f"wall={elapsed:.2f}s reported={reported}s\n{_strip_ansi(out)}"
        )

    def test_no_overwrite_without_force(self, proxy_cert, pilot_dir, tmp_path):
        """Without -f and no --compare, cp to an existing dst returns EEXIST (17).
        --compare none skips always; --compare checksum skips when content matches."""
        src = tmp_path / "overwrite_src.bin"
        src.write_bytes(b"original")
        dst = f"{pilot_dir}/overwrite_test.bin"

        # Initial upload
        rc, out, err = _run("cp", proxy_cert, src.as_uri(), dst)
        assert rc == 0, err

        # Default (no --compare): EEXIST (17) — destination already exists
        rc, out, err = _run("cp", proxy_cert, src.as_uri(), dst)
        assert rc == 17, (
            f"Expected EEXIST (17) when dst exists and no --compare set: {err}"
        )

        # Re-upload using -f so subsequent steps have a known remote state
        rc, out, err = _run("cp", proxy_cert, "-f", src.as_uri(), dst)
        assert rc == 0, err

        # --compare none: always skips without any check (rc=0, content unchanged)
        src_v2 = tmp_path / "overwrite_src_v2.bin"
        src_v2.write_bytes(b"updated content")
        rc, out, err = _run("cp", proxy_cert, "--compare", "none", src_v2.as_uri(), dst)
        assert rc == 0, err
        assert "Skipping existing file" in out

        # Verify dst still holds the original content (none did not overwrite)
        local_copy = tmp_path / "verify.bin"
        rc, out, err = _run("cp", proxy_cert, dst, local_copy.as_uri())
        assert rc == 0, err
        assert local_copy.read_bytes() == b"original"

        # --compare checksum: skip when checksums match
        rc, out, err = _run(
            "cp", proxy_cert, "--compare", "checksum", src.as_uri(), dst
        )
        assert rc == 0, err
        assert "matching ADLER32 checksum" in out

        # --compare checksum: overwrite when checksums differ (content changed)
        rc, out, err = _run(
            "cp", proxy_cert, "--compare", "checksum", src_v2.as_uri(), dst
        )
        assert rc == 0, err
        local_copy2 = tmp_path / "verify2.bin"
        rc, out, err = _run("cp", proxy_cert, dst, local_copy2.as_uri())
        assert rc == 0, err
        assert local_copy2.read_bytes() == b"updated content"

    def test_force_overwrite(self, proxy_cert, pilot_dir, tmp_path):
        """Copying to an existing destination with -f should succeed."""
        src = tmp_path / "force_src.bin"
        src.write_bytes(b"version1")
        dst = f"{pilot_dir}/force_test.bin"

        rc, out, err = _run("cp", proxy_cert, src.as_uri(), dst)
        assert rc == 0, err

        src.write_bytes(b"version2")
        rc, out, err = _run("cp", proxy_cert, "-f", src.as_uri(), dst)
        assert rc == 0, err

        # Download and verify v2 is there
        dl = tmp_path / "force_dl.bin"
        rc, out, err = _run("cp", proxy_cert, dst, dl.as_uri())
        assert rc == 0, err
        assert dl.read_bytes() == b"version2"

    def test_copy_missing_source_fails(self, proxy_cert, pilot_dir):
        """Copying a non-existent source should fail with non-zero exit."""
        missing_src = f"{_PILOT_BASE}/this_does_not_exist_gfal_test_src.bin"
        dst = f"{pilot_dir}/should_not_exist.bin"
        rc, out, err = _run("cp", proxy_cert, missing_src, dst)
        assert rc != 0

    def test_copy_permission_denied(self, proxy_cert, tmp_path):
        """Copying to a directory without permissions should fail (403)."""
        src = tmp_path / "denied.bin"
        src.write_bytes(b"denied")
        dst = f"{_PILOT_NO_ACCESS}/denied.bin"
        rc, out, err = _run("cp", proxy_cert, src.as_uri(), dst)
        assert rc != 0
        assert "Permission denied" in err or "403" in err

    def test_copy_multiple_files(self, proxy_cert, pilot_dir, tmp_path):
        """Upload several files; all should succeed independently."""
        for i in range(3):
            src = tmp_path / f"multi_{i}.bin"
            src.write_bytes(f"file {i} content".encode())
            dst = f"{pilot_dir}/multi_{i}.bin"
            rc, out, err = _run("cp", proxy_cert, src.as_uri(), dst)
            assert rc == 0, f"file {i} failed: {err}"

    def test_copy_ten_local_files_from_file_with_checksum(
        self, proxy_cert, pilot_dir, tmp_path
    ):
        """Batch-upload 10 local files to EOS and verify checksum-checked success."""
        records = _write_batch_sources(tmp_path)
        sources_file = _write_sources_file(tmp_path, records)

        rc, _out, err = _run(
            "cp",
            proxy_cert,
            "--from-file",
            str(sources_file),
            "-K",
            "ADLER32",
            pilot_dir,
        )

        assert rc == 0, err
        _verify_remote_batch(proxy_cert, pilot_dir, records)

    def test_copy_ten_files_compare_checksum_with_existing_matches(
        self, proxy_cert, pilot_dir, tmp_path
    ):
        """Existing matching remote files should be skipped while missing ones are copied."""
        records = _write_batch_sources(tmp_path)
        sources_file = _write_sources_file(tmp_path, records)
        existing = records[:4]

        for record in existing:
            rc, out, err = _run(
                "cp",
                proxy_cert,
                record["uri"],
                f"{pilot_dir}/{record['name']}",
            )
            assert rc == 0, err

        rc, out, err = _run(
            "cp",
            proxy_cert,
            "--from-file",
            str(sources_file),
            "--compare",
            "checksum",
            pilot_dir,
        )

        assert rc == 0, err
        for record in existing:
            assert record["name"] in out
            assert "matching ADLER32 checksum" in out
        _verify_remote_batch(proxy_cert, pilot_dir, records)

    def test_copy_ten_files_compare_checksum_copies_on_mismatch(
        self, proxy_cert, pilot_dir, tmp_path
    ):
        """A pre-existing remote file with wrong content should be overwritten."""
        records = _write_batch_sources(tmp_path)
        sources_file = _write_sources_file(tmp_path, records)
        matching = records[:3]
        mismatched = records[3]
        absent = records[4:]

        for record in matching:
            rc, out, err = _run(
                "cp",
                proxy_cert,
                record["uri"],
                f"{pilot_dir}/{record['name']}",
            )
            assert rc == 0, err

        wrong_local = tmp_path / "wrong_checksum.bin"
        wrong_local.write_bytes(b"remote checksum mismatch\n")
        rc, out, err = _run(
            "cp",
            proxy_cert,
            wrong_local.as_uri(),
            f"{pilot_dir}/{mismatched['name']}",
        )
        assert rc == 0, err

        rc, out, err = _run(
            "cp",
            proxy_cert,
            "--from-file",
            str(sources_file),
            "--compare",
            "checksum",
            pilot_dir,
        )

        assert rc == 0
        for record in matching:
            assert record["name"] in out
            assert "matching ADLER32 checksum" in out

        # Mismatched file should have been overwritten with the correct content
        remote_mismatch = f"{pilot_dir}/{mismatched['name']}"
        rc, out, err = _run("sum", proxy_cert, remote_mismatch, "ADLER32")
        assert rc == 0, err
        assert mismatched["adler32"] in out.lower(), (
            f"Expected correct checksum {mismatched['adler32']} after overwrite, got: {out}"
        )

        for record in absent:
            remote = f"{pilot_dir}/{record['name']}"
            rc, out, err = _run("sum", proxy_cert, remote, "ADLER32")
            assert rc == 0, f"missing copied file {record['name']}: {err}"
            assert record["adler32"] in out.lower(), out

    def test_copy_preserves_binary_content(self, proxy_cert, pilot_dir, tmp_path):
        """Binary data including null bytes should survive the round-trip."""
        data = bytes(range(256)) * 100
        src = tmp_path / "binary.bin"
        src.write_bytes(data)

        dst = f"{pilot_dir}/binary.bin"
        rc, out, err = _run("cp", proxy_cert, src.as_uri(), dst)
        assert rc == 0, err

        dl = tmp_path / "binary_dl.bin"
        rc, out, err = _run("cp", proxy_cert, dst, dl.as_uri())
        assert rc == 0, err
        assert dl.read_bytes() == data


@requires_eospilot
@requires_proxy
@requires_docker
class TestEosPilotStreamingCopyDocker:
    @pytest.mark.parametrize(
        ("src_kind", "dst_kind"),
        [
            ("local", "https"),
            ("https", "local"),
            ("local", "root"),
            ("root", "local"),
            ("https", "https"),
            ("https", "root"),
            ("root", "https"),
            ("root", "root"),
        ],
        ids=[
            "local_to_https",
            "https_to_local",
            "local_to_root",
            "root_to_local",
            "https_to_https",
            "https_to_root",
            "root_to_https",
            "root_to_root",
        ],
    )
    def test_preserve_times_across_local_https_and_xrootd(
        self, proxy_cert, src_kind, dst_kind
    ):
        src_name = f"pytest-preserve-src-{uuid.uuid4().hex[:8]}.txt"
        dst_name = f"pytest-preserve-dst-{uuid.uuid4().hex[:8]}.txt"
        src_url = _preserve_times_url(
            src_kind, "src.txt" if src_kind == "local" else src_name
        )
        dst_url = _preserve_times_url(
            dst_kind, "dst.txt" if dst_kind == "local" else dst_name
        )
        setup_src = _preserve_times_setup(src_kind, src_url)
        verify_dst = _preserve_times_verify(dst_kind, dst_url)
        cleanup_src = _preserve_times_cleanup(src_kind, src_url)
        cleanup_dst = _preserve_times_cleanup(dst_kind, dst_url)

        script = textwrap.dedent(
            f"""
            set -e
            printf 'preserve times\\n' >/tmp/src.txt
            touch -t 200001010101 /tmp/src.txt
            {setup_src}gfal cp -t 20 --no-verify --preserve-times '{src_url}' '{dst_url}' >/tmp/cp.out 2>/tmp/cp.err
            {verify_dst}{cleanup_src}{cleanup_dst}cat /tmp/cp.out
            cat /tmp/cp.err >&2
            if [ -f /tmp/stat.out ]; then cat /tmp/stat.out; fi
            if [ -f /tmp/stat.err ]; then cat /tmp/stat.err >&2; fi
            """
        )
        rc, out, err = _run_repo_gfal_docker_script(script, proxy_cert)

        assert rc == 0, err
        assert "--preserve-times is only supported for local destinations" not in err

    def test_compare_checksum_skips_matching_remote_and_copies_on_mismatch(
        self, proxy_cert
    ):
        remote = f"{_PILOT_BASE}/pytest-compare-checksum-{uuid.uuid4().hex[:8]}.txt"
        script = f"""
set -e
printf 'same-content\\n' >/tmp/src.txt
gfal cp -t 20 file:///tmp/src.txt '{remote}'
gfal cp -t 20 --compare checksum file:///tmp/src.txt '{remote}' >/tmp/match.out 2>/tmp/match.err
printf 'different-content\\n' >/tmp/src2.txt
set +e
gfal cp -t 20 --compare checksum file:///tmp/src2.txt '{remote}' >/tmp/mismatch.out 2>/tmp/mismatch.err
mismatch_rc=$?
set -e
gfal cat '{remote}' >/tmp/final.out 2>/tmp/final.err
gfal rm -t 20 '{remote}' >/tmp/rm.out 2>/tmp/rm.err || true
echo "MISMATCH_RC=$mismatch_rc"
cat /tmp/match.out
cat /tmp/mismatch.out
cat /tmp/final.out
cat /tmp/match.err >&2
cat /tmp/mismatch.err >&2
cat /tmp/final.err >&2
cat /tmp/rm.err >&2
"""
        rc, out, err = _run_repo_gfal_docker_script(script, proxy_cert)

        assert rc == 0, err
        assert "matching ADLER32 checksum" in out
        assert "MISMATCH_RC=0" in out  # different content → copy (overwrite), not error
        # After overwrite the remote should contain the new content
        assert "different-content" in out


# ---------------------------------------------------------------------------
# TestEosPilotStat
# ---------------------------------------------------------------------------


@requires_eospilot
@requires_proxy
class TestEosPilotStat:
    def test_stat_file(self, proxy_cert, pilot_dir, tmp_path):
        """stat on an uploaded file should show size and 'regular file'."""
        data = b"stat me " * 10
        src = tmp_path / "stat_me.bin"
        src.write_bytes(data)

        dst = f"{pilot_dir}/stat_me.bin"
        rc, out, err = _run("cp", proxy_cert, src.as_uri(), dst)
        assert rc == 0, err

        rc, out, err = _run("stat", proxy_cert, dst)
        assert rc == 0, err
        assert str(len(data)) in out
        assert "regular file" in out

    def test_stat_directory(self, proxy_cert, pilot_dir):
        """stat on the scratch directory should succeed and show 'directory'."""
        rc, out, err = _run("stat", proxy_cert, pilot_dir)
        assert rc == 0, err
        assert "File:" in out
        assert "directory" in out

    def test_stat_nonexistent_fails(self, proxy_cert, pilot_dir):
        """stat on a missing path should exit non-zero."""
        missing = f"{pilot_dir}/no_such_file_gfal_test.bin"
        rc, out, err = _run("stat", proxy_cert, missing)
        assert rc != 0

    def test_stat_shows_mtime(self, proxy_cert, pilot_dir, tmp_path):
        """stat output should contain a modification time."""
        src = tmp_path / "mtime.bin"
        src.write_bytes(b"mtime test")
        dst = f"{pilot_dir}/mtime.bin"
        rc, out, err = _run("cp", proxy_cert, src.as_uri(), dst)
        assert rc == 0, err

        rc, out, err = _run("stat", proxy_cert, dst)
        assert rc == 0, err
        # Stat output contains "Modify:" line
        assert "Modify" in out or "Modification" in out or "2026" in out

    def test_stat_no_access_dir(self, proxy_cert):
        """stat on the no-access directory should not crash (visible but unwritable)."""
        rc, out, err = _run("stat", proxy_cert, _PILOT_NO_ACCESS)
        # The directory exists; EOS may allow stat even if writes are denied
        # What matters is we get a clean exit or a clean error, not a traceback
        assert "Traceback" not in err


# ---------------------------------------------------------------------------
# TestEosPilotLs
# ---------------------------------------------------------------------------


@requires_eospilot
@requires_proxy
class TestEosPilotLs:
    def test_ls_empty_directory(self, proxy_cert, pilot_dir):
        """ls on a freshly-created empty directory should return no output."""
        rc, out, err = _run("ls", proxy_cert, pilot_dir)
        assert rc == 0, err
        assert out.strip() == ""

    def test_ls_shows_uploaded_file(self, proxy_cert, pilot_dir, tmp_path):
        """A file uploaded to the pilot dir should appear in ls output."""
        src = tmp_path / "ls_test.bin"
        src.write_bytes(b"list me")
        dst = f"{pilot_dir}/ls_test.bin"
        rc, out, err = _run("cp", proxy_cert, src.as_uri(), dst)
        assert rc == 0, err

        rc, out, err = _run("ls", proxy_cert, pilot_dir)
        assert rc == 0, err
        assert "ls_test.bin" in out

    def test_ls_shows_multiple_files(self, proxy_cert, pilot_dir, tmp_path):
        """All uploaded files should appear in ls output."""
        names = ["alpha.bin", "beta.bin", "gamma.bin"]
        for name in names:
            src = tmp_path / name
            src.write_bytes(b"data")
            _run("cp", proxy_cert, src.as_uri(), f"{pilot_dir}/{name}")

        rc, out, err = _run("ls", proxy_cert, pilot_dir)
        assert rc == 0, err
        for name in names:
            assert name in out

    def test_ls_long_format(self, proxy_cert, pilot_dir, tmp_path):
        """ls -l should succeed and show the filename and size."""
        data = b"long format test"
        src = tmp_path / "ls_long.bin"
        src.write_bytes(data)
        dst = f"{pilot_dir}/ls_long.bin"
        rc, out, err = _run("cp", proxy_cert, src.as_uri(), dst)
        assert rc == 0, err

        rc, out, err = _run("ls", proxy_cert, "-l", pilot_dir)
        assert rc == 0, err
        assert "ls_long.bin" in out
        assert str(len(data)) in out

    def test_ls_directory_flag(self, proxy_cert, pilot_dir):
        """ls -d should show the directory itself as a single entry."""
        rc, out, err = _run("ls", proxy_cert, "-d", pilot_dir)
        assert rc == 0, err
        lines = [ln for ln in out.splitlines() if ln.strip()]
        assert len(lines) >= 1

    def test_ls_shows_subdirectory(self, proxy_cert, pilot_dir):
        """A subdirectory should appear in ls output."""
        subdir = f"{pilot_dir}/subdir_ls_test"
        rc, out, err = _run("mkdir", proxy_cert, subdir)
        assert rc == 0, err

        rc, out, err = _run("ls", proxy_cert, pilot_dir)
        assert rc == 0, err
        assert "subdir_ls_test" in out

    def test_ls_no_access_dir_is_visible(self, proxy_cert):
        """The no-access directory itself is stat-able but has no listable children."""
        rc, out, err = _run("ls", proxy_cert, _PILOT_NO_ACCESS)
        # EOS allows PROPFIND Depth:0 (the directory is visible) but returns no
        # children for a directory the service account cannot read.  Either an
        # empty listing (rc=0, out empty) or just the directory name is valid;
        # what must NOT happen is a successful listing that exposes file contents.
        if rc == 0:
            lines = [ln for ln in out.splitlines() if ln.strip()]
            # At most one entry and it must be the directory itself, not a child
            assert len(lines) <= 1

    def test_ls_nonexistent_fails(self, proxy_cert, pilot_dir):
        """ls on a non-existent path should exit non-zero."""
        missing = f"{pilot_dir}/does_not_exist_ls/"
        rc, out, err = _run("ls", proxy_cert, missing)
        assert rc != 0


# ---------------------------------------------------------------------------
# TestEosPilotMkdirRm
# ---------------------------------------------------------------------------


@requires_eospilot
@requires_proxy
class TestEosPilotMkdirRm:
    def test_mkdir(self, proxy_cert, pilot_dir):
        """Create a subdirectory and verify it exists with stat."""
        subdir = f"{pilot_dir}/subdir_mkdir_test"
        rc, out, err = _run("mkdir", proxy_cert, subdir)
        assert rc == 0, err

        rc, out, err = _run("stat", proxy_cert, subdir)
        assert rc == 0, err

    def test_mkdir_parents(self, proxy_cert, pilot_dir):
        """mkdir -p should create nested directories."""
        deep = f"{pilot_dir}/deep/nested"
        rc, out, err = _run("mkdir", proxy_cert, "-p", deep)
        assert rc == 0, err

        rc, out, err = _run("stat", proxy_cert, deep)
        assert rc == 0, err

    def test_mkdir_existing_fails_without_p(self, proxy_cert, pilot_dir):
        """mkdir on an already-existing directory without -p should fail."""
        subdir = f"{pilot_dir}/dup_mkdir"
        rc, out, err = _run("mkdir", proxy_cert, subdir)
        assert rc == 0, err

        rc, out, err = _run("mkdir", proxy_cert, subdir)
        assert rc != 0

    def test_mkdir_existing_ok_with_p(self, proxy_cert, pilot_dir):
        """mkdir -p on an already-existing directory should succeed silently."""
        subdir = f"{pilot_dir}/dup_mkdir_p"
        rc, out, err = _run("mkdir", proxy_cert, subdir)
        assert rc == 0, err

        rc, out, err = _run("mkdir", proxy_cert, "-p", subdir)
        assert rc == 0, err

    def test_rm_file(self, proxy_cert, pilot_dir, tmp_path):
        """Upload a file, delete it, verify it is gone."""
        src = tmp_path / "rm_me.bin"
        src.write_bytes(b"delete me")
        dst = f"{pilot_dir}/rm_me.bin"
        rc, out, err = _run("cp", proxy_cert, src.as_uri(), dst)
        assert rc == 0, err

        rc, out, err = _run("rm", proxy_cert, dst)
        assert rc == 0, err

        rc, out, err = _run("stat", proxy_cert, dst)
        assert rc != 0

    def test_rm_directory(self, proxy_cert, pilot_dir):
        """Create a directory, rm it, verify it is gone."""
        subdir = f"{pilot_dir}/rm_dir_test"
        rc, out, err = _run("mkdir", proxy_cert, subdir)
        assert rc == 0, err

        rc, out, err = _run("rm", proxy_cert, "-r", subdir)
        assert rc == 0, err

        rc, out, err = _run("stat", proxy_cert, subdir)
        assert rc != 0

    def test_rm_recursive(self, proxy_cert, pilot_dir, tmp_path):
        """rm -r on a directory with files should remove everything."""
        subdir = f"{pilot_dir}/rm_recursive"
        rc, out, err = _run("mkdir", proxy_cert, subdir)
        assert rc == 0, err

        for i in range(3):
            src = tmp_path / f"f{i}.bin"
            src.write_bytes(b"data")
            _run("cp", proxy_cert, src.as_uri(), f"{subdir}/f{i}.bin")

        rc, out, err = _run("rm", proxy_cert, "-r", subdir)
        assert rc == 0, err

        rc, out, err = _run("stat", proxy_cert, subdir)
        assert rc != 0

    def test_rm_nonexistent_fails(self, proxy_cert, pilot_dir):
        """Deleting a non-existent file should exit non-zero."""
        missing = f"{pilot_dir}/no_such_file_rm_test.bin"
        rc, out, err = _run("rm", proxy_cert, missing)
        assert rc != 0

    def test_mkdir_permission_denied(self, proxy_cert):
        """Creating a directory in a no-access location should fail."""
        subdir = f"{_PILOT_NO_ACCESS}/subdir_denied"
        rc, out, err = _run("mkdir", proxy_cert, subdir)
        assert rc != 0
        assert "Permission denied" in err or "403" in err


# ---------------------------------------------------------------------------
# TestEosPilotRename
# ---------------------------------------------------------------------------


@requires_eospilot
@requires_proxy
class TestEosPilotRename:
    def test_rename_file(self, proxy_cert, pilot_dir, tmp_path):
        """Upload a file, rename it, verify src is gone and dst exists."""
        src = tmp_path / "rename_src.bin"
        src.write_bytes(b"rename me")
        src_url = f"{pilot_dir}/rename_src.bin"
        dst_url = f"{pilot_dir}/rename_dst.bin"

        rc, out, err = _run("cp", proxy_cert, src.as_uri(), src_url)
        assert rc == 0, err

        rc, out, err = _run("rename", proxy_cert, src_url, dst_url)
        assert rc == 0, err

        # Source should be gone
        rc, out, err = _run("stat", proxy_cert, src_url)
        assert rc != 0

        # Destination should exist
        rc, out, err = _run("stat", proxy_cert, dst_url)
        assert rc == 0, err

    def test_rename_preserves_content(self, proxy_cert, pilot_dir, tmp_path):
        """Renamed file should have identical content to the original."""
        data = b"content to preserve through rename"
        src = tmp_path / "ren_content_src.bin"
        src.write_bytes(data)
        src_url = f"{pilot_dir}/ren_content_src.bin"
        dst_url = f"{pilot_dir}/ren_content_dst.bin"

        rc, out, err = _run("cp", proxy_cert, src.as_uri(), src_url)
        assert rc == 0, err

        rc, out, err = _run("rename", proxy_cert, src_url, dst_url)
        assert rc == 0, err

        dl = tmp_path / "ren_dl.bin"
        rc, out, err = _run("cp", proxy_cert, dst_url, dl.as_uri())
        assert rc == 0, err
        assert dl.read_bytes() == data

    def test_rename_directory(self, proxy_cert, pilot_dir):
        """Renaming a directory should work."""
        src_url = f"{pilot_dir}/ren_dir_src"
        dst_url = f"{pilot_dir}/ren_dir_dst"

        rc, out, err = _run("mkdir", proxy_cert, src_url)
        assert rc == 0, err

        rc, out, err = _run("rename", proxy_cert, src_url, dst_url)
        assert rc == 0, err

        rc, out, err = _run("stat", proxy_cert, src_url)
        assert rc != 0

        rc, out, err = _run("stat", proxy_cert, dst_url)
        assert rc == 0, err

    def test_rename_nonexistent_fails(self, proxy_cert, pilot_dir):
        """Renaming a non-existent source should fail."""
        src_url = f"{pilot_dir}/no_such_rename_src.bin"
        dst_url = f"{pilot_dir}/no_such_rename_dst.bin"
        rc, out, err = _run("rename", proxy_cert, src_url, dst_url)
        assert rc != 0


# ---------------------------------------------------------------------------
# TestEosPilotCat
# ---------------------------------------------------------------------------


@requires_eospilot
@requires_proxy
class TestEosPilotCat:
    def test_cat_text_file(self, proxy_cert, pilot_dir, tmp_path):
        """cat should print the file contents to stdout."""
        content = "hello from eospilot\nline two\n"
        src = tmp_path / "cat_test.txt"
        src.write_text(content)
        dst = f"{pilot_dir}/cat_test.txt"
        rc, out, err = _run("cp", proxy_cert, src.as_uri(), dst)
        assert rc == 0, err

        rc, out, err = _run("cat", proxy_cert, dst)
        assert rc == 0, err
        assert out == content

    def test_cat_binary_file(self, proxy_cert, pilot_dir, tmp_path):
        """cat on a binary file should produce the exact bytes."""
        from helpers import run_gfal_binary

        data = bytes(range(256))
        src = tmp_path / "cat_bin.bin"
        src.write_bytes(data)
        dst = f"{pilot_dir}/cat_bin.bin"
        rc, out, err = _run("cp", proxy_cert, src.as_uri(), dst)
        assert rc == 0, err

        rc, out_bytes, _ = run_gfal_binary("cat", "-E", proxy_cert, "--no-verify", dst)
        assert rc == 0
        assert out_bytes == data

    def test_cat_nonexistent_fails(self, proxy_cert, pilot_dir):
        """cat on a missing file should exit non-zero."""
        missing = f"{pilot_dir}/no_such_cat_file.txt"
        rc, out, err = _run("cat", proxy_cert, missing)
        assert rc != 0


# ---------------------------------------------------------------------------
# TestEosPilotSum
# ---------------------------------------------------------------------------


@requires_eospilot
@requires_proxy
class TestEosPilotSum:
    def test_sum_md5(self, proxy_cert, pilot_dir, tmp_path):
        """Upload a file with known content and verify its MD5 checksum."""
        data = b"sum test data for md5 verification"
        src = tmp_path / "sum_md5.bin"
        src.write_bytes(data)
        expected_md5 = hashlib.md5(data).hexdigest()

        dst = f"{pilot_dir}/sum_md5.bin"
        rc, out, err = _run("cp", proxy_cert, src.as_uri(), dst)
        assert rc == 0, err

        rc, out, err = _run("sum", proxy_cert, dst, "MD5")
        assert rc == 0, err
        assert expected_md5 in out

    def test_sum_adler32(self, proxy_cert, pilot_dir, tmp_path):
        """Upload a file and verify ADLER32 output format (8 hex chars)."""
        data = b"sum test data for adler32 verification"
        src = tmp_path / "sum_adler32.bin"
        src.write_bytes(data)

        dst = f"{pilot_dir}/sum_adler32.bin"
        rc, out, err = _run("cp", proxy_cert, src.as_uri(), dst)
        assert rc == 0, err

        rc, out, err = _run("sum", proxy_cert, dst, "ADLER32")
        assert rc == 0, err
        parts = out.strip().split()
        assert len(parts) == 2
        assert len(parts[1]) == 8

    def test_sum_nonexistent_fails(self, proxy_cert, pilot_dir):
        """sum on a missing file should exit non-zero."""
        missing = f"{pilot_dir}/no_such_sum_file.bin"
        rc, out, err = _run("sum", proxy_cert, missing, "MD5")
        assert rc != 0

    def test_sum_empty_file(self, proxy_cert, pilot_dir, tmp_path):
        """MD5 of an empty file is the well-known constant."""
        src = tmp_path / "empty_sum.bin"
        src.write_bytes(b"")
        dst = f"{pilot_dir}/empty_sum.bin"
        rc, out, err = _run("cp", proxy_cert, src.as_uri(), dst)
        assert rc == 0, err

        rc, out, err = _run("sum", proxy_cert, dst, "MD5")
        assert rc == 0, err
        # MD5 of empty string
        assert hashlib.md5(b"").hexdigest() in out

    def test_sum_sha256(self, proxy_cert, pilot_dir, tmp_path):
        """SHA256 checksum should match the locally computed value."""
        data = b"sha256 verification data for eospilot"
        src = tmp_path / "sum_sha256.bin"
        src.write_bytes(data)
        expected_sha256 = hashlib.sha256(data).hexdigest()
        dst = f"{pilot_dir}/sum_sha256.bin"

        rc, out, err = _run("cp", proxy_cert, src.as_uri(), dst)
        assert rc == 0, err

        rc, out, err = _run("sum", proxy_cert, dst, "SHA256")
        assert rc == 0, err
        assert expected_sha256 in out


# ---------------------------------------------------------------------------
# TestEosPilotSave
# ---------------------------------------------------------------------------


@requires_eospilot
@requires_proxy
class TestEosPilotSave:
    def test_save_from_stdin(self, proxy_cert, pilot_dir):
        """gfal save should upload stdin content to the destination."""
        content = "saved from stdin on eospilot\n"
        dst = f"{pilot_dir}/saved_stdin.txt"

        rc, out, err = run_gfal(
            "save", "-E", proxy_cert, "--no-verify", dst, input=content
        )
        assert rc == 0, err

        rc, out, err = _run("cat", proxy_cert, dst)
        assert rc == 0, err
        assert out == content

    def test_save_and_cat_roundtrip(self, proxy_cert, pilot_dir):
        """save then cat should recover identical content."""
        data = "line1\nline2\nline3\n"
        dst = f"{pilot_dir}/save_roundtrip.txt"

        rc, out, err = run_gfal(
            "save", "-E", proxy_cert, "--no-verify", dst, input=data
        )
        assert rc == 0, err

        rc, out, err = _run("cat", proxy_cert, dst)
        assert rc == 0, err
        assert out == data

    def test_save_binary_data(self, proxy_cert, pilot_dir):
        """save with binary stdin should round-trip byte-for-byte via cat."""
        from helpers import run_gfal_binary

        data = bytes(range(256))
        dst = f"{pilot_dir}/saved_binary.bin"

        rc, _out, err = run_gfal_binary(
            "save", "-E", proxy_cert, "--no-verify", dst, input_bytes=data
        )
        assert rc == 0, (
            err.decode("utf-8", errors="replace") if isinstance(err, bytes) else err
        )

        rc, out_bytes, _ = run_gfal_binary("cat", "-E", proxy_cert, "--no-verify", dst)
        assert rc == 0
        assert out_bytes == data

    def test_save_overwrites_existing_file(self, proxy_cert, pilot_dir):
        """A second save to the same path should replace the first content."""
        dst = f"{pilot_dir}/save_overwrite.txt"

        rc, out, err = run_gfal(
            "save", "-E", proxy_cert, "--no-verify", dst, input="first version\n"
        )
        assert rc == 0, err

        rc, out, err = run_gfal(
            "save", "-E", proxy_cert, "--no-verify", dst, input="second version\n"
        )
        assert rc == 0, err

        rc, out, err = _run("cat", proxy_cert, dst)
        assert rc == 0, err
        assert out == "second version\n"


# ---------------------------------------------------------------------------
# TestEosPilotTpc
# ---------------------------------------------------------------------------


@requires_eospilot
@requires_proxy
class TestEosPilotTpc:
    def test_tpc_copy_from_public(self, proxy_cert, pilot_dir):
        """Server-side copy from eospublic to eospilot using --tpc."""
        dst = f"{pilot_dir}/tpc_from_public.C"
        rc, out, err = _run("cp", proxy_cert, "--tpc", _PUBSRC, dst)
        assert rc == 0, err

        rc, out, err = _run("stat", proxy_cert, dst)
        assert rc == 0, err
        assert str(_PUBSRC_SIZE) in out

    def test_tpc_preserves_size(self, proxy_cert, pilot_dir):
        """TPC copy size must match the known source size."""
        dst = f"{pilot_dir}/tpc_size_check.C"
        rc, out, err = _run("cp", proxy_cert, "--tpc", _PUBSRC, dst)
        assert rc == 0, err

        rc, out, err = _run("stat", proxy_cert, dst)
        assert rc == 0, err
        assert str(_PUBSRC_SIZE) in out

    def test_tpc_only_fails_for_local_src(self, proxy_cert, pilot_dir, tmp_path):
        """--tpc-only from a local source must fail (TPC not applicable)."""
        src = tmp_path / "local_src.bin"
        src.write_bytes(b"local data")
        dst = f"{pilot_dir}/tpc_only_local.bin"

        rc, out, err = _run("cp", proxy_cert, "--tpc-only", src.as_uri(), dst)
        assert rc != 0

    def test_tpc_with_checksum(self, proxy_cert, pilot_dir):
        """--tpc combined with -K ADLER32 should verify the transferred file."""
        dst = f"{pilot_dir}/tpc_checksum.C"
        rc, out, err = _run("cp", proxy_cert, "--tpc", "-K", "ADLER32", _PUBSRC, dst)
        assert rc == 0, err

    def test_tpc_force_overwrite(self, proxy_cert, pilot_dir):
        """TPC with -f should overwrite an existing destination."""
        dst = f"{pilot_dir}/tpc_force.C"
        rc, out, err = _run("cp", proxy_cert, "--tpc", _PUBSRC, dst)
        assert rc == 0, err

        rc, out, err = _run("cp", proxy_cert, "--tpc", "-f", _PUBSRC, dst)
        assert rc == 0, err


# ---------------------------------------------------------------------------
# TestEosPilotXrootd  (root:// protocol)
# ---------------------------------------------------------------------------


def _xrootd_gsi_native() -> bool:
    """True if XRootD GSI auth is expected to work natively.

    On Linux, GSI works when the CERN CA certificates are in
    /etc/grid-security/certificates (pointed to by XRD_CADIR).  On macOS the
    pip-installed xrootd lacks a proper cert dir and falls back to Kerberos,
    so we use Docker instead.
    """
    if sys.platform != "linux":
        return False
    cadir = os.environ.get("XRD_CADIR", "")
    if cadir and Path(cadir).is_dir():
        return True
    return Path("/etc/grid-security/certificates").is_dir()


requires_xrootd_env = pytest.mark.skipif(
    (not docker_available() and not _xrootd_gsi_native()) and not CI,
    reason=(
        "XRootD GSI tests require either the xrootd-cern-test Docker image "
        "or Linux with /etc/grid-security/certificates (XRD_CADIR) set up"
    ),
)


@requires_eospilot
@requires_proxy
@requires_xrootd_env
class TestEosPilotXrootd:
    """XRootD protocol tests.

    On macOS: run inside the xrootd-cern-test Docker container (GSI auth
    requires CERN CAs in /etc/grid-security which the container provides).
    On Linux (CI): run natively — the integration job sets up XRD_CADIR.

    GSI authentication requires /etc/grid-security/certificates with proper
    CERN CA setup, which is already baked into the xrootd-cern-test image.
    On macOS the pip-installed xrootd falls back to Kerberos without the CA
    directory, so we delegate to Docker instead.
    """

    _XROOTD_BASE = "root://eospilot.cern.ch//eos/pilot/opstest/dteam/python3-gfal/tmp"
    _XROOTD_NO_ACCESS = "root://eospilot.cern.ch//eos/pilot/opstest/dteam/python3-gfal/dteam-has-no-permissions-here"

    def _run(self, cmd, proxy_cert, *args):
        # Do NOT pass -E to gfal: base.py would then set X509_USER_CERT/KEY and
        # remove X509_USER_PROXY, causing XRootD to use cert auth instead of
        # proxy auth (which the server rejects as "unauthorized identity").
        # X509_USER_PROXY must be set in the environment instead.
        if _xrootd_gsi_native():
            # Linux CI: run natively; X509_USER_PROXY is already in the env.
            return run_gfal(cmd, *args)
        # macOS (and other non-Linux): use Docker where CAs are pre-installed.
        return run_gfal_docker(cmd, *args, proxy_cert=proxy_cert)

    @pytest.fixture
    def xrootd_pilot_dir(self, proxy_cert):
        """Scratch dir created and cleaned up via HTTPS; yielded as root:// URL."""
        name = f"pytest-xrd-{uuid.uuid4().hex[:8]}"
        https_url = f"{_PILOT_BASE}/{name}"
        xrd_url = f"{self._XROOTD_BASE}/{name}"
        rc, out, err = run_gfal("mkdir", "-E", proxy_cert, "--no-verify", https_url)
        require_test_prereq(
            rc == 0, f"Could not create xrootd_pilot_dir: {err.strip()}"
        )
        yield xrd_url
        run_gfal("rm", "-r", "-E", proxy_cert, "--no-verify", https_url)

    def test_copy_local_to_xrootd(self, proxy_cert, xrootd_pilot_dir):
        """Upload /etc/hostname via root:// and stat it."""
        dst = f"{xrootd_pilot_dir}/xrd_up.bin"
        rc, out, err = self._run("cp", proxy_cert, "/etc/hostname", dst)
        assert rc == 0, err

        rc, out, err = self._run("stat", proxy_cert, dst)
        assert rc == 0, err
        assert "regular file" in out

    def test_copy_xrootd_to_xrootd(self, proxy_cert, xrootd_pilot_dir):
        """Copy one file to another within eospilot via root://."""
        src = f"{xrootd_pilot_dir}/src.bin"
        dst = f"{xrootd_pilot_dir}/dst.bin"

        rc, out, err = self._run("cp", proxy_cert, "/etc/hostname", src)
        assert rc == 0, err

        rc, out, err = self._run("cp", proxy_cert, src, dst)
        assert rc == 0, err

        rc, out, err = self._run("stat", proxy_cert, dst)
        assert rc == 0, err

    def test_ls_xrootd_directory(self, proxy_cert, xrootd_pilot_dir):
        """ls over root:// should list uploaded files."""
        rc, out, err = self._run(
            "cp", proxy_cert, "/etc/hostname", f"{xrootd_pilot_dir}/xrd_ls.bin"
        )
        assert rc == 0, err

        rc, out, err = self._run("ls", proxy_cert, xrootd_pilot_dir)
        assert rc == 0, err
        assert "xrd_ls.bin" in out

    def test_stat_xrootd_directory(self, proxy_cert, xrootd_pilot_dir):
        """stat over root:// on the directory should report 'directory'."""
        rc, out, err = self._run("stat", proxy_cert, xrootd_pilot_dir)
        assert rc == 0, err
        assert "directory" in out

    def test_xrootd_permission_denied(self, proxy_cert):
        """Writing to the no-access path over root:// should fail."""
        dst = f"{self._XROOTD_NO_ACCESS}/xrd_denied.bin"
        rc, out, err = self._run("cp", proxy_cert, "/etc/hostname", dst)
        assert rc == 13
        assert (
            "permission denied" in err.lower()
            or "operation not permitted" in err.lower()
        )

    def test_mkdir_xrootd(self, proxy_cert, xrootd_pilot_dir):
        """mkdir over root:// should create a subdirectory."""
        subdir = f"{xrootd_pilot_dir}/xrd_mkdir_sub"
        rc, out, err = self._run("mkdir", proxy_cert, subdir)
        assert rc == 0, err

        rc, out, err = self._run("stat", proxy_cert, subdir)
        assert rc == 0, err

    def test_rm_xrootd(self, proxy_cert, xrootd_pilot_dir):
        """rm over root:// should delete a file."""
        dst = f"{xrootd_pilot_dir}/xrd_rm.bin"
        rc, out, err = self._run("cp", proxy_cert, "/etc/hostname", dst)
        assert rc == 0, err

        rc, out, err = self._run("rm", proxy_cert, dst)
        assert rc == 0, err

        rc, out, err = self._run("stat", proxy_cert, dst)
        assert rc != 0

    def test_https_to_xrootd_copy(self, proxy_cert, xrootd_pilot_dir):
        """Cross-protocol: upload via HTTPS, read back via XRootD."""
        https_dst = (
            xrootd_pilot_dir.replace(
                "root://eospilot.cern.ch/", "https://eospilot.cern.ch/", 1
            )
            + "/cross.bin"
        )
        rc, out, err = run_gfal(
            "cp", "-E", proxy_cert, "--no-verify", "/etc/hosts", https_dst
        )
        assert rc == 0, err

        xrd_src = f"{xrootd_pilot_dir}/cross.bin"
        rc, out, err = self._run("stat", proxy_cert, xrd_src)
        assert rc == 0, err

    def test_cat_xrootd(self, proxy_cert, xrootd_pilot_dir):
        """cat a file over root:// should return the correct content."""
        dst = f"{xrootd_pilot_dir}/xrd_cat.txt"
        rc, out, err = self._run("cp", proxy_cert, "/etc/hostname", dst)
        assert rc == 0, err

        rc, out, err = self._run("cat", proxy_cert, dst)
        assert rc == 0, err
        # /etc/hostname contains the container/machine hostname — just non-empty
        assert out.strip()

    def test_sum_adler32_xrootd(self, proxy_cert, xrootd_pilot_dir, tmp_path):
        """ADLER32 checksum via root:// must match a locally computed value."""
        import zlib

        # Use a temp file with known content instead of /etc/hostname, because
        # when Docker is used (non-native GSI) the container has a different
        # /etc/hostname from the host.
        data = b"adler32 checksum test content for xrootd\n"
        src = tmp_path / "xrd_sum_src.txt"
        src.write_bytes(data)
        expected = f"{zlib.adler32(data) & 0xFFFFFFFF:08x}"

        dst = f"{xrootd_pilot_dir}/xrd_sum.txt"
        rc, out, err = self._run("cp", proxy_cert, src.as_uri(), dst)
        assert rc == 0, err

        rc, out, err = self._run("sum", proxy_cert, dst, "ADLER32")
        assert rc == 0, err
        assert expected in out.lower()

    def test_ls_long_format_xrootd(self, proxy_cert, xrootd_pilot_dir):
        """ls -l over root:// should list uploaded files with their sizes."""
        dst = f"{xrootd_pilot_dir}/xrd_lslong.bin"
        rc, out, err = self._run("cp", proxy_cert, "/etc/hostname", dst)
        assert rc == 0, err

        rc, out, err = self._run("ls", proxy_cert, "-l", xrootd_pilot_dir)
        assert rc == 0, err
        assert "xrd_lslong.bin" in out

    def test_rename_xrootd(self, proxy_cert, xrootd_pilot_dir):
        """rename over root:// should move the file atomically."""
        src = f"{xrootd_pilot_dir}/xrd_ren_src.bin"
        dst = f"{xrootd_pilot_dir}/xrd_ren_dst.bin"

        rc, out, err = self._run("cp", proxy_cert, "/etc/hostname", src)
        assert rc == 0, err

        rc, out, err = self._run("rename", proxy_cert, src, dst)
        assert rc == 0, err

        rc, out, err = self._run("stat", proxy_cert, src)
        assert rc != 0  # source gone

        rc, out, err = self._run("stat", proxy_cert, dst)
        assert rc == 0, err  # destination present

    def test_rm_recursive_xrootd(self, proxy_cert, xrootd_pilot_dir):
        """rm -r over root:// should delete a directory and all its contents."""
        subdir = f"{xrootd_pilot_dir}/xrd_rmr_sub"
        rc, out, err = self._run("mkdir", proxy_cert, subdir)
        assert rc == 0, err

        rc, out, err = self._run(
            "cp", proxy_cert, "/etc/hostname", f"{subdir}/xrd_f1.bin"
        )
        assert rc == 0, err

        rc, out, err = self._run("rm", proxy_cert, "-r", subdir)
        assert rc == 0, err

        rc, out, err = self._run("stat", proxy_cert, subdir)
        assert rc != 0

    def test_mkdir_parents_xrootd(self, proxy_cert, xrootd_pilot_dir):
        """mkdir -p over root:// should create nested directories in one call."""
        deep = f"{xrootd_pilot_dir}/xrd_deep/nested"
        rc, out, err = self._run("mkdir", proxy_cert, "-p", deep)
        assert rc == 0, err

        rc, out, err = self._run("stat", proxy_cert, deep)
        assert rc == 0, err
        assert "directory" in out


# ---------------------------------------------------------------------------
# TestEosPilotCompare
# ---------------------------------------------------------------------------


@requires_eospilot
@requires_proxy
class TestEosPilotCompare:
    """Tests for --compare size and --compare size_mtime skip logic."""

    def test_compare_size_skips_same_size(self, proxy_cert, pilot_dir, tmp_path):
        """--compare size should skip when the destination has the same size."""
        data = b"content for size comparison test"
        src = tmp_path / "cmp_size.bin"
        src.write_bytes(data)
        dst = f"{pilot_dir}/cmp_size.bin"

        rc, out, err = _run("cp", proxy_cert, src.as_uri(), dst)
        assert rc == 0, err

        rc, out, err = _run("cp", proxy_cert, "--compare", "size", src.as_uri(), dst)
        assert rc == 0, err
        assert "Skipping existing file" in out
        assert "matching size" in out

    def test_compare_size_overwrites_on_size_mismatch(
        self, proxy_cert, pilot_dir, tmp_path
    ):
        """--compare size should overwrite when the destination size differs."""
        small = tmp_path / "small.bin"
        small.write_bytes(b"small")
        dst = f"{pilot_dir}/cmp_size_diff.bin"

        rc, out, err = _run("cp", proxy_cert, small.as_uri(), dst)
        assert rc == 0, err

        large = tmp_path / "large.bin"
        large.write_bytes(b"much larger content to force size mismatch")

        rc, out, err = _run("cp", proxy_cert, "--compare", "size", large.as_uri(), dst)
        assert rc == 0, err
        # Verify destination was overwritten with the larger content
        dl = tmp_path / "downloaded.bin"
        rc, out, err = _run("cp", proxy_cert, dst, dl.as_uri())
        assert rc == 0, err
        assert dl.read_bytes() == large.read_bytes()

    def test_compare_size_mtime_skips_matching(self, proxy_cert, pilot_dir, tmp_path):
        """--compare size_mtime should skip when both size and mtime match."""
        data = b"size_mtime comparison data"
        src = tmp_path / "cmp_smtime.bin"
        src.write_bytes(data)
        dst = f"{pilot_dir}/cmp_smtime.bin"

        # Initial upload; explicit --preserve-times injects eos.mtime into the
        # URL so the server records the source file's mtime (not the upload time).
        rc, out, err = _run("cp", proxy_cert, "--preserve-times", src.as_uri(), dst)
        assert rc == 0, err

        # Second upload with --compare size_mtime: same local file → skip
        rc, out, err = _run(
            "cp", proxy_cert, "--compare", "size_mtime", src.as_uri(), dst
        )
        assert rc == 0, err
        assert "Skipping existing file" in out

    def test_compare_size_mtime_copies_on_mtime_change(
        self, proxy_cert, pilot_dir, tmp_path
    ):
        """--compare size_mtime should copy when mtime differs even with same size."""
        import time

        data = b"same-size-but-mtime-changes"
        src = tmp_path / "cmp_smtime2.bin"
        src.write_bytes(data)
        dst = f"{pilot_dir}/cmp_smtime2.bin"

        rc, out, err = _run("cp", proxy_cert, src.as_uri(), dst)
        assert rc == 0, err

        # Touch the source to bump its mtime (sleep 1 s to ensure ≥1 s difference)
        time.sleep(1)
        src.write_bytes(data)  # same content, new mtime

        rc, out, err = _run(
            "cp", proxy_cert, "--compare", "size_mtime", src.as_uri(), dst
        )
        assert rc == 0, err
        # Should have copied (overwritten), not skipped
        assert "Skipping existing file" not in out


# ---------------------------------------------------------------------------
# TestEosPilotCopyMode
# ---------------------------------------------------------------------------


@requires_eospilot
@requires_proxy
class TestEosPilotCopyMode:
    """Tests for the --copy-mode flag."""

    def test_copy_mode_streamed_forces_client_side_copy(self, proxy_cert, pilot_dir):
        """--copy-mode streamed must always use client-side streaming."""
        dst = f"{pilot_dir}/copy_mode_streamed.C"
        rc, out, err = _run("cp", proxy_cert, "--copy-mode", "streamed", _PUBSRC, dst)

        assert rc == 0, err
        assert "(streamed)" in out

    def test_copy_mode_pull_from_public(self, proxy_cert, pilot_dir):
        """--copy-mode pull attempts TPC pull; streaming fallback is acceptable."""
        dst = f"{pilot_dir}/copy_mode_pull.C"
        rc, out, err = _run("cp", proxy_cert, "--copy-mode", "pull", _PUBSRC, dst)

        assert rc == 0, err
        # Server may not support TPC; both outcomes are valid
        assert "(TPC pull)" in out or "(streamed)" in out

    def test_copy_mode_streamed_checksum(self, proxy_cert, pilot_dir):
        """--copy-mode streamed combined with -K ADLER32 should verify the copy."""
        dst = f"{pilot_dir}/copy_mode_streamed_ck.C"
        rc, out, err = _run(
            "cp", proxy_cert, "--copy-mode", "streamed", "-K", "ADLER32", _PUBSRC, dst
        )

        assert rc == 0, err


# ---------------------------------------------------------------------------
# TestEosPilotRecursiveCopy
# ---------------------------------------------------------------------------


@requires_eospilot
@requires_proxy
class TestEosPilotRecursiveCopy:
    """Recursive copy (-r) between local and EOS HTTPS."""

    def test_upload_directory_tree_to_eos(self, proxy_cert, pilot_dir, tmp_path):
        """cp -r of a local dir to EOS should upload all files recursively."""
        srcdir = tmp_path / "srcdir"
        srcdir.mkdir()
        (srcdir / "a.txt").write_text("file a")
        (srcdir / "b.txt").write_text("file b")
        subdir = srcdir / "sub"
        subdir.mkdir()
        (subdir / "c.txt").write_text("file c")

        dst = f"{pilot_dir}/uploaded_tree"
        rc, out, err = _run("cp", proxy_cert, "-r", srcdir.as_uri(), dst, timeout=60)
        assert rc == 0, err

        # Verify each file exists and has the correct content
        for name, content in [("a.txt", "file a"), ("b.txt", "file b")]:
            rc, out, err = _run("cat", proxy_cert, f"{dst}/{name}")
            assert rc == 0, f"{name}: {err}"
            assert out == content

        rc, out, err = _run("cat", proxy_cert, f"{dst}/sub/c.txt")
        assert rc == 0, err
        assert out == "file c"

    def test_download_directory_tree_from_eos(self, proxy_cert, pilot_dir, tmp_path):
        """cp -r of an EOS dir to local should recreate the full tree."""
        # First upload a tree
        srcdir = tmp_path / "src"
        srcdir.mkdir()
        (srcdir / "x.bin").write_bytes(b"xdata")
        (srcdir / "y.bin").write_bytes(b"ydata")
        sub = srcdir / "nested"
        sub.mkdir()
        (sub / "z.bin").write_bytes(b"zdata")

        remote = f"{pilot_dir}/dl_tree"
        rc, out, err = _run("cp", proxy_cert, "-r", srcdir.as_uri(), remote, timeout=60)
        assert rc == 0, err

        # Now download it back
        dstdir = tmp_path / "dst"
        rc, out, err = _run("cp", proxy_cert, "-r", remote, dstdir.as_uri(), timeout=60)
        assert rc == 0, err

        assert (dstdir / "x.bin").read_bytes() == b"xdata"
        assert (dstdir / "y.bin").read_bytes() == b"ydata"
        assert (dstdir / "nested" / "z.bin").read_bytes() == b"zdata"

    def test_recursive_copy_with_parallel_1(self, proxy_cert, pilot_dir, tmp_path):
        """--parallel 1 forces sequential upload; all files should still arrive."""
        srcdir = tmp_path / "par1_src"
        srcdir.mkdir()
        for i in range(5):
            (srcdir / f"f{i}.bin").write_bytes(f"content {i}".encode())

        dst = f"{pilot_dir}/par1_dst"
        rc, out, err = _run(
            "cp", proxy_cert, "-r", "--parallel", "1", srcdir.as_uri(), dst, timeout=90
        )
        assert rc == 0, err

        for i in range(5):
            rc, out, err = _run("stat", proxy_cert, f"{dst}/f{i}.bin")
            assert rc == 0, f"f{i}.bin missing: {err}"

    def test_recursive_copy_with_parallel_3(self, proxy_cert, pilot_dir, tmp_path):
        """--parallel 3 uploads 3 files concurrently; all files should arrive."""
        srcdir = tmp_path / "par3_src"
        srcdir.mkdir()
        for i in range(6):
            (srcdir / f"p{i}.bin").write_bytes(f"parallel file {i}".encode())

        dst = f"{pilot_dir}/par3_dst"
        rc, out, err = _run(
            "cp", proxy_cert, "-r", "--parallel", "3", srcdir.as_uri(), dst, timeout=90
        )
        assert rc == 0, err

        for i in range(6):
            rc, out, err = _run("stat", proxy_cert, f"{dst}/p{i}.bin")
            assert rc == 0, f"p{i}.bin missing: {err}"

    def test_recursive_copy_continues_after_partial_failure(
        self, proxy_cert, pilot_dir, tmp_path
    ):
        """Without --abort-on-failure, recursive copy continues past EEXIST errors."""
        srcdir = tmp_path / "partial_src"
        srcdir.mkdir()
        (srcdir / "existing.bin").write_bytes(b"original")
        (srcdir / "new.bin").write_bytes(b"fresh")

        dst = f"{pilot_dir}/partial_dst"
        # Create the destination dir and pre-upload one file so it already exists
        rc, out, err = _run("mkdir", proxy_cert, dst)
        assert rc == 0, err
        rc, out, err = _run(
            "cp", proxy_cert, (srcdir / "existing.bin").as_uri(), f"{dst}/existing.bin"
        )
        assert rc == 0, err

        # Recursive copy: existing.bin should fail with EEXIST, new.bin should succeed
        rc, out, err = _run("cp", proxy_cert, "-r", srcdir.as_uri(), dst, timeout=60)

        assert rc == errno.EEXIST, f"Expected EEXIST (17) but got rc={rc}: {err}"

        # The new file must have been copied despite the partial failure
        rc_new, _, _ = _run("stat", proxy_cert, f"{dst}/new.bin")
        assert rc_new == 0, "new.bin should have been copied despite partial failure"

    def test_recursive_copy_abort_on_failure(self, proxy_cert, pilot_dir, tmp_path):
        """--abort-on-failure stops recursive copy after the first error."""
        srcdir = tmp_path / "abort_src"
        srcdir.mkdir()
        (srcdir / "existing.bin").write_bytes(b"pre-exists")
        (srcdir / "also_new.bin").write_bytes(b"also new")

        dst = f"{pilot_dir}/abort_dst"
        rc, out, err = _run("mkdir", proxy_cert, dst)
        assert rc == 0, err
        rc, out, err = _run(
            "cp", proxy_cert, (srcdir / "existing.bin").as_uri(), f"{dst}/existing.bin"
        )
        assert rc == 0, err

        # With --abort-on-failure, the copy should fail immediately
        rc, out, err = _run(
            "cp",
            proxy_cert,
            "-r",
            "--abort-on-failure",
            srcdir.as_uri(),
            dst,
            timeout=60,
        )
        assert rc != 0, "Expected failure with --abort-on-failure"

    def test_recursive_copy_with_checksum(self, proxy_cert, pilot_dir, tmp_path):
        """Recursive copy with -K ADLER32 should verify each file."""
        srcdir = tmp_path / "ck_src"
        srcdir.mkdir()
        (srcdir / "ck1.bin").write_bytes(b"checksum data 1")
        (srcdir / "ck2.bin").write_bytes(b"checksum data 2")

        dst = f"{pilot_dir}/ck_dst"
        rc, out, err = _run(
            "cp", proxy_cert, "-r", "-K", "ADLER32", srcdir.as_uri(), dst, timeout=60
        )
        assert rc == 0, err

    def test_recursive_dry_run_prints_but_does_not_copy(
        self, proxy_cert, pilot_dir, tmp_path
    ):
        """--dry-run on a directory should print the plan but upload nothing."""
        srcdir = tmp_path / "dry_src"
        srcdir.mkdir()
        (srcdir / "dry1.bin").write_bytes(b"dry run data")

        dst = f"{pilot_dir}/dry_dst"
        rc, out, err = _run(
            "cp", proxy_cert, "-r", "--dry-run", srcdir.as_uri(), dst, timeout=30
        )
        assert rc == 0, err
        assert "dry1.bin" in out or "Copy" in out

        # Destination directory should NOT have been created
        rc, _, _ = _run("stat", proxy_cert, dst)
        assert rc != 0, "dry-run should not create the destination"

    def test_recursive_copy_force_overwrites_existing_files(
        self, proxy_cert, pilot_dir, tmp_path
    ):
        """With -f, recursive copy should overwrite existing destination files."""
        srcdir = tmp_path / "force_src"
        srcdir.mkdir()
        (srcdir / "overwrite_me.bin").write_bytes(b"version1")

        dst = f"{pilot_dir}/force_dst"
        # First upload
        rc, out, err = _run("cp", proxy_cert, "-r", srcdir.as_uri(), dst, timeout=60)
        assert rc == 0, err

        # Modify the source and force-overwrite
        (srcdir / "overwrite_me.bin").write_bytes(b"version2")
        rc, out, err = _run(
            "cp", proxy_cert, "-r", "-f", srcdir.as_uri(), dst, timeout=60
        )
        assert rc == 0, err

        # Verify the new content
        rc, out, err = _run("cat", proxy_cert, f"{dst}/overwrite_me.bin")
        assert rc == 0, err
        assert out == "version2"

    def test_recursive_copy_compare_checksum_skips_matching(
        self, proxy_cert, pilot_dir, tmp_path
    ):
        """Recursive --compare checksum should skip unchanged files."""
        srcdir = tmp_path / "cmp_ck_src"
        srcdir.mkdir()
        (srcdir / "same.bin").write_bytes(b"same content")
        (srcdir / "new.bin").write_bytes(b"new content")

        dst = f"{pilot_dir}/cmp_ck_dst"
        # Pre-upload the "same" file so it already exists with matching content
        rc, out, err = _run("mkdir", proxy_cert, dst)
        assert rc == 0, err
        rc, out, err = _run(
            "cp", proxy_cert, (srcdir / "same.bin").as_uri(), f"{dst}/same.bin"
        )
        assert rc == 0, err

        rc, out, err = _run(
            "cp",
            proxy_cert,
            "-r",
            "--compare",
            "checksum",
            srcdir.as_uri(),
            dst,
            timeout=60,
        )
        assert rc == 0, err
        assert "same.bin" in out
        assert "matching" in out


# ---------------------------------------------------------------------------
# TestEosPilotCopyToStdout
# ---------------------------------------------------------------------------


@requires_eospilot
@requires_proxy
class TestEosPilotCopyToStdout:
    """Tests for ``gfal cp <eos-url> -`` (stream remote content to stdout)."""

    def test_copy_remote_file_to_stdout(self, proxy_cert, pilot_dir, tmp_path):
        """Download from EOS to stdout using '-' as destination."""
        content = b"streamed to stdout from eospilot\n"
        src = tmp_path / "stdout_src.bin"
        src.write_bytes(content)

        dst = f"{pilot_dir}/stdout_src.bin"
        rc, out, err = _run("cp", proxy_cert, src.as_uri(), dst)
        assert rc == 0, err

        rc, out, err = _run("cp", proxy_cert, dst, "-")
        assert rc == 0, err
        assert content.decode() in out

    def test_copy_remote_text_file_to_stdout(self, proxy_cert, pilot_dir, tmp_path):
        """Text file downloaded to stdout via '-' should match the original."""
        text = "line one\nline two\nline three\n"
        src = tmp_path / "stdout_text.txt"
        src.write_text(text)

        dst = f"{pilot_dir}/stdout_text.txt"
        rc, out, err = _run("cp", proxy_cert, src.as_uri(), dst)
        assert rc == 0, err

        rc, out, err = _run("cp", proxy_cert, dst, "-")
        assert rc == 0, err
        assert out == text

    def test_copy_public_file_to_stdout(self, proxy_cert):
        """The well-known public file can be streamed to stdout."""
        rc, out, err = _run("cp", proxy_cert, _PUBSRC, "-")
        assert rc == 0, err
        # The public file is the ROOT C macro; just check it has content
        assert len(out) == _PUBSRC_SIZE


# ---------------------------------------------------------------------------
# TestEosPilotXrootdRecursiveCopy
# ---------------------------------------------------------------------------


@requires_eospilot
@requires_proxy
@requires_xrootd_env
class TestEosPilotXrootdRecursiveCopy:
    """Recursive copy (-r) tests over root:// to/from eospilot."""

    def _run(self, cmd, proxy_cert, *args, **kwargs):
        if _xrootd_gsi_native():
            return run_gfal(cmd, *args, **kwargs)
        return run_gfal_docker(cmd, *args, proxy_cert=proxy_cert)

    @pytest.fixture
    def xrootd_pilot_dir(self, proxy_cert):
        """Create and clean up a scratch dir accessible as root://."""
        name = f"pytest-xrd-rec-{uuid.uuid4().hex[:8]}"
        https_url = f"{_PILOT_BASE}/{name}"
        xrd_url = f"{_PILOT_ROOT_BASE}/{name}"
        rc, out, err = run_gfal("mkdir", "-E", proxy_cert, "--no-verify", https_url)
        require_test_prereq(
            rc == 0, f"Could not create xrootd_pilot_dir: {err.strip()}"
        )
        yield xrd_url
        run_gfal("rm", "-r", "-E", proxy_cert, "--no-verify", https_url)

    def test_upload_directory_to_xrootd(self, proxy_cert, xrootd_pilot_dir, tmp_path):
        """cp -r local_dir root://... should upload all files."""
        srcdir = tmp_path / "xrd_rec_src"
        srcdir.mkdir()
        (srcdir / "xrd_f1.bin").write_bytes(b"xrd file one")
        (srcdir / "xrd_f2.bin").write_bytes(b"xrd file two")

        dst = f"{xrootd_pilot_dir}/xrd_rec_dst"
        rc, out, err = self._run(
            "cp", proxy_cert, "-r", srcdir.as_uri(), dst, timeout=90
        )
        assert rc == 0, err

        rc, out, err = self._run("stat", proxy_cert, f"{dst}/xrd_f1.bin")
        assert rc == 0, err

        rc, out, err = self._run("stat", proxy_cert, f"{dst}/xrd_f2.bin")
        assert rc == 0, err

    def test_download_directory_from_xrootd(
        self, proxy_cert, xrootd_pilot_dir, tmp_path
    ):
        """cp -r root://... local_dir should recreate all files locally."""
        srcdir = tmp_path / "xrd_dl_src"
        srcdir.mkdir()
        (srcdir / "dl_a.bin").write_bytes(b"download a")
        (srcdir / "dl_b.bin").write_bytes(b"download b")

        remote = f"{xrootd_pilot_dir}/xrd_dl_tree"
        rc, out, err = self._run(
            "cp", proxy_cert, "-r", srcdir.as_uri(), remote, timeout=90
        )
        assert rc == 0, err

        dstdir = tmp_path / "xrd_dl_dst"
        rc, out, err = self._run(
            "cp", proxy_cert, "-r", remote, dstdir.as_uri(), timeout=90
        )
        assert rc == 0, err

        assert (dstdir / "dl_a.bin").read_bytes() == b"download a"
        assert (dstdir / "dl_b.bin").read_bytes() == b"download b"

    def test_recursive_xrootd_to_xrootd(self, proxy_cert, xrootd_pilot_dir, tmp_path):
        """cp -r root://...src root://...dst should copy all files server-side."""
        srcdir = tmp_path / "xrd_ss_src"
        srcdir.mkdir()
        (srcdir / "ss_f1.bin").write_bytes(b"server side 1")
        (srcdir / "ss_f2.bin").write_bytes(b"server side 2")

        src_remote = f"{xrootd_pilot_dir}/xrd_ss_src"
        dst_remote = f"{xrootd_pilot_dir}/xrd_ss_dst"

        # Upload via local→root
        rc, out, err = self._run(
            "cp", proxy_cert, "-r", srcdir.as_uri(), src_remote, timeout=90
        )
        assert rc == 0, err

        # Copy root→root
        rc, out, err = self._run(
            "cp", proxy_cert, "-r", src_remote, dst_remote, timeout=90
        )
        assert rc == 0, err

        rc, out, err = self._run("stat", proxy_cert, f"{dst_remote}/ss_f1.bin")
        assert rc == 0, err

        rc, out, err = self._run("stat", proxy_cert, f"{dst_remote}/ss_f2.bin")
        assert rc == 0, err
