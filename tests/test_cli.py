"""
Tests that exercise the installed ``gfal`` executable (console script).

These verify that:
  - the ``gfal`` entry point declared in pyproject.toml is installed and on PATH
  - the shebang / wrapper script invokes the right Python entry point
  - basic end-to-end behaviour works through the real binary, not via python -c

All tests skip gracefully when the binary is not found (e.g. in a bare venv
that hasn't run ``pip install -e .``).
"""

import shutil
import subprocess
import sys

import pytest

from helpers import _subprocess_env

# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------


def _find_binary(cmd: str) -> str | None:
    """Locate the gfal binary, preferring the active venv over PATH.

    Using shutil.which() alone can pick up a system-installed binary whose
    shebang points at a Python that lacks this project's dependencies. The
    venv that runs pytest always has the right packages.
    """
    from pathlib import Path

    venv_bin = Path(sys.executable).parent / cmd
    if venv_bin.is_file():
        return str(venv_bin)
    return shutil.which(cmd)


def run_bin(*args, input=None, check=False):
    """Run the installed ``gfal`` binary directly with the given arguments."""
    binary = _find_binary("gfal")
    if binary is None:
        pytest.skip("'gfal' not found — run 'pip install -e .'")
    proc = subprocess.run(
        [binary, *[str(a) for a in args]],
        capture_output=True,
        text=True,
        encoding="utf-8",
        input=input,
        env=_subprocess_env(),
    )
    return proc.returncode, proc.stdout, proc.stderr


def run_bin_binary(*args, input_bytes=None):
    """Like run_bin but captures stdout as raw bytes."""
    binary = _find_binary("gfal")
    if binary is None:
        pytest.skip("'gfal' not found — run 'pip install -e .'")
    proc = subprocess.run(
        [binary, *[str(a) for a in args]],
        capture_output=True,
        input=input_bytes,
        env=_subprocess_env(),
    )
    return proc.returncode, proc.stdout, proc.stderr


# ---------------------------------------------------------------------------
# The gfal executable must be installed
# ---------------------------------------------------------------------------


def test_binary_installed():
    """The gfal console_script entry point must be on PATH."""
    assert _find_binary("gfal") is not None, "'gfal' not found — run 'pip install -e .'"


# ---------------------------------------------------------------------------
# --version works
# ---------------------------------------------------------------------------

SUBCOMMANDS = [
    "ls",
    "cp",
    "rm",
    "mkdir",
    "stat",
    "cat",
    "save",
    "rename",
    "chmod",
    "sum",
    "xattr",
]


@pytest.mark.parametrize("subcmd", SUBCOMMANDS)
def test_version(subcmd):
    rc, out, err = run_bin(subcmd, "--version")
    assert rc == 0
    output = out + err
    assert "gfal" in output


# ---------------------------------------------------------------------------
# gfal cp
# ---------------------------------------------------------------------------


def test_cp_binary(tmp_path):
    src = tmp_path / "src.txt"
    dst = tmp_path / "dst.txt"
    src.write_bytes(b"hello from gfal cp")

    rc, out, err = run_bin("cp", src.as_uri(), dst.as_uri())

    assert rc == 0
    assert dst.read_bytes() == b"hello from gfal cp"


# ---------------------------------------------------------------------------
# gfal ls
# ---------------------------------------------------------------------------


def test_ls_binary(tmp_path):
    (tmp_path / "a.txt").write_text("a")
    (tmp_path / "b.txt").write_text("b")

    rc, out, err = run_bin("ls", tmp_path.as_uri())

    assert rc == 0
    assert "a.txt" in out
    assert "b.txt" in out


def test_ls_long_binary(tmp_path):
    f = tmp_path / "file.txt"
    f.write_bytes(b"x" * 1025)

    rc, out, err = run_bin("ls", "-lH", tmp_path.as_uri())

    assert rc == 0
    assert "1.1K" in out


# ---------------------------------------------------------------------------
# gfal stat
# ---------------------------------------------------------------------------


def test_stat_binary(tmp_path):
    f = tmp_path / "test.txt"
    f.write_bytes(b"hello world")

    rc, out, err = run_bin("stat", f.as_uri())

    assert rc == 0
    assert "11" in out
    assert "regular file" in out


# ---------------------------------------------------------------------------
# gfal cat
# ---------------------------------------------------------------------------


def test_cat_binary(tmp_path):
    f = tmp_path / "test.txt"
    f.write_text("hello world\n")

    rc, out, err = run_bin("cat", f.as_uri())

    assert rc == 0
    assert out == "hello world\n"


def test_cat_binary_content(tmp_path):
    data = bytes(range(256))
    f = tmp_path / "binary.bin"
    f.write_bytes(data)

    rc, stdout, stderr = run_bin_binary("cat", f.as_uri())

    assert rc == 0
    assert stdout == data


# ---------------------------------------------------------------------------
# gfal save
# ---------------------------------------------------------------------------


def test_save_binary(tmp_path):
    f = tmp_path / "out.txt"

    rc, out, err = run_bin("save", f.as_uri(), input="hello save\n")

    assert rc == 0
    assert f.read_text() == "hello save\n"


# ---------------------------------------------------------------------------
# gfal mkdir
# ---------------------------------------------------------------------------


def test_mkdir_binary(tmp_path):
    d = tmp_path / "newdir"

    rc, out, err = run_bin("mkdir", d.as_uri())

    assert rc == 0
    assert d.is_dir()


def test_mkdir_parents_binary(tmp_path):
    d = tmp_path / "a" / "b" / "c"

    rc, out, err = run_bin("mkdir", "-p", d.as_uri())

    assert rc == 0
    assert d.is_dir()


# ---------------------------------------------------------------------------
# gfal rm
# ---------------------------------------------------------------------------


def test_rm_binary(tmp_path):
    f = tmp_path / "file.txt"
    f.write_text("x")

    rc, out, err = run_bin("rm", f.as_uri())

    assert rc == 0
    assert not f.exists()
    assert "DELETED" in out


def test_rm_recursive_binary(tmp_path):
    d = tmp_path / "mydir"
    d.mkdir()
    (d / "f.txt").write_text("x")

    rc, out, err = run_bin("rm", "-r", d.as_uri())

    assert rc == 0
    assert not d.exists()


# ---------------------------------------------------------------------------
# gfal rename
# ---------------------------------------------------------------------------


def test_rename_binary(tmp_path):
    src = tmp_path / "old.txt"
    dst = tmp_path / "new.txt"
    src.write_text("content")

    rc, out, err = run_bin("rename", src.as_uri(), dst.as_uri())

    assert rc == 0
    assert not src.exists()
    assert dst.read_text() == "content"


# ---------------------------------------------------------------------------
# gfal chmod
# ---------------------------------------------------------------------------


@pytest.mark.skipif(
    sys.platform == "win32", reason="POSIX chmod semantics not available on Windows"
)
def test_chmod_binary(tmp_path):
    f = tmp_path / "test.txt"
    f.write_text("x")

    rc, out, err = run_bin("chmod", "600", f.as_uri())

    assert rc == 0
    assert (f.stat().st_mode & 0o777) == 0o600


# ---------------------------------------------------------------------------
# gfal sum
# ---------------------------------------------------------------------------


def test_sum_binary(tmp_path):
    import zlib

    data = b"hello world"
    f = tmp_path / "test.bin"
    f.write_bytes(data)
    expected = f"{zlib.adler32(data) & 0xFFFFFFFF:08x}"

    rc, out, err = run_bin("sum", f.as_uri(), "ADLER32")

    assert rc == 0
    assert expected in out


# ---------------------------------------------------------------------------
# gfal xattr (basic invocation — just verify it runs)
# ---------------------------------------------------------------------------


def test_xattr_binary_no_attrs(tmp_path):
    """gfal xattr on a local file with no xattrs should exit cleanly."""
    f = tmp_path / "test.txt"
    f.write_text("x")

    rc, out, err = run_bin("xattr", f.as_uri())

    # May succeed (empty output) or fail if xattr not supported; must not crash.
    # We allow 0, 1, or platform-specific EOPNOTSUPP (95 on Linux, 102 on macOS).
    import errno

    assert rc in (0, 1, getattr(errno, "EOPNOTSUPP", 95), getattr(errno, "ENOTSUP", 95))


# ---------------------------------------------------------------------------
# Error exit codes — binary must propagate non-zero exits
# ---------------------------------------------------------------------------


def test_nonexistent_file_nonzero_exit(tmp_path):
    rc, out, err = run_bin("stat", (tmp_path / "no_such").as_uri())
    assert rc != 0


def test_rm_directory_without_recursive_nonzero(tmp_path):
    d = tmp_path / "d"
    d.mkdir()
    rc, out, err = run_bin("rm", d.as_uri())
    assert rc != 0
