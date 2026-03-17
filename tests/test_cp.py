"""Tests for gfal-cp / gfal-copy."""

from helpers import run_gfal


def test_copy_basic(tmp_path):
    src = tmp_path / "src.txt"
    dst = tmp_path / "dst.txt"
    src.write_bytes(b"hello world")

    rc, out, err = run_gfal("cp", src.as_uri(), dst.as_uri())

    assert rc == 0
    assert dst.read_bytes() == b"hello world"


def test_copy_to_directory(tmp_path):
    """Copying to a directory should place the file there under its basename."""
    src = tmp_path / "src.txt"
    dstdir = tmp_path / "dstdir"
    src.write_bytes(b"hello")
    dstdir.mkdir()

    rc, out, err = run_gfal("cp", src.as_uri(), dstdir.as_uri())

    assert rc == 0
    assert (dstdir / "src.txt").read_bytes() == b"hello"


def test_copy_no_overwrite_by_default(tmp_path):
    src = tmp_path / "src.txt"
    dst = tmp_path / "dst.txt"
    src.write_bytes(b"new content")
    dst.write_bytes(b"old content")

    rc, out, err = run_gfal("cp", src.as_uri(), dst.as_uri())

    assert rc != 0
    assert dst.read_bytes() == b"old content"


def test_copy_force_overwrite(tmp_path):
    src = tmp_path / "src.txt"
    dst = tmp_path / "dst.txt"
    src.write_bytes(b"new content")
    dst.write_bytes(b"old content")

    rc, out, err = run_gfal("cp", "-f", src.as_uri(), dst.as_uri())

    assert rc == 0
    assert dst.read_bytes() == b"new content"


def test_copy_chain(tmp_path):
    """Multiple destinations are chained: src→dst1, dst1→dst2."""
    src = tmp_path / "src.txt"
    dst1 = tmp_path / "dst1.txt"
    dst2 = tmp_path / "dst2.txt"
    src.write_bytes(b"chained data")

    rc, out, err = run_gfal("cp", src.as_uri(), dst1.as_uri(), dst2.as_uri())

    assert rc == 0
    assert dst1.read_bytes() == b"chained data"
    assert dst2.read_bytes() == b"chained data"


def test_copy_chain_to_directory(tmp_path):
    """Last destination is a directory; basename from intermediate copy is used."""
    src = tmp_path / "src.txt"
    dstdir = tmp_path / "dstdir"
    dstdir.mkdir()
    src.write_bytes(b"data")

    rc, out, err = run_gfal("cp", src.as_uri(), dstdir.as_uri())

    assert rc == 0
    assert (dstdir / "src.txt").read_bytes() == b"data"


def test_copy_recursive(tmp_path):
    srcdir = tmp_path / "srcdir"
    srcdir.mkdir()
    (srcdir / "file1.txt").write_bytes(b"f1")
    (srcdir / "file2.txt").write_bytes(b"f2")
    dstdir = tmp_path / "dstdir"

    rc, out, err = run_gfal("cp", "-r", srcdir.as_uri(), dstdir.as_uri())

    assert rc == 0
    assert (dstdir / "file1.txt").read_bytes() == b"f1"
    assert (dstdir / "file2.txt").read_bytes() == b"f2"


def test_copy_dir_skipped_without_recursive(tmp_path):
    """Copying a directory without -r prints a skip message but exits 0."""
    srcdir = tmp_path / "srcdir"
    srcdir.mkdir()
    dstdir = tmp_path / "dstdir"

    rc, out, err = run_gfal("cp", srcdir.as_uri(), dstdir.as_uri())

    assert rc == 0
    assert not dstdir.exists()
    assert "Skipping directory" in out or "skip" in out.lower()


def test_copy_parent_creates_dirs(tmp_path):
    src = tmp_path / "src.txt"
    src.write_bytes(b"data")
    dst = tmp_path / "nested" / "deep" / "dst.txt"

    rc, out, err = run_gfal("cp", "-p", src.as_uri(), dst.as_uri())

    assert rc == 0
    assert dst.read_bytes() == b"data"


def test_copy_parent_fails_without_flag(tmp_path):
    src = tmp_path / "src.txt"
    src.write_bytes(b"data")
    dst = tmp_path / "nested" / "deep" / "dst.txt"

    rc, out, err = run_gfal("cp", src.as_uri(), dst.as_uri())

    assert rc != 0
    assert not dst.exists()


def test_copy_checksum_adler32(tmp_path):
    src = tmp_path / "src.txt"
    dst = tmp_path / "dst.txt"
    src.write_bytes(b"hello world")

    rc, out, err = run_gfal("cp", "-K", "ADLER32", src.as_uri(), dst.as_uri())

    assert rc == 0
    assert dst.read_bytes() == b"hello world"


def test_copy_checksum_md5(tmp_path):
    src = tmp_path / "src.txt"
    dst = tmp_path / "dst.txt"
    src.write_bytes(b"hello world")

    rc, out, err = run_gfal("cp", "-K", "MD5", src.as_uri(), dst.as_uri())

    assert rc == 0
    assert dst.read_bytes() == b"hello world"


def test_copy_checksum_with_expected_value(tmp_path):
    import zlib

    data = b"hello world"
    src = tmp_path / "src.txt"
    dst = tmp_path / "dst.txt"
    src.write_bytes(data)
    expected = f"{zlib.adler32(data) & 0xFFFFFFFF:08x}"

    rc, out, err = run_gfal(
        "cp", "-K", f"ADLER32:{expected}", src.as_uri(), dst.as_uri()
    )

    assert rc == 0
    assert dst.read_bytes() == data


def test_copy_checksum_wrong_expected_value(tmp_path):
    src = tmp_path / "src.txt"
    dst = tmp_path / "dst.txt"
    src.write_bytes(b"hello world")

    rc, out, err = run_gfal("cp", "-K", "ADLER32:00000000", src.as_uri(), dst.as_uri())

    assert rc != 0


def test_copy_dry_run(tmp_path):
    src = tmp_path / "src.txt"
    dst = tmp_path / "dst.txt"
    src.write_bytes(b"hello")

    rc, out, err = run_gfal("cp", "--dry-run", src.as_uri(), dst.as_uri())

    assert rc == 0
    assert not dst.exists()
    assert "Copy" in out


def test_copy_from_file(tmp_path):
    src = tmp_path / "src.txt"
    src.write_bytes(b"from-file content")
    dstdir = tmp_path / "dstdir"
    dstdir.mkdir()

    sources_file = tmp_path / "sources.txt"
    sources_file.write_text(f"{src.as_uri()}\n")

    rc, out, err = run_gfal("cp", "--from-file", str(sources_file), dstdir.as_uri())

    assert rc == 0
    assert (dstdir / "src.txt").read_bytes() == b"from-file content"


def test_copy_from_file_cannot_combine_with_src(tmp_path):
    src = tmp_path / "src.txt"
    src.write_text("x")
    dst = tmp_path / "dst.txt"
    sources_file = tmp_path / "srcs.txt"
    sources_file.write_text(src.as_uri())

    rc, out, err = run_gfal(
        "cp", "--from-file", str(sources_file), src.as_uri(), dst.as_uri()
    )

    assert rc != 0


def test_copy_abort_on_failure(tmp_path):
    """With --abort-on-failure the second copy is not attempted after the first fails."""
    src = tmp_path / "src.txt"
    src.write_bytes(b"data")
    # dst1 already exists (no --force → failure)
    dst1 = tmp_path / "dst1.txt"
    dst1.write_bytes(b"old")

    rc, out, err = run_gfal("cp", "--abort-on-failure", src.as_uri(), dst1.as_uri())

    assert rc != 0
    assert dst1.read_bytes() == b"old"


def test_copy_missing_source(tmp_path):
    src = tmp_path / "no_such_file.txt"
    dst = tmp_path / "dst.txt"

    rc, out, err = run_gfal("cp", src.as_uri(), dst.as_uri())

    assert rc != 0


def test_copy_large_file(tmp_path):
    """Copy a file larger than CHUNK_SIZE (4 MiB)."""
    src = tmp_path / "large.bin"
    data = b"A" * (5 * 1024 * 1024)  # 5 MiB
    src.write_bytes(data)
    dst = tmp_path / "large_dst.bin"

    rc, out, err = run_gfal("cp", src.as_uri(), dst.as_uri())

    assert rc == 0
    assert dst.read_bytes() == data


def test_cp_alias(tmp_path):
    """``gfal-cp`` is an alias for ``gfal-copy``."""
    src = tmp_path / "src.txt"
    dst = tmp_path / "dst.txt"
    src.write_bytes(b"alias test")

    rc, out, err = run_gfal("cp", src.as_uri(), dst.as_uri())

    assert rc == 0
    assert dst.read_bytes() == b"alias test"
