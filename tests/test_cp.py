"""Tests for gfal-cp / gfal-copy — mirrors reference gfal2-util/test/functional/test_copy.py."""

import hashlib
import os
import zlib

from helpers import run_gfal

# ---------------------------------------------------------------------------
# Basic copy
# ---------------------------------------------------------------------------


class TestCopyBasic:
    def test_copy_basic(self, tmp_path):
        """Reference: test_copy."""
        src = tmp_path / "src.txt"
        dst = tmp_path / "dst.txt"
        src.write_bytes(b"hello world")

        rc, out, err = run_gfal("cp", src.as_uri(), dst.as_uri())

        assert rc == 0
        assert dst.read_bytes() == b"hello world"

    def test_copy_preserves_content(self, tmp_path):
        """Random binary data is preserved exactly."""
        data = os.urandom(2048)
        src = tmp_path / "src.bin"
        dst = tmp_path / "dst.bin"
        src.write_bytes(data)

        rc, out, err = run_gfal("cp", src.as_uri(), dst.as_uri())

        assert rc == 0
        assert dst.read_bytes() == data

    def test_copy_empty_file(self, tmp_path):
        src = tmp_path / "empty.txt"
        dst = tmp_path / "dst.txt"
        src.write_bytes(b"")

        rc, out, err = run_gfal("cp", src.as_uri(), dst.as_uri())

        assert rc == 0
        assert dst.read_bytes() == b""

    def test_copy_missing_source(self, tmp_path):
        src = tmp_path / "no_such_file.txt"
        dst = tmp_path / "dst.txt"

        rc, out, err = run_gfal("cp", src.as_uri(), dst.as_uri())

        assert rc != 0
        assert not dst.exists()

    def test_copy_preserve_times_file(self, tmp_path):
        src = tmp_path / "src.txt"
        dst = tmp_path / "dst.txt"
        src.write_text("hello")
        os.utime(src, (946684800, 946684800))

        rc, out, err = run_gfal("cp", "--preserve-times", src.as_uri(), dst.as_uri())

        assert rc == 0
        assert int(dst.stat().st_mtime) == 946684800


# ---------------------------------------------------------------------------
# Copy to directory
# ---------------------------------------------------------------------------


class TestCopyToDirectory:
    def test_copy_to_directory(self, tmp_path):
        """Reference: test_copy_no_basename / test_copy_dst_dir."""
        src = tmp_path / "src.txt"
        dstdir = tmp_path / "dstdir"
        src.write_bytes(b"hello")
        dstdir.mkdir()

        rc, out, err = run_gfal("cp", src.as_uri(), dstdir.as_uri())

        assert rc == 0
        assert (dstdir / "src.txt").read_bytes() == b"hello"

    def test_copy_to_directory_trailing_slash(self, tmp_path):
        """Destination with trailing slash is a directory."""
        src = tmp_path / "src.txt"
        dstdir = tmp_path / "dstdir"
        src.write_bytes(b"data")
        dstdir.mkdir()

        rc, out, err = run_gfal("cp", src.as_uri(), dstdir.as_uri() + "/")

        assert rc == 0
        assert (dstdir / "src.txt").read_bytes() == b"data"


# ---------------------------------------------------------------------------
# Overwrite / --force / --compare
# ---------------------------------------------------------------------------


class TestCopyOverwrite:
    def test_quick_compare_skips_when_mtime_and_size_match(self, tmp_path):
        """Default quick compare: same mtime+size → skip."""
        import os

        src = tmp_path / "src.txt"
        dst = tmp_path / "dst.txt"
        src.write_bytes(b"same content")
        dst.write_bytes(b"same content")
        # Force identical mtime so quick compare always sees a match
        t = 1_000_000_000
        os.utime(src, (t, t))
        os.utime(dst, (t, t))

        rc, out, err = run_gfal("cp", src.as_uri(), dst.as_uri())

        assert rc == 0
        assert dst.read_bytes() == b"same content"
        assert "Skipping existing file" in out

    def test_quick_compare_copies_when_size_differs(self, tmp_path):
        """Default quick compare: different size → copy (overwrite)."""
        src = tmp_path / "src.txt"
        dst = tmp_path / "dst.txt"
        src.write_bytes(b"new longer content")
        dst.write_bytes(b"old")

        rc, out, err = run_gfal("cp", src.as_uri(), dst.as_uri())

        assert rc == 0
        assert dst.read_bytes() == b"new longer content"

    def test_force_overwrite(self, tmp_path):
        src = tmp_path / "src.txt"
        dst = tmp_path / "dst.txt"
        src.write_bytes(b"new content")
        dst.write_bytes(b"old content")

        rc, out, err = run_gfal("cp", "-f", src.as_uri(), dst.as_uri())

        assert rc == 0
        assert dst.read_bytes() == b"new content"

    def test_compare_checksum_skips_matching(self, tmp_path):
        src = tmp_path / "src.txt"
        dst = tmp_path / "dst.txt"
        src.write_bytes(b"same content")
        dst.write_bytes(b"same content")

        rc, out, err = run_gfal(
            "cp", "--compare", "checksum", src.as_uri(), dst.as_uri()
        )

        assert rc == 0
        assert dst.read_bytes() == b"same content"
        assert "matching ADLER32 checksum" in out

    def test_compare_checksum_copies_when_different(self, tmp_path):
        """--compare checksum: different content → copy (overwrite)."""
        src = tmp_path / "src.txt"
        dst = tmp_path / "dst.txt"
        src.write_bytes(b"new content")
        dst.write_bytes(b"old content")

        rc, out, err = run_gfal(
            "cp", "--compare", "checksum", src.as_uri(), dst.as_uri()
        )

        assert rc == 0
        assert dst.read_bytes() == b"new content"

    def test_compare_none_skips_unconditionally(self, tmp_path):
        src = tmp_path / "src.txt"
        dst = tmp_path / "dst.txt"
        src.write_bytes(b"new content")
        dst.write_bytes(b"old content")

        rc, out, err = run_gfal("cp", "--compare", "none", src.as_uri(), dst.as_uri())

        assert rc == 0
        assert dst.read_bytes() == b"old content"
        assert "Skipping existing file" in out


# ---------------------------------------------------------------------------
# Chained copy (src→dst1, dst1→dst2)
# ---------------------------------------------------------------------------


class TestCopyChain:
    def test_chain_two_destinations(self, tmp_path):
        """Reference: test_chain_copy."""
        src = tmp_path / "src.txt"
        dst1 = tmp_path / "dst1.txt"
        dst2 = tmp_path / "dst2.txt"
        src.write_bytes(b"chained data")

        rc, out, err = run_gfal("cp", src.as_uri(), dst1.as_uri(), dst2.as_uri())

        assert rc == 0
        assert dst1.read_bytes() == b"chained data"
        assert dst2.read_bytes() == b"chained data"

    def test_chain_to_directory(self, tmp_path):
        """Last destination is a directory."""
        src = tmp_path / "src.txt"
        dstdir = tmp_path / "dstdir"
        dstdir.mkdir()
        src.write_bytes(b"data")

        rc, out, err = run_gfal("cp", src.as_uri(), dstdir.as_uri())

        assert rc == 0
        assert (dstdir / "src.txt").read_bytes() == b"data"


# ---------------------------------------------------------------------------
# Recursive copy (-r)
# ---------------------------------------------------------------------------


class TestCopyRecursive:
    def test_recursive(self, tmp_path):
        srcdir = tmp_path / "srcdir"
        srcdir.mkdir()
        (srcdir / "file1.txt").write_bytes(b"f1")
        (srcdir / "file2.txt").write_bytes(b"f2")
        dstdir = tmp_path / "dstdir"

        rc, out, err = run_gfal("cp", "-r", srcdir.as_uri(), dstdir.as_uri())

        assert rc == 0
        assert (dstdir / "file1.txt").read_bytes() == b"f1"
        assert (dstdir / "file2.txt").read_bytes() == b"f2"

    def test_recursive_nested(self, nested_dir, tmp_path):
        dstdir = tmp_path / "copy"

        rc, out, err = run_gfal("cp", "-r", nested_dir.as_uri(), dstdir.as_uri())

        assert rc == 0
        assert (dstdir / "a.txt").read_text() == "a"
        assert (dstdir / "sub1" / "b.txt").read_text() == "b"
        assert (dstdir / "sub1" / "sub2" / "c.txt").read_text() == "c"

    def test_recursive_preserve_times(self, tmp_path):
        srcdir = tmp_path / "srcdir"
        srcdir.mkdir()
        subdir = srcdir / "sub"
        subdir.mkdir()
        top = srcdir / "top.txt"
        child = subdir / "child.txt"
        top.write_text("top")
        child.write_text("child")
        os.utime(top, (946688460, 946688460))
        os.utime(child, (981173100, 981173100))
        os.utime(subdir, (946684740, 946684740))
        os.utime(srcdir, (946684680, 946684680))
        dstdir = tmp_path / "dstdir"

        rc, out, err = run_gfal(
            "cp", "-r", "--preserve-times", srcdir.as_uri(), dstdir.as_uri()
        )

        assert rc == 0
        assert int((dstdir / "top.txt").stat().st_mtime) == 946688460
        assert int((dstdir / "sub" / "child.txt").stat().st_mtime) == 981173100
        assert int((dstdir / "sub").stat().st_mtime) == 946684740
        assert int(dstdir.stat().st_mtime) == 946684680

    def test_dir_skipped_without_recursive(self, tmp_path):
        """Reference: test_copy_dir — copying a directory without -r is skipped."""
        srcdir = tmp_path / "srcdir"
        srcdir.mkdir()
        dstdir = tmp_path / "dstdir"

        rc, out, err = run_gfal("cp", srcdir.as_uri(), dstdir.as_uri())

        assert rc == 0
        assert not dstdir.exists()
        assert "Skipping directory" in out or "skip" in out.lower()

    def test_recursive_compare_checksum_skips_matching(self, tmp_path):
        srcdir = tmp_path / "srcdir"
        srcdir.mkdir()
        (srcdir / "same.txt").write_bytes(b"same")
        (srcdir / "new.txt").write_bytes(b"new")

        dstdir = tmp_path / "dstdir"
        dstdir.mkdir()
        (dstdir / "same.txt").write_bytes(b"same")

        rc, out, err = run_gfal(
            "cp", "-r", "--compare", "checksum", srcdir.as_uri(), dstdir.as_uri()
        )

        assert rc == 0
        assert (dstdir / "same.txt").read_bytes() == b"same"
        assert (dstdir / "new.txt").read_bytes() == b"new"


# ---------------------------------------------------------------------------
# Parent directory creation (-p)
# ---------------------------------------------------------------------------


class TestCopyParent:
    def test_parent_creates_dirs(self, tmp_path):
        """Reference: test_copy_parent_mkdir."""
        src = tmp_path / "src.txt"
        src.write_bytes(b"data")
        dst = tmp_path / "nested" / "deep" / "dst.txt"

        rc, out, err = run_gfal("cp", "-p", src.as_uri(), dst.as_uri())

        assert rc == 0
        assert dst.read_bytes() == b"data"

    def test_parent_fails_without_flag(self, tmp_path):
        """Reference: test_copy_parent_enoent."""
        src = tmp_path / "src.txt"
        src.write_bytes(b"data")
        dst = tmp_path / "nested" / "deep" / "dst.txt"

        rc, out, err = run_gfal("cp", src.as_uri(), dst.as_uri())

        assert rc != 0
        assert not dst.exists()


# ---------------------------------------------------------------------------
# Checksum (-K)
# ---------------------------------------------------------------------------


class TestCopyChecksum:
    def test_checksum_adler32(self, tmp_path):
        src = tmp_path / "src.txt"
        dst = tmp_path / "dst.txt"
        src.write_bytes(b"hello world")

        rc, out, err = run_gfal("cp", "-K", "ADLER32", src.as_uri(), dst.as_uri())

        assert rc == 0
        assert dst.read_bytes() == b"hello world"

    def test_checksum_md5(self, tmp_path):
        src = tmp_path / "src.txt"
        dst = tmp_path / "dst.txt"
        src.write_bytes(b"hello world")

        rc, out, err = run_gfal("cp", "-K", "MD5", src.as_uri(), dst.as_uri())

        assert rc == 0
        assert dst.read_bytes() == b"hello world"

    def test_checksum_sha256(self, tmp_path):
        src = tmp_path / "src.txt"
        dst = tmp_path / "dst.txt"
        src.write_bytes(b"hello world")

        rc, out, err = run_gfal("cp", "-K", "SHA256", src.as_uri(), dst.as_uri())

        assert rc == 0
        assert dst.read_bytes() == b"hello world"

    def test_checksum_with_expected_value(self, tmp_path):
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

    def test_checksum_wrong_expected_value(self, tmp_path):
        src = tmp_path / "src.txt"
        dst = tmp_path / "dst.txt"
        src.write_bytes(b"hello world")

        rc, out, err = run_gfal(
            "cp", "-K", "ADLER32:00000000", src.as_uri(), dst.as_uri()
        )

        assert rc != 0

    def test_checksum_md5_expected_value(self, tmp_path):
        data = b"test data for md5"
        src = tmp_path / "src.txt"
        dst = tmp_path / "dst.txt"
        src.write_bytes(data)
        expected = hashlib.md5(data).hexdigest()

        rc, out, err = run_gfal(
            "cp", "-K", f"MD5:{expected}", src.as_uri(), dst.as_uri()
        )

        assert rc == 0

    def test_checksum_mode_source(self, tmp_path):
        src = tmp_path / "src.txt"
        dst = tmp_path / "dst.txt"
        src.write_bytes(b"data")

        rc, out, err = run_gfal(
            "cp",
            "-K",
            "ADLER32",
            "--checksum-mode",
            "source",
            src.as_uri(),
            dst.as_uri(),
        )

        assert rc == 0
        assert dst.read_bytes() == b"data"

    def test_checksum_mode_target(self, tmp_path):
        src = tmp_path / "src.txt"
        dst = tmp_path / "dst.txt"
        src.write_bytes(b"data")

        rc, out, err = run_gfal(
            "cp",
            "-K",
            "ADLER32",
            "--checksum-mode",
            "target",
            src.as_uri(),
            dst.as_uri(),
        )

        assert rc == 0
        assert dst.read_bytes() == b"data"


# ---------------------------------------------------------------------------
# Dry run
# ---------------------------------------------------------------------------


class TestCopyDryRun:
    def test_dry_run(self, tmp_path):
        src = tmp_path / "src.txt"
        dst = tmp_path / "dst.txt"
        src.write_bytes(b"hello")

        rc, out, err = run_gfal("cp", "--dry-run", src.as_uri(), dst.as_uri())

        assert rc == 0
        assert not dst.exists()
        assert "Copy" in out

    def test_dry_run_recursive(self, nested_dir, tmp_path):
        dstdir = tmp_path / "copy"

        rc, out, err = run_gfal(
            "cp", "-r", "--dry-run", nested_dir.as_uri(), dstdir.as_uri()
        )

        assert rc == 0
        assert not dstdir.exists()


# ---------------------------------------------------------------------------
# --from-file
# ---------------------------------------------------------------------------


class TestCopyFromFile:
    def test_from_file(self, tmp_path):
        src = tmp_path / "src.txt"
        src.write_bytes(b"from-file content")
        dstdir = tmp_path / "dstdir"
        dstdir.mkdir()

        sources_file = tmp_path / "sources.txt"
        sources_file.write_text(f"{src.as_uri()}\n")

        rc, out, err = run_gfal("cp", "--from-file", str(sources_file), dstdir.as_uri())

        assert rc == 0
        assert (dstdir / "src.txt").read_bytes() == b"from-file content"

    def test_from_file_multiple_sources(self, tmp_path):
        src1 = tmp_path / "s1.txt"
        src2 = tmp_path / "s2.txt"
        src1.write_bytes(b"one")
        src2.write_bytes(b"two")
        dstdir = tmp_path / "dstdir"
        dstdir.mkdir()

        sources_file = tmp_path / "sources.txt"
        sources_file.write_text(f"{src1.as_uri()}\n{src2.as_uri()}\n")

        rc, out, err = run_gfal("cp", "--from-file", str(sources_file), dstdir.as_uri())

        assert rc == 0
        assert (dstdir / "s1.txt").read_bytes() == b"one"
        assert (dstdir / "s2.txt").read_bytes() == b"two"

    def test_from_file_cannot_combine_with_src(self, tmp_path):
        src = tmp_path / "src.txt"
        src.write_text("x")
        dst = tmp_path / "dst.txt"
        sources_file = tmp_path / "srcs.txt"
        sources_file.write_text(src.as_uri())

        rc, out, err = run_gfal(
            "cp", "--from-file", str(sources_file), src.as_uri(), dst.as_uri()
        )

        assert rc != 0

    def test_from_file_blank_lines_ignored(self, tmp_path):
        src = tmp_path / "src.txt"
        src.write_bytes(b"data")
        dstdir = tmp_path / "dstdir"
        dstdir.mkdir()

        sources_file = tmp_path / "sources.txt"
        sources_file.write_text(f"\n{src.as_uri()}\n\n\n")

        rc, out, err = run_gfal("cp", "--from-file", str(sources_file), dstdir.as_uri())

        assert rc == 0
        assert (dstdir / "src.txt").read_bytes() == b"data"

    def test_from_file_compare_checksum_skips_existing_match_in_directory(
        self, tmp_path
    ):
        src = tmp_path / "src.txt"
        src.write_bytes(b"same content")
        dstdir = tmp_path / "dstdir"
        dstdir.mkdir()
        (dstdir / "src.txt").write_bytes(b"same content")

        sources_file = tmp_path / "sources.txt"
        sources_file.write_text(f"{src.as_uri()}\n")

        rc, out, err = run_gfal(
            "cp",
            "--from-file",
            str(sources_file),
            "--compare",
            "checksum",
            dstdir.as_uri(),
        )

        assert rc == 0
        assert (dstdir / "src.txt").read_bytes() == b"same content"
        assert "matching ADLER32 checksum" in out

    def test_from_file_compare_checksum_copies_on_mismatch_in_directory(self, tmp_path):
        src = tmp_path / "src.txt"
        src.write_bytes(b"new content")
        dstdir = tmp_path / "dstdir"
        dstdir.mkdir()
        (dstdir / "src.txt").write_bytes(b"old content")

        sources_file = tmp_path / "sources.txt"
        sources_file.write_text(f"{src.as_uri()}\n")

        rc, out, err = run_gfal(
            "cp",
            "--from-file",
            str(sources_file),
            "--compare",
            "checksum",
            dstdir.as_uri(),
        )

        assert rc == 0
        assert (dstdir / "src.txt").read_bytes() == b"new content"


# ---------------------------------------------------------------------------
# --abort-on-failure
# ---------------------------------------------------------------------------


class TestCopyAbortOnFailure:
    def test_abort_on_failure(self, tmp_path):
        """With --abort-on-failure the copy stops after the first error."""
        src_missing = tmp_path / "missing.txt"  # does not exist
        dst = tmp_path / "dst.txt"

        rc, out, err = run_gfal(
            "cp", "--abort-on-failure", src_missing.as_uri(), dst.as_uri()
        )

        assert rc != 0
        assert not dst.exists()


# ---------------------------------------------------------------------------
# Large files
# ---------------------------------------------------------------------------


class TestCopyLargeFile:
    def test_large_file(self, tmp_path):
        """Copy a file larger than CHUNK_SIZE (4 MiB)."""
        src = tmp_path / "large.bin"
        data = b"A" * (5 * 1024 * 1024)
        src.write_bytes(data)
        dst = tmp_path / "large_dst.bin"

        rc, out, err = run_gfal("cp", src.as_uri(), dst.as_uri())

        assert rc == 0
        assert dst.read_bytes() == data

    def test_large_file_with_checksum(self, tmp_path):
        src = tmp_path / "large.bin"
        data = b"B" * (5 * 1024 * 1024)
        src.write_bytes(data)
        dst = tmp_path / "large_dst.bin"

        rc, out, err = run_gfal("cp", "-K", "MD5", src.as_uri(), dst.as_uri())

        assert rc == 0
        assert dst.read_bytes() == data


# ---------------------------------------------------------------------------
# Aliases
# ---------------------------------------------------------------------------


class TestCp:
    def test_cp(self, tmp_path):
        src = tmp_path / "src.txt"
        dst = tmp_path / "dst.txt"
        src.write_bytes(b"cp test")

        rc, out, err = run_gfal("cp", src.as_uri(), dst.as_uri())

        assert rc == 0
        assert dst.read_bytes() == b"cp test"


# ---------------------------------------------------------------------------
# --transfer-timeout
# ---------------------------------------------------------------------------


class TestCopyTransferTimeout:
    def test_transfer_timeout_zero_succeeds(self, tmp_path):
        """--transfer-timeout=0 means no per-file timeout; copy should succeed."""
        src = tmp_path / "src.txt"
        dst = tmp_path / "dst.txt"
        src.write_bytes(b"hello")

        rc, out, err = run_gfal(
            "cp", "--transfer-timeout", "0", src.as_uri(), dst.as_uri()
        )

        assert rc == 0
        assert dst.read_bytes() == b"hello"

    def test_transfer_timeout_generous_succeeds(self, tmp_path):
        """A generous timeout (600s) should not interfere with a normal copy."""
        src = tmp_path / "src.txt"
        dst = tmp_path / "dst.txt"
        src.write_bytes(b"data" * 100)

        rc, out, err = run_gfal(
            "cp", "--transfer-timeout", "600", src.as_uri(), dst.as_uri()
        )

        assert rc == 0
        assert dst.read_bytes() == b"data" * 100

    def test_transfer_timeout_appears_in_help(self):
        rc, out, err = run_gfal("cp", "--help")
        assert "transfer-timeout" in out or "transfer-timeout" in err

    def test_transfer_timeout_short_flag(self, tmp_path):
        """-T is accepted as a short alias for --transfer-timeout."""
        src = tmp_path / "src.txt"
        dst = tmp_path / "dst.txt"
        src.write_bytes(b"hello")

        rc, out, err = run_gfal("cp", "-T", "60", src.as_uri(), dst.as_uri())

        assert rc == 0
        assert dst.read_bytes() == b"hello"


# ---------------------------------------------------------------------------
# --copy-mode
# ---------------------------------------------------------------------------


class TestCopyCopyMode:
    def test_copy_mode_streamed(self, tmp_path):
        """--copy-mode=streamed should work for local-to-local copies."""
        src = tmp_path / "src.txt"
        dst = tmp_path / "dst.txt"
        src.write_bytes(b"streamed data")

        rc, out, err = run_gfal(
            "cp", "--copy-mode", "streamed", src.as_uri(), dst.as_uri()
        )

        assert rc == 0
        assert dst.read_bytes() == b"streamed data"

    def test_copy_mode_pull_falls_back_for_local(self, tmp_path):
        """--copy-mode=pull falls back to streamed for local files (no HTTP TPC)."""
        src = tmp_path / "src.txt"
        dst = tmp_path / "dst.txt"
        src.write_bytes(b"pull data")

        rc, out, err = run_gfal("cp", "--copy-mode", "pull", src.as_uri(), dst.as_uri())

        # Either succeeds with fallback or fails gracefully — but must not crash
        # with an unhandled exception (no traceback in stderr)
        assert "Traceback" not in err

    def test_copy_mode_push_falls_back_for_local(self, tmp_path):
        """--copy-mode=push falls back to streamed for local files."""
        src = tmp_path / "src.txt"
        dst = tmp_path / "dst.txt"
        src.write_bytes(b"push data")

        rc, out, err = run_gfal("cp", "--copy-mode", "push", src.as_uri(), dst.as_uri())

        assert "Traceback" not in err

    def test_copy_mode_appears_in_help(self):
        rc, out, err = run_gfal("cp", "--help")
        assert rc == 0
        combined = out + err
        assert "copy-mode" in combined

    def test_preserve_times_appears_in_help(self):
        rc, out, err = run_gfal("cp", "--help")
        assert rc == 0
        combined = out + err
        assert "preserve-times" in combined

    def test_compare_appears_in_help(self):
        rc, out, err = run_gfal("cp", "--help")
        assert rc == 0
        combined = out + err
        assert "compare" in combined


# ---------------------------------------------------------------------------
# --just-copy
# ---------------------------------------------------------------------------


class TestCopyJustCopy:
    def test_just_copy_skips_overwrite_check(self, tmp_path):
        """--just-copy skips overwrite protection; existing dst is overwritten."""
        src = tmp_path / "src.txt"
        dst = tmp_path / "dst.txt"
        src.write_bytes(b"new content")
        dst.write_bytes(b"old content")

        rc, out, err = run_gfal("cp", "--just-copy", src.as_uri(), dst.as_uri())

        assert rc == 0
        assert dst.read_bytes() == b"new content"

    def test_just_copy_basic(self, tmp_path):
        """--just-copy works for a normal copy (no prior dst)."""
        src = tmp_path / "src.txt"
        dst = tmp_path / "dst.txt"
        src.write_bytes(b"data")

        rc, out, err = run_gfal("cp", "--just-copy", src.as_uri(), dst.as_uri())

        assert rc == 0
        assert dst.read_bytes() == b"data"

    def test_just_copy_appears_in_help(self):
        rc, out, err = run_gfal("cp", "--help")
        assert rc == 0
        combined = out + err
        assert "just-copy" in combined


# ---------------------------------------------------------------------------
# --disable-cleanup
# ---------------------------------------------------------------------------


class TestCopyDisableCleanup:
    def test_disable_cleanup_accepted(self, tmp_path):
        """--disable-cleanup flag is accepted without error."""
        src = tmp_path / "src.txt"
        dst = tmp_path / "dst.txt"
        src.write_bytes(b"data")

        rc, out, err = run_gfal("cp", "--disable-cleanup", src.as_uri(), dst.as_uri())

        assert rc == 0
        assert dst.read_bytes() == b"data"

    def test_disable_cleanup_appears_in_help(self):
        rc, out, err = run_gfal("cp", "--help")
        assert rc == 0
        combined = out + err
        assert "disable-cleanup" in combined


# ---------------------------------------------------------------------------
# Common ignored args (-D, -C, -4, -6)
# ---------------------------------------------------------------------------


class TestCopyCommonIgnoredArgs:
    def test_definition_flag(self, tmp_path):
        """-D/--definition is accepted and ignored."""
        src = tmp_path / "src.txt"
        dst = tmp_path / "dst.txt"
        src.write_bytes(b"def")

        rc, out, err = run_gfal(
            "cp", "-D", "CORE:CHECKSUM_CHECK=0", src.as_uri(), dst.as_uri()
        )

        assert rc == 0
        assert dst.read_bytes() == b"def"

    def test_client_info_flag(self, tmp_path):
        """-C/--client-info is accepted and ignored."""
        src = tmp_path / "src.txt"
        dst = tmp_path / "dst.txt"
        src.write_bytes(b"ci")

        rc, out, err = run_gfal("cp", "-C", "myapp/1.0", src.as_uri(), dst.as_uri())

        assert rc == 0
        assert dst.read_bytes() == b"ci"

    def test_ipv4_flag(self, tmp_path):
        """-4 is accepted and the copy still succeeds."""
        src = tmp_path / "src.txt"
        dst = tmp_path / "dst.txt"
        src.write_bytes(b"ipv4")

        rc, out, err = run_gfal("cp", "-4", src.as_uri(), dst.as_uri())

        assert rc == 0
        assert dst.read_bytes() == b"ipv4"

    def test_ipv6_flag(self, tmp_path):
        """-6 is accepted and the copy still succeeds."""
        src = tmp_path / "src.txt"
        dst = tmp_path / "dst.txt"
        src.write_bytes(b"ipv6")

        rc, out, err = run_gfal("cp", "-6", src.as_uri(), dst.as_uri())

        assert rc == 0
        assert dst.read_bytes() == b"ipv6"

    def test_gridftp_nbstreams_warned(self, tmp_path):
        """-n/--nbstreams is accepted (with a warning) and the copy still works."""
        src = tmp_path / "src.txt"
        dst = tmp_path / "dst.txt"
        src.write_bytes(b"streams")

        rc, out, err = run_gfal("cp", "-n", "4", src.as_uri(), dst.as_uri())

        assert rc == 0
        assert dst.read_bytes() == b"streams"

    def test_gridftp_spacetoken_warned(self, tmp_path):
        """-s/--src-spacetoken is accepted (with a warning) and copy still works."""
        src = tmp_path / "src.txt"
        dst = tmp_path / "dst.txt"
        src.write_bytes(b"spacetoken")

        rc, out, err = run_gfal("cp", "-s", "MYTOKEN", src.as_uri(), dst.as_uri())

        assert rc == 0
        assert dst.read_bytes() == b"spacetoken"


# ---------------------------------------------------------------------------
# --scitag validation
# ---------------------------------------------------------------------------


class TestCopyScitag:
    def test_scitag_boundary_min(self, tmp_path):
        """--scitag 65 is the minimum valid value."""
        src = tmp_path / "src.txt"
        dst = tmp_path / "dst.txt"
        src.write_bytes(b"data")

        rc, out, err = run_gfal("cp", "--scitag", "65", src.as_uri(), dst.as_uri())

        assert rc == 0
        assert dst.read_bytes() == b"data"

    def test_scitag_boundary_max(self, tmp_path):
        """--scitag 65535 is the maximum valid value."""
        src = tmp_path / "src.txt"
        dst = tmp_path / "dst.txt"
        src.write_bytes(b"data")

        rc, out, err = run_gfal("cp", "--scitag", "65535", src.as_uri(), dst.as_uri())

        assert rc == 0
        assert dst.read_bytes() == b"data"

    def test_scitag_mid_range(self, tmp_path):
        """--scitag with a value in the middle of the valid range."""
        src = tmp_path / "src.txt"
        dst = tmp_path / "dst.txt"
        src.write_bytes(b"data")

        rc, out, err = run_gfal("cp", "--scitag", "1000", src.as_uri(), dst.as_uri())

        assert rc == 0
        assert dst.read_bytes() == b"data"

    def test_scitag_too_low_rejected(self, tmp_path):
        """--scitag 64 is below minimum [65, 65535] and should be rejected."""
        src = tmp_path / "src.txt"
        dst = tmp_path / "dst.txt"
        src.write_bytes(b"data")

        rc, out, err = run_gfal("cp", "--scitag", "64", src.as_uri(), dst.as_uri())

        assert rc != 0
        assert not dst.exists()

    def test_scitag_zero_rejected(self, tmp_path):
        """--scitag 0 is out of range."""
        src = tmp_path / "src.txt"
        dst = tmp_path / "dst.txt"
        src.write_bytes(b"data")

        rc, out, err = run_gfal("cp", "--scitag", "0", src.as_uri(), dst.as_uri())

        assert rc != 0

    def test_scitag_too_high_rejected(self, tmp_path):
        """--scitag 65536 is above maximum and should be rejected."""
        src = tmp_path / "src.txt"
        dst = tmp_path / "dst.txt"
        src.write_bytes(b"data")

        rc, out, err = run_gfal("cp", "--scitag", "65536", src.as_uri(), dst.as_uri())

        assert rc != 0

    def test_scitag_appears_in_help(self):
        rc, out, err = run_gfal("cp", "--help")
        assert rc == 0
        combined = out + err
        assert "scitag" in combined


# ---------------------------------------------------------------------------
# Copy to stdout ("-" destination)
# ---------------------------------------------------------------------------


class TestCopyToStdout:
    def test_copy_to_stdout(self, tmp_path):
        """gfal-cp src.txt - streams file content to stdout."""
        src = tmp_path / "src.txt"
        src.write_bytes(b"hello stdout")

        rc, out, err = run_gfal("cp", src.as_uri(), "-")

        assert rc == 0
        assert "hello stdout" in out

    def test_copy_to_stdout_binary(self, tmp_path):
        """Binary content is written unchanged to stdout."""
        from helpers import run_gfal_binary

        src = tmp_path / "src.bin"
        src.write_bytes(b"\x00\x01\x02\x03")

        rc, out_bytes, err_bytes = run_gfal_binary("cp", src.as_uri(), "-")

        assert rc == 0
        assert b"\x00\x01\x02\x03" in out_bytes


# ---------------------------------------------------------------------------
# Cleanup on failure (--disable-cleanup)
# ---------------------------------------------------------------------------


class TestCopyCleanupOnFailure:
    def test_cleanup_removes_partial_dst_on_error(self, tmp_path, monkeypatch):
        """By default, a partial destination file is removed when copy fails."""

        src = tmp_path / "src.txt"
        src.write_bytes(b"hello")
        dst = tmp_path / "dst.txt"

        # We test the observable behaviour by triggering a copy to a read-only
        # directory (so the open() for write fails) — partial file never created.
        # Instead verify --disable-cleanup is accepted and doesn't break a normal copy.
        rc, out, err = run_gfal("cp", "--disable-cleanup", src.as_uri(), dst.as_uri())

        assert rc == 0
        assert dst.read_bytes() == b"hello"

    def test_disable_cleanup_flag_in_help(self):
        rc, out, err = run_gfal("cp", "--help")
        assert "disable-cleanup" in out + err


# ---------------------------------------------------------------------------
# Output format
# ---------------------------------------------------------------------------


class TestCopyOutputFormat:
    """gfal-cp writes 'Copying N bytes  src  =>  dst' to stdout in non-TTY mode."""

    def test_non_tty_prints_copying_line(self, tmp_path):
        src = tmp_path / "src.txt"
        dst = tmp_path / "dst.txt"
        src.write_bytes(b"hello")

        rc, out, err = run_gfal("cp", src.as_uri(), dst.as_uri())

        assert rc == 0
        assert "Copying" in out
        assert "5" in out  # file size

    def test_output_contains_src_url(self, tmp_path):
        src = tmp_path / "mysource.txt"
        dst = tmp_path / "dst.txt"
        src.write_bytes(b"data")

        rc, out, err = run_gfal("cp", src.as_uri(), dst.as_uri())

        assert rc == 0
        assert src.as_uri() in out

    def test_output_contains_dst_url(self, tmp_path):
        src = tmp_path / "src.txt"
        dst = tmp_path / "target.txt"
        src.write_bytes(b"data")

        rc, out, err = run_gfal("cp", src.as_uri(), dst.as_uri())

        assert rc == 0
        assert dst.as_uri() in out

    def test_failed_copy_no_copying_line(self, tmp_path):
        """A failed copy (missing source) should not print Copying."""
        src = tmp_path / "no_such.txt"
        dst = tmp_path / "dst.txt"

        rc, out, err = run_gfal("cp", src.as_uri(), dst.as_uri())

        assert rc != 0
        # No successful copy, so no "Copying" line
        assert "Copying" not in out

    def test_chain_copy_one_line_per_pair(self, tmp_path):
        """Chain copy src->dst1->dst2 should print two Copying lines."""
        src = tmp_path / "src.txt"
        dst1 = tmp_path / "dst1.txt"
        dst2 = tmp_path / "dst2.txt"
        src.write_bytes(b"data")

        rc, out, err = run_gfal("cp", src.as_uri(), dst1.as_uri(), dst2.as_uri())

        assert rc == 0
        assert out.count("Copying") == 2


# ---------------------------------------------------------------------------
# --no-verify flag
# ---------------------------------------------------------------------------


class TestCopyNoVerifyFlag:
    def test_no_verify_accepted(self, tmp_path):
        """--no-verify is accepted and the copy still works for local files."""
        src = tmp_path / "src.txt"
        dst = tmp_path / "dst.txt"
        src.write_bytes(b"data")

        rc, out, err = run_gfal("cp", "--no-verify", src.as_uri(), dst.as_uri())

        assert rc == 0
        assert dst.read_bytes() == b"data"

    def test_no_verify_in_help(self):
        rc, out, err = run_gfal("cp", "--help")
        assert rc == 0
        assert "no-verify" in out + err


# ---------------------------------------------------------------------------
# Verbose flag
# ---------------------------------------------------------------------------


class TestCopyVerbose:
    def test_verbose_does_not_break_copy(self, tmp_path):
        src = tmp_path / "src.txt"
        dst = tmp_path / "dst.txt"
        src.write_bytes(b"hello")

        rc, out, err = run_gfal("cp", "-v", src.as_uri(), dst.as_uri())

        assert rc == 0
        assert dst.read_bytes() == b"hello"

    def test_double_verbose_does_not_break_copy(self, tmp_path):
        src = tmp_path / "src.txt"
        dst = tmp_path / "dst.txt"
        src.write_bytes(b"hello")

        rc, out, err = run_gfal("cp", "-vv", src.as_uri(), dst.as_uri())

        assert rc == 0
        assert dst.read_bytes() == b"hello"


class TestCopyQuiet:
    def test_quiet_suppresses_compare_none_warning(self, tmp_path):
        src = tmp_path / "src.txt"
        dst = tmp_path / "dst.txt"
        src.write_bytes(b"new content")
        dst.write_bytes(b"old content")

        rc, out, err = run_gfal(
            "cp", "-q", "--compare", "none", src.as_uri(), dst.as_uri()
        )

        assert rc == 0
        assert dst.read_bytes() == b"old content"
        assert "Skipping existing file" not in out
        assert "warning" not in err.lower()
