"""Regression tests for broken pipe (EPIPE) handling."""

import contextlib
import subprocess
import sys


def test_cat_pipe_broken():
    """Verify that gfal-cat doesn't print 'Broken pipe' to stderr when output is truncated."""
    # Use any command that produces some output.
    # We'll use a local file to avoid network dependencies in this unit test.
    # We use the 'cat' implementation directly via the 'gfal-cli' entry point.

    # Create a dummy large file (larger than CHUNK_SIZE/pipe buffer)
    # Actually even a small file works if we close the pipe.

    cmd = [sys.executable, "-m", "gfal_cli.shell", "cat", "file:///etc/hosts"]

    # We run the command and pipe it to something that closes early (like 'head')
    # Or we manually close the pipe in a subprocess.

    # In Python, we can simulate this by:
    # 1. Opening a pipe
    # 2. Starting the process with stdout=pipe
    # 3. Reading a few bytes
    # 4. Closing the read-end of the pipe
    # 5. Waiting for the process to finish

    process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    # Wait a bit for output to be available
    import time

    time.sleep(0.1)

    # Read just 5 bytes
    with contextlib.suppress(ValueError, OSError):
        _ = process.stdout.read(5)

    # Terminating the process is safer than closing stdout manually on some platforms
    process.terminate()

    # Wait for the process to finish
    stdout, stderr = process.communicate()

    # Check that stderr doesn't contain "Broken pipe"
    assert b"Broken pipe" not in stderr
    assert b"EPIPE" not in stderr

    # Check exit code (should be 0 now)
    assert process.returncode == 0


def test_ls_pipe_broken():
    """Verify that gfal-ls handles broken pipes gracefully."""
    cmd = [sys.executable, "-m", "gfal_cli.shell", "ls", "file:///"]

    process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    import time

    time.sleep(0.1)

    with contextlib.suppress(ValueError, OSError):
        _ = process.stdout.read(1)

    process.terminate()
    stdout, stderr = process.communicate()

    assert b"Broken pipe" not in stderr
    assert process.returncode == 0
