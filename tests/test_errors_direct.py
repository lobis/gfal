"""Direct unit tests for GfalError subclasses (src/gfal/core/errors.py).

These tests exercise each error class directly to improve coverage.
"""

import errno

from gfal.core.errors import (
    GfalError,
    GfalFileExistsError,
    GfalFileNotFoundError,
    GfalIsADirectoryError,
    GfalNotADirectoryError,
    GfalPermissionError,
    GfalTimeoutError,
)


class TestGfalError:
    def test_base_error_message(self):
        e = GfalError("something failed", code=errno.EIO)
        assert str(e) == "something failed"
        assert e.errno == errno.EIO

    def test_base_error_no_code(self):
        e = GfalError("oops")
        assert e.errno is None

    def test_is_oserror(self):
        e = GfalError("test", code=1)
        assert isinstance(e, OSError)


class TestGfalPermissionError:
    def test_message(self):
        e = GfalPermissionError("access denied")
        assert str(e) == "access denied"

    def test_errno(self):
        e = GfalPermissionError("x")
        assert e.errno == errno.EACCES

    def test_is_gfal_error(self):
        e = GfalPermissionError("x")
        assert isinstance(e, GfalError)


class TestGfalFileNotFoundError:
    def test_message(self):
        e = GfalFileNotFoundError("no such file")
        assert str(e) == "no such file"

    def test_errno(self):
        e = GfalFileNotFoundError("x")
        assert e.errno == errno.ENOENT

    def test_is_gfal_error(self):
        e = GfalFileNotFoundError("x")
        assert isinstance(e, GfalError)


class TestGfalFileExistsError:
    def test_message(self):
        e = GfalFileExistsError("file exists")
        assert str(e) == "file exists"

    def test_errno(self):
        e = GfalFileExistsError("x")
        assert e.errno == errno.EEXIST

    def test_is_gfal_error(self):
        e = GfalFileExistsError("x")
        assert isinstance(e, GfalError)


class TestGfalNotADirectoryError:
    def test_message(self):
        e = GfalNotADirectoryError("not a dir")
        assert str(e) == "not a dir"

    def test_errno(self):
        e = GfalNotADirectoryError("x")
        assert e.errno == errno.ENOTDIR

    def test_is_gfal_error(self):
        e = GfalNotADirectoryError("x")
        assert isinstance(e, GfalError)


class TestGfalIsADirectoryError:
    def test_message(self):
        e = GfalIsADirectoryError("is a dir")
        assert str(e) == "is a dir"

    def test_errno(self):
        e = GfalIsADirectoryError("x")
        assert e.errno == errno.EISDIR

    def test_is_gfal_error(self):
        e = GfalIsADirectoryError("x")
        assert isinstance(e, GfalError)


class TestGfalTimeoutError:
    def test_message(self):
        e = GfalTimeoutError("timed out")
        assert str(e) == "timed out"

    def test_errno(self):
        e = GfalTimeoutError("x")
        assert e.errno == errno.ETIMEDOUT

    def test_is_gfal_error(self):
        e = GfalTimeoutError("x")
        assert isinstance(e, GfalError)
