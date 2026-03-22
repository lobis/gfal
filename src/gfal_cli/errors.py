import errno
from typing import Optional


class GfalError(OSError):
    """Base class for all library-specific exceptions."""

    def __init__(self, message: str, code: Optional[int] = None):
        super().__init__(message)
        self.errno = code


class GfalPermissionError(GfalError):
    """Raised when access is denied (EACCES)."""

    def __init__(self, message: str):
        super().__init__(message, errno.EACCES)


class GfalFileNotFoundError(GfalError):
    """Raised when a file or directory does not exist (ENOENT)."""

    def __init__(self, message: str):
        super().__init__(message, errno.ENOENT)


class GfalFileExistsError(GfalError):
    """Raised when a file or directory already exists (EEXIST)."""

    def __init__(self, message: str):
        super().__init__(message, errno.EEXIST)


class GfalNotADirectoryError(GfalError):
    """Raised when a directory operation is attempted on a non-directory (ENOTDIR)."""

    def __init__(self, message: str):
        super().__init__(message, errno.ENOTDIR)


class GfalIsADirectoryError(GfalError):
    """Raised when a file operation is attempted on a directory (EISDIR)."""

    def __init__(self, message: str):
        super().__init__(message, errno.EISDIR)


class GfalTimeoutError(GfalError):
    """Raised when an operation times out (ETIMEDOUT)."""

    def __init__(self, message: str):
        super().__init__(message, errno.ETIMEDOUT)
