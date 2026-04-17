import errno
from typing import Optional


def is_xrootd_not_found_message(message: str) -> bool:
    """Return True for XRootD "not found" style failures.

    "Server responded with an error" is an XRootD-specific error phrase; HTTP
    404s are handled upstream via the ``status`` attribute and never reach this
    helper, so the broad marker is safe to include.
    """
    lower = message.lower()
    not_found_markers = (
        "no such file or directory",
        "unable to stat",
        "[3011]",
    )
    return (
        "xroot" in lower
        or "root://" in lower
        or "server responded with an error" in lower
    ) and any(marker in lower for marker in not_found_markers)


def is_xrootd_permission_message(message: str) -> bool:
    """Return True for XRootD authorization/access-denied style failures."""
    lower = message.lower()
    # Explicit not-found text: never a permission error regardless of other markers.
    if "no such file or directory" in lower:
        return False
    xrootd_markers = (
        "[3010]",
        "unable to give access",
        "user access restricted",
        "unauthorized identity used",
        "permission denied",
        "operation not permitted",
    )
    return (
        "xroot" in lower
        or "root://" in lower
        or "server responded with an error" in lower
    ) and any(marker in lower for marker in xrootd_markers)


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


class GfalPartialFailureError(GfalError):
    """Raised when a multi-item operation reports child failures individually."""

    def __init__(self, message: str, failures: list[Exception]):
        first = failures[0] if failures else None
        code = getattr(first, "errno", None) or errno.EIO
        super().__init__(message, code)
        self.failures = failures
        self.first_error = first
        self.already_reported = True
