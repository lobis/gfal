"""
gfal: GFAL2-compatible CLI tools based on fsspec.
"""

from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as _pkg_version

from .core.api import (
    AsyncGfalClient,
    ChecksumPolicy,
    ClientConfig,
    CopyOptions,
    GfalClient,
    StatResult,
    TransferHandle,
)
from .core.errors import (
    GfalError,
    GfalFileExistsError,
    GfalFileNotFoundError,
    GfalIsADirectoryError,
    GfalNotADirectoryError,
    GfalPermissionError,
    GfalTimeoutError,
)

try:
    __version__ = _pkg_version("gfal")
except PackageNotFoundError:
    __version__ = "0.0.0+unknown"

__all__ = [
    "AsyncGfalClient",
    "ChecksumPolicy",
    "ClientConfig",
    "CopyOptions",
    "GfalClient",
    "GfalError",
    "StatResult",
    "TransferHandle",
    "GfalPermissionError",
    "GfalFileNotFoundError",
    "GfalFileExistsError",
    "GfalNotADirectoryError",
    "GfalIsADirectoryError",
    "GfalTimeoutError",
    "__version__",
]
