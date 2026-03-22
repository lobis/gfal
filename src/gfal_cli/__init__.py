"""
gfal-cli: GFAL2-compatible CLI tools based on fsspec.
"""

from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as _pkg_version

from .api import GfalClient
from .errors import (
    GfalError,
    GfalFileExistsError,
    GfalFileNotFoundError,
    GfalIsADirectoryError,
    GfalNotADirectoryError,
    GfalPermissionError,
    GfalTimeoutError,
)

try:
    __version__ = _pkg_version("gfal-cli")
except PackageNotFoundError:
    __version__ = "0.0.0+unknown"

__all__ = [
    "GfalClient",
    "GfalError",
    "GfalPermissionError",
    "GfalFileNotFoundError",
    "GfalFileExistsError",
    "GfalNotADirectoryError",
    "GfalIsADirectoryError",
    "GfalTimeoutError",
    "__version__",
]
