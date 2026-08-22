"""GFAL-compatible command-line tools.

The public Python API is imported lazily so the lightweight command wrappers do
not load optional protocol libraries merely by importing :mod:`gfal`.
"""

from importlib import import_module
from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as _pkg_version

try:
    __version__ = _pkg_version("gfal")
except PackageNotFoundError:
    __version__ = "0.0.0+unknown"


_LAZY_EXPORTS = {
    "AsyncGfalClient": ("gfal.core.api", "AsyncGfalClient"),
    "ChecksumPolicy": ("gfal.core.api", "ChecksumPolicy"),
    "ClientConfig": ("gfal.core.api", "ClientConfig"),
    "CopyOptions": ("gfal.core.api", "CopyOptions"),
    "GfalClient": ("gfal.core.api", "GfalClient"),
    "StatResult": ("gfal.core.api", "StatResult"),
    "TransferHandle": ("gfal.core.api", "TransferHandle"),
    "GfalError": ("gfal.core.errors", "GfalError"),
    "GfalFileExistsError": ("gfal.core.errors", "GfalFileExistsError"),
    "GfalFileNotFoundError": ("gfal.core.errors", "GfalFileNotFoundError"),
    "GfalIsADirectoryError": ("gfal.core.errors", "GfalIsADirectoryError"),
    "GfalNotADirectoryError": ("gfal.core.errors", "GfalNotADirectoryError"),
    "GfalPermissionError": ("gfal.core.errors", "GfalPermissionError"),
    "GfalTimeoutError": ("gfal.core.errors", "GfalTimeoutError"),
}


def __getattr__(name):
    """Load the optional Python API on first attribute access."""
    try:
        module_name, attribute_name = _LAZY_EXPORTS[name]
    except KeyError as exc:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}") from exc

    value = getattr(import_module(module_name), attribute_name)
    globals()[name] = value
    return value


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
