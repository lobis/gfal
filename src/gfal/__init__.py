"""GFAL-compatible command-line tools backed by XRootD clients."""

from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as _pkg_version

try:
    __version__ = _pkg_version("gfal")
except PackageNotFoundError:
    __version__ = "0.0.0+unknown"


__all__ = ["__version__"]
