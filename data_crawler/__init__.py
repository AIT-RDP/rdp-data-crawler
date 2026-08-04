
from importlib.metadata import PackageNotFoundError, version as _package_version

try:
    __version__ = _package_version("rdp-data-crawler")
except PackageNotFoundError:  # The package metadata is unavailable, e.g. when running from a plain source checkout
    __version__ = "0+unknown"
