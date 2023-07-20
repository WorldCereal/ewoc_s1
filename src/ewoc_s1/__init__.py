
from importlib.metadata import PackageNotFoundError, version  # pragma: no cover

try:
    # Change here if project is renamed and does not equal the package name
    dist_name = __name__
    __version__ = version(dist_name)
except PackageNotFoundError:  # pragma: no cover
    __version__ = "unknown"
finally:
    del version, PackageNotFoundError

EWOC_S1_UNEXPECTED_ERROR = 1
EWOC_S1_DEM_DOWNLOAD_ERROR = 2
EWOC_S1_INPUT_DOWNLOAD_ERROR = 3
EWOC_S1_PROCESSOR_ERROR = 4
EWOC_S1_ARD_FORMAT_ERROR = 5
