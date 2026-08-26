"""drt — data reverse tool.

Reverse ETL as code — no UI, no lock-in, no per-row bill.
"""

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("drt-core")
except PackageNotFoundError:
    __version__ = "0.0.0+unknown"
