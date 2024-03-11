"""
Copyright (c) 2024 Andrey Fedorov. All rights reserved.

idc-index-data: ImagingDataCommons index to query and download data.
"""

from __future__ import annotations

import sys
from pathlib import Path

if sys.version_info >= (3, 10):
    from importlib.metadata import distribution
else:
    from importlib_metadata import distribution

from ._version import version as __version__

__all__ = ["__version__", "IDC_INDEX_CSV_ARCHIVE_FILEPATH"]


def _lookup(path: str) -> Path:
    """Support editable installation by looking up path using distribution API."""
    files = distribution("idc_index_data").files
    if files is not None:
        for _file in files:
            if str(_file) == path:
                return Path(str(_file.locate())).resolve(strict=True)
    msg = f"Failed to lookup '{path}`."
    raise FileNotFoundError(msg)


IDC_INDEX_CSV_ARCHIVE_FILEPATH: Path = _lookup("idc_index_data/idc_index.csv.zip")
