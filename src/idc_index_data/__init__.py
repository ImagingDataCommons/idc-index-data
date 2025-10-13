"""
Copyright (c) 2024 Andrey Fedorov. All rights reserved.

idc-index-data: ImagingDataCommons index to query and download data.
"""

from __future__ import annotations

from importlib.metadata import distribution
from pathlib import Path

from ._version import version as __version__

__all__ = [
    "IDC_INDEX_CSV_ARCHIVE_FILEPATH",
    "IDC_INDEX_PARQUET_FILEPATH",
    "PRIOR_VERSIONS_INDEX_PARQUET_FILEPATH",
    "__version__",
]


def _lookup(path: str, optional: bool = False) -> Path | None:
    """Support editable installation by looking up path using distribution API."""
    files = distribution("idc_index_data").files
    if files is not None:
        for _file in files:
            if str(_file) == path:
                return Path(str(_file.locate())).resolve(strict=True)
    if optional:
        return None

    msg = f"Failed to lookup '{path}`."
    raise FileNotFoundError(msg)


IDC_INDEX_CSV_ARCHIVE_FILEPATH: Path | None = _lookup(
    "idc_index_data/idc_index.csv.zip", optional=True
)
IDC_INDEX_PARQUET_FILEPATH: Path | None = _lookup("idc_index_data/idc_index.parquet")
PRIOR_VERSIONS_INDEX_PARQUET_FILEPATH: Path | None = _lookup(
    "idc_index_data/prior_versions_index.parquet"
)
