"""
Copyright (c) 2024 Andrey Fedorov. All rights reserved.

idc-index-data: ImagingDataCommons index to query and download data.
"""

from __future__ import annotations

import json
from importlib.metadata import distribution
from pathlib import Path

from ._version import version as __version__

__all__ = [
    "IDC_INDEX_CSV_ARCHIVE_FILEPATH",
    "IDC_INDEX_PARQUET_FILEPATH",
    "INDEX_METADATA",
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


def _load_json(path: Path | None) -> dict[str, object] | None:
    """Load JSON file and return as dictionary."""
    if path is None:
        return None
    try:
        with path.open() as f:
            return json.load(f)  # type: ignore[no-any-return]
    except (FileNotFoundError, json.JSONDecodeError):
        return None


def _load_text(path: Path | None) -> str | None:
    """Load text file and return as string."""
    if path is None:
        return None
    try:
        with path.open() as f:
            return f.read()
    except FileNotFoundError:
        return None


IDC_INDEX_CSV_ARCHIVE_FILEPATH: Path | None = _lookup(
    "idc_index_data/idc_index.csv.zip", optional=True
)
IDC_INDEX_PARQUET_FILEPATH: Path | None = _lookup("idc_index_data/idc_index.parquet")
PRIOR_VERSIONS_INDEX_PARQUET_FILEPATH: Path | None = _lookup(
    "idc_index_data/prior_versions_index.parquet"
)

# Build unified metadata dictionary for all 7 indices
_ALL_INDICES = [
    "idc_index",
    "prior_versions_index",
    "collections_index",
    "analysis_results_index",
    "clinical_index",
    "sm_index",
    "sm_instance_index",
]

INDEX_METADATA: dict[str, dict[str, Path | dict[str, object] | str | None]] = {}

for index_name in _ALL_INDICES:
    # Lookup file paths
    schema_path = _lookup(f"idc_index_data/{index_name}_schema.json", optional=True)
    sql_path = _lookup(f"idc_index_data/{index_name}.sql", optional=True)

    # Load file contents
    schema_dict = _load_json(schema_path)
    sql_text = _load_text(sql_path)

    # Store in unified structure
    INDEX_METADATA[index_name] = {
        "schema_path": schema_path,
        "schema": schema_dict,
        "sql_path": sql_path,
        "sql": sql_text,
    }
