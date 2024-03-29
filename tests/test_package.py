from __future__ import annotations

import importlib.metadata

from packaging.version import Version

import idc_index_data as m

EXPECTED_IDC_INDEX_VERSION = 17


def test_version():
    assert importlib.metadata.version("idc_index_data") == m.__version__


def test_idc_index_version():
    assert Version(m.__version__).major == EXPECTED_IDC_INDEX_VERSION


def test_filepath():
    if m.IDC_INDEX_CSV_ARCHIVE_FILEPATH is not None:
        assert m.IDC_INDEX_CSV_ARCHIVE_FILEPATH.is_file()
        assert m.IDC_INDEX_CSV_ARCHIVE_FILEPATH.name == "idc_index.csv.zip"

    if m.IDC_INDEX_PARQUET_FILEPATH is not None:
        assert m.IDC_INDEX_PARQUET_FILEPATH.is_file()
        assert m.IDC_INDEX_PARQUET_FILEPATH.name == "idc_index.parquet"
