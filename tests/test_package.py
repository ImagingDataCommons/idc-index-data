from __future__ import annotations

import importlib.metadata

import pandas as pd
from packaging.version import Version

import idc_index_data as m

EXPECTED_IDC_INDEX_VERSION = 23


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


def test_reading_index():
    if m.IDC_INDEX_CSV_ARCHIVE_FILEPATH is not None:
        assert m.IDC_INDEX_CSV_ARCHIVE_FILEPATH.is_file()
        df_csv = pd.read_csv(m.IDC_INDEX_CSV_ARCHIVE_FILEPATH)
        assert not df_csv.empty

    if m.IDC_INDEX_PARQUET_FILEPATH is not None:
        assert m.IDC_INDEX_PARQUET_FILEPATH.is_file()
        df_parquet = pd.read_parquet(m.IDC_INDEX_PARQUET_FILEPATH)
        assert not df_parquet.empty

    if m.PRIOR_VERSIONS_INDEX_PARQUET_FILEPATH is not None:
        assert m.PRIOR_VERSIONS_INDEX_PARQUET_FILEPATH.is_file()
        df_parquet = pd.read_parquet(m.PRIOR_VERSIONS_INDEX_PARQUET_FILEPATH)
        assert not df_parquet.empty
