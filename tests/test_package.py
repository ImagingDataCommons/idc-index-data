from __future__ import annotations

import importlib.metadata

import idc_index_data as m


def test_version():
    assert importlib.metadata.version("idc_index_data") == m.__version__


def test_filepath():
    assert m.IDC_INDEX_CSV_ARCHIVE_FILEPATH.is_file()
    assert m.IDC_INDEX_CSV_ARCHIVE_FILEPATH.name == "idc_index.csv.zip"
