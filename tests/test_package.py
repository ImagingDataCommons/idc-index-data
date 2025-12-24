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


def test_parquet_files_are_bundled():
    """Test that parquet files are always included in the installed package.

    The build system (hatchling with custom build hook) should generate and
    include parquet files during the build process. These files are required
    (not optional) and should always be present after installation.
    """
    # Main index parquet file must be present (not optional)
    assert m.IDC_INDEX_PARQUET_FILEPATH is not None, (
        "idc_index.parquet must be included in the package"
    )
    assert m.IDC_INDEX_PARQUET_FILEPATH.exists(), (
        f"idc_index.parquet not found at {m.IDC_INDEX_PARQUET_FILEPATH}"
    )
    assert m.IDC_INDEX_PARQUET_FILEPATH.is_file(), (
        "idc_index.parquet must be a file, not a directory"
    )

    # Prior versions index parquet file must be present (not optional)
    assert m.PRIOR_VERSIONS_INDEX_PARQUET_FILEPATH is not None, (
        "prior_versions_index.parquet must be included in the package"
    )
    assert m.PRIOR_VERSIONS_INDEX_PARQUET_FILEPATH.exists(), (
        f"prior_versions_index.parquet not found at {m.PRIOR_VERSIONS_INDEX_PARQUET_FILEPATH}"
    )
    assert m.PRIOR_VERSIONS_INDEX_PARQUET_FILEPATH.is_file(), (
        "prior_versions_index.parquet must be a file, not a directory"
    )


def test_parquet_files_are_readable():
    """Test that bundled parquet files can be read and contain data.

    This verifies not only that the files exist, but that they were properly
    generated with valid parquet data during the build process.
    """
    # Read main index parquet file
    assert m.IDC_INDEX_PARQUET_FILEPATH is not None
    df_main = pd.read_parquet(m.IDC_INDEX_PARQUET_FILEPATH)
    assert not df_main.empty, "idc_index.parquet should contain data"
    assert len(df_main) > 0, "idc_index.parquet should have at least one row"

    # Read prior versions index parquet file
    assert m.PRIOR_VERSIONS_INDEX_PARQUET_FILEPATH is not None
    df_prior = pd.read_parquet(m.PRIOR_VERSIONS_INDEX_PARQUET_FILEPATH)
    assert not df_prior.empty, "prior_versions_index.parquet should contain data"
    assert len(df_prior) > 0, (
        "prior_versions_index.parquet should have at least one row"
    )


def test_parquet_file_locations():
    """Test that parquet files are in the expected package location.

    Files should be located within the idc_index_data package directory,
    as configured in the hatchling build hook.
    """
    assert m.IDC_INDEX_PARQUET_FILEPATH is not None
    assert m.PRIOR_VERSIONS_INDEX_PARQUET_FILEPATH is not None

    # Both files should be in the same directory (idc_index_data package)
    assert (
        m.IDC_INDEX_PARQUET_FILEPATH.parent
        == m.PRIOR_VERSIONS_INDEX_PARQUET_FILEPATH.parent
    ), "Both parquet files should be in the same package directory"

    # Verify the parent directory name is idc_index_data
    assert m.IDC_INDEX_PARQUET_FILEPATH.parent.name == "idc_index_data", (
        "Parquet files should be in the idc_index_data directory"
    )
