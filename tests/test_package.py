from __future__ import annotations

import importlib.metadata
from pathlib import Path

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


def test_index_metadata_exists():
    """Test that INDEX_METADATA dictionary exists and is properly structured."""
    assert m.INDEX_METADATA is not None, "INDEX_METADATA should be defined"
    assert isinstance(m.INDEX_METADATA, dict), "INDEX_METADATA should be a dictionary"
    assert len(m.INDEX_METADATA) > 0, "INDEX_METADATA should not be empty"


def test_index_metadata_has_all_indices():
    """Test that INDEX_METADATA contains all 7 expected indices."""
    expected_indices = [
        "idc_index",
        "prior_versions_index",
        "collections_index",
        "analysis_results_index",
        "clinical_index",
        "sm_index",
        "sm_instance_index",
    ]

    for index_name in expected_indices:
        assert index_name in m.INDEX_METADATA, (
            f"INDEX_METADATA should contain '{index_name}'"
        )


def test_index_metadata_structure():
    """Test that each index entry has the expected structure."""
    required_keys = ["parquet_filepath", "schema_path", "schema", "sql_path", "sql"]

    for index_name, metadata in m.INDEX_METADATA.items():
        assert isinstance(metadata, dict), (
            f"{index_name} metadata should be a dictionary"
        )

        for key in required_keys:
            assert key in metadata, f"{index_name} metadata should have '{key}' key"


def test_index_metadata_schema_content():
    """Test that schema content is properly loaded and structured.

    Note: Schemas may be None if the package was built without schema files.
    If schemas are present, they should have the expected structure.
    """
    for index_name, metadata in m.INDEX_METADATA.items():
        schema = metadata["schema"]

        # If schema is present, validate its structure
        if schema is not None:
            assert isinstance(schema, dict), (
                f"{index_name} schema should be a dictionary"
            )

            # All schemas should have a columns array
            assert "columns" in schema, (
                f"{index_name} schema should have 'columns' field"
            )
            assert isinstance(schema["columns"], list), (
                f"{index_name} schema columns should be a list"
            )
            assert len(schema["columns"]) > 0, (
                f"{index_name} schema should have at least one column"
            )

            # Validate column structure
            for col in schema["columns"]:
                assert "name" in col, "Column should have 'name' field"
                assert "type" in col, "Column should have 'type' field"
                assert "mode" in col, "Column should have 'mode' field"


def test_index_metadata_sql_content():
    """Test that SQL content is properly loaded.

    Note: SQL may be None if the package was built without SQL files.
    If SQL is present, it should be a non-empty string.
    """
    for index_name, metadata in m.INDEX_METADATA.items():
        sql = metadata["sql"]

        # If SQL is present, validate it
        if sql is not None:
            assert isinstance(sql, str), f"{index_name} SQL should be a string"
            assert len(sql) > 0, f"{index_name} SQL should not be empty"
            assert "SELECT" in sql.upper(), (
                f"{index_name} SQL should contain SELECT statement"
            )


def test_index_metadata_paths():
    """Test that parquet_filepath, schema_path and sql_path are valid Path objects or None."""
    for index_name, metadata in m.INDEX_METADATA.items():
        parquet_path = metadata["parquet_filepath"]
        schema_path = metadata["schema_path"]
        sql_path = metadata["sql_path"]

        # Paths should be Path objects or None
        assert parquet_path is None or isinstance(parquet_path, Path), (
            f"{index_name} parquet_filepath should be Path or None"
        )
        assert schema_path is None or isinstance(schema_path, Path), (
            f"{index_name} schema_path should be Path or None"
        )
        assert sql_path is None or isinstance(sql_path, Path), (
            f"{index_name} sql_path should be Path or None"
        )

        # If paths exist, they should point to existing files
        if parquet_path is not None:
            assert parquet_path.exists(), (
                f"{index_name} parquet file should exist at {parquet_path}"
            )
            assert parquet_path.is_file(), (
                f"{index_name} parquet_filepath should point to a file"
            )

        if schema_path is not None:
            assert schema_path.exists(), (
                f"{index_name} schema file should exist at {schema_path}"
            )
            assert schema_path.is_file(), (
                f"{index_name} schema_path should point to a file"
            )

        if sql_path is not None:
            assert sql_path.exists(), (
                f"{index_name} SQL file should exist at {sql_path}"
            )
            assert sql_path.is_file(), f"{index_name} sql_path should point to a file"


def test_index_metadata_consistency():
    """Test consistency between paths and loaded content.

    If a schema_path exists, schema should be loaded (not None).
    If a sql_path exists, sql should be loaded (not None).
    """
    for index_name, metadata in m.INDEX_METADATA.items():
        schema_path = metadata["schema_path"]
        schema = metadata["schema"]
        sql_path = metadata["sql_path"]
        sql = metadata["sql"]

        # If path exists, content should be loaded
        if schema_path is not None:
            assert schema is not None, (
                f"{index_name} schema should be loaded when schema_path exists"
            )

        if sql_path is not None:
            assert sql is not None, (
                f"{index_name} SQL should be loaded when sql_path exists"
            )


def test_index_metadata_main_indices_bundled():
    """Test that main indices (idc_index, prior_versions_index) have parquet and schemas.

    These are the core indices that should always have parquet, schema and SQL files
    bundled in the package when built with default settings.
    """
    main_indices = ["idc_index", "prior_versions_index"]

    for index_name in main_indices:
        metadata = m.INDEX_METADATA[index_name]

        # Main indices should always have parquet files
        parquet_path = metadata["parquet_filepath"]
        assert parquet_path is not None, (
            f"{index_name} parquet file should be included in the package"
        )
        assert isinstance(parquet_path, Path), (
            f"{index_name} parquet_filepath should be a Path object"
        )
        assert parquet_path.exists(), (
            f"{index_name} parquet file should exist at {parquet_path}"
        )

        # Main indices should have schema and SQL when package is built properly
        # Note: This test may be skipped if building without schema generation
        if metadata["schema_path"] is not None:
            assert metadata["schema"] is not None, (
                f"{index_name} schema should be loaded"
            )
            assert metadata["sql"] is not None, f"{index_name} SQL should be loaded"
