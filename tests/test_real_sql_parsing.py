from __future__ import annotations

from pathlib import Path

from scripts.python.idc_index_data_manager import IDCIndexDataManager

"""Test script to verify column description parsing with real SQL files."""


def test_real_sql_files() -> None:
    """Test parsing descriptions from actual SQL files in the repository."""
    scripts_dir = Path(__file__).parent.parent / "scripts"
    sql_dir = scripts_dir / "sql"

    # Test collections_index.sql
    collections_sql_path = sql_dir / "collections_index.sql"
    if collections_sql_path.exists():
        with collections_sql_path.open("r") as f:
            sql_query = f.read()

        descriptions = IDCIndexDataManager.parse_column_descriptions(sql_query)
        print("\n=== collections_index.sql ===")
        print(f"Found {len(descriptions)} column descriptions:")
        for col_name, desc in descriptions.items():
            print(f"  {col_name}: {desc[:60]}...")

        # Validate expected columns
        expected_columns = [
            "collection_name",
            "collection_id",
            "CancerTypes",
            "TumorLocations",
            "Subjects",
            "Species",
            "Sources",
            "SupportingData",
            "Program",
            "Status",
            "Updated",
            "Description",
        ]
        for col in expected_columns:
            assert col in descriptions, (
                f"Expected column '{col}' not found in descriptions"
            )
        print("✓ All expected columns found")

    # Test idc_index.sql
    idc_index_sql_path = sql_dir / "idc_index.sql"
    if idc_index_sql_path.exists():
        with idc_index_sql_path.open("r") as f:
            sql_query = f.read()

        descriptions = IDCIndexDataManager.parse_column_descriptions(sql_query)
        print("\n=== idc_index.sql ===")
        print(f"Found {len(descriptions)} column descriptions:")

        # Show first 10 descriptions
        for i, (col_name, desc) in enumerate(descriptions.items()):
            if i < 10:
                print(f"  {col_name}: {desc[:60]}...")
            else:
                break

        if len(descriptions) > 10:
            print(f"  ... and {len(descriptions) - 10} more")

        # Validate some expected columns
        expected_columns = [
            "collection_id",
            "analysis_result_id",
            "PatientID",
            "SeriesInstanceUID",
            "StudyInstanceUID",
            "source_DOI",
            "PatientAge",
            "PatientSex",
            "StudyDate",
            "series_size_MB",
        ]
        for col in expected_columns:
            if col in descriptions:
                print(f"✓ Found expected column: {col}")
            else:
                print(f"✗ Missing expected column: {col}")


if __name__ == "__main__":
    test_real_sql_files()
