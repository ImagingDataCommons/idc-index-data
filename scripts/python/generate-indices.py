# new_script.py
from __future__ import annotations

import json
import os
from pathlib import Path

import pyarrow as pa
from idc_index_data_manager import IDCIndexDataManager


def save_schema_to_json(df, output_basename: str) -> None:
    """
    Saves the schema of a DataFrame to a JSON file.

    Args:
        df: The DataFrame to extract schema from
        output_basename: The base name for the output file
    """
    # Convert DataFrame to PyArrow table to get schema
    table = pa.Table.from_pandas(df)
    schema = table.schema

    # Convert schema to JSON-serializable format
    schema_dict = {
        "fields": [
            {
                "name": field.name,
                "type": str(field.type),
                "nullable": field.nullable,
            }
            for field in schema
        ]
    }

    # Save to JSON file
    json_file_name = f"{output_basename}.json"
    with Path(json_file_name).open("w") as f:
        json.dump(schema_dict, f, indent=2)


def main():
    project_id = os.getenv("PROJECT_ID")
    manager = IDCIndexDataManager(project_id=project_id)
    scripts_dir = Path(__file__).resolve().parent.parent

    assets_dir = scripts_dir.parent / "assets"

    # Collecting all .sql files from sql_dir and assets_dir
    sql_files = [f for f in Path.iterdir(assets_dir) if str(f).endswith(".sql")]

    for file_name in sql_files:
        file_path = assets_dir / file_name
        index_df, output_basename = manager.execute_sql_query(file_path)
        index_df.to_parquet(f"{output_basename}.parquet")
        save_schema_to_json(index_df, output_basename)

    core_indices_dir = scripts_dir.parent / "scripts" / "sql"

    sql_files = [f for f in Path.iterdir(core_indices_dir) if str(f).endswith(".sql")]

    for file_name in sql_files:
        file_path = core_indices_dir / file_name
        index_df, output_basename = manager.execute_sql_query(file_path)
        index_df.to_parquet(f"{output_basename}.parquet")
        save_schema_to_json(index_df, output_basename)


if __name__ == "__main__":
    main()
