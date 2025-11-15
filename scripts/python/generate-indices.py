# new_script.py
from __future__ import annotations

import os
from pathlib import Path

from idc_index_data_manager import IDCIndexDataManager


def main():
    project_id = os.getenv("PROJECT_ID")
    manager = IDCIndexDataManager(project_id=project_id)
    scripts_dir = Path(__file__).resolve().parent.parent

    # Create dedicated output directory for release artifacts
    output_dir = scripts_dir.parent / "release_artifacts"
    output_dir.mkdir(parents=True, exist_ok=True)

    assets_dir = scripts_dir.parent / "assets"

    # Collecting all .sql files from sql_dir and assets_dir
    sql_files = [f for f in Path.iterdir(assets_dir) if str(f).endswith(".sql")]

    for file_name in sql_files:
        file_path = assets_dir / file_name
        index_df, output_basename, schema, sql_query = manager.execute_sql_query(
            file_path
        )
        parquet_file_path = output_dir / f"{output_basename}.parquet"
        index_df.to_parquet(parquet_file_path)
        manager.save_schema_to_json(schema, output_basename, sql_query, output_dir)
        manager.save_sql_query(sql_query, output_basename, output_dir)

    core_indices_dir = scripts_dir.parent / "scripts" / "sql"

    sql_files = [f for f in Path.iterdir(core_indices_dir) if str(f).endswith(".sql")]

    for file_name in sql_files:
        file_path = core_indices_dir / file_name
        index_df, output_basename, schema, sql_query = manager.execute_sql_query(
            file_path
        )
        parquet_file_path = output_dir / f"{output_basename}.parquet"
        index_df.to_parquet(parquet_file_path)
        if output_basename == "prior_versions_index":
            # For prior_versions_index, save schema without descriptions
            manager.save_schema_to_json(schema, output_basename, None, output_dir)
        else:
            manager.save_schema_to_json(schema, output_basename, sql_query, output_dir)
        manager.save_sql_query(sql_query, output_basename, output_dir)


if __name__ == "__main__":
    main()
