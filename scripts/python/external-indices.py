# new_script.py
from __future__ import annotations

import os
from pathlib import Path

from idc_index_data_manager import IDCIndexDataManager


def main():
    project_id = os.getenv("PROJECT_ID")
    manager = IDCIndexDataManager(project_id=project_id)
    scripts_dir = Path(__file__).resolve().parent.parent
    assets_dir = scripts_dir.parent / "assets"

    # Collecting all .sql files from sql_dir and assets_dir
    sql_files = [f for f in os.listdir(assets_dir) if f.endswith(".sql")]

    for file_name in sql_files:
        file_path = assets_dir / file_name
        index_df, output_basename = manager.execute_sql_query(file_path)
        index_df.to_parquet(f"{output_basename}.parquet")


if __name__ == "__main__":
    main()
