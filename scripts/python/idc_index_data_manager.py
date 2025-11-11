from __future__ import annotations

import json
import logging
import os
from pathlib import Path

import pandas as pd
from google.cloud import bigquery

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


class IDCIndexDataManager:
    def __init__(self, project_id: str):
        """
        Initializes the IDCIndexDataManager using the Google Cloud Platform project ID.
        """
        self.project_id = project_id
        self.client = bigquery.Client(project=project_id)
        logger.debug("IDCIndexDataManager initialized with project ID: %s", project_id)

    def execute_sql_query(
        self, file_path: str
    ) -> tuple[pd.DataFrame, str, list[bigquery.SchemaField]]:
        """
        Executes the SQL query in the specified file.

        Returns:
            Tuple[pd.DataFrame, str, List[bigquery.SchemaField]]: A tuple containing
            the DataFrame with query results, the output basename, and the BigQuery schema.
        """
        with Path(file_path).open("r") as file:
            sql_query = file.read()
        query_job_result = self.client.query(sql_query).result()
        schema = query_job_result.schema  # Get schema from BigQuery QueryJob
        index_df = query_job_result.to_dataframe()
        if "StudyDate" in index_df.columns:
            index_df["StudyDate"] = index_df["StudyDate"].astype(str)
        output_basename = Path(file_path).name.split(".")[0]
        logger.debug("Executed SQL query from file: %s", file_path)
        return index_df, output_basename, schema

    def save_schema_to_json(
        self,
        schema: list[bigquery.SchemaField],
        output_basename: str,
        output_dir: Path | None = None,
    ) -> None:
        """
        Saves the BigQuery schema to a JSON file.

        Args:
            schema: List of BigQuery SchemaField objects from the query result
            output_basename: The base name for the output file
            output_dir: Optional directory path for the output file
        """
        # Convert BigQuery schema to JSON-serializable format
        schema_dict = {
            "fields": [
                {
                    "name": field.name,
                    "type": field.field_type,
                    "mode": field.mode,
                }
                for field in schema
            ]
        }

        # Save to JSON file
        if output_dir:
            output_dir.mkdir(parents=True, exist_ok=True)
            json_file_path = output_dir / f"{output_basename}.json"
        else:
            json_file_path = Path(f"{output_basename}.json")

        with json_file_path.open("w") as f:
            json.dump(schema_dict, f, indent=2)
        logger.debug("Created schema JSON file: %s", json_file_path)

    def generate_index_data_files(
        self,
        generate_compressed_csv: bool = True,
        generate_parquet: bool = False,
        output_dir: Path | None = None,
    ) -> None:
        """
        Generates index-data files locally by executing queries against
        the Google Cloud Platform IDC project tables.

        This method iterates over SQL files in the 'scripts/sql' directory,
        executing each query using :func:`execute_sql_query` and generating a DataFrame,
        'index_df'. The DataFrame is then saved as compressed CSV and/or Parquet file.

        Args:
            generate_compressed_csv: Whether to generate compressed CSV files
            generate_parquet: Whether to generate Parquet files
            output_dir: Optional directory path for the output files
        """

        scripts_dir = Path(__file__).parent.parent
        sql_dir = scripts_dir / "sql"

        if output_dir:
            output_dir.mkdir(parents=True, exist_ok=True)

        for file_name in Path.iterdir(sql_dir):
            if str(file_name).endswith(".sql"):
                file_path = Path(sql_dir) / file_name
                index_df, output_basename, schema = self.execute_sql_query(file_path)
                logger.debug(
                    "Executed and processed SQL queries from file: %s", file_path
                )
                if generate_compressed_csv:
                    csv_file_path = (
                        output_dir / f"{output_basename}.csv.zip"
                        if output_dir
                        else Path(f"{output_basename}.csv.zip")
                    )
                    index_df.to_csv(
                        csv_file_path, compression={"method": "zip"}, escapechar="\\"
                    )
                    logger.debug("Created CSV zip file: %s", csv_file_path)

                if generate_parquet:
                    parquet_file_path = (
                        output_dir / f"{output_basename}.parquet"
                        if output_dir
                        else Path(f"{output_basename}.parquet")
                    )
                    index_df.to_parquet(parquet_file_path, compression="zstd")
                    logger.debug("Created Parquet file: %s", parquet_file_path)

                    # Save schema to JSON file
                    self.save_schema_to_json(schema, output_basename, output_dir)

    def retrieve_latest_idc_release_version(self) -> int:
        """
        Retrieves the latest IDC release version.

        This function executes a SQL query on the `version_metadata` table in the
        `idc_current` dataset of the BigQuery client. It retrieves the maximum
        `idc_version` and returns it as an integer.
        """
        query = """
        SELECT
            MAX(idc_version) AS latest_idc_release_version
        FROM
            `bigquery-public-data.idc_current.version_metadata`
        """
        query_job = self.client.query(query)
        result = query_job.result()
        return int(next(result).latest_idc_release_version)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--project",
        default=os.environ.get("GCP_PROJECT", None),
        help="Google Cloud Platform Project ID (default from GCP_PROJECT env. variable)",
    )
    parser.add_argument(
        "--generate-csv-archive",
        action="store_true",
        help="Generate idc_index.csv.zip file",
    )
    parser.add_argument(
        "--generate-parquet",
        action="store_true",
        help="Generate idc_index.parquet file",
    )
    parser.add_argument(
        "--retrieve-latest-idc-release-version",
        action="store_true",
        help="Retrieve and display the latest IDC release version",
    )

    args = parser.parse_args()

    if not args.project:
        parser.error(
            "Set GCP_PROJECT environment variable or specify --project argument"
        )

    if any([args.generate_csv_archive, args.generate_parquet]):
        IDCIndexDataManager(args.project).generate_index_data_files(
            generate_compressed_csv=args.generate_csv_archive,
            generate_parquet=args.generate_parquet,
        )
    elif args.retrieve_latest_idc_release_version:
        logging.basicConfig(level=logging.ERROR, force=True)
        logger.setLevel(logging.ERROR)
        version = IDCIndexDataManager(
            args.project
        ).retrieve_latest_idc_release_version()
        print(f"{version}")  # noqa: T201
    else:
        parser.print_help()
