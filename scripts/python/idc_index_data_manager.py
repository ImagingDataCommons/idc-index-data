from __future__ import annotations

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

    def execute_sql_query(self, file_path: str) -> tuple[pd.DataFrame, str]:
        """
        Executes the SQL query in the specified file.

        Returns:
            Tuple[pd.DataFrame, str]: A tuple containing the DataFrame with query results,
            the output basename.
        """
        with Path(file_path).open("r") as file:
            sql_query = file.read()
        index_df = self.client.query(sql_query).to_dataframe()
        output_basename = Path(file_path).name.split(".")[0]
        logger.debug("Executed SQL query from file: %s", file_path)
        return index_df, output_basename

    def generate_index_data_files(
        self, generate_compressed_csv: bool = True, generate_parquet: bool = False
    ) -> None:
        """
        Executes SQL queries in the specified folder and creates a
        compressed CSV file and/or Parquet file from a pandas DataFrame.

        This method iterates over all .sql files in the 'scripts/sql' directory,
        executes each query using the 'execute_sql_query' method, and generates
        a DataFrame 'index_df'. The DataFrame is then saved as a compressed CSV
        and/or a Parquet file, depending on the method arguments.

        """

        scripts_dir = Path(__file__).parent.parent
        sql_dir = scripts_dir / "sql"

        for file_name in os.listdir(sql_dir):
            if file_name.endswith(".sql"):
                file_path = Path(sql_dir) / file_name
                index_df, output_basename = self.execute_sql_query(file_path)
                logger.debug(
                    "Executed and processed SQL queries from file: %s", file_path
                )
            if generate_compressed_csv:
                csv_file_name = f"{output_basename}.csv.zip"
                index_df.to_csv(
                    csv_file_name, compression={"method": "zip"}, escapechar="\\"
                )
                logger.debug("Created CSV zip file: %s", csv_file_name)

            if generate_parquet:
                parquet_file_name = f"{output_basename}.parquet"
                index_df.to_parquet(parquet_file_name)
                logger.debug("Created Parquet file: %s", parquet_file_name)

    def run(self) -> None:
        """
        Runs the IDCIndexDataManager to locally generate a index-data file (.czv.zip) by running queries against the Google Cloud Platform IDC project tables.
        """
        self.generate_index_data_files(
            generate_compressed_csv=True, generate_parquet=False
        )


if __name__ == "__main__":
    project_id = os.environ["GCP_PROJECT"]
    manager = IDCIndexDataManager(project_id)
    manager.run()
