from __future__ import annotations

import logging
import os
import re
import uuid
from pathlib import Path

import pandas as pd
from google.cloud import bigquery

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


class IDCIndexDataManager:
    def __init__(self, project_id: str):
        """
        Initializes the IDCIndexDataManager.

        Args:
            project_id (str): The Google Cloud Platform project ID.
        """
        self.project_id = project_id
        self.client = bigquery.Client(project=project_id)
        logger.debug("IDCIndexDataManager initialized with project ID: %s", project_id)

    def get_latest_idc_release_version(self) -> int:
        """
        Retrieves the latest IDC release version from BigQuery.

        Returns:
            int: The latest IDC release version.
        """
        query = """
        SELECT
            MAX(idc_version) AS latest_idc_release_version
        FROM
            `bigquery-public-data.idc_current.version_metadata`
        """
        query_job = self.client.query(query)
        result = query_job.result()
        latest_idc_release_version = int(next(result).latest_idc_release_version)
        logger.debug(
            "Retrieved latest IDC release version: %d", latest_idc_release_version
        )
        return latest_idc_release_version

    def extract_current_index_version(self, file_path: str) -> int:
        """
        Extracts the current index version from the specified file.

        Args:
            file_path (str): The path to the file containing the index version.

        Returns:
            int: The current index version.
        """
        try:
            with Path(file_path).open("r") as file:
                for line in file:
                    if "bigquery-public-data" in line:
                        match = re.findall(r"bigquery-public-data.(\w+).(\w+)", line)
                        if match:
                            dataset_name, table_name = match[0]
                            current_index_version = int(
                                re.findall(r"idc_v(\d+)", dataset_name)[0]
                            )
                            logger.debug(
                                "Extracted current index version: %d",
                                current_index_version,
                            )
                            return current_index_version
        except FileNotFoundError:
            logger.debug("File %s not found.", file_path)
        except Exception as e:
            logger.debug("An error occurred while extracting index version: %s", str(e))
        return None

    def update_sql_queries_folder(
        self, dir_path: str, current_index_version: int, latest_idc_release_version: int
    ) -> None:
        """
        Updates SQL queries in the specified folder.

        Args:
            dir_path (str): The path to the folder containing SQL queries.
            current_index_version (int): The current index version.
            latest_idc_release_version (int): The latest IDC release version.
        """
        for file_name in os.listdir(dir_path):
            if file_name.endswith(".sql"):
                file_path = Path(dir_path) / file_name
                with Path(file_path).open("r") as file:
                    sql_query = file.read()
                modified_sql_query = sql_query.replace(
                    f"idc_v{current_index_version}",
                    f"idc_v{latest_idc_release_version}",
                )
                with Path(file_path, "w").open() as file:
                    file.write(modified_sql_query)
                logger.debug("Updated SQL queries in file: %s", file_path)

    def execute_sql_query(self, file_path: str) -> tuple[pd.DataFrame, str, str]:
        """
        Executes the SQL query in the specified file.

        Args:
            file_path (str): The path to the file containing the SQL query.

        Returns:
            Tuple[pd.DataFrame, str, str]: A tuple containing the DataFrame with query results,
            the CSV file name, and the Parquet file name.
        """
        with Path(file_path).open("r") as file:
            sql_query = file.read()
        index_df = self.client.query(sql_query).to_dataframe()
        file_name = Path(file_path).name.split(".")[0]
        csv_file_name = f"{file_name}.csv.zip"
        parquet_file_name = f"{file_name}.parquet"
        logger.debug("Executed SQL query from file: %s", file_path)
        return index_df, csv_file_name, parquet_file_name

    def create_csv_zip_from_df(
        self, index_df: pd.DataFrame, csv_file_name: str
    ) -> None:
        """
        Creates a compressed CSV file from a pandas DataFrame.

        Args:
            index_df (pd.DataFrame): The pandas DataFrame to be saved as a CSV.
            csv_file_name (str): The desired name for the resulting ZIP file (including the ".csv.zip" extension).
        """
        index_df.to_csv(csv_file_name, compression={"method": "zip"}, escapechar="\\")
        logger.debug("Created CSV zip file: %s", csv_file_name)

    def create_parquet_from_df(
        self, index_df: pd.DataFrame, parquet_file_name: str
    ) -> None:
        """
        Creates a Parquet file from a pandas DataFrame.

        Args:
            index_df (pd.DataFrame): The pandas DataFrame to be saved as a Parquet file.
            parquet_file_name (str): The desired name for the resulting Parquet file.
        """
        index_df.to_parquet(parquet_file_name)
        logger.debug("Created Parquet file: %s", parquet_file_name)

    def run_queries_folder(self, dir_path: str) -> None:
        """
        Executes SQL queries in the specified folder.

        Args:
            dir_path (str): The path to the folder containing SQL query files.
        """
        for file_name in os.listdir(dir_path):
            if file_name.endswith(".sql"):
                file_path = Path(dir_path) / file_name
                index_df, csv_file_name, parquet_file_name = self.execute_sql_query(
                    file_path
                )
                self.create_csv_zip_from_df(index_df, csv_file_name)
                self.create_parquet_from_df(index_df, parquet_file_name)
                logger.debug(
                    "Executed and processed SQL queries from folder: %s", dir_path
                )

    def set_multiline_output(self, name: str, value: str) -> None:
        """
        Sets multiline output with a specified name and value.

        Args:
            name (str): The name of the output.
            value (str): The value of the output.
        """
        with Path(os.environ["GITHUB_OUTPUT"]).open("a") as fh:
            delimiter = uuid.uuid1()
            fh.write(f"{name}<<{delimiter}\n")
            fh.write(f"{value}\n")
            fh.write(f"{delimiter}\n")
        logger.debug("Set multiline output with name: %s and value: %s", name, value)

    def run(self) -> None:
        """
        Runs the IDCIndexDataManager process.
        """
        latest_idc_release_version = self.get_latest_idc_release_version()
        current_index_version = self.extract_current_index_version(
            "src/sql/idc_index.sql"
        )
        self.set_multiline_output("current_index_version", str(current_index_version))
        self.set_multiline_output(
            "latest_idc_release_version", str(latest_idc_release_version)
        )


if __name__ == "__main__":
    manager = IDCIndexDataManager("gcp-project-id")
    manager.run()
