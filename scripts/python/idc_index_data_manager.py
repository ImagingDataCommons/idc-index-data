from __future__ import annotations

import json
import logging
import os
import re
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

    @staticmethod
    def parse_table_description(sql_query: str) -> str:
        """
        Parses the table description from SQL query comments.

        The method looks for comments following the pattern:
        # table-description:
        # description text continues here
        # and can span multiple lines

        Args:
            sql_query: The SQL query string containing comments

        Returns:
            The table description as a string
        """
        description_lines = []
        logger.debug("Parsing table description from SQL query comments")
        logger.debug(sql_query)
        lines = sql_query.split("\n")

        for i, line in enumerate(lines):
            stripped = line.strip()
            if stripped == "# table-description:":
                # Collect description lines until we hit a non-comment line
                j = i + 1
                while j < len(lines):
                    next_line = lines[j]
                    next_stripped = next_line.strip()
                    if next_stripped.startswith("#") and next_stripped != "#":
                        # Remove the leading # and whitespace
                        desc_text = next_stripped[1:].strip()
                        if desc_text:
                            description_lines.append(desc_text)
                        j += 1
                    elif next_stripped.startswith("#"):
                        # Empty comment line, skip
                        j += 1
                    else:
                        # Non-comment line, stop collecting
                        break
                break

        return " ".join(description_lines)

    @staticmethod
    def parse_column_descriptions(sql_query: str) -> dict[str, str]:
        """
        Parses column descriptions from SQL query comments.

        The method looks for comments following the pattern:
        # description:
        # description text continues here
        # and can span multiple lines
        column_name or expression AS column_name,

        Args:
            sql_query: The SQL query string containing comments

        Returns:
            Dictionary mapping column names to their descriptions
        """
        descriptions: dict[str, str] = {}
        logger.debug("Parsing column descriptions from SQL query comments")
        logger.debug(sql_query)
        lines = sql_query.split("\n")

        i = 0
        while i < len(lines):
            line = lines[i]
            stripped = line.strip()

            # Check if this line starts a description comment
            if stripped == "# description:":
                # Collect description lines until we hit a non-comment line
                description_lines = []
                i += 1

                while i < len(lines):
                    next_line = lines[i]
                    next_stripped = next_line.strip()

                    # If it's a description comment line (starts with #)
                    if next_stripped.startswith("#") and next_stripped != "#":
                        # Remove the leading # and whitespace
                        desc_text = next_stripped[1:].strip()
                        if desc_text:
                            description_lines.append(desc_text)
                        i += 1
                    elif next_stripped.startswith("#"):
                        # Empty comment line, skip
                        i += 1
                    else:
                        # Non-comment line - this should contain the column definition
                        break

                # Now parse the column definition
                if i < len(lines) and description_lines:
                    # Join the description lines
                    description = " ".join(description_lines)

                    # Find the column name by parsing the SELECT clause
                    # We need to handle multi-line column definitions with nested structures
                    column_def = ""
                    paren_depth = (
                        0  # Track parentheses depth to handle nested SELECT/FROM
                    )

                    while i < len(lines):
                        current_line = lines[i]
                        current_stripped = current_line.strip()

                        # Count parentheses to track nesting depth
                        paren_depth += current_line.count("(") - current_line.count(")")

                        # Only check for top-level SQL keywords when not inside nested structures
                        if paren_depth == 0 and any(
                            current_stripped.upper().startswith(keyword)
                            for keyword in [
                                "FROM",
                                "WHERE",
                                "GROUP BY",
                                "ORDER BY",
                                "JOIN",
                                "LEFT",
                                "RIGHT",
                                "INNER",
                                "OUTER",
                            ]
                        ):
                            # Don't include this line in column_def
                            # Don't increment i here - let outer loop handle it
                            break

                        column_def += " " + current_stripped
                        i += 1

                        # Check if we've found a complete column definition
                        # (has a comma at depth 0)
                        if paren_depth == 0 and "," in current_line:
                            break

                        # Safety check: if we've gone too deep, break
                        if paren_depth < 0:
                            break

                    # Extract column name from the definition
                    column_name = IDCIndexDataManager._extract_column_name(column_def)
                    if column_name:
                        descriptions[column_name] = description
                        logger.debug(
                            "Parsed description for column '%s': %s",
                            column_name,
                            description,
                        )
                        # throw exception if description is empty
                        if not description:
                            raise ValueError(
                                "Description for column '"
                                + column_name
                                + "' is empty, and empty descriptions are not allowed."
                            )

                else:
                    i += 1
            else:
                i += 1

        return descriptions

    @staticmethod
    def _extract_column_name(column_def: str) -> str | None:
        """
        Extracts the column name from a column definition.

        Handles various formats:
        - column_name,
        - expression AS column_name,
        - ANY_VALUE(column) AS column_name,
        - Complex expressions with nested parentheses

        Args:
            column_def: The column definition string

        Returns:
            The column name or None if not found
        """
        # Remove trailing comma and whitespace
        column_def = column_def.strip().rstrip(",").strip()

        # Look for the last AS clause (to handle nested AS in CAST expressions)
        # Use a regex that finds the rightmost AS followed by a word
        as_matches = list(re.finditer(r"\bAS\b\s+(\w+)", column_def, re.IGNORECASE))
        if as_matches:
            # Return the last match (rightmost AS clause)
            return as_matches[-1].group(1)

        # If no AS clause, try to get the column name
        # Remove function calls and get the last word before comma
        # Handle cases like: column_name, or just column_name
        parts = column_def.split()
        if parts:
            # Get the last word that looks like an identifier
            for original_part in reversed(parts):
                # Remove trailing punctuation
                part = original_part.rstrip(",").strip()
                # Check if it's a valid identifier (word characters only)
                if re.match(r"^\w+$", part):
                    return part

        return None

    def execute_sql_query(
        self, file_path: str
    ) -> tuple[pd.DataFrame, str, list[bigquery.SchemaField], str]:
        """
        Executes the SQL query in the specified file.

        Returns:
            Tuple[pd.DataFrame, str, List[bigquery.SchemaField], str]: A tuple containing
            the DataFrame with query results, the output basename, the BigQuery schema, and
            the SQL query string.
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
        return index_df, output_basename, schema, sql_query

    def save_schema_to_json(
        self,
        schema: list[bigquery.SchemaField],
        output_basename: str,
        sql_query: str | None = None,
        output_dir: Path | None = None,
    ) -> None:
        """
        Saves the BigQuery schema to a JSON file, including column descriptions
        parsed from SQL query comments.

        Args:
            schema: List of BigQuery SchemaField objects from the query result
            output_basename: The base name for the output file
            sql_query: The SQL query string to parse for column descriptions
            output_dir: Optional directory path for the output file
        """
        # Parse column descriptions from SQL comments
        logger.debug("Parsing column descriptions from SQL query comments")
        logger.debug(sql_query)
        if sql_query is not None:
            table_description = self.parse_table_description(sql_query)
            logger.debug("Parsed table description: %s", table_description)
            descriptions = self.parse_column_descriptions(sql_query)

            # Convert BigQuery schema to JSON-serializable format
            schema_dict = {
                "table_description": table_description,
                "columns": [
                    {
                        "name": field.name,
                        "type": field.field_type,
                        "mode": field.mode,
                        "description": descriptions.get(field.name, ""),
                    }
                    for field in schema
                ],
            }
        else:
            # If no SQL query provided, save schema without descriptions
            schema_dict = {
                "columns": [
                    {
                        "name": field.name,
                        "type": field.field_type,
                        "mode": field.mode,
                        "description": "",
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

    def save_sql_query(
        self,
        sql_query: str,
        output_basename: str,
        output_dir: Path | None = None,
    ) -> None:
        """
        Saves the SQL query to a file.

        Args:
            sql_query: The SQL query string
            output_basename: The base name for the output file
            output_dir: Optional directory path for the output file
        """

        if output_dir:
            output_dir.mkdir(parents=True, exist_ok=True)
            query_file_path = output_dir / f"{output_basename}.sql"
        else:
            query_file_path = Path(f"{output_basename}.sql")

        with query_file_path.open("w") as f:
            f.write(sql_query)
        logger.debug("Created SQL query file: %s", query_file_path)

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
                index_df, output_basename, schema, sql_query = self.execute_sql_query(
                    str(file_path)
                )
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
                # Skip parsing descriptions for prior_versions_index as it has dynamic SQL
                if output_basename != "prior_versions_index":
                    self.save_schema_to_json(
                        schema, output_basename, sql_query, output_dir
                    )
                else:
                    # For prior_versions_index, save schema without descriptions
                    self.save_schema_to_json(schema, output_basename, None, output_dir)
                # Save SQL query to file
                self.save_sql_query(sql_query, output_basename, output_dir)

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
            `bigquery-public-data.idc_v23.version_metadata`
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
