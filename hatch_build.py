"""Custom build hook for generating IDC index data files during build."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path
from typing import ClassVar

from hatchling.builders.hooks.plugin.interface import BuildHookInterface


class IDCBuildHook(BuildHookInterface):
    """Build hook to generate index data files before packaging."""

    PLUGIN_NAME = "custom"

    # Parquet files to exclude from the package to reduce size
    PARQUET_EXCLUDE_LIST: ClassVar[set[str]] = {
        "sm_index.parquet",
        "sm_instance_index.parquet",
        "clinical_index.parquet",
        "seg_index.parquet",
        "ann_index.parquet",
        "ann_group_index.parquet",
        "contrast_index.parquet",
    }

    def _prune_excluded_parquet_files(self) -> None:
        """Remove parquet files in the exclude list from the package dir."""
        package_dir = Path(self.root) / "src" / "idc_index_data"
        if not package_dir.exists():
            return

        for parquet_file in package_dir.glob("*.parquet"):
            if parquet_file.name in self.PARQUET_EXCLUDE_LIST:
                parquet_file.unlink()
                self.app.display_info(f"Removed excluded parquet: {parquet_file.name}")

    def initialize(self, version: str, build_data: dict) -> None:  # noqa: ARG002
        """
        Generate data files before build.

        This method:
        1. Validates GCP_PROJECT environment variable is set
        2. Determines which files to generate based on options
        3. Executes idc_index_data_manager.py to generate data
        4. Registers generated files for inclusion in wheel

        Args:
            version: The project version being built
            build_data: Dictionary containing build configuration
                - artifacts: List of additional files to include
                - force_include: Dict mapping source -> dest paths
        """
        # Remove parquet files that are optional to reduce package size
        self._prune_excluded_parquet_files()

        # 1. Validate environment
        if not os.environ.get("GCP_PROJECT"):
            self.app.display_warning("Skipping data generation: GCP_PROJECT not set")
            return

        # 2. Read build options from environment or config
        # Allow override via env vars, matching CMake behavior
        generate_csv = os.environ.get(
            "IDC_INDEX_DATA_GENERATE_CSV_ARCHIVE", "0"
        ).lower() in ("1", "true", "on", "yes")

        generate_parquet = os.environ.get(
            "IDC_INDEX_DATA_GENERATE_PARQUET", "1"
        ).lower() in ("1", "true", "on", "yes")

        if not (generate_csv or generate_parquet):
            self.app.display_warning(
                "No data files will be generated (both CSV and Parquet disabled)"
            )
            return

        # 3. Execute data generation script
        self.app.display_info("Generating IDC index data files...")

        script_path = (
            Path(__file__).parent / "scripts" / "python" / "idc_index_data_manager.py"
        )

        root_path = Path(self.root)

        cmd = [sys.executable, str(script_path), "--output-dir", str(root_path)]
        if generate_csv:
            cmd.append("--generate-csv-archive")
        if generate_parquet:
            cmd.append("--generate-parquet")

        try:
            result = subprocess.run(
                cmd,
                check=True,
                capture_output=True,
                text=True,
                cwd=self.root,
            )
            self.app.display_info(f"Data generation output: {result.stdout}")
        except subprocess.CalledProcessError as e:
            self.app.display_error(f"Data generation failed: {e.stderr}")
            msg = f"Failed to generate index data files: {e.stderr}"
            raise RuntimeError(msg) from e

        # 4. Register generated files for inclusion
        # Files are generated in project root by default
        # Need to map them to idc_index_data/ in the wheel

        root_path = Path(self.root)

        if generate_csv:
            csv_file = root_path / "idc_index.csv.zip"
            if csv_file.exists():
                build_data.setdefault("force_include", {})[str(csv_file)] = (
                    "idc_index_data/idc_index.csv.zip"
                )
                self.app.display_info(f"Registered: {csv_file.name}")

        if generate_parquet:
            # Register parquet files (excluding large indices to reduce package size)
            # Dynamically discover all generated parquet files
            for parquet_file in root_path.glob("*.parquet"):
                if parquet_file.name not in self.PARQUET_EXCLUDE_LIST:
                    build_data.setdefault("force_include", {})[str(parquet_file)] = (
                        f"idc_index_data/{parquet_file.name}"
                    )
                    self.app.display_info(f"Registered: {parquet_file.name}")

            # Also register schema and SQL files for all indices
            # Discover index names from generated schema files
            for schema_file in root_path.glob("*_schema.json"):
                index_name = schema_file.stem.replace("_schema", "")

                # Register schema JSON file
                build_data.setdefault("force_include", {})[str(schema_file)] = (
                    f"idc_index_data/{schema_file.name}"
                )
                self.app.display_info(f"Registered: {schema_file.name}")

                # Register corresponding SQL file if it exists
                sql_file = root_path / f"{index_name}.sql"
                if sql_file.exists():
                    build_data.setdefault("force_include", {})[str(sql_file)] = (
                        f"idc_index_data/{sql_file.name}"
                    )
                    self.app.display_info(f"Registered: {sql_file.name}")
