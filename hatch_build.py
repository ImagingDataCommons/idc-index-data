"""Custom build hook for generating IDC index data files during build."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

from hatchling.builders.hooks.plugin.interface import BuildHookInterface


class IDCBuildHook(BuildHookInterface):
    """Build hook to generate index data files before packaging."""

    PLUGIN_NAME = "custom"

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
        # 1. Validate environment
        if not os.environ.get("GCP_PROJECT"):
            msg = (
                "GCP_PROJECT environment variable is not set. "
                "This is required to generate index data files."
            )
            raise OSError(msg)

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

        cmd = [sys.executable, str(script_path)]
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
                # Map: <source> = <destination in wheel>
                build_data.setdefault("force_include", {})[str(csv_file)] = (
                    "idc_index_data/idc_index.csv.zip"
                )
                self.app.display_info(f"Registered: {csv_file.name}")

        if generate_parquet:
            parquet_files = [
                "idc_index.parquet",
                "prior_versions_index.parquet",
            ]
            for filename in parquet_files:
                parquet_file = root_path / filename
                if parquet_file.exists():
                    build_data.setdefault("force_include", {})[str(parquet_file)] = (
                        f"idc_index_data/{filename}"
                    )
                    self.app.display_info(f"Registered: {filename}")
