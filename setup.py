"""Custom setup.py for idc-index-data.

This file provides a custom build command that generates the IDC index data files
(Parquet and optionally CSV) at build time by querying BigQuery. This replaces the
previous CMake-based build system.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

from idc_index_data_manager import IDCIndexDataManager
from setuptools import setup
from setuptools.command.build_py import build_py


class BuildPyCommand(build_py):
    """Custom build_py command that generates index data files before building."""

    def run(self) -> None:
        """Generate index data files, then run the standard build."""
        # Check for required GCP_PROJECT environment variable
        project_id = os.environ.get("GCP_PROJECT")
        if not project_id:
            msg = (
                "GCP_PROJECT environment variable must be set to build this package. "
                "This is required to query BigQuery for the index data. "
                "For local testing, use: GCP_PROJECT=idc-sandbox-000"
            )
            raise RuntimeError(msg)

        # Read build options from environment variables
        # Default: Parquet=on, CSV=off (matching CMake defaults)
        generate_csv = os.environ.get("IDC_INDEX_DATA_GENERATE_CSV_ARCHIVE", "0") == "1"
        generate_parquet = os.environ.get("IDC_INDEX_DATA_GENERATE_PARQUET", "1") == "1"

        # Set output directory to src/idc_index_data/
        output_dir = Path(__file__).parent / "src" / "idc_index_data"
        output_dir.mkdir(parents=True, exist_ok=True)

        # Import and run the index data manager
        # Add scripts/python to sys.path to import the module
        scripts_dir = Path(__file__).parent / "scripts" / "python"
        sys.path.insert(0, str(scripts_dir))

        try:
            print(f"Generating index data files in {output_dir}...")  # noqa: T201
            print(f"  CSV archive: {generate_csv}")  # noqa: T201
            print(f"  Parquet: {generate_parquet}")  # noqa: T201

            manager = IDCIndexDataManager(project_id)
            manager.generate_index_data_files(
                output_dir=str(output_dir),
                generate_compressed_csv=generate_csv,
                generate_parquet=generate_parquet,
            )
            print("Index data files generated successfully.")  # noqa: T201

        except Exception as e:
            msg = f"Failed to generate index data files: {e}"
            raise RuntimeError(msg) from e
        finally:
            # Remove scripts directory from sys.path
            sys.path.remove(str(scripts_dir))

        # Continue with standard build
        super().run()


# Use setup() with cmdclass to register the custom command
setup(
    cmdclass={
        "build_py": BuildPyCommand,
    },
)
