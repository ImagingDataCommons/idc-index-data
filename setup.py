"""Custom setup.py for idc-index-data.

This file provides a custom build command that generates the IDC index data files
(Parquet and optionally CSV) at build time by querying BigQuery. This replaces the
previous CMake-based build system.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

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

        if not generate_csv and not generate_parquet:
            print("Skipping index data generation (no formats requested)")  # noqa: T201
            super().run()
            return

        # Set output directory to src/idc_index_data/
        output_dir = Path(__file__).parent / "src" / "idc_index_data"
        output_dir.mkdir(parents=True, exist_ok=True)

        # Path to the index data manager script
        script_path = (
            Path(__file__).parent / "scripts" / "python" / "idc_index_data_manager. py"
        )

        if not script_path.exists():
            msg = f"Index data manager script not found at {script_path}"
            raise RuntimeError(msg)

        print(f"Generating index data files in {output_dir}...")  # noqa: T201
        print(f"  CSV archive:  {generate_csv}")  # noqa: T201
        print(f"  Parquet: {generate_parquet}")  # noqa: T201

        # Build command arguments
        cmd = [sys.executable, str(script_path)]
        if generate_csv:
            cmd.append("--generate-csv-archive")
        if generate_parquet:
            cmd.append("--generate-parquet")

        # Run the script as a subprocess
        try:
            result = subprocess.run(
                cmd,
                cwd=output_dir,
                check=True,
                capture_output=True,
                text=True,
            )
            print(result.stdout)  # noqa: T201
            if result.stderr:
                print(result.stderr, file=sys.stderr)  # noqa: T201
            print("Index data files generated successfully.")  # noqa: T201

        except subprocess.CalledProcessError as e:
            print(f"STDOUT: {e.stdout}", file=sys.stderr)  # noqa: T201
            print(f"STDERR: {e.stderr}", file=sys.stderr)  # noqa: T201
            msg = f"Failed to generate index data files (exit code {e.returncode})"
            raise RuntimeError(msg) from e

        # Continue with standard build
        super().run()


# Use setup() with cmdclass to register the custom command
setup(
    cmdclass={
        "build_py": BuildPyCommand,
    },
)
