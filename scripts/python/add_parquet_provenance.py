"""Add provenance to parquet files: version in schema metadata + SHA256 sidecar."""

from __future__ import annotations

import argparse
import hashlib
from pathlib import Path

import pyarrow.parquet as pq


def add_version_metadata(path: Path, version: str) -> None:
    table = pq.read_table(str(path))
    existing = table.schema.metadata or {}
    table = table.replace_schema_metadata(
        {**existing, b"idc_index_data_version": version.encode()}
    )
    pq.write_table(table, str(path), compression="zstd")


def write_sha256(path: Path) -> None:
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    (path.parent / (path.name + ".sha256")).write_text(f"{digest}  {path.name}\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Embed version metadata and write SHA256 sidecars for parquet files."
    )
    parser.add_argument(
        "directory", type=Path, help="Directory containing parquet files"
    )
    parser.add_argument(
        "--version", required=True, help="idc-index-data version string to embed"
    )
    args = parser.parse_args()

    for parquet_file in sorted(args.directory.glob("*.parquet")):
        add_version_metadata(parquet_file, args.version)
        write_sha256(parquet_file)
        print(f"Processed: {parquet_file.name}")
