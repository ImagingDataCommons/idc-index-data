"""Generate a TCIA subset parquet from the IDC main index.

Extracts a subset of columns useful for TCIA workflows (e.g. downloading
content into a hierarchy of collection/patient/study/series folders).

See https://github.com/ImagingDataCommons/idc-index-data/issues/114
"""

from __future__ import annotations

import argparse
from pathlib import Path

import duckdb

COLUMNS = [
    "collection_id",
    "PatientID",
    "StudyInstanceUID",
    "SeriesInstanceUID",
    "series_size_MB",
    "series_aws_url",
]

OUTPUT_PARQUET = "tcia_idc_subset.parquet"


def generate(input_parquet: str, output_path: str = OUTPUT_PARQUET) -> None:
    """Read the IDC index parquet and write a column subset for TCIA."""
    cols = ", ".join(COLUMNS)
    con = duckdb.connect()
    try:
        con.execute(
            f"COPY (SELECT {cols} FROM read_parquet('{input_parquet}')) "
            f"TO '{output_path}' (FORMAT PARQUET, COMPRESSION ZSTD)"
        )
        row_count = con.execute(
            f"SELECT count(*) FROM read_parquet('{output_path}')"
        ).fetchone()[0]
        size_mb = Path(output_path).stat().st_size / 1_000_000
        print(f"Saved {row_count} rows to {output_path} ({size_mb:.2f} MB)")
    finally:
        con.close()


def main() -> None:
    """Main entry point."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "input_parquet",
        help="Path to the idc_index.parquet file",
    )
    parser.add_argument(
        "-o",
        "--output",
        default=OUTPUT_PARQUET,
        help=f"Output parquet path (default: {OUTPUT_PARQUET})",
    )
    args = parser.parse_args()

    input_path = Path(args.input_parquet)
    if not input_path.exists():
        msg = f"Input file not found: {input_path}"
        raise FileNotFoundError(msg)

    generate(str(input_path), args.output)


if __name__ == "__main__":
    main()
