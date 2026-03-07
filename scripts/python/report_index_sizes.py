"""Report sizes and row counts for all parquet files in a directory."""

from __future__ import annotations

import sys
from pathlib import Path

import duckdb


def human_size(n_bytes: int) -> str:
    for unit in ("B", "K", "M", "G"):
        if n_bytes < 1024:
            return f"{n_bytes:.0f}{unit}" if unit == "B" else f"{n_bytes:.1f}{unit}"
        n_bytes /= 1024
    return f"{n_bytes:.1f}T"


def main() -> None:
    directory = Path(sys.argv[1]) if len(sys.argv) > 1 else Path()
    files = sorted(directory.glob("*.parquet"))

    if not files:
        print(f"No parquet files found in {directory}", file=sys.stderr)
        sys.exit(1)

    print("## Generated Index Sizes")
    print()
    print("| Index | Rows | File Size |")
    print("|-------|-----:|----------:|")

    total_bytes = 0
    for f in files:
        size_bytes = f.stat().st_size
        total_bytes += size_bytes
        rows = duckdb.execute(f"SELECT COUNT(*) FROM read_parquet('{f}')").fetchone()[0]
        print(f"| {f.stem} | {rows:,} | {human_size(size_bytes)} |")

    print()
    print(f"**Total parquet size: {human_size(total_bytes)}**")


if __name__ == "__main__":
    main()
