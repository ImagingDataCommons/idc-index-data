#!/usr/bin/env python3
"""
Script to compare two Parquet files.

This script loads two Parquet files and performs various comparison operations:
- Schema comparison (columns, data types)
- Shape comparison (rows, columns)
- Value comparison (differences in data)
- Summary statistics comparison

The script can join tables by a specified column (default: SeriesInstanceUID) to
compare matching rows, or sort and compare all rows if no join column is specified.

Usage:
    python compare_parquet.py file1.parquet file2.parquet
    python compare_parquet.py file1.parquet file2.parquet --detailed
    python compare_parquet.py file1.parquet file2.parquet --output report.txt
    python compare_parquet.py file1.parquet file2.parquet --join-column PatientID
    python compare_parquet.py file1.parquet file2.parquet --join-column none
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import polars as pl


def load_parquet_files(file1: str, file2: str) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Load two Parquet files."""
    print(f"Loading {file1}...")
    df1 = pl.read_parquet(file1)

    print(f"Loading {file2}...")
    df2 = pl.read_parquet(file2)

    return df1, df2


def compare_file_info(file1: str, file2: str, output_file=None):
    """Compare file sizes and metadata."""
    import os

    try:
        import pyarrow.parquet as pq
    except ImportError:
        print("⚠ pyarrow not available for detailed Parquet metadata")
        pq = None

    output = []
    output.append("\n" + "=" * 80)
    output.append("FILE INFORMATION")
    output.append("=" * 80)

    # Get file sizes
    size1 = os.path.getsize(file1)
    size2 = os.path.getsize(file2)

    def format_bytes(size):
        for unit in ["B", "KB", "MB", "GB"]:
            if size < 1024.0:
                return f"{size:.2f} {unit}"
            size /= 1024.0
        return f"{size:.2f} TB"

    output.append("\nFile sizes on disk:")
    output.append(f"  File 1: {format_bytes(size1)} ({size1:,} bytes)")
    output.append(f"  File 2: {format_bytes(size2)} ({size2:,} bytes)")

    if size1 != size2:
        diff = abs(size1 - size2)
        pct_diff = (diff / max(size1, size2)) * 100
        larger = "File 1" if size1 > size2 else "File 2"
        output.append(
            f"  Difference: {format_bytes(diff)} ({pct_diff:.2f}%) - {larger} is larger"
        )
    else:
        output.append("  ✓ File sizes are identical")

    # Get Parquet metadata if available
    if pq:
        try:
            output.append("\nParquet metadata:")

            # File 1 metadata
            parquet_file1 = pq.ParquetFile(file1)
            output.append("\n  File 1:")
            output.append(f"    Row groups: {parquet_file1.num_row_groups}")
            output.append(f"    Total rows: {parquet_file1.metadata.num_rows:,}")
            output.append(f"    Columns: {parquet_file1.metadata.num_columns}")

            # Get compression info from first row group
            if parquet_file1.num_row_groups > 0:
                rg = parquet_file1.metadata.row_group(0)
                compressions = set()
                for i in range(rg.num_columns):
                    col = rg.column(i)
                    compressions.add(col.compression)
                output.append(f"    Compression: {', '.join(compressions)}")

            # File 2 metadata
            parquet_file2 = pq.ParquetFile(file2)
            output.append("\n  File 2:")
            output.append(f"    Row groups: {parquet_file2.num_row_groups}")
            output.append(f"    Total rows: {parquet_file2.metadata.num_rows:,}")
            output.append(f"    Columns: {parquet_file2.metadata.num_columns}")

            # Get compression info from first row group
            if parquet_file2.num_row_groups > 0:
                rg = parquet_file2.metadata.row_group(0)
                compressions = set()
                for i in range(rg.num_columns):
                    col = rg.column(i)
                    compressions.add(col.compression)
                output.append(f"    Compression: {', '.join(compressions)}")

            # Calculate compression ratios
            uncompressed_size1 = sum(
                parquet_file1.metadata.row_group(i).total_byte_size
                for i in range(parquet_file1.num_row_groups)
            )
            uncompressed_size2 = sum(
                parquet_file2.metadata.row_group(i).total_byte_size
                for i in range(parquet_file2.num_row_groups)
            )

            ratio1 = uncompressed_size1 / size1 if size1 > 0 else 0
            ratio2 = uncompressed_size2 / size2 if size2 > 0 else 0

            output.append("\n  Compression ratios:")
            output.append(
                f"    File 1: {ratio1:.2f}x (uncompressed: {format_bytes(uncompressed_size1)})"
            )
            output.append(
                f"    File 2: {ratio2:.2f}x (uncompressed: {format_bytes(uncompressed_size2)})"
            )

            if abs(ratio1 - ratio2) > 0.1:
                output.append(
                    "    ⚠ Different compression ratios may explain file size difference"
                )

        except Exception as e:
            output.append(f"\n  ⚠ Could not read Parquet metadata: {e}")

    result = "\n".join(output)
    print(result)

    if output_file:
        with open(output_file, "a") as f:
            f.write(result + "\n")


def compare_schemas(df1: pl.DataFrame, df2: pl.DataFrame, output_file=None) -> bool:
    """Compare schemas of two DataFrames."""
    schema1 = df1.schema
    schema2 = df2.schema

    output = []
    output.append("\n" + "=" * 80)
    output.append("SCHEMA COMPARISON")
    output.append("=" * 80)

    # Compare column names
    cols1 = set(schema1.keys())
    cols2 = set(schema2.keys())

    if cols1 == cols2:
        output.append("✓ Column names match")
    else:
        output.append("✗ Column names differ:")
        if cols1 - cols2:
            output.append(f"  Only in file 1: {sorted(cols1 - cols2)}")
        if cols2 - cols1:
            output.append(f"  Only in file 2: {sorted(cols2 - cols1)}")

    # Compare data types for common columns
    common_cols = cols1 & cols2
    type_differences = []

    for col in sorted(common_cols):
        type1 = schema1[col]
        type2 = schema2[col]
        if type1 != type2:
            type_differences.append(f"  {col}: {type1} vs {type2}")

    if type_differences:
        output.append("\n✗ Data type differences:")
        output.extend(type_differences)
    else:
        output.append("✓ Data types match for all common columns")

    result = "\n".join(output)
    print(result)

    if output_file:
        with open(output_file, "a") as f:
            f.write(result + "\n")

    return cols1 == cols2 and not type_differences


def compare_shapes(df1: pl.DataFrame, df2: pl.DataFrame, output_file=None) -> bool:
    """Compare shapes (dimensions) of two DataFrames."""
    output = []
    output.append("\n" + "=" * 80)
    output.append("SHAPE COMPARISON")
    output.append("=" * 80)

    shape1 = df1.shape
    shape2 = df2.shape

    output.append(f"File 1: {shape1[0]:,} rows × {shape1[1]} columns")
    output.append(f"File 2: {shape2[0]:,} rows × {shape2[1]} columns")

    if shape1 == shape2:
        output.append("✓ Shapes match")
        match = True
    else:
        output.append("✗ Shapes differ:")
        if shape1[0] != shape2[0]:
            output.append(f"  Row difference: {abs(shape1[0] - shape2[0]):,} rows")
        if shape1[1] != shape2[1]:
            output.append(f"  Column difference: {abs(shape1[1] - shape2[1])} columns")
        match = False

    result = "\n".join(output)
    print(result)

    if output_file:
        with open(output_file, "a") as f:
            f.write(result + "\n")

    return match


def compare_values(
    df1: pl.DataFrame,
    df2: pl.DataFrame,
    join_column: str = None,
    detailed: bool = False,
    output_file=None,
):
    """Compare values in two DataFrames, optionally joining by a key column."""
    output = []
    output.append("\n" + "=" * 80)
    output.append("VALUE COMPARISON")
    output.append("=" * 80)

    # Get common columns
    common_cols = set(df1.columns) & set(df2.columns)

    if not common_cols:
        output.append("⚠ No common columns to compare")
        result = "\n".join(output)
        print(result)
        if output_file:
            with open(output_file, "a") as f:
                f.write(result + "\n")
        return

    # If join_column is specified, perform an inner join
    if join_column:
        if join_column not in df1.columns:
            output.append(f"⚠ Error: Join column '{join_column}' not found in file 1")
            result = "\n".join(output)
            print(result)
            if output_file:
                with open(output_file, "a") as f:
                    f.write(result + "\n")
            return

        if join_column not in df2.columns:
            output.append(f"⚠ Error: Join column '{join_column}' not found in file 2")
            result = "\n".join(output)
            print(result)
            if output_file:
                with open(output_file, "a") as f:
                    f.write(result + "\n")
            return

        output.append(f"ℹ Joining DataFrames on column: '{join_column}'")

        # Perform inner join
        try:
            # Rename columns to distinguish between the two files
            df1_renamed = df1.select(
                [pl.col(join_column)]
                + [
                    pl.col(c).alias(f"{c}_file1") if c != join_column else pl.col(c)
                    for c in df1.columns
                    if c != join_column
                ]
            )

            df2_renamed = df2.select(
                [pl.col(join_column)]
                + [
                    pl.col(c).alias(f"{c}_file2") if c != join_column else pl.col(c)
                    for c in df2.columns
                    if c != join_column
                ]
            )

            # Join on the key column
            joined = df1_renamed.join(df2_renamed, on=join_column, how="inner")

            output.append(f"  Rows in file 1: {df1.height:,}")
            output.append(f"  Rows in file 2: {df2.height:,}")
            output.append(f"  Rows after inner join: {joined.height:,}")

            if joined.height == 0:
                output.append("⚠ No matching rows found after join")
                result = "\n".join(output)
                print(result)
                if output_file:
                    with open(output_file, "a") as f:
                        f.write(result + "\n")
                return

            # Compare columns from both files
            common_cols_without_key = common_cols - {join_column}

        except Exception as e:
            output.append(f"⚠ Error performing join: {e}")
            result = "\n".join(output)
            print(result)
            if output_file:
                with open(output_file, "a") as f:
                    f.write(result + "\n")
            return
    else:
        # No join - sort and compare as before
        if df1.shape != df2.shape:
            output.append("⚠ Cannot compare values: DataFrames have different shapes")
            output.append("  Consider using --join-column to compare matching rows")
            result = "\n".join(output)
            print(result)
            if output_file:
                with open(output_file, "a") as f:
                    f.write(result + "\n")
            return

        output.append(
            "ℹ Sorting both DataFrames by all common columns before comparison..."
        )
        common_cols_list = sorted(common_cols)
        try:
            df1_sorted = df1.select(common_cols_list).sort(common_cols_list)
            df2_sorted = df2.select(common_cols_list).sort(common_cols_list)
        except Exception as e:
            output.append(f"⚠ Warning: Could not sort DataFrames: {e}")
            output.append("  Proceeding with unsorted comparison...")
            df1_sorted = df1.select(common_cols_list)
            df2_sorted = df2.select(common_cols_list)

        joined = None
        common_cols_without_key = common_cols

    # Compare each common column
    differences = {}
    identical_cols = []
    diff_samples = {}  # Store sample differences for each column

    if join_column:
        # Compare joined data
        for col in sorted(common_cols_without_key):
            col1_name = f"{col}_file1"
            col2_name = f"{col}_file2"

            if col1_name not in joined.columns or col2_name not in joined.columns:
                continue

            try:
                # Compare columns
                comparison = joined.select(pl.col(col1_name)).equals(
                    joined.select(pl.col(col2_name))
                )

                if comparison:
                    identical_cols.append(col)
                else:
                    # Count and collect differences
                    diff_mask = joined[col1_name] != joined[col2_name]
                    diff_count = diff_mask.sum()
                    differences[col] = diff_count

                    # Collect sample differences (always collect 5 for summary)
                    diff_rows = (
                        joined.filter(diff_mask)
                        .select([join_column, col1_name, col2_name])
                        .head(5)
                    )

                    samples = []
                    for i in range(min(5, diff_rows.height)):
                        key_val = diff_rows[join_column][i]
                        val1 = diff_rows[col1_name][i]
                        val2 = diff_rows[col2_name][i]
                        samples.append((key_val, val1, val2))
                    diff_samples[col] = samples

            except Exception as e:
                output.append(f"⚠ Error comparing column '{col}': {e}")
    else:
        # Compare sorted data
        for col in common_cols_list:
            try:
                col1 = df1_sorted.select(pl.col(col))
                col2 = df2_sorted.select(pl.col(col))

                comparison = col1.equals(col2)

                if comparison:
                    identical_cols.append(col)
                else:
                    # Count and collect differences
                    diff_mask = df1_sorted[col] != df2_sorted[col]
                    diff_count = diff_mask.sum()
                    differences[col] = diff_count

                    # Collect sample differences (always collect 5 for summary)
                    diff_rows_f1 = df1_sorted.filter(diff_mask).select([col]).head(5)
                    diff_rows_f2 = df2_sorted.filter(diff_mask).select([col]).head(5)

                    samples = []
                    for i in range(min(5, diff_rows_f1.height)):
                        val1 = diff_rows_f1[col][i]
                        val2 = diff_rows_f2[col][i]
                        samples.append((i, val1, val2))
                    diff_samples[col] = samples

            except Exception as e:
                output.append(f"⚠ Error comparing column '{col}': {e}")

    # Report results
    comparison_note = " (after join)" if join_column else " (after sorting)"

    if identical_cols:
        output.append(
            f"\n✓ {len(identical_cols)} columns are identical{comparison_note}:"
        )
        if detailed:
            for col in identical_cols[:10]:
                output.append(f"  • {col}")
            if len(identical_cols) > 10:
                output.append(f"  ... and {len(identical_cols) - 10} more")

    if differences:
        output.append(
            f"\n✗ {len(differences)} columns have differences{comparison_note}:"
        )
        total_rows = joined.height if join_column else df1_sorted.height

        for col, count in sorted(differences.items(), key=lambda x: x[1], reverse=True):
            pct = (count / total_rows) * 100
            output.append(f"\n  • {col}: {count:,} differences ({pct:.2f}%)")

            # Always show 5 sample differences in summary
            if diff_samples.get(col):
                output.append("    Sample differences:")
                for sample in diff_samples[col]:
                    if join_column:
                        key_val, val1, val2 = sample
                        output.append(
                            f"      {join_column}={key_val}: {val1} vs {val2}"
                        )
                    else:
                        row_idx, val1, val2 = sample
                        output.append(f"      Row {row_idx}: {val1} vs {val2}")
    else:
        output.append(f"\n✓ All common columns are identical{comparison_note}!")

    result = "\n".join(output)
    print(result)

    if output_file:
        with open(output_file, "a") as f:
            f.write(result + "\n")


def compare_column_sizes(df1: pl.DataFrame, df2: pl.DataFrame, output_file=None):
    """Calculate and compare the total size of each column in bytes."""
    output = []
    output.append("\n" + "=" * 80)
    output.append("COLUMN SIZE COMPARISON")
    output.append("=" * 80)

    # Get all columns from both files
    all_cols = set(df1.columns) | set(df2.columns)
    common_cols = set(df1.columns) & set(df2.columns)
    unique_to_1 = set(df1.columns) - set(df2.columns)
    unique_to_2 = set(df2.columns) - set(df1.columns)

    # Calculate sizes for each column
    column_sizes_1 = {}
    column_sizes_2 = {}

    # Process columns in file 1
    for col in df1.columns:
        try:
            col_data_1 = df1.select(pl.col(col)).to_arrow()
            chunked_array_1 = col_data_1.column(0)

            size_1 = 0
            for chunk in chunked_array_1.chunks:
                for buf in chunk.buffers():
                    if buf is not None:
                        size_1 += buf.size

            column_sizes_1[col] = size_1
        except Exception as e:
            output.append(f"⚠ Error calculating size for column '{col}' in file 1: {e}")

    # Process columns in file 2
    for col in df2.columns:
        try:
            col_data_2 = df2.select(pl.col(col)).to_arrow()
            chunked_array_2 = col_data_2.column(0)

            size_2 = 0
            for chunk in chunked_array_2.chunks:
                for buf in chunk.buffers():
                    if buf is not None:
                        size_2 += buf.size

            column_sizes_2[col] = size_2
        except Exception as e:
            output.append(f"⚠ Error calculating size for column '{col}' in file 2: {e}")

    # Calculate total sizes
    total_size_1 = sum(column_sizes_1.values())
    total_size_2 = sum(column_sizes_2.values())
    total_common_1 = sum(column_sizes_1.get(col, 0) for col in common_cols)
    total_common_2 = sum(column_sizes_2.get(col, 0) for col in common_cols)
    total_unique_1 = sum(column_sizes_1.get(col, 0) for col in unique_to_1)
    total_unique_2 = sum(column_sizes_2.get(col, 0) for col in unique_to_2)

    # Helper function to format bytes
    def format_bytes(size):
        for unit in ["B", "KB", "MB", "GB"]:
            if size < 1024.0:
                return f"{size:.2f} {unit}"
            size /= 1024.0
        return f"{size:.2f} TB"

    # Report overall sizes
    output.append("\nOverall data size breakdown:")
    output.append(
        f"  File 1 total: {format_bytes(total_size_1)} ({total_size_1:,} bytes)"
    )
    output.append(
        f"    - Common columns: {format_bytes(total_common_1)} ({len(common_cols)} columns)"
    )
    if unique_to_1:
        output.append(
            f"    - Unique columns: {format_bytes(total_unique_1)} ({len(unique_to_1)} columns)"
        )

    output.append(
        f"\n  File 2 total: {format_bytes(total_size_2)} ({total_size_2:,} bytes)"
    )
    output.append(
        f"    - Common columns: {format_bytes(total_common_2)} ({len(common_cols)} columns)"
    )
    if unique_to_2:
        output.append(
            f"    - Unique columns: {format_bytes(total_unique_2)} ({len(unique_to_2)} columns)"
        )

    if total_size_1 != total_size_2:
        diff = abs(total_size_1 - total_size_2)
        pct_diff = (diff / max(total_size_1, total_size_2)) * 100
        output.append(f"\n  Overall difference: {format_bytes(diff)} ({pct_diff:.2f}%)")
    else:
        output.append("\n  ✓ Total sizes are identical")

    # Report unique columns if any
    if unique_to_1:
        output.append(f"\n⚠ Columns only in File 1 ({len(unique_to_1)}):")
        for col in sorted(unique_to_1)[:10]:
            size = column_sizes_1.get(col, 0)
            output.append(f"  • {col}: {format_bytes(size)}")
        if len(unique_to_1) > 10:
            output.append(f"  ... and {len(unique_to_1) - 10} more")

    if unique_to_2:
        output.append(f"\n⚠ Columns only in File 2 ({len(unique_to_2)}):")
        for col in sorted(unique_to_2)[:10]:
            size = column_sizes_2.get(col, 0)
            output.append(f"  • {col}: {format_bytes(size)}")
        if len(unique_to_2) > 10:
            output.append(f"  ... and {len(unique_to_2) - 10} more")

    # Report per-column sizes for common columns
    if common_cols:
        output.append("\nPer-column size comparison (common columns only):")
        output.append(
            f"{'Column':<40} {'File 1':<20} {'File 2':<20} {'Difference':<20}"
        )
        output.append("-" * 100)

        # Sort by size difference (largest first)
        size_diffs = []
        for col in common_cols:
            if col in column_sizes_1 and col in column_sizes_2:
                size_1 = column_sizes_1[col]
                size_2 = column_sizes_2[col]
                diff = abs(size_1 - size_2)
                size_diffs.append((col, size_1, size_2, diff))

        size_diffs.sort(key=lambda x: x[3], reverse=True)

        # Show columns with differences first, then identical ones
        cols_with_diff = [x for x in size_diffs if x[3] > 0]
        cols_identical = [x for x in size_diffs if x[3] == 0]

        if cols_with_diff:
            output.append(f"\nColumns with size differences ({len(cols_with_diff)}):")
            for col, size_1, size_2, diff in cols_with_diff[:20]:  # Show top 20
                pct_diff = (
                    (diff / max(size_1, size_2)) * 100 if max(size_1, size_2) > 0 else 0
                )
                output.append(
                    f"  {col:<38} {format_bytes(size_1):<18} {format_bytes(size_2):<18} {format_bytes(diff):<18} ({pct_diff:.1f}%)"
                )

            if len(cols_with_diff) > 20:
                output.append(
                    f"  ... and {len(cols_with_diff) - 20} more columns with differences"
                )

        if cols_identical:
            output.append(f"\n✓ {len(cols_identical)} columns have identical sizes")

    result = "\n".join(output)
    print(result)

    if output_file:
        with open(output_file, "a") as f:
            f.write(result + "\n")


def compare_statistics(df1: pl.DataFrame, df2: pl.DataFrame, output_file=None):
    """Compare summary statistics of numeric columns."""
    output = []
    output.append("\n" + "=" * 80)
    output.append("STATISTICS COMPARISON")
    output.append("=" * 80)

    # Get numeric columns
    numeric_cols1 = [
        col
        for col, dtype in df1.schema.items()
        if dtype
        in [
            pl.Int8,
            pl.Int16,
            pl.Int32,
            pl.Int64,
            pl.UInt8,
            pl.UInt16,
            pl.UInt32,
            pl.UInt64,
            pl.Float32,
            pl.Float64,
        ]
    ]

    numeric_cols2 = [
        col
        for col, dtype in df2.schema.items()
        if dtype
        in [
            pl.Int8,
            pl.Int16,
            pl.Int32,
            pl.Int64,
            pl.UInt8,
            pl.UInt16,
            pl.UInt32,
            pl.UInt64,
            pl.Float32,
            pl.Float64,
        ]
    ]

    common_numeric = set(numeric_cols1) & set(numeric_cols2)

    if not common_numeric:
        output.append("⚠ No common numeric columns found")
        result = "\n".join(output)
        print(result)
        if output_file:
            with open(output_file, "a") as f:
                f.write(result + "\n")
        return

    output.append(
        f"\nComparing statistics for {len(common_numeric)} numeric columns:\n"
    )

    for col in sorted(common_numeric):
        stats1 = df1.select(
            [
                pl.col(col).mean().alias("mean"),
                pl.col(col).median().alias("median"),
                pl.col(col).std().alias("std"),
                pl.col(col).min().alias("min"),
                pl.col(col).max().alias("max"),
            ]
        ).to_dicts()[0]

        stats2 = df2.select(
            [
                pl.col(col).mean().alias("mean"),
                pl.col(col).median().alias("median"),
                pl.col(col).std().alias("std"),
                pl.col(col).min().alias("min"),
                pl.col(col).max().alias("max"),
            ]
        ).to_dicts()[0]

        output.append(f"Column: {col}")
        output.append(f"  Mean:   {stats1['mean']:.4f} vs {stats2['mean']:.4f}")
        output.append(f"  Median: {stats1['median']:.4f} vs {stats2['median']:.4f}")
        output.append(f"  Std:    {stats1['std']:.4f} vs {stats2['std']:.4f}")
        output.append(f"  Min:    {stats1['min']:.4f} vs {stats2['min']:.4f}")
        output.append(f"  Max:    {stats1['max']:.4f} vs {stats2['max']:.4f}")
        output.append("")

    result = "\n".join(output)
    print(result)

    if output_file:
        with open(output_file, "a") as f:
            f.write(result + "\n")


def main():
    parser = argparse.ArgumentParser(
        description="Compare two Parquet files",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s file1.parquet file2.parquet
  %(prog)s file1.parquet file2.parquet --detailed
  %(prog)s file1.parquet file2.parquet --output report.txt
  %(prog)s file1.parquet file2.parquet --join-column SeriesInstanceUID
  %(prog)s file1.parquet file2.parquet --join-column PatientID --detailed
        """,
    )

    parser.add_argument("file1", help="First Parquet file")
    parser.add_argument("file2", help="Second Parquet file")
    parser.add_argument(
        "-j",
        "--join-column",
        default="SeriesInstanceUID",
        help='Column to join on for comparison (default: SeriesInstanceUID). Use "none" to disable join.',
    )
    parser.add_argument(
        "-d",
        "--detailed",
        action="store_true",
        help="Show detailed comparison including sample differences",
    )
    parser.add_argument("-o", "--output", help="Output file for comparison report")
    parser.add_argument(
        "--no-stats", action="store_true", help="Skip statistics comparison"
    )

    args = parser.parse_args()

    # Handle "none" as a special value to disable join
    join_column = None if args.join_column.lower() == "none" else args.join_column

    # Validate files exist
    if not Path(args.file1).exists():
        print(f"Error: File not found: {args.file1}", file=sys.stderr)
        sys.exit(1)

    if not Path(args.file2).exists():
        print(f"Error: File not found: {args.file2}", file=sys.stderr)
        sys.exit(1)

    # Clear output file if it exists
    if args.output:
        with open(args.output, "w") as f:
            f.write(f"Comparison Report: {args.file1} vs {args.file2}\n")
            if join_column:
                f.write(f"Join Column: {join_column}\n")
            f.write("=" * 80 + "\n")

    # Load files
    try:
        df1, df2 = load_parquet_files(args.file1, args.file2)
    except Exception as e:
        print(f"Error loading files: {e}", file=sys.stderr)
        sys.exit(1)

    # Perform comparisons
    compare_file_info(args.file1, args.file2, args.output)
    compare_schemas(df1, df2, args.output)
    compare_shapes(df1, df2, args.output)
    compare_column_sizes(df1, df2, args.output)
    compare_values(df1, df2, join_column, args.detailed, args.output)

    if not args.no_stats:
        compare_statistics(df1, df2, args.output)

    if args.output:
        print(f"\n✓ Report saved to: {args.output}")


if __name__ == "__main__":
    main()
