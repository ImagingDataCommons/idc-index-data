"""Stage release artifacts for publication to the Hugging Face Hub.

Copies the Parquet indices and their schema sidecars from a release artifacts
directory into a payload directory, rewriting each Parquet file with bounded
row groups.

``add_parquet_provenance.py`` rewrites every artifact with
``pq.write_table(..., compression="zstd")`` and no ``row_group_size``, so each
file ends up as a single row group -- 292 MB uncompressed for ``idc_index``.
The Hub declines to serve such files directly and regenerates them instead,
because it streams a row group at a time and recommends 100-300 MB:
https://huggingface.co/docs/dataset-viewer/en/parquet

Repacking here rather than in ``add_parquet_provenance.py`` keeps the bytes
published to GitHub, GCS and PyPI (and their ``.sha256`` sidecars) untouched.
"""

from __future__ import annotations

import argparse
import json
import re
import shutil
import sys
from pathlib import Path

import pyarrow.parquet as pq

# Indices deliberately not published to the Hub.
#
# tcia_idc_subset is a strict column projection of idc_index: identical row
# count, and every one of its six columns is already in idc_index. On the Hub
# it would cost 45 MB per release and present a near-duplicate config with no
# schema sidecar to document it.
EXCLUDED_INDICES = frozenset({"tcia_idc_subset"})

# Refuse to publish unless these are present, so a partially failed
# generate-indices run cannot quietly drop indices from the Hub via
# `hf upload --delete`.
REQUIRED_INDICES = frozenset(
    {
        "idc_index",
        "collections_index",
        "analysis_results_index",
        "prior_versions_index",
        "clinical_index",
        "version_metadata_index",
    }
)
MIN_EXPECTED_INDICES = 15

# Uncompressed bytes per row group. Comfortably inside the Hub's 100-300 MB
# band, and small enough that the viewer's random access stays responsive.
TARGET_ROW_GROUP_BYTES = 64 * 1024 * 1024
MIN_ROW_GROUP_ROWS = 20_000
MAX_ROW_GROUP_ROWS = 500_000

# git describe --tags --long, e.g. "24.2.2-0-g92b56dd"
_DESCRIBE_RE = re.compile(r"^(?P<tag>.+)-(?P<distance>\d+)-g(?P<sha>[0-9a-f]+)$")


def read_embedded_version(parquet_path: Path) -> str | None:
    """Return the idc_index_data_version embedded in a Parquet file, if any."""
    metadata = pq.ParquetFile(parquet_path).schema_arrow.metadata or {}
    raw = metadata.get(b"idc_index_data_version")
    return raw.decode() if raw else None


def resolve_version(describe: str, *, allow_untagged: bool) -> str:
    """Turn a `git describe` string into the tag to publish under.

    Refuses anything that is not exactly a release tag, so a manual run from a
    development commit cannot be published as if it were a release.
    """
    match = _DESCRIBE_RE.match(describe)
    if match is None:
        # Already a plain tag (e.g. passed via --version).
        return describe

    tag, distance = match["tag"], int(match["distance"])
    if distance == 0:
        return tag

    if not allow_untagged:
        msg = (
            f"{describe!r} is {distance} commit(s) past tag {tag!r}; these"
            " artifacts do not correspond to a release. Re-run the workflow"
            " from a release tag, or pass --allow-untagged to publish anyway."
        )
        raise SystemExit(msg)

    print(f"warning: publishing untagged build {describe!r}", file=sys.stderr)
    return describe


def row_group_size(parquet_file: pq.ParquetFile) -> int:
    """Pick a row count that keeps each row group near TARGET_ROW_GROUP_BYTES."""
    metadata = parquet_file.metadata
    if metadata.num_rows == 0:
        return MAX_ROW_GROUP_ROWS

    uncompressed = sum(
        metadata.row_group(i).total_byte_size for i in range(metadata.num_row_groups)
    )
    bytes_per_row = max(uncompressed / metadata.num_rows, 1.0)
    rows = int(TARGET_ROW_GROUP_BYTES / bytes_per_row)
    return max(MIN_ROW_GROUP_ROWS, min(rows, MAX_ROW_GROUP_ROWS))


def repack(source: Path, destination: Path) -> int:
    """Rewrite a Parquet file with bounded row groups. Returns the group count."""
    parquet_file = pq.ParquetFile(source)
    table = parquet_file.read()
    pq.write_table(
        table,
        destination,
        compression="zstd",
        row_group_size=row_group_size(parquet_file),
    )
    return pq.ParquetFile(destination).metadata.num_row_groups


def stage(artifacts: Path, payload: Path) -> list[str]:
    """Copy included indices into the payload directory. Returns their names."""
    payload.mkdir(parents=True, exist_ok=True)

    names = sorted(
        path.stem
        for path in artifacts.glob("*.parquet")
        if path.stem not in EXCLUDED_INDICES
    )
    if not names:
        msg = f"No publishable parquet files found in {artifacts}"
        raise SystemExit(msg)

    missing = REQUIRED_INDICES - set(names)
    if missing:
        msg = (
            f"Required indices missing from {artifacts}: {', '.join(sorted(missing))}."
            " Refusing to publish a partial set, which would delete them on the Hub."
        )
        raise SystemExit(msg)

    if len(names) < MIN_EXPECTED_INDICES:
        msg = (
            f"Only {len(names)} indices found in {artifacts}, expected at least"
            f" {MIN_EXPECTED_INDICES}. Refusing to publish a partial set."
        )
        raise SystemExit(msg)

    for name in names:
        source = artifacts / f"{name}.parquet"
        groups = repack(source, payload / f"{name}.parquet")
        size_mb = (payload / f"{name}.parquet").stat().st_size / 1e6
        print(f"  {name:<28} {size_mb:7.2f} MB  {groups} row group(s)")

        schema = artifacts / f"{name}_schema.json"
        if schema.is_file():
            shutil.copy2(schema, payload / schema.name)
        else:
            print(f"  {'':<28} no schema sidecar for {name}")

    return names


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("artifacts", type=Path, help="Release artifacts directory")
    parser.add_argument(
        "-o",
        "--output-dir",
        type=Path,
        required=True,
        help="Directory to stage the Hub payload in",
    )
    parser.add_argument(
        "--version",
        default=None,
        help="Version to publish under; defaults to the version embedded in "
        "idc_index.parquet",
    )
    parser.add_argument(
        "--allow-untagged",
        action="store_true",
        help="Publish even if the artifacts are not from an exact release tag",
    )
    args = parser.parse_args()

    index_parquet = args.artifacts / "idc_index.parquet"
    if not index_parquet.is_file():
        msg = f"{index_parquet} not found"
        raise SystemExit(msg)

    describe = args.version or read_embedded_version(index_parquet)
    if not describe:
        msg = (
            f"No version given and none embedded in {index_parquet};"
            " pass --version explicitly."
        )
        raise SystemExit(msg)

    version = resolve_version(describe, allow_untagged=args.allow_untagged)

    print(f"Staging idc-index-data {version} for the Hugging Face Hub")
    names = stage(args.artifacts, args.output_dir)

    manifest = args.output_dir / "hf_payload.json"
    manifest.write_text(
        json.dumps({"version": version, "indices": names}, indent=2) + "\n"
    )

    total_mb = sum(p.stat().st_size for p in args.output_dir.glob("*.parquet")) / 1e6
    print(f"Staged {len(names)} indices ({total_mb:.1f} MB) in {args.output_dir}")
    print(f"version={version}")


if __name__ == "__main__":
    main()
