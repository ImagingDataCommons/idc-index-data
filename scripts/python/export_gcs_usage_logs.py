# /// script
# requires-python = ">=3.10"
# dependencies = ["duckdb", "google-cloud-storage"]
# ///
"""Export GCS usage logs of the artifacts bucket to parquet and summarize them.

Usage logs of gs://idc-index-data-artifacts are written hourly to
gs://idc-index-data-artifacts-logs (see docs/dev/repository-overview.md). This
script downloads the log files not yet cached locally, combines all of them into
a single parquet file (one row per request), and prints download statistics as
markdown tables.

The logs contain client IP addresses: keep the cache and the parquet file out
of the repository and do not publish them.

Run with application default credentials of an account that can read the log
bucket:

    uv run scripts/python/export_gcs_usage_logs.py --weeks 4
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

import duckdb
from google.cloud import storage
from google.cloud.storage import transfer_manager

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

LOG_BUCKET = "idc-index-data-artifacts-logs"
USAGE_PREFIX = "idc-index-data-artifacts_usage_"
DEFAULT_CACHE_DIR = Path.home() / ".cache" / "idc-index-data" / "gcs-usage-logs"

# Column layout of GCS usage logs:
# https://cloud.google.com/storage/docs/access-logs#format
USAGE_LOG_COLUMNS = {
    "time_micros": "BIGINT",
    "c_ip": "VARCHAR",
    "c_ip_type": "INTEGER",
    "c_ip_region": "VARCHAR",
    "cs_method": "VARCHAR",
    "cs_uri": "VARCHAR",
    "sc_status": "INTEGER",
    "cs_bytes": "BIGINT",
    "sc_bytes": "BIGINT",
    "time_taken_micros": "BIGINT",
    "cs_host": "VARCHAR",
    "cs_referer": "VARCHAR",
    "cs_user_agent": "VARCHAR",
    "s_request_id": "VARCHAR",
    "cs_operation": "VARCHAR",
    "cs_bucket": "VARCHAR",
    "cs_object": "VARCHAR",
}


def sync_logs(cache_dir: Path) -> None:
    """Download usage log files that are missing or incomplete in cache_dir."""
    cache_dir.mkdir(parents=True, exist_ok=True)
    bucket = storage.Client().bucket(LOG_BUCKET)
    blobs = bucket.list_blobs(prefix=USAGE_PREFIX)
    # Log objects are never rewritten, so a local file of the same size is current
    missing = [
        blob.name
        for blob in blobs
        if not (cache_dir / blob.name).exists()
        or (cache_dir / blob.name).stat().st_size != blob.size
    ]
    logger.info("Downloading %d new log files to %s", len(missing), cache_dir)
    transfer_manager.download_many_to_path(
        bucket,
        missing,
        destination_directory=str(cache_dir),
        worker_type=transfer_manager.THREAD,
        raise_exception=True,
    )


def build_table(con: duckdb.DuckDBPyConnection, cache_dir: Path) -> None:
    """Load all cached usage logs into the `usage` table, one row per request."""
    columns = ", ".join(
        f"'{name}': '{type_}'" for name, type_ in USAGE_LOG_COLUMNS.items()
    )
    con.execute(f"""
        CREATE TABLE usage AS
        SELECT
          make_timestamp(time_micros) AS request_time,
          *,
          -- `current` or a release version such as `23.5.0`
          split_part(cs_object, '/', 1) AS release,
          regexp_extract(cs_object, '[^/]+$') AS file_name,
          nullif(regexp_extract(cs_object, '([^/]+?)(_schema\\.json|\\.parquet|\\.sql)$', 1), '')
            AS index_name,
          -- first product token of the user agent, e.g. `Python-urllib` or `Mozilla`
          nullif(regexp_extract(cs_user_agent, '^[^/ ,;(]+'), '') AS client,
        FROM read_csv(
          '{cache_dir}/{USAGE_PREFIX}*',
          header = true,
          columns = {{{columns}}}
        )
        -- GCS may log a request more than once
        QUALIFY row_number() OVER (PARTITION BY s_request_id ORDER BY time_micros) = 1
        ORDER BY time_micros
    """)


def print_table(con: duckdb.DuckDBPyConnection, title: str, query: str) -> None:
    result = con.execute(query)
    names = [d[0] for d in result.description]
    print(f"### {title}\n")
    print("| " + " | ".join(names) + " |")
    print("|" + "|".join("---" for _ in names) + "|")
    for row in result.fetchall():
        print(
            "| "
            + " | ".join(
                "" if v is None else f"{v:,}" if isinstance(v, int) else str(v)
                for v in row
            )
            + " |"
        )
    print()


def print_summary(con: duckdb.DuckDBPyConnection, weeks: int) -> None:
    """Print download statistics for the last `weeks` weeks as markdown."""
    # Successful object reads; 206 is a ranged read, e.g. DuckDB or a browser
    # reading part of a parquet file, so request counts overstate downloads.
    con.execute(f"""
        CREATE VIEW downloads AS
        SELECT * FROM usage
        WHERE cs_method = 'GET' AND sc_status IN (200, 206) AND cs_object <> ''
          AND request_time >= now()::TIMESTAMP - INTERVAL {int(weeks)} WEEK
    """)
    first, last = con.execute(
        "SELECT min(request_time)::TIMESTAMP_S, max(request_time)::TIMESTAMP_S FROM downloads"
    ).fetchone()
    print(f"## Downloads from gs://idc-index-data-artifacts, {first} to {last} UTC\n")
    stats = """
        count(*) AS requests,
        count(DISTINCT c_ip) AS client_ips,
        round(sum(sc_bytes) / 1e9, 2) AS GB
    """
    print_table(
        con,
        "By week",
        f"SELECT date_trunc('week', request_time)::DATE AS week, {stats} FROM downloads GROUP BY ALL ORDER BY week",
    )
    print_table(
        con,
        "By index",
        f"SELECT coalesce(index_name, file_name) AS index_name, {stats} FROM downloads GROUP BY ALL ORDER BY requests DESC",
    )
    print_table(
        con,
        "By release path",
        f"SELECT release, {stats} FROM downloads GROUP BY ALL ORDER BY requests DESC",
    )
    print_table(
        con,
        "By client",
        f"SELECT client, {stats} FROM downloads GROUP BY ALL ORDER BY requests DESC",
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    parser.add_argument(
        "--cache-dir",
        type=Path,
        default=DEFAULT_CACHE_DIR,
        help=f"Directory for downloaded log files (default: {DEFAULT_CACHE_DIR})",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_CACHE_DIR.parent / "gcs_usage_logs.parquet",
        help="Parquet file with one row per logged request (default: %(default)s)",
    )
    parser.add_argument(
        "--no-sync",
        action="store_true",
        help="Use the cached log files without downloading new ones",
    )
    parser.add_argument(
        "--weeks",
        type=int,
        default=4,
        help="Number of most recent weeks to summarize (default: %(default)s)",
    )
    args = parser.parse_args()

    if not args.no_sync:
        sync_logs(args.cache_dir)

    con = duckdb.connect()
    build_table(con, args.cache_dir)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    con.execute(f"COPY usage TO '{args.output}' (FORMAT parquet)")
    rows = con.execute("SELECT count(*) FROM usage").fetchone()[0]
    logger.info("Wrote %d requests to %s\n", rows, args.output)

    print_summary(con, args.weeks)


if __name__ == "__main__":
    main()
