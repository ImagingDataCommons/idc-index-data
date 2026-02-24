"""Check which IDC PatientIDs from GDC-related collections exist in the Genomic Data Commons.

PatientID is only unique within a collection/project, so the GDC lookup is
scoped by project.  IDC collection_id maps to GDC project.project_id with one
special case: all CPTAC sub-collections in IDC (cptac_brca, cptac_luad, ...)
fall under CPTAC-2 or CPTAC-3 in GDC, so those are queried together.
"""

from __future__ import annotations

import os
import time
from collections import defaultdict
from pathlib import Path

import duckdb
import pandas as pd
import requests
from google.cloud import bigquery

# NOTE: Requires Python 3.10+ (tested on Python 3.10+).

GDC_BASE = "https://api.gdc.cancer.gov/cases"
BATCH_SIZE = 500
SLEEP_BETWEEN_REQUESTS = 0.12
SQL_FILE = Path(__file__).parent / "idc_gdc_selection.sql"
OUTPUT_PARQUET = "idc_gdc_patient_check.parquet"

# IDC CPTAC sub-collections map to these GDC umbrella projects.
GDC_CPTAC_PROJECTS = ["CPTAC-2", "CPTAC-3"]


def idc_collection_to_gdc_projects(collection_id: str) -> list[str]:
    """Map an IDC collection_id to the corresponding GDC project_id(s).

    For most collections the mapping is simply upper-case + hyphens
    (e.g. tcga_brca -> TCGA-BRCA).  CPTAC is the exception: IDC has
    per-cancer collections but GDC groups them under CPTAC-2 / CPTAC-3.
    """
    if collection_id.startswith("cptac_"):
        return GDC_CPTAC_PROJECTS
    return [collection_id.replace("_", "-").upper()]


def run_bigquery(project_id: str | None = None) -> pd.DataFrame:
    """Execute the BigQuery SQL from idc_gdc_selection.sql and return a DataFrame.

    Returns a DataFrame with columns:
        collection_id, PatientID, StudyInstanceUID, StudyDate,
        StudyDescription, study_type
    """
    sql = SQL_FILE.read_text()
    print(f"Running BigQuery query from {SQL_FILE} ...")
    client = bigquery.Client(project=project_id)
    df = client.query(sql).result().to_dataframe()
    print(
        f"BigQuery returned {len(df)} study rows, "
        f"{df['PatientID'].nunique()} unique patients."
    )
    return df


def _gdc_post(filters: dict, fields: str, size: int, offset: int = 0) -> dict:
    """Send a POST request to the GDC cases endpoint and return the JSON data."""
    body = {
        "filters": filters,
        "fields": fields,
        "size": size,
        "from": offset,
    }
    resp = requests.post(
        GDC_BASE,
        json=body,
        headers={"Content-Type": "application/json", "Accept": "application/json"},
    )
    resp.raise_for_status()
    return resp.json().get("data", {})


def _extract_sid_to_uuid(hits: list[dict]) -> dict[str, str]:
    """Extract submitter_id -> case UUID mapping from GDC API hits."""
    mapping: dict[str, str] = {}
    for hit in hits:
        sid = hit.get("submitter_id")
        case_uuid = hit.get("id") or hit.get("case_id")
        if sid and case_uuid:
            mapping[sid] = case_uuid
    return mapping


def _fetch_remaining_pages(
    filters: dict, already_fetched: int, total: int
) -> dict[str, str]:
    """Page through remaining GDC results for a single batch filter.

    Returns submitter_id -> case UUID mapping.
    """
    found: dict[str, str] = {}
    page_size = 500
    offset = already_fetched

    while offset < total:
        try:
            data = _gdc_post(
                filters, "submitter_id,case_id", page_size, offset
            )
            hits = data.get("hits", [])
            if not hits:
                break
            found.update(_extract_sid_to_uuid(hits))
            offset += len(hits)
        except requests.RequestException as exc:
            print(f"    WARNING: GDC pagination error at offset {offset}: {exc}")
            break
        time.sleep(SLEEP_BETWEEN_REQUESTS)

    return found


def check_patients_in_gdc_by_project(
    project_ids: list[str],
    patient_ids: list[str],
    batch_size: int = BATCH_SIZE,
) -> dict[str, str]:
    """Query the GDC API for patients within specific project(s).

    Uses POST requests to avoid URL length limits with large batches.

    Args:
        project_ids: GDC project_id(s) to scope the search (e.g. ["TCGA-BRCA"]
                     or ["CPTAC-2", "CPTAC-3"]).
        patient_ids: PatientIDs (submitter_ids) to look up.
        batch_size:  Number of IDs per API request.

    Returns:
        Dict mapping submitter_id -> GDC case UUID for patients found in GDC.
    """
    found: dict[str, str] = {}
    total = len(patient_ids)
    num_batches = (total + batch_size - 1) // batch_size

    for i in range(num_batches):
        start = i * batch_size
        end = min(start + batch_size, total)
        batch = patient_ids[start:end]

        filters = {
            "op": "and",
            "content": [
                {
                    "op": "in",
                    "content": {
                        "field": "project.project_id",
                        "value": project_ids,
                    },
                },
                {
                    "op": "in",
                    "content": {
                        "field": "submitter_id",
                        "value": batch,
                    },
                },
            ],
        }

        print(
            f"    batch {i + 1}/{num_batches} "
            f"(IDs {start + 1}-{end} of {total}) ..."
        )

        try:
            data = _gdc_post(filters, "submitter_id,case_id", len(batch))
            hits = data.get("hits", [])
            found.update(_extract_sid_to_uuid(hits))

            pagination = data.get("pagination", {})
            api_total = pagination.get("total", 0)
            if api_total > len(hits):
                found.update(
                    _fetch_remaining_pages(
                        filters, already_fetched=len(hits), total=api_total
                    )
                )
        except requests.RequestException as exc:
            print(f"    WARNING: GDC API error for batch {i + 1}: {exc}")

        time.sleep(SLEEP_BETWEEN_REQUESTS)

    return found


def check_all_patients(
    studies_df: pd.DataFrame,
) -> dict[tuple[str, str], str]:
    """Check all patients against GDC, grouped by project.

    Returns a dict mapping (collection_id, PatientID) -> GDC case UUID.
    """
    # Group unique patients by the GDC project(s) they map to.
    # Key: tuple of GDC project IDs -> dict of collection_id -> list of PatientIDs.
    project_to_patients: dict[tuple[str, ...], dict[str, list[str]]] = defaultdict(
        lambda: defaultdict(list)
    )

    pairs = (
        studies_df[["collection_id", "PatientID"]]
        .dropna()
        .drop_duplicates()
    )
    for _, row in pairs.iterrows():
        cid = row["collection_id"]
        pid = row["PatientID"]
        gdc_projects = tuple(idc_collection_to_gdc_projects(cid))
        project_to_patients[gdc_projects][cid].append(pid)

    found_map: dict[tuple[str, str], str] = {}

    for gdc_projects, collections in sorted(project_to_patients.items()):
        # Collect all unique PatientIDs across sub-collections that share
        # the same GDC project(s) (relevant for CPTAC).
        all_pids: list[str] = sorted(
            {pid for pids in collections.values() for pid in pids}
        )
        print(
            f"  GDC project(s) {', '.join(gdc_projects)}: "
            f"{len(all_pids)} unique patients ..."
        )

        sid_to_uuid = check_patients_in_gdc_by_project(
            list(gdc_projects), all_pids
        )

        # Map found submitter_ids back to their (collection_id, PatientID) pairs.
        for cid, pids in collections.items():
            for pid in pids:
                if pid in sid_to_uuid:
                    found_map[(cid, pid)] = sid_to_uuid[pid]

    return found_map


def save_results(df: pd.DataFrame, output_path: str = OUTPUT_PARQUET) -> None:
    """Save the results DataFrame to Parquet using duckdb."""
    # Convert BigQuery db_dtypes (e.g. dbdate) to strings so duckdb can handle them.
    for col in df.columns:
        if df[col].dtype.name not in ("object", "bool", "int64", "float64"):
            df[col] = df[col].astype(str)
    con = duckdb.connect()
    try:
        con.register("result_df", df)
        con.execute(
            f"COPY (SELECT * FROM result_df) TO '{output_path}' "
            f"(FORMAT PARQUET, COMPRESSION ZSTD)"
        )
        print(f"Saved {len(df)} rows to {output_path}")
    finally:
        con.close()


def main() -> None:
    """Main entry point."""
    project_id = os.environ.get("GCP_PROJECT")

    studies_df = run_bigquery(project_id)

    if studies_df.empty:
        print("No studies returned from BigQuery. Exiting.")
        return

    n_patients = studies_df["PatientID"].nunique()
    print(f"Checking {n_patients} unique PatientIDs against GDC API "
          f"(scoped by project) ...")

    found_map = check_all_patients(studies_df)

    # Add gdc_case_id column (UUID) and in_gdc boolean.
    studies_df["gdc_case_id"] = [
        found_map.get((row["collection_id"], row["PatientID"]))
        for _, row in studies_df[["collection_id", "PatientID"]].iterrows()
    ]
    studies_df["in_gdc"] = studies_df["gdc_case_id"].notna()

    n_found = studies_df["in_gdc"].sum()
    n_total = len(studies_df)
    n_patients_found = len(found_map)
    print(f"\nSummary:")
    print(f"  Studies: {n_found}/{n_total} rows have PatientID in GDC")
    print(f"  Patients: {n_patients_found}/{n_patients} unique (collection, PatientID) pairs in GDC")

    save_results(studies_df)


if __name__ == "__main__":
    main()
