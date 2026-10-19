"""Generate the Hugging Face dataset card for the idc-index-data Hub repo.

Renders ``README.md``: YAML front matter (one config per Parquet file) plus the
body sections. Counts, licenses and field tables are derived from the artifacts
themselves so the card cannot drift from the data it describes.

The wording is not here. It lives in ``card_template.md`` next to this file, as
plain Markdown with ``{{placeholder}}`` tokens where the generated values go, so
revising a sentence does not mean editing Python. This module computes the
values, renders the tables, and substitutes them into that template.

Rendering reads a ``CardFacts``, not a directory, because the card is published
on two different schedules. A release publishes it from the payload directory
staged by ``prepare_hf_payload.py``; between releases, ``refresh_dataset_card``
rebuilds the same facts from the Parquet files already on the Hub, so prose can
be revised without re-running the index build.

The YAML front matter stays in Python rather than moving into the template. It
carries no prose to review -- a license list and a config list, both derived
from the artifacts -- and a template holding a partial YAML block would not
parse as the front matter it becomes.
"""

from __future__ import annotations

import argparse
import dataclasses
import json
import re
from collections.abc import Iterable
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

TEMPLATE = Path(__file__).parent / "card_template.md"

PRETTY_NAME = "NCI Imaging Data Commons (IDC) index"
HUB_REPO = "ImagingDataCommons/idc-index-data"
HUB_URL = f"https://huggingface.co/datasets/{HUB_REPO}"
GITHUB_REPO = "https://github.com/ImagingDataCommons/idc-index-data"
GCS_MIRROR = "https://storage.googleapis.com/idc-index-data-artifacts"
# imaging.datacommons.cancer.gov 301s here; link the destination directly.
IDC_PORTAL = "https://portal.imaging.datacommons.cancer.gov"
# Documentation, not the viewer itself: the viewer's root URL is an empty shell,
# because a viewer URL is only meaningful with a study in it. Per-series links
# come from IDCClient.get_viewer_URL(), which picks OHIF or slim by modality and
# keeps the URL shapes owned upstream.
IDC_VISUALIZATION = "https://learn.canceridc.dev/portal/visualization"
# The two open-source viewers IDC serves. Slim lives in the IDC org itself:
# MGHComputationalPathology/slim 301s here and resolves to the same repo id, so
# this is its home rather than a fork, whatever older documentation says.
OHIF_REPO = "https://github.com/OHIF/Viewers"
SLIM_REPO = "https://github.com/ImagingDataCommons/slim"
# Index page for IDC's MCP server, agent skill and REST API. Link the index
# rather than the server URL: the hosted server is in beta and its endpoint may
# move, while this page is where IDC documents whatever the current one is.
IDC_AGENTS = "https://learn.canceridc.dev/ai-assistants/agents"

CARD_TAGS = (
    "medical",
    "cancer",
    "dicom",
    "radiology",
    "pathology",
    "catalog",
    "imaging-data-commons",
)

# license_short_name as it appears in idc_index -> Hub license identifier.
# Licenses with no Hub identifier are described in prose instead of being
# declared as `other`, which would hide the dataset from the real filters.
LICENSE_IDS = {
    "CC BY 4.0": "cc-by-4.0",
    "CC BY 3.0": "cc-by-3.0",
    "CC BY-NC 4.0": "cc-by-nc-4.0",
    "CC BY-NC 3.0": "cc-by-nc-3.0",
}
LICENSE_ORDER = ("cc-by-4.0", "cc-by-3.0", "cc-by-nc-4.0", "cc-by-nc-3.0")

DEFAULT_CONFIG = "idc_index"

# The only columns of idc_index the card needs. SeriesInstanceUID is
# deliberately absent: the series count is the row count, and that column alone
# is 14.7 MB compressed against 11.6 MB for all five of these together. It cost
# nothing to read from a local payload, but refresh_dataset_card.py reads these
# over the network from the Hub, where it would more than double the transfer.
SUMMARY_COLUMNS = (
    "collection_id",
    "PatientID",
    "StudyInstanceUID",
    "license_short_name",
    "series_size_MB",
)

# A lowercase identifier only, so the pattern cannot match the braces in the
# card's BibTeX block, where `{{Protected Title}}` is idiomatic.
PLACEHOLDER_RE = re.compile(r"\{\{([a-z_][a-z0-9_]*)\}\}")

# Formatter directives belong to the template, not to the artifact rendered from
# it. The template needs one: prettier collapses `> [!NOTE]` onto the line below
# it, and the Hub only renders the alert when that marker is on a line of its
# own.
PRETTIER_IGNORE_RE = re.compile(r"^<!-- prettier-ignore -->\n", re.MULTILINE)


@dataclasses.dataclass(frozen=True)
class CardFacts:
    """Everything the card renders, detached from where it was read.

    ``rows`` and ``sizes`` are per config; ``sizes`` is bytes on disk (or on the
    Hub, which stores the same bytes). ``idc`` is the (IDC version, release
    date) pair from version_metadata_index, or None if that index is absent.
    """

    version: str
    names: list[str]
    schemas: dict[str, dict[str, Any]]
    rows: dict[str, int]
    sizes: dict[str, int]
    summary: dict[str, Any]
    idc: tuple[int, str] | None


def order_names(names: Iterable[str]) -> list[str]:
    """Config names, default first, then alphabetical for stable diffs."""
    ordered = sorted(names)
    if DEFAULT_CONFIG in ordered:
        ordered.remove(DEFAULT_CONFIG)
        ordered.insert(0, DEFAULT_CONFIG)
    return ordered


def summarize_index(table: pa.Table) -> dict[str, Any]:
    """Compute headline counts and the license breakdown from idc_index."""
    licenses = [
        (row["values"], row["counts"])
        for row in table.column("license_short_name").value_counts().to_pylist()
    ]
    licenses.sort(key=lambda item: item[1], reverse=True)

    return {
        "series": table.num_rows,
        "patients": pc.count_distinct(table.column("PatientID")).as_py(),
        "studies": pc.count_distinct(table.column("StudyInstanceUID")).as_py(),
        "collections": pc.count_distinct(table.column("collection_id")).as_py(),
        "size_tb": pc.sum(table.column("series_size_MB")).as_py() / 1e6,
        "licenses": licenses,
    }


def latest_idc_version(table: pa.Table) -> tuple[int, str] | None:
    """Return the latest (IDC version, release date) from version_metadata_index."""
    versions = table.column("idc_version").to_pylist()
    timestamps = table.column("version_timestamp").to_pylist()
    if not versions:
        return None

    latest = max(range(len(versions)), key=lambda i: versions[i])
    return versions[latest], timestamps[latest]


def facts_from_payload(payload: Path, version: str) -> CardFacts:
    """Gather the card's facts from a payload directory staged for upload."""
    names = order_names(path.stem for path in payload.glob("*.parquet"))

    schemas = {}
    for path in sorted(payload.glob("*_schema.json")):
        schemas[path.name.removesuffix("_schema.json")] = json.loads(path.read_text())

    versions_path = payload / "version_metadata_index.parquet"
    return CardFacts(
        version=version,
        names=names,
        schemas=schemas,
        rows={
            name: pq.ParquetFile(payload / f"{name}.parquet").metadata.num_rows
            for name in names
        },
        sizes={name: (payload / f"{name}.parquet").stat().st_size for name in names},
        summary=summarize_index(
            pq.read_table(payload / "idc_index.parquet", columns=list(SUMMARY_COLUMNS))
        ),
        idc=(
            latest_idc_version(pq.read_table(versions_path))
            if versions_path.is_file()
            else None
        ),
    )


def front_matter(names: list[str], summary: dict[str, Any]) -> str:
    """Render the YAML block. `license` is a list so each one gets a Hub filter."""
    present = {
        LICENSE_IDS[name] for name, _ in summary["licenses"] if name in LICENSE_IDS
    }
    licenses = [lid for lid in LICENSE_ORDER if lid in present]

    lines = [
        "---",
        f"pretty_name: {PRETTY_NAME}",
        "license:",
        *(f"  - {lid}" for lid in licenses),
        "language:",
        "  - en",
        "tags:",
        *(f"  - {tag}" for tag in CARD_TAGS),
        "size_categories:",
        "  - 1M<n<10M",
        "configs:",
    ]
    for name in names:
        lines.append(f"  - config_name: {name}")
        lines.append(f"    data_files: {name}.parquet")
        if name == DEFAULT_CONFIG:
            lines.append("    default: true")
    lines.append("---")
    return "\n".join(lines)


def describe(name: str, schemas: dict[str, dict[str, Any]]) -> str:
    """One-line description of an index, from its schema sidecar.

    Every published index currently has one. A new index whose SQL lacks a
    `# table-description:` comment shows an empty cell here; fix it upstream in
    the SQL rather than hard-coding the text, so the PyPI and GCS sidecars get
    it too.
    """
    schema = schemas.get(name)
    if schema and schema.get("table_description"):
        return " ".join(schema["table_description"].split())
    return ""


def indices_table(facts: CardFacts) -> str:
    """One row per config: rows, size on disk, and its sidecar's description."""
    lines = ["| Config | Rows | Size | Description |", "|---|---:|---:|---|"]
    for name in facts.names:
        size_mb = facts.sizes[name] / 1e6
        default = " (default)" if name == DEFAULT_CONFIG else ""
        lines.append(
            f"| `{name}`{default} | {facts.rows[name]:,} | {size_mb:.1f} MB |"
            f" {describe(name, facts.schemas)} |"
        )
    return "\n".join(lines)


def column_table(schema: dict[str, Any]) -> list[str]:
    """Render a schema's columns as a markdown table."""
    lines = ["| Column | Type | Description |", "|---|---|---|"]
    for column in schema.get("columns", []):
        description = " ".join((column.get("description") or "").split())
        lines.append(
            f"| `{column['name']}` | {column.get('type', '')} | {description} |"
        )
    return lines


def default_columns_table(schemas: dict[str, dict[str, Any]]) -> str:
    """Every column of the default config, which the card documents in full."""
    schema = schemas.get(DEFAULT_CONFIG)
    if schema is None:
        msg = (
            f"No {DEFAULT_CONFIG}_schema.json among the artifacts, so the card"
            f" would document none of {DEFAULT_CONFIG}'s columns. Refusing to"
            " publish a card that silently drops its field documentation."
        )
        raise SystemExit(msg)
    return "\n".join(column_table(schema))


def other_schemas_table(names: list[str], schemas: dict[str, dict[str, Any]]) -> str:
    """Point every non-default config at its sidecar instead of its columns.

    Spelling out every column of every index made the card three times longer
    than the part anyone reads, for tables most visitors never open. The
    sidecars ship next to the Parquet files and say the same thing.
    """
    lines = ["| Config | Columns | Schema |", "|---|---:|---|"]
    for name in names:
        if name == DEFAULT_CONFIG:
            continue
        schema = schemas.get(name)
        if schema is None:
            lines.append(f"| `{name}` | -- | no sidecar published |")
            continue
        count = len(schema.get("columns", []))
        sidecar = f"{name}_schema.json"
        lines.append(
            f"| `{name}` | {count} | [`{sidecar}`]({HUB_URL}/blob/main/{sidecar}) |"
        )
    return "\n".join(lines)


def license_table(summary: dict[str, Any]) -> str:
    """Series count per license, with whether commercial use is allowed."""
    lines = ["| License | Series | Commercial use |", "|---|---:|---|"]
    for name, count in summary["licenses"]:
        commercial = "not allowed" if "NC" in name else "allowed"
        if name not in LICENSE_IDS:
            commercial = "see terms"
        lines.append(f"| {name} | {count:,} | {commercial} |")
    return "\n".join(lines)


def card_values(facts: CardFacts) -> dict[str, str]:
    """Every value the template can substitute, keyed by placeholder name."""
    summary = facts.summary
    idc_label = (
        f"IDC v{facts.idc[0]} (released {facts.idc[1]})"
        if facts.idc
        else "the current IDC release"
    )

    return {
        # URLs the prose links to. They stay here rather than being inlined in
        # the template so the comments above them -- which record why this URL
        # and not the obvious one -- stay next to the value they explain.
        "pretty_name": PRETTY_NAME,
        "hub_repo": HUB_REPO,
        "hub_url": HUB_URL,
        "github_repo": GITHUB_REPO,
        "gcs_mirror": GCS_MIRROR,
        "idc_portal": IDC_PORTAL,
        "idc_visualization": IDC_VISUALIZATION,
        "idc_agents": IDC_AGENTS,
        "ohif_repo": OHIF_REPO,
        "slim_repo": SLIM_REPO,
        "default_config": DEFAULT_CONFIG,
        # Read from the artifacts.
        "version": facts.version,
        "idc_label": idc_label,
        "series": f"{summary['series']:,}",
        "studies": f"{summary['studies']:,}",
        "patients": f"{summary['patients']:,}",
        "collections": str(summary["collections"]),
        "size_tb": f"{summary['size_tb']:.1f}",
        "size_tb_whole": f"{summary['size_tb']:.0f}",
        # Generated tables.
        "indices_table": indices_table(facts),
        "default_columns_table": default_columns_table(facts.schemas),
        "other_schemas_table": other_schemas_table(facts.names, facts.schemas),
        "license_table": license_table(summary),
    }


def render(template: str, values: dict[str, str]) -> str:
    """Substitute every ``{{name}}`` token, refusing any mismatch either way.

    Strict in both directions on purpose. A placeholder only the template knows
    about would otherwise publish a literal ``{{patients}}`` to the Hub, and one
    only the generator knows about would drop a count from the card silently.
    Both are worse than a failed build.
    """
    wanted = set(PLACEHOLDER_RE.findall(template))
    supplied = set(values)

    if missing := wanted - supplied:
        msg = (
            "The template uses placeholders the generator does not supply:"
            f" {', '.join(sorted(missing))}"
        )
        raise SystemExit(msg)
    if unused := supplied - wanted:
        msg = (
            "The generator supplies placeholders the template does not use:"
            f" {', '.join(sorted(unused))}"
        )
        raise SystemExit(msg)

    # Replace via a function, not a string: a schema description could contain
    # a backslash, which re.sub would read as a group reference.
    card = PLACEHOLDER_RE.sub(lambda match: values[match.group(1)], template)
    return PRETTIER_IGNORE_RE.sub("", card)


def build_card(facts: CardFacts, template: str | None = None) -> str:
    """Render the full card. ``template`` defaults to ``card_template.md``."""
    if template is None:
        template = TEMPLATE.read_text()

    body = render(template, card_values(facts))
    return front_matter(facts.names, facts.summary) + "\n\n" + body.rstrip() + "\n"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("payload", type=Path, help="Staged payload directory")
    parser.add_argument("--version", default=None, help="Release tag being published")
    parser.add_argument("-o", "--output", type=Path, default=None)
    args = parser.parse_args()

    version = args.version
    manifest = args.payload / "hf_payload.json"
    if version is None and manifest.is_file():
        version = json.loads(manifest.read_text())["version"]
    if not version:
        msg = "No --version given and no hf_payload.json in the payload directory"
        raise SystemExit(msg)

    card = build_card(facts_from_payload(args.payload, version))
    output = args.output or args.payload / "README.md"
    output.write_text(card)
    print(f"Wrote {output} ({output.stat().st_size:,} bytes)")


if __name__ == "__main__":
    main()
