"""Generate the Hugging Face dataset card for the idc-index-data Hub repo.

Reads a payload directory staged by ``prepare_hf_payload.py`` and renders
``README.md``: YAML front matter (one config per Parquet file) plus the body
sections. Counts, licenses and field tables are derived from the artifacts
themselves so the card cannot drift from the data it describes.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import pyarrow.compute as pc
import pyarrow.parquet as pq

PRETTY_NAME = "NCI Imaging Data Commons (IDC) index"
HUB_REPO = "ImagingDataCommons/idc-index-data"
HUB_URL = f"https://huggingface.co/datasets/{HUB_REPO}"
GITHUB_REPO = "https://github.com/ImagingDataCommons/idc-index-data"
GCS_MIRROR = "https://storage.googleapis.com/idc-index-data-artifacts"
# imaging.datacommons.cancer.gov 301s here; link the destination directly.
IDC_PORTAL = "https://portal.imaging.datacommons.cancer.gov"
# Root only. Deep-link URLs are deliberately not hard-coded here, because the
# right viewer depends on the modality -- OHIF for radiology, slim for
# microscopy. The card points at IDCClient.get_viewer_URL(), which makes that
# choice and keeps the URL shapes owned upstream.
IDC_VIEWER = "https://viewer.imaging.datacommons.cancer.gov/"
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


def load_schemas(payload: Path) -> dict[str, dict[str, Any]]:
    """Load every *_schema.json sidecar, keyed by index name."""
    schemas = {}
    for path in sorted(payload.glob("*_schema.json")):
        name = path.name.removesuffix("_schema.json")
        schemas[name] = json.loads(path.read_text())
    return schemas


def index_names(payload: Path) -> list[str]:
    """Config names, default first, then alphabetical for stable diffs."""
    names = sorted(path.stem for path in payload.glob("*.parquet"))
    if DEFAULT_CONFIG in names:
        names.remove(DEFAULT_CONFIG)
        names.insert(0, DEFAULT_CONFIG)
    return names


def summarize_index(payload: Path) -> dict[str, Any]:
    """Compute headline counts and the license breakdown from idc_index."""
    table = pq.read_table(
        payload / "idc_index.parquet",
        columns=[
            "collection_id",
            "PatientID",
            "StudyInstanceUID",
            "SeriesInstanceUID",
            "license_short_name",
            "series_size_MB",
        ],
    )

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


def idc_version(payload: Path) -> tuple[int, str] | None:
    """Return the latest (IDC version, release date) from version_metadata_index."""
    path = payload / "version_metadata_index.parquet"
    if not path.is_file():
        return None

    table = pq.read_table(path)
    versions = table.column("idc_version").to_pylist()
    timestamps = table.column("version_timestamp").to_pylist()
    if not versions:
        return None

    latest = max(range(len(versions)), key=lambda i: versions[i])
    return versions[latest], timestamps[latest]


# Rendered markdown hides HTML comments, so this is invisible on the dataset
# page but sits at the top of the raw file -- in front of whoever is about to
# edit the card on the Hub, which the Versioning section alone cannot reach.
GENERATED_BANNER = f"""<!--
  GENERATED FILE -- DO NOT EDIT ON THE HUB.

  This card, YAML front matter included, is regenerated from the release
  artifacts and committed over whatever is here on every publish. Edits made
  through the Hub UI, and community pull requests merged into it, are reverted
  by the next publish without warning.

  Change the generator instead:
  {GITHUB_REPO}/blob/main/scripts/hf/generate_dataset_card.py
-->"""


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


def indices_section(
    payload: Path, names: list[str], schemas: dict[str, dict[str, Any]]
) -> str:
    lines = [
        "## Indices",
        "",
        (
            "Each index is a separate config (subset). Load one with the `name`"
            " argument of `load_dataset`, or select it from the dropdown in the"
            " dataset viewer."
        ),
        "",
        "| Config | Rows | Size | Description |",
        "|---|---:|---:|---|",
    ]
    for name in names:
        path = payload / f"{name}.parquet"
        rows = pq.ParquetFile(path).metadata.num_rows
        size_mb = path.stat().st_size / 1e6
        default = " (default)" if name == DEFAULT_CONFIG else ""
        lines.append(
            f"| `{name}`{default} | {rows:,} | {size_mb:.1f} MB |"
            f" {describe(name, schemas)} |"
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


def fields_section(names: list[str], schemas: dict[str, dict[str, Any]]) -> str:
    """Document the default config in full; point the rest at their sidecars.

    Spelling out every column of every index made the card three times longer
    than the part anyone reads, for tables most visitors never open. The
    sidecars ship next to the Parquet files and say the same thing.
    """
    lines = ["## Data fields", ""]

    default_schema = schemas.get(DEFAULT_CONFIG)
    if default_schema is not None:
        lines += [
            f"Columns of `{DEFAULT_CONFIG}`, the default config:",
            "",
            *column_table(default_schema),
            "",
        ]

    others = [name for name in names if name != DEFAULT_CONFIG]
    if not others:
        return "\n".join(lines).rstrip()

    lines += [
        (
            "Every other config is described by a `<config>_schema.json` sidecar"
            " in this repository, carrying the same table and column"
            " descriptions:"
        ),
        "",
        "| Config | Columns | Schema |",
        "|---|---:|---|",
    ]
    for name in others:
        schema = schemas.get(name)
        if schema is None:
            lines.append(f"| `{name}` | -- | no sidecar published |")
            continue
        count = len(schema.get("columns", []))
        sidecar = f"{name}_schema.json"
        lines.append(
            f"| `{name}` | {count} | [`{sidecar}`]({HUB_URL}/blob/main/{sidecar}) |"
        )

    lines += [
        "",
        "They are plain JSON, so you can read one without downloading the data:",
        "",
        "```python",
        "import json, urllib.request",
        "",
        f'url = "{HUB_URL}/resolve/main/seg_index_schema.json"',
        "schema = json.load(urllib.request.urlopen(url))",
        'print(schema["table_description"])',
        'for column in schema["columns"]:',
        '    print(column["name"], "--", column.get("description", ""))',
        "```",
    ]
    return "\n".join(lines).rstrip()


def licensing_section(summary: dict[str, Any]) -> str:
    lines = [
        "## Licensing",
        "",
        (
            "**The images are not covered by a single license.** Every row carries"
            " a `license_short_name` giving the license of that series; the YAML"
            " above lists all of them so the dataset appears under each one's Hub"
            " filter. Check it per series before redistributing or using data"
            " commercially."
        ),
        "",
        "| License | Series | Commercial use |",
        "|---|---:|---|",
    ]
    for name, count in summary["licenses"]:
        commercial = "not allowed" if "NC" in name else "allowed"
        if name not in LICENSE_IDS:
            commercial = "see terms"
        lines.append(f"| {name} | {count:,} | {commercial} |")

    lines += [
        "",
        (
            "Series under *National Library of Medicine Terms and Conditions* are"
            " governed by"
            " <https://www.nlm.nih.gov/databases/download/terms_and_conditions.html>."
        ),
        "",
        (
            "Every license IDC uses -- CC BY and CC BY-NC alike -- requires"
            " **attribution**. See [IDC licensing and"
            " attribution](https://learn.canceridc.dev/data/licensing)."
        ),
        "",
        (
            "The index files in this repository are a factual catalog of that"
            " content and are distributed under the license of the"
            f" [idc-index-data repository]({GITHUB_REPO}/blob/main/LICENSE). That"
            " license covers the tables only, never the referenced images."
        ),
    ]
    return "\n".join(lines)


def citation_section() -> str:
    return """## Attribution and citation

Attribution is required by every license in this catalog, and it is owed to the
**source dataset**, not to IDC. Each row's `source_DOI` identifies the dataset
the series came from; resolve it to a formatted citation with IDC's citations
API or `IDCClient.citations_from_selection()`.

Many IDC collections originate from [The Cancer Imaging Archive
(TCIA)](https://www.cancerimagingarchive.net/); IDC is a TCIA Data Analysis
Center. Those collections additionally carry TCIA's [data usage policies and
restrictions](https://www.cancerimagingarchive.net/data-usage-policies-and-restrictions/),
including obligations on downstream attribution.

Please also acknowledge IDC itself:

```bibtex
@article{fedorov2023idc,
  title   = {National Cancer Institute Imaging Data Commons: Toward Transparency,
             Reproducibility, and Scalability in Imaging Artificial Intelligence},
  author  = {Fedorov, Andrey and Longabaugh, William J. R. and Pot, David and
             Clunie, David A. and Pieper, Steven D. and Gibbs, David L. and
             Bridge, Christopher and Herrmann, Markus D. and Homeyer, Andr\\'e and
             Lewis, Rob and Aerts, Hugo J. W. L. and Krishnaswamy, Deepa and
             Thiriveedhi, Vamsi K. and Ciausu, Cosmin and Schacherer, David P. and
             Bontempi, Dennis and Pihl, Todd and Wagner, Ulrike and
             Farahani, Keyvan and Kim, Erika and Kikinis, Ron},
  journal = {RadioGraphics},
  volume  = {43},
  number  = {12},
  year    = {2023},
  doi     = {10.1148/rg.230180}
}
```"""


def build_card(payload: Path, version: str) -> str:
    schemas = load_schemas(payload)
    names = index_names(payload)
    summary = summarize_index(payload)
    idc = idc_version(payload)
    idc_label = (
        f"IDC v{idc[0]} (released {idc[1]})" if idc else "the current IDC release"
    )

    quickstart = f'''## Quickstart

```bash
pip install datasets idc-index
```

`datasets` reads this catalog;
[`idc-index`](https://pypi.org/project/idc-index/) is the client that downloads
the DICOM files it points at. Filter here, download there:

```python
from datasets import load_dataset

idx = load_dataset("{HUB_REPO}", "idc_index", split="train")
sel = idx.filter(
    lambda r: r["collection_id"] == "nsclc_radiomics" and r["Modality"] == "SEG"
)

from idc_index import IDCClient

client = IDCClient()
client.download_from_selection(
    seriesInstanceUID=sel["SeriesInstanceUID"], downloadDir="./idc_data"
)
```

Downloads come directly from IDC's public AWS and GCS buckets at no cost to you.
What lands on disk is DICOM; read it with [pydicom](https://pydicom.github.io/)
or [highdicom](https://highdicom.readthedocs.io/).

To look at a series before downloading it, get a viewer link for it. This picks
the right viewer for the modality -- OHIF for radiology, slim for microscopy:

```python
print(client.get_viewer_URL(seriesInstanceUID=sel["SeriesInstanceUID"][0]))
```

Query the catalog without downloading anything, using DuckDB:

```sql
SELECT collection_id, COUNT(*) AS series, SUM(series_size_MB) / 1e6 AS size_TB
FROM 'hf://datasets/{HUB_REPO}/idc_index.parquet'
GROUP BY 1 ORDER BY size_TB DESC LIMIT 10;
```

The same queries run in the **SQL Console** tab on this page, with no local setup.'''

    versioning = f"""## Versioning

Tags on this repo match the [idc-index-data releases]({GITHUB_REPO}/releases)
one for one, and `main` always holds the most recent published release. Pin a
version to keep results reproducible:

```python
load_dataset("{HUB_REPO}", "idc_index", revision="{version}")
```

This release, `{version}`, indexes {idc_label}. Not every idc-index-data release
is published here; tags on this repo are a subset of the GitHub releases.

This card is generated, not maintained here. Every publish regenerates
`README.md` -- YAML front matter and all -- from the release artifacts and
commits it over whatever the Hub currently holds. Edits made through the Hub UI
and community pull requests merged into this card are reverted by the next
publish, with no warning and no notification to whoever made them. The old text
survives only in this repo's commit history.

So please don't send card fixes as pull requests here; they will not last.
Open them against the generator,
[`scripts/hf/generate_dataset_card.py`]({GITHUB_REPO}/blob/main/scripts/hf/generate_dataset_card.py),
and they will appear at the next release. Nothing else on the Hub is affected:
discussions persist, and only the Parquet files, their `*_schema.json` sidecars
and this card are ever written or removed by the publishing job."""

    links = f"""## Links

- [IDC portal]({IDC_PORTAL}/explore/) -- browse the data and build cohorts interactively
- [IDC viewer]({IDC_VIEWER}) -- view images in the browser; get per-series links with `IDCClient.get_viewer_URL()`
- [IDC agent interfaces]({IDC_AGENTS}) -- search IDC, size a cohort and get a download command by asking: hosted MCP server, agent skill, or REST API
- [IDC documentation](https://learn.canceridc.dev/)
- [`idc-index` Python package](https://github.com/ImagingDataCommons/idc-index) -- the download client (`pip install idc-index`)
- [`idc-index-data` on GitHub]({GITHUB_REPO}) -- how these tables are built (SQL included)
- [GCS mirror of the release artifacts]({GCS_MIRROR}?prefix=current/release_artifacts/)
  -- fetch a single file directly, e.g.
  `{GCS_MIRROR}/current/release_artifacts/idc_index.parquet`
- [IDC user forum](https://discourse.canceridc.dev/)"""

    summary_text = f"""# {PRETTY_NAME}

**This dataset is a catalog. It contains metadata and cloud locations for every
DICOM series in the NCI Imaging Data Commons; it does not contain pixel data.**

[IDC]({IDC_PORTAL}) is an NCI Cancer
Research Data Commons repository of publicly available cancer imaging data,
co-located with analysis tools in the cloud. To explore it interactively
instead, use the [IDC portal]({IDC_PORTAL}/explore/).
To query IDC in plain language, point an AI assistant at its
[agent interfaces]({IDC_AGENTS}) --
a hosted MCP server, an agent skill, and a REST API over the same metadata.

This catalog describes {idc_label}:
**{summary["series"]:,} series** across {summary["studies"]:,} studies,
{summary["patients"]:,} patients and {summary["collections"]} collections,
totalling **{summary["size_tb"]:.1f} TB** of imaging data.

One row is one DICOM series, with its collection, patient, study and series
attributes, its license and source DOI, and the S3 URL to fetch it from. Use it
to find the data you want here, then download only that -- the alternative is
sifting through {summary["size_tb"]:.0f} TB.

These are the same Parquet files published with each
[idc-index-data release]({GITHUB_REPO}/releases), republished here for the
dataset viewer, the SQL Console, automatic
[Croissant](https://huggingface.co/docs/dataset-viewer/en/croissant) metadata,
and a citable, versioned record you can pin.

Every config has a single split named `train`, the Hub default, because many
downstream tools assume it exists. It carries no train/test meaning."""

    note = """> [!NOTE]
> `clinical_index` is a *dictionary* of the clinical tables and columns
> available per collection -- not the clinical data itself. The clinical tables
> are not among these artifacts; retrieve them with
> `IDCClient.get_clinical_table()`."""

    return (
        "\n\n".join(
            [
                front_matter(names, summary),
                GENERATED_BANNER,
                summary_text,
                quickstart,
                indices_section(payload, names, schemas),
                note,
                fields_section(names, schemas),
                licensing_section(summary),
                citation_section(),
                versioning,
                links,
            ]
        )
        + "\n"
    )


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

    card = build_card(args.payload, version)
    output = args.output or args.payload / "README.md"
    output.write_text(card)
    print(f"Wrote {output} ({len(card):,} bytes)")


if __name__ == "__main__":
    main()
