# Publishing the Index to Hugging Face

This guide explains how the index artifacts are published as the Hugging Face
(HF) dataset repo
[`ImagingDataCommons/idc-index-data`](https://huggingface.co/datasets/ImagingDataCommons/idc-index-data),
and covers the one-time Hub setup that cannot be automated.

## Overview

The HF dataset is a **catalog, not a data mirror**: the same Parquet index files
we already publish to GitHub releases and GCS, republished so they are
discoverable, browsable in the Data Studio viewer, queryable from the SQL
Console, and citable at a pinned version. No pixel data is involved; users still
fetch DICOM with the `idc-index` package.

There are two publishing paths, because the data and the prose describing it
change on different schedules:

| Workflow                     | Publishes                             | When                              |
| ---------------------------- | ------------------------------------- | --------------------------------- |
| `cd.yml`, `upload-to-hf` job | Parquet, schema sidecars and the card | Opt-in, per release               |
| `hf-card.yml`                | The card alone                        | Whenever the card's prose changes |

The release path is the `upload-to-hf` job in `.github/workflows/cd.yml`:

```
generate-indices  ->  prepare_hf_payload.py  ->  generate_dataset_card.py  ->  hf upload  ->  hf repos tag create
```

| Step                                  | What it does                                                                        |
| ------------------------------------- | ----------------------------------------------------------------------------------- |
| `scripts/hf/prepare_hf_payload.py`    | Stages the publishable indexes, repacking each Parquet file with bounded row groups |
| `scripts/hf/generate_dataset_card.py` | Renders `README.md` (YAML front matter plus body) from the payload                  |
| `hf upload`                           | Pushes Parquet, schema JSON and the card, authenticating via a Trusted Publisher    |
| `hf repos tag create`                 | Tags the Hub repo with the release version                                          |

The card-only path is `scripts/hf/refresh_dataset_card.py`, covered under
[Refreshing the Card Between Releases](#refreshing-the-card-between-releases).

## Publishing Is Opt-In

The job is **not** wired to `release: published`. There are roughly 30
idc-index-data releases a year, most of them packaging-only patch releases, and
each publish rewrites every Parquet file (~120 MB) into permanent Hub history.

To publish, dispatch the CD workflow **from the release tag**:

```bash
gh workflow run cd.yml --ref <tag> -f publish_to_hf=true
```

Use `-f hf_version=<tag>` only to override the version label; by default it is
read from the `idc_index_data_version` metadata embedded in the artifacts.

```{important}
`workflow_dispatch` runs the workflow **as it exists at `--ref`**, not as it
exists on `main`. Tags cut before the `upload-to-hf` job was added have no such
job and no `publish_to_hf` input, so dispatching one fails outright with
`Unexpected inputs provided`. Those releases can only be published by hand --
see [Backfilling an Existing Release](#backfilling-an-existing-release).
```

`prepare_hf_payload.py` refuses to run if that version is not an exact release
tag (for example `24.2.2-9-gabc1234`), so a dispatch from `main` fails loudly
rather than publishing a development build as a release.

## What Gets Published

Flat files at the repo root, one config per Parquet file, `idc_index` as the
default. Every config has a single split named `train` -- the Hub default, which
many downstream tools assume exists. It carries no train/test meaning.

Excluded from the upload:

- **`*.sql` and `*.sha256`** -- useful on GitHub and GCS, noise in the Hub file
  browser. The version is embedded in the Parquet metadata anyway.
- **`tcia_idc_subset.parquet`** -- a strict column projection of `idc_index`
  (identical rows; all six of its columns are already there). It would add 45 MB
  per release and a confusing near-duplicate config.
- **`gdc_idc_mapping.parquet`** -- absent from `_ALL_INDICES` in
  `src/idc_index_data/__init__.py`, so it is not part of the package API, and it
  ships no schema sidecar. Its audience (joining IDC patients to GDC cases) is
  narrow enough that the GCS mirror covers it.
- **`prior_versions_index.parquet`** -- catalogs series that are _no longer_ in
  IDC, contradicting what the rest of the dataset claims to be: one row per
  series in the current release. It is also one of the five indexes the PyPI
  wheel ships, so every `idc-index` install already has it locally, and its
  sidecar carries no column descriptions (see below), so it would have been the
  one config the Hub could not document.

All three are listed in `EXCLUDED_INDICES` in `prepare_hf_payload.py`, leaving
16 configs.

```{note}
The specialized indexes are worth publishing even though users need
`idc-index` to download DICOM. The PyPI wheel ships only five of them --
`idc_index`, `prior_versions_index`, `collections_index`,
`analysis_results_index` and `version_metadata_index`. The other twelve are
looked up with `optional=True` and resolve to `None`; they are fetched on
demand from the GCS mirror. On the Hub they become directly queryable, which is
a bigger gain than for `idc_index`, the one file every install already has.
```

### Why the Parquet Files Are Repacked

`add_parquet_provenance.py` rewrites every artifact without a `row_group_size`,
so each file ends up as a **single row group** -- 292 MB uncompressed for
`idc_index`. The Hub streams a row group at a time, recommends 100-300 MB, and
[regenerates files whose row groups are too big](https://huggingface.co/docs/dataset-viewer/en/parquet)
instead of serving them directly.

`prepare_hf_payload.py` repacks to ~64 MB row groups. The table contents and
schema are unchanged, so only the HF copy differs byte-wise from the GitHub and
GCS artifacts and their `.sha256` sidecars stay valid.

```{note}
`idc_index` currently has 1,032,911 rows, just under pyarrow's default
`row_group_size` of 1,048,576. Once IDC crosses that, the release artifacts will
start producing two row groups on their own. Repacking is still needed.
```

### Guards Against Partial Publishes

`hf upload --delete` removes Hub files matching the pattern that are not in the
upload, which is how a dropped index stops lingering. That also means an
incomplete artifact set would silently delete indexes from the Hub, so
`prepare_hf_payload.py` refuses to stage unless every index in
`REQUIRED_INDICES` is present and at least `MIN_EXPECTED_INDICES` are found.

## The Dataset Card

`README.md` is **generated on every publish** and overwrites whatever is on the
Hub, so Hub UI edits and merged community PRs against the card are both lost.
Change `scripts/hf/generate_dataset_card.py` instead. The card says so twice: in
the Versioning section, and in a `GENERATED_BANNER` HTML comment at the top of
the file, which is invisible when rendered but sits in front of anyone opening
the Hub editor.

Counts, the license table and the per-config field tables are all derived from
the artifacts, so the card cannot drift from the data. Descriptions come from
the `table_description` and per-column `description` values in each
`*_schema.json`.

The card documents the columns of `idc_index` in full and lists every other
config with its column count and a link to its `*_schema.json`. Spelling out
every column table made the field section 63% of a 52 KB card, for tables most
visitors never open; the sidecars ship beside the Parquet files and say the same
thing.

Every published index has a `table_description` in its sidecar, so no
hand-written descriptions are carried here. A new index whose SQL lacks a
`# table-description:` comment renders an empty cell in the Indices table; fix
it in the SQL rather than in the generator, so the PyPI and GCS sidecars get the
text too. (`prior_versions_index.sql` is the known offender -- procedural SQL
commented with `--`, which the parser in `idc_index_data_manager.py` does not
recognise -- and it is excluded from the upload for other reasons anyway.)

Preview the card without publishing:

```bash
python scripts/hf/prepare_hf_payload.py release_artifacts -o hf_payload
python scripts/hf/generate_dataset_card.py hf_payload
```

You can load the result exactly as the Hub will, straight from the payload
directory:

```python
from datasets import load_dataset, get_dataset_config_names

get_dataset_config_names("hf_payload")  # 16 configs, idc_index first
load_dataset("hf_payload", split="train")  # the default config
load_dataset("hf_payload", "seg_index", split="train")
```

### Refreshing the Card Between Releases

Expect to republish the card more often than a release is tagged. Wording, links
and guidance get revised whenever someone reads the page with fresh eyes; the
counts and schemas only move when a new index build is published. Going through
`cd.yml` to fix a sentence would mean a BigQuery index build and 117 MB of
Parquet rewritten into permanent Hub history.

`scripts/hf/refresh_dataset_card.py` regenerates the card from the files
**already on the Hub** and uploads `README.md` alone: row counts from the
published Parquet footers, file sizes from the Hub API, the headline counts and
license table from five columns of `idc_index` (~12 MB of a 73 MB file), and the
version from the `idc_index_data_version` key embedded in it. That is why
`generate_dataset_card.py` renders from a `CardFacts` rather than from a
directory -- the payload directory and the Hub both produce one.

Reading from the Hub is deliberate, not a convenience: the card must describe
the bytes the Hub is serving, and that is the only copy guaranteed to be those
bytes. The GCS mirror's `current/` folder is refreshed on every GitHub release
while publishing here is opt-in per release, so a card built from GCS could
report counts for data that is not on the Hub and cannot be loaded from it.

Render and diff it without pushing:

```bash
python scripts/hf/refresh_dataset_card.py -o card.md
```

That prints the resolved version, the config count, and a unified diff against
the published card -- or `already up to date` when the two match. Add `--push`
to upload. Reading a private repo needs a token just as writing does; use a
fine-grained token scoped to write on this one repo and delete it afterwards.

Or dispatch the workflow, which needs no local token:

```bash
gh workflow run hf-card.yml -f push=true
```

Leave `push` off for a dry run: the diff lands in the run's step summary and the
rendered card is attached as a build artifact.

```{note}
The refresh writes to `main` only. Tags are immutable snapshots of a release,
card included, so `revision="24.2.2"` keeps serving the card as it stood when
24.2.2 was published. After a refresh, `main` and the most recent tag hold the
same Parquet files and different prose. That is what pinning a revision means,
not a drift to repair.
```

`--revision` reads the facts from somewhere other than `main`, to see what the
card would say at an older commit. Pushing is refused in that case, so a card
rendered from a tag cannot land on `main` by accident.

## One-Time Hub Setup

Needs the **Write** role in the `ImagingDataCommons` org.

1. Create the dataset repo `ImagingDataCommons/idc-index-data`. The Trusted
   Publisher token is scoped to an existing repo, so this must be done before
   the first CI publish.

   Creating it **private** first is worth doing: you can stage a full publish,
   inspect the rendered card and the file list, and flip it public only once it
   looks right. Note that the dataset viewer does not run on private datasets
   outside a PRO/Enterprise org, so the viewer, SQL Console and Croissant
   acceptance checks below can only be confirmed after it goes public.

2. In the repo's **Settings -> Trusted Publishers**, add:
   - Provider: **GitHub Actions**
   - `repository` = `ImagingDataCommons/idc-index-data`
   - `workflow` = `cd.yml`
   - **Do not set `branch`.** Claims are matched exactly, and publish runs
     execute on `refs/tags/<tag>`, so a branch pin makes the token exchange fail
     on exactly the runs that matter.

3. Add a **second** entry, identical except `workflow` = `hf-card.yml`, for the
   card-only refresh workflow. A repo holds a list of publishers and claims are
   matched exactly, so every workflow file that publishes needs its own entry.
   Without it, `hf auth token` in that workflow fails with `invalid_grant`.

No `HF_TOKEN` secret is stored. The `hf` CLI detects GitHub Actions, exchanges
the OIDC token for a one-hour, single-repo write token, and uses it; the job
only sets `HF_OIDC_RESOURCE`, with the `datasets/` prefix because this is not a
model repo. `cd.yml` lets `hf upload` do the exchange internally; `hf-card.yml`
runs `hf auth token` to get the same token explicitly, because the refresh
script reads the repo through the Python API before writing to it.

## Dry Run Before the First Real Publish

The cheapest dry run is a **manual publish into the still-private repo**, using
the commands under
[Backfilling an Existing Release](#backfilling-an-existing-release). It
exercises the real payload, the real card and the real repo, and nothing is
visible until you make the repo public.

Before uploading, confirm every link in the generated card resolves -- the card
points at GitHub paths that only exist once this code is merged:

```bash
python - <<'PY' payload/README.md
import re, sys, urllib.request
for url in sorted({u.rstrip('.,;`') for u in
                   re.findall(r'https?://[^\s)>"`]+', open(sys.argv[1]).read())}):
    req = urllib.request.Request(url, method="HEAD")
    try:
        code = urllib.request.urlopen(req, timeout=20).status
    except Exception as exc:  # noqa: BLE001
        code = getattr(exc, "code", exc.__class__.__name__)
    print(code, url)
PY
```

```{note}
Read the output rather than counting non-200s. A status code is not proof
either way here: the IDC viewer is a single-page app whose host serves the
working app shell with a **404** status for any client-routed path, and the Hub
returns **401** for schema links while the dataset repo is still private.
Confirm a suspicious URL in a browser before "fixing" it.
```

To exercise the **CI path** specifically -- the OIDC token exchange, which a
manual publish does not touch -- use a fork:

1. Fork idc-index-data. Do **not** test with a pre-release on the main repo;
   that would also publish to PyPI.
2. Create a scratch dataset repo under your personal HF account.
3. Add a Trusted Publisher on it pointing at the fork.
4. Edit `HF_REPO` and `HF_OIDC_RESOURCE` in the job to the scratch repo, and
   dispatch the workflow with `publish_to_hf=true`.

A claims mismatch surfaces as `invalid_grant` with a Request ID; include that
when reporting an exchange failure.

## Backfilling an Existing Release

A release tagged _after_ the `upload-to-hf` job landed can be backfilled by
dispatching from its tag, exactly as above.

Anything older -- 24.2.2 included -- must be published by hand, because the
workflow at that tag predates the job. Download that release's artifacts, then
authenticate with a personal fine-grained token scoped to write on this one
repo, and delete the token afterwards:

```bash
python scripts/hf/prepare_hf_payload.py release_artifacts -o hf_payload
python scripts/hf/generate_dataset_card.py hf_payload
hf upload ImagingDataCommons/idc-index-data hf_payload . --repo-type dataset \
  --include "*.parquet" --include "*_schema.json" --include "README.md" \
  --delete "*.parquet" --delete "*_schema.json" \
  --commit-message "idc-index-data 24.2.2"
hf repos tag create ImagingDataCommons/idc-index-data 24.2.2 --repo-type dataset
```

```{warning}
`--include`, `--exclude` and `--delete` take **one pattern per flag** and must be
repeated. The `hf` CLI is Typer-based; passing several patterns after one flag
silently reinterprets the extras as positional arguments. Some HF documentation
still shows the older argparse behaviour.
```

## Acceptance Checks

- [ ] Dataset page shows the Data Studio viewer with all configs, `idc_index`
      selected by default
- [ ] SQL Console runs
      `SELECT collection_id, COUNT(*) FROM idc_index GROUP BY 1`
- [ ] `load_dataset("ImagingDataCommons/idc-index-data", "seg_index", revision="<tag>")`
      works, and `main` resolves to the same content
- [ ] `curl https://huggingface.co/api/datasets/ImagingDataCommons/idc-index-data/croissant`
      returns JSON-LD with a `recordSet` per config
- [ ] `SELECT COUNT(*) FROM 'hf://datasets/ImagingDataCommons/idc-index-data/idc_index.parquet'`
      matches the release row count (1,032,911 for 24.2.2)
- [ ] The `/parquet` API reports `"partial": false` -- confirms the Hub is
      serving our files rather than regenerating them because of row group size
- [ ] Quickstart code in the card runs end to end and produces DICOM on disk
- [ ] Hub search for "imaging data commons" returns the dataset, and the
      `cc-by-4.0` license filter lists it
