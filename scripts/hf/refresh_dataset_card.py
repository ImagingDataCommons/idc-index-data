"""Regenerate the Hub dataset card from the data already published there.

The card's prose changes on a different schedule from the data it describes.
Wording, links and guidance get revised whenever someone reads the page with
fresh eyes; the counts, schemas and file sizes only move when a new index build
is published. The release pipeline couples the two -- ``README.md`` is written
by the ``upload-to-hf`` job in ``cd.yml``, behind a BigQuery index build -- so
fixing a sentence would otherwise mean regenerating every index and rewriting
117 MB of Parquet into permanent Hub history.

This reads the facts back out of the published files instead: row counts from
the Parquet footers, file sizes from the Hub API, the headline counts and
license table from five columns of ``idc_index`` (~12 MB of a 73 MB file), and
the release version from the ``idc_index_data_version`` key embedded in it. It
renders the card with the current generator and uploads ``README.md`` alone.

Reading from the Hub rather than from a local artifacts directory is the point,
not a convenience. The card must describe the bytes the Hub is serving, and the
Hub is the only copy guaranteed to be those bytes: the GCS mirror's ``current/``
folder is refreshed on every GitHub release, while publishing here is opt-in per
release, so a card built from GCS could report counts for data that is not on
the Hub and cannot be loaded from it.

Reading a private repo, and pushing to a public one, both need a token. In CI
the Trusted Publisher exchange provides it; locally, use a fine-grained token
scoped to write on this one repo and delete it afterwards.
"""

from __future__ import annotations

import argparse
import difflib
import json
import sys
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq
from generate_dataset_card import (
    DEFAULT_CONFIG,
    HUB_REPO,
    SUMMARY_COLUMNS,
    CardFacts,
    build_card,
    latest_idc_version,
    order_names,
    summarize_index,
)
from huggingface_hub import HfApi, HfFileSystem, hf_hub_download
from huggingface_hub.errors import EntryNotFoundError
from prepare_hf_payload import resolve_version

# The card is only ever written to the default branch. Tags are immutable
# snapshots of a release, card included: a reader who pins `revision="24.2.2"`
# should get the card as it stood when 24.2.2 was published, not today's prose.
DEFAULT_REVISION = "main"

CARD = "README.md"
VERSION_KEY = b"idc_index_data_version"


def hub_facts(
    api: HfApi, fs: HfFileSystem, repo: str, revision: str, version: str | None
) -> CardFacts:
    """Gather the card's facts from the Parquet files published on the Hub."""
    info = api.repo_info(
        repo, repo_type="dataset", revision=revision, files_metadata=True
    )

    # Root only, mirroring the non-recursive glob on the payload directory:
    # configs are flat files at the repo root, and a nested Parquet file would
    # otherwise become a config named after its path.
    sizes = {
        sibling.rfilename.removesuffix(".parquet"): sibling.size
        for sibling in info.siblings
        if sibling.rfilename.endswith(".parquet") and "/" not in sibling.rfilename
    }
    if DEFAULT_CONFIG not in sizes:
        msg = (
            f"No {DEFAULT_CONFIG}.parquet at the root of {repo}@{revision};"
            f" found {', '.join(sorted(sizes)) or 'no Parquet files at all'}."
            " The card's headline counts come from it."
        )
        raise SystemExit(msg)
    missing = sorted(name for name, size in sizes.items() if size is None)
    if missing:
        msg = f"The Hub reported no size for: {', '.join(missing)}"
        raise SystemExit(msg)
    names = order_names(sizes)

    def open_parquet(name: str) -> Any:
        return fs.open(f"datasets/{repo}@{revision}/{name}.parquet", "rb")

    # One footer read per config: a couple of range requests each, no column
    # data. Cheap enough that there is no reason to cache it.
    rows = {}
    for name in names:
        with open_parquet(name) as handle:
            rows[name] = pq.ParquetFile(handle).metadata.num_rows

    with open_parquet(DEFAULT_CONFIG) as handle:
        parquet = pq.ParquetFile(handle)
        embedded = (parquet.schema_arrow.metadata or {}).get(VERSION_KEY)
        summary = summarize_index(parquet.read(columns=list(SUMMARY_COLUMNS)))

    if version is None:
        if embedded is None:
            msg = (
                f"No --version given and no {VERSION_KEY.decode()} embedded in"
                f" the published idc_index.parquet"
            )
            raise SystemExit(msg)
        # allow_untagged, unlike at publish time: whatever these artifacts are,
        # they are already published, and refusing to fix the prose over a
        # version string helps nobody. The tag cross-check below is the warning.
        version = resolve_version(embedded.decode(), allow_untagged=True)

    idc = None
    if "version_metadata_index" in names:
        with open_parquet("version_metadata_index") as handle:
            idc = latest_idc_version(pq.ParquetFile(handle).read())

    return CardFacts(
        version=version,
        names=names,
        schemas=load_hub_schemas(api, repo, revision, info.siblings),
        rows=rows,
        sizes=sizes,
        summary=summary,
        idc=idc,
    )


def load_hub_schemas(
    api: HfApi, repo: str, revision: str, siblings: list[Any]
) -> dict[str, dict[str, Any]]:
    """Download and parse every *_schema.json sidecar in the repo."""
    schemas = {}
    for sibling in sorted(siblings, key=lambda s: s.rfilename):
        if not sibling.rfilename.endswith("_schema.json"):
            continue
        if "/" in sibling.rfilename:  # root only, as with the Parquet files
            continue
        path = hf_hub_download(
            repo,
            sibling.rfilename,
            repo_type="dataset",
            revision=revision,
            token=api.token,
        )
        name = sibling.rfilename.removesuffix("_schema.json")
        schemas[name] = json.loads(Path(path).read_text())
    return schemas


def published_card(api: HfApi, repo: str, revision: str) -> str:
    """The card currently on the Hub, or "" if there is none."""
    try:
        path = hf_hub_download(
            repo,
            CARD,
            repo_type="dataset",
            revision=revision,
            token=api.token,
            # The card is the one file this tool overwrites; a stale cache hit
            # would report "no changes" for a card it has not actually seen.
            force_download=True,
        )
    except EntryNotFoundError:
        return ""
    return Path(path).read_text()


def warn_unless_tagged(api: HfApi, repo: str, version: str) -> None:
    """Warn if the version the card claims has no matching tag on the Hub."""
    tags = {ref.name for ref in api.list_repo_refs(repo, repo_type="dataset").tags}
    if version not in tags:
        print(
            f"warning: the card will claim version {version!r}, which is not a"
            f" tag on {repo}. Known tags: {', '.join(sorted(tags)) or 'none'}",
            file=sys.stderr,
        )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", default=HUB_REPO, help="Hub dataset repo")
    parser.add_argument(
        "--revision",
        default=DEFAULT_REVISION,
        help="Revision to read the facts from (default: %(default)s)",
    )
    parser.add_argument(
        "--version",
        default=None,
        help="Release tag the card should claim; default: read from the"
        " published artifacts",
    )
    parser.add_argument(
        "-o",
        "--output",
        type=Path,
        default=Path(CARD),
        help="Where to write the rendered card (default: %(default)s)",
    )
    parser.add_argument(
        "--push",
        action="store_true",
        help=f"Upload {CARD} to the Hub. Without this, the card is only"
        " rendered and diffed.",
    )
    parser.add_argument(
        "--commit-message",
        default="Refresh the dataset card",
        help="Commit message for the upload (default: %(default)s)",
    )
    args = parser.parse_args()

    if args.push and args.revision != DEFAULT_REVISION:
        msg = (
            f"Refusing to read {args.revision!r} and push to {DEFAULT_REVISION!r}."
            f" Re-run without --revision to refresh the card on"
            f" {DEFAULT_REVISION}."
        )
        raise SystemExit(msg)

    api = HfApi()
    fs = HfFileSystem(token=api.token)

    print(f"Reading {args.repo} at {args.revision}")
    facts = hub_facts(api, fs, args.repo, args.revision, args.version)
    print(f"  version {facts.version}, {len(facts.names)} configs,")
    print(f"  {facts.summary['series']:,} series, {facts.summary['size_tb']:.1f} TB")
    warn_unless_tagged(api, args.repo, facts.version)

    card = build_card(facts)
    args.output.write_text(card)
    print(f"Wrote {args.output} ({args.output.stat().st_size:,} bytes)")

    current = published_card(api, args.repo, args.revision)
    if current == card:
        print(f"{CARD} on the Hub is already up to date; nothing to push.")
        return

    diff = difflib.unified_diff(
        current.splitlines(keepends=True),
        card.splitlines(keepends=True),
        fromfile=f"{args.repo}/{CARD}",
        tofile=f"{args.output} (generated)",
    )
    print("".join(diff))

    if not args.push:
        print("Not pushing. Re-run with --push to upload this card.")
        return

    commit = api.upload_file(
        path_or_fileobj=card.encode(),
        path_in_repo=CARD,
        repo_id=args.repo,
        repo_type="dataset",
        revision=DEFAULT_REVISION,
        commit_message=args.commit_message,
    )
    print(f"Pushed: {commit.commit_url}")


if __name__ == "__main__":
    main()
