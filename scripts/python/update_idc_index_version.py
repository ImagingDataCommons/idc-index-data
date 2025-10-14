#!/usr/bin/env python3
"""
Command line executable allowing to update source files given a IDC index version.
"""

from __future__ import annotations

import argparse
import contextlib
import os
import re
import textwrap
from pathlib import Path

ROOT_DIR = Path(__file__).parent / "../.."


@contextlib.contextmanager
def _log(txt, verbose=True):
    if verbose:
        print(txt)  # noqa: T201
    yield
    if verbose:
        print(f"{txt} - done")  # noqa: T201


def _update_file(filepath, regex, replacement):
    rel_path = os.path.relpath(str(filepath), ROOT_DIR)
    msg = f"Updating {rel_path}"
    with _log(msg):
        pattern = re.compile(regex)
        with filepath.open() as doc_file:
            lines = doc_file.readlines()
            updated_content = []
            for line in lines:
                updated_content.append(re.sub(pattern, replacement, line))
        with filepath.open("w") as doc_file:
            doc_file.writelines(updated_content)


def update_pyproject_toml(idc_index_version):
    pattern = re.compile(r'^version = "[\w\.]+"$')
    replacement = f'version = "{idc_index_version}.0.0"'
    _update_file(ROOT_DIR / "pyproject.toml", pattern, replacement)


def update_sql_scripts(idc_index_version):
    pattern = re.compile(r"idc_v\d+")
    replacement = f"idc_v{idc_index_version}"
    _update_file(ROOT_DIR / "scripts/sql/idc_index.sql", pattern, replacement)


def update_tests(idc_index_version):
    pattern = re.compile(r"EXPECTED_IDC_INDEX_VERSION = \d+")
    replacement = f"EXPECTED_IDC_INDEX_VERSION = {idc_index_version}"
    _update_file(ROOT_DIR / "tests/test_package.py", pattern, replacement)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "idc_index_version",
        metavar="IDC_INDEX_VERSION",
        type=int,
        help="IDC index version of the form NN",
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Hide the output",
    )

    args = parser.parse_args()

    update_pyproject_toml(args.idc_index_version)
    update_sql_scripts(args.idc_index_version)
    update_tests(args.idc_index_version)

    if not args.quiet:
        msg = """\
            Complete! Now run:

            git switch -c update-to-idc-index-{release}
            git add -u pyproject.toml scripts/sql/idc_index.sql tests/test_package.py
            git commit -m "Update to IDC index {release}"
            gh pr create --fill --body "Created by update_idc_index_version.py"
            """
        print(textwrap.dedent(msg.format(release=args.idc_index_version)))  # noqa: T201


if __name__ == "__main__":
    main()
