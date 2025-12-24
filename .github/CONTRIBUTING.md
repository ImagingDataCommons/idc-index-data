See the [Scientific Python Developer Guide][spc-dev-intro] for a detailed
description of best practices for developing scientific packages.

[spc-dev-intro]: https://learn.scientific-python.org/development/

# Quick development

The fastest way to start with development is to use nox. If you don't have nox,
you can use `pipx run nox` to run it without installing, or `pipx install nox`.
If you don't have pipx (pip for applications), then you can install with
`pip install pipx` (the only case were installing an application with regular
pip is reasonable). If you use macOS, then pipx and nox are both in brew, use
`brew install pipx nox`.

To use, run `nox`. This will lint and test using every installed version of
Python on your system, skipping ones that are not installed. You can also run
specific jobs:

```console
$ nox -s lint  # Lint only
$ nox -s tests  # Python tests
$ nox -s docs -- --serve  # Build and serve the docs
$ nox -s build  # Make an SDist and wheel
```

Nox handles everything for you, including setting up an temporary virtual
environment for each run.

# Setting up a development environment manually

You can set up a development environment by running:

```bash
python3 -m venv .venv
source ./.venv/bin/activate
pip install -v -e .[dev]
```

If you have the
[Python Launcher for Unix](https://github.com/brettcannon/python-launcher), you
can instead do:

```bash
py -m venv .venv
py -m install -v -e .[dev]
```

# Post setup

You should prepare pre-commit, which will help you by checking that commits pass
required checks:

```bash
pip install pre-commit # or brew install pre-commit on macOS
pre-commit install # Will install a pre-commit hook into the git repo
```

You can also/alternatively run `pre-commit run` (changes only) or
`pre-commit run --all-files` to check even without installing the hook.

# Testing

Use pytest to run the unit checks:

```bash
pytest
```

# Coverage

Use pytest-cov to generate coverage reports:

```bash
pytest --cov=idc-index-data
```

# Building docs

You can build the docs using:

```bash
nox -s docs
```

You can see a preview with:

```bash
nox -s docs -- --serve
```

# Pre-commit

This project uses pre-commit for all style checking. While you can run it with
nox, this is such an important tool that it deserves to be installed on its own.
Install pre-commit and run:

```bash
pre-commit run -a
```

to check all files.

# Version Management

This project uses **hatch-vcs** for dynamic versioning based on git tags.

## Version Structure

- Package version follows semantic versioning: `MAJOR.MINOR.PATCH`
- `MAJOR` version = IDC dataset version (e.g., 23, 24)
- `MINOR.PATCH` = package updates while using the same IDC data

## Updating for New IDC Data Release

When IDC releases a new dataset version (e.g., v23 → v24):

```bash
export GCP_PROJECT=idc-external-025
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/keyfile.json
nox -s bump -- --commit
```

This will:

1. Update SQL scripts to query the new BigQuery dataset
2. Update test expectations
3. Create a new branch
4. Commit the changes
5. Create a git tag (e.g., `24.0.0`)
6. Print instructions for creating a PR

You can also specify a version explicitly:

```bash
nox -s bump -- --commit 24
```

Or run without `--commit` to see what would be updated:

```bash
nox -s bump -- 24
```

## Updating Package Only (Same IDC Data)

For bug fixes, documentation, or code improvements that don't change the IDC
data:

1. Make your changes and commit them normally
2. Get the current version: `git describe --tags --abbrev=0`
3. Tag with bumped patch version:
   ```bash
   git tag -a 23.0.4 -m "Fix bug XYZ"
   ```
4. Push the tag:
   ```bash
   git push origin 23.0.4
   ```

## How Versioning Works

- Version is determined by git tags (not `pyproject.toml`)
- `hatch-vcs` reads the latest git tag to determine the version
- During build, `_version.py` is auto-generated (not tracked in git)
- `pyproject.toml` uses `dynamic = ["version"]` to defer to hatch-vcs

## Tagging a Release

You can print the instructions for tagging a release using:

```bash
nox -s tag_release
```

This will show guidance for both IDC data updates and package-only updates.
