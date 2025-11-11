# GitHub Copilot Instructions for idc-index-data

## Project Overview

`idc-index-data` is a Python package that bundles the index data for the NCI
Imaging Data Commons (IDC). The package provides Parquet files containing
metadata about imaging data hosted by IDC, intended to be used by the
`idc-index` Python package.

## Technology Stack

- **Build System**: scikit-build-core with CMake
- **Package Manager**: pip
- **Python Versions**: 3.10, 3.11, 3.12
- **Testing**: pytest with pytest-cov
- **Task Runner**: nox
- **Linting**: ruff, pylint, mypy, pre-commit hooks
- **Documentation**: Sphinx with MyST parser and Furo theme
- **Data Processing**: pandas, pyarrow, Google Cloud BigQuery

## Development Workflow

### Setting Up Development Environment

```bash
python3 -m venv .venv
source ./.venv/bin/activate
pip install -v -e .[dev]
pre-commit install
```

### Common Commands

- **Run all checks**: `nox` (runs lint, pylint, and tests by default)
- **Lint code**: `nox -s lint`
- **Run pylint**: `nox -s pylint`
- **Run tests**: `nox -s tests`
- **Build docs**: `nox -s docs`
- **Serve docs**: `nox -s docs -- --serve`
- **Build package**: `nox -s build`
- **Update IDC index version**: `nox -s bump -- <version>` (or leave off version
  for latest)
- **Tag release**: `nox -s tag_release` (shows instructions)

### Pre-commit Checks

Always run pre-commit before committing:

```bash
pre-commit run --all-files
```

## Code Style and Conventions

### Python Code Style

- **Import Statement**: All files must include
  `from __future__ import annotations` at the top
- **Type Hints**: Use type hints throughout; strict type checking is enabled for
  `idc_index_data.*` modules
- **Linting**: Follow ruff and pylint rules configured in `pyproject.toml`
- **Formatting**: Code is formatted with ruff formatter
- **Line Length**: Not strictly enforced but keep reasonable
- **Docstrings**: Use when appropriate, especially for public APIs

### Key Ruff Rules

The project uses extensive ruff rules including:

- `B` - flake8-bugbear
- `I` - isort (import sorting)
- `ARG` - flake8-unused-arguments
- `UP` - pyupgrade
- `PTH` - flake8-use-pathlib (prefer pathlib over os.path)
- `NPY` - NumPy specific rules
- `PD` - pandas-vet

### Type Checking

- Python 3.8 minimum target
- Strict mypy checking for package code
- Use `typing.TYPE_CHECKING` for import cycles

## Project Structure

```
idc-index-data/
├── src/idc_index_data/     # Main package source
│   ├── __init__.py         # Package exports and file path lookups
│   └── _version.py         # Auto-generated version file
├── scripts/                # Management scripts
│   ├── python/             # Python scripts for index management
│   └── sql/                # SQL queries for BigQuery
├── tests/                  # Test files
│   └── test_package.py     # Package tests
├── docs/                   # Sphinx documentation
├── pyproject.toml          # Project configuration
├── noxfile.py              # Nox session definitions
└── CMakeLists.txt          # Build configuration
```

## Important Considerations

### Package Purpose

This package is a **data package** - it bundles index files (CSV and Parquet)
and provides file paths to locate them. It does not contain complex business
logic but rather serves as a data distribution mechanism.

### Version Management

- Version is defined in `pyproject.toml`
- Use `nox -s bump` to update to new IDC index versions
- The version should match the IDC release version
- Always update both index files and test expectations when bumping version

### Data Files

The package includes:

- `idc_index.csv.zip` - Compressed CSV index (optional)
- `idc_index.parquet` - Parquet format index
- `prior_versions_index.parquet` - Historical version index

### Google Cloud Integration

- Some operations require Google Cloud credentials
- BigQuery is used to fetch latest index data
- Scripts need `GCP_PROJECT` and `GOOGLE_APPLICATION_CREDENTIALS` environment
  variables

### Testing

- Tests verify package installation and file accessibility
- Coverage reporting is configured but codecov upload is currently disabled
- Tests should work across platforms (Linux, macOS, Windows)

## Release Process

1. Update index version: `nox -s bump -- --commit <version>`
2. Create PR: `gh pr create --fill`
3. After merge, tag release: follow instructions from `nox -s tag_release`
4. Push tag: `git push origin <version>`
5. GitHub Actions will automatically build and publish to PyPI

## CI/CD

- **Format check**: pre-commit hooks + pylint
- **Tests**: Run on Python 3.10 and 3.12 across Linux, macOS, and Windows
- **Publishing**: Automated through GitHub Actions on tagged releases

## Additional Resources

- [Contributing Guide](.github/CONTRIBUTING.md)
- [Scientific Python Developer Guide](https://learn.scientific-python.org/development/)
- [IDC Homepage](https://imaging.datacommons.cancer.gov)
- [IDC Discourse Forum](https://discourse.canceridc.dev/)

## When Making Changes

1. **Always** run tests before and after changes: `nox -s tests`
2. **Always** run linters: `nox -s lint`
3. **Never** commit without running pre-commit checks
4. **Prefer** pathlib over os.path for file operations
5. **Use** type hints for all new code
6. **Update** tests if changing package structure or exports
7. **Follow** existing patterns in the codebase
8. **Keep** changes minimal and focused
9. **Document** any new public APIs
10. **Test** across Python versions when changing core functionality
