# Repository Overview

This document provides a comprehensive overview of the idc-index-data repository
architecture, data flow, and key components to help contributors understand the
codebase.

## Purpose

The `idc-index-data` package provides metadata indexes for the
[NCI Imaging Data Commons (IDC)](https://imaging.datacommons.cancer.gov). It
bundles pre-computed data indexes used by the
[idc-index](https://pypi.org/project/idc-index/) Python package to enable users
to query and download medical imaging data.

## What are "Indexes"?

Indexes are **metadata tables** queried from Google BigQuery that describe DICOM
imaging data available in the IDC. Each index:

- Extracts and transforms DICOM metadata from BigQuery public datasets
- Provides attributes at different hierarchical levels (collection, patient,
  study, series)
- Includes download information (AWS S3 bucket locations, URLs)
- Is distributed as Parquet files for efficient data science workflows

### Available Indexes

| Index                    | Description                                | Granularity                       | In Package |
| ------------------------ | ------------------------------------------ | --------------------------------- | :--------: |
| `idc_index`              | Main index with core DICOM series metadata | One row per series                |    Yes     |
| `collections_index`      | Collection-level metadata                  | One row per collection            |    Yes     |
| `prior_versions_index`   | Historical version tracking                | One row per prior version         |    Yes     |
| `analysis_results_index` | Analysis results metadata                  | One row per analysis result       |    Yes     |
| `clinical_index`         | Clinical/tabular data metadata             | One row per clinical table column |     No     |
| `sm_index`               | Slide Microscopy series metadata           | One row per SM series             |     No     |
| `sm_instance_index`      | Slide Microscopy instance metadata         | One row per SM instance           |     No     |
| `seg_index`              | Segmentation series metadata               | One row per SEG series            |     No     |

**In Package** indicates whether the Parquet data file is bundled in the PyPI
package. Indexes marked "No" have large Parquet files that are excluded to keep
the package size manageable. For all indexes, the schema JSON and SQL files are
always included in the package.

### Accessing Indexes Not in Package

Indexes excluded from the package can be obtained from GitHub release assets:

```
https://github.com/ImagingDataCommons/idc-index-data/releases/download/v{VERSION}/{index_name}.parquet
```

For example:

```
https://github.com/ImagingDataCommons/idc-index-data/releases/download/v23.2.0/sm_index.parquet
```

The `idc-index` package (the consumer of this data package) handles fetching
these files automatically when needed.

## Directory Structure

```
idc-index-data/
├── assets/                          # SQL source files for specialized indexes
│   ├── README.md
│   ├── clinical_index.sql
│   ├── seg_index.sql
│   ├── sm_index.sql
│   └── sm_instance_index.sql
├── scripts/
│   ├── python/
│   │   └── idc_index_data_manager.py    # Main data generation script
│   └── sql/
│       ├── idc_index.sql                # Main index SQL
│       ├── collections_index.sql
│       ├── analysis_results_index.sql
│       └── prior_versions_index.sql
├── src/idc_index_data/               # Python package source
│   ├── __init__.py                   # Index metadata registry
│   └── _version.py                   # Version from VCS
├── tests/                            # Test suite
├── docs/                             # Sphinx documentation
├── .github/workflows/                # GitHub Actions CI/CD
├── hatch_build.py                    # Custom build hook
├── noxfile.py                        # Task automation
└── pyproject.toml                    # Project configuration
```

## Key Components

### 1. SQL Query Files

SQL files define what data each index contains. They are located in two
directories:

- **`scripts/sql/`** - Core indexes (idc_index, collections, prior_versions,
  analysis_results)
- **`assets/`** - Specialized indexes (clinical, sm, sm_instance, seg)

Each SQL file uses a self-documenting comment format:

```sql
# table-description:
# Description of the entire table

SELECT
  # description:
  # Description of this column
  column_name,
```

### 2. Data Generation Script

**File:** `scripts/python/idc_index_data_manager.py`

The `IDCIndexDataManager` class orchestrates data generation:

1. **Discovers** SQL files from both `scripts/sql/` and `assets/` directories
2. **Parses** table and column descriptions from SQL comments
3. **Executes** queries against Google BigQuery
4. **Generates** output files:
   - `{index_name}.parquet` - Data in columnar format (zstd compressed)
   - `{index_name}_schema.json` - Schema with column descriptions
   - `{index_name}.sql` - Copy of the executed query

### 3. Build System

**File:** `hatch_build.py`

Custom Hatchling build hook that:

1. Removes large parquet files from package to reduce size (defined in
   `PARQUET_EXCLUDE_LIST`)
2. Triggers data generation when `GCP_PROJECT` environment variable is set
3. Registers generated files for inclusion in the wheel package

### 4. Package Interface

**File:** `src/idc_index_data/__init__.py`

Provides the `INDEX_METADATA` dictionary for programmatic access to all indexes:

```python
from idc_index_data import INDEX_METADATA

# Access index metadata
idc_index = INDEX_METADATA["idc_index"]
parquet_path = idc_index["parquet_filepath"]
schema = idc_index["schema"]
sql_query = idc_index["sql"]
```

The `_ALL_INDICES` list defines which indexes are registered.

## Data Flow

```
┌─────────────────────────────────────────────────────────────┐
│                    SQL Query Files                          │
│         (scripts/sql/*.sql and assets/*.sql)                │
└─────────────────────────────┬───────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│              idc_index_data_manager.py                      │
│  1. Parse SQL comments (table/column descriptions)          │
│  2. Execute queries against BigQuery                        │
│  3. Generate Parquet, Schema JSON, SQL files                │
└─────────────────────────────┬───────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                   Generated Files                           │
│  - {index_name}.parquet (data)                              │
│  - {index_name}_schema.json (metadata)                      │
│  - {index_name}.sql (query reference)                       │
└─────────────────────────────┬───────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                    hatch_build.py                           │
│  1. Prune large parquet files (PARQUET_EXCLUDE_LIST)        │
│  2. Register files for wheel packaging                      │
└─────────────────────────────┬───────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                   Python Package                            │
│  - Parquet files (small indexes only)                       │
│  - Schema JSON files (all indexes)                          │
│  - SQL files (all indexes)                                  │
└─────────────────────────────────────────────────────────────┘
```

## CI/CD Pipelines

### Continuous Integration (`ci.yml`)

- **pre-commit**: Runs formatting and linting checks
- **checks**: Runs pytest on Ubuntu, macOS, and Windows with Python 3.12

### Continuous Deployment (`cd.yml`)

1. **generate-indices**: Executes all SQL queries against BigQuery and generates
   all artifacts (parquet, schema JSON, SQL files). Uploads artifacts to GitHub
   Actions for use by subsequent jobs.
2. **dist**: Downloads artifacts from generate-indices job, moves files to
   `src/idc_index_data/`, builds wheel package (which triggers `hatch_build.py`
   to prune large parquet files), and validates wheel contents.
3. **attach-release-assets**: (Only on release) Downloads artifacts and attaches
   all generated files to the GitHub release as downloadable assets.
4. **publish**: (Only on release) Publishes the wheel to PyPI using OIDC token
   authentication.

### GitHub Release Assets

When a new version is released, the following files are attached to the GitHub
release:

| File Type | Files                           | Purpose                             |
| --------- | ------------------------------- | ----------------------------------- |
| Parquet   | `*.parquet` (all 8 indexes)     | Complete data files for all indexes |
| Schema    | `*_schema.json` (all 8 indexes) | Column metadata and descriptions    |
| SQL       | `*.sql` (all 8 indexes)         | Source queries for reproducibility  |

This allows users to:

- Download large indexes directly without installing via PyPI
- Access specific index files programmatically
- Verify how indexes were generated by examining the SQL

## Common Tasks

### Run Tests

```bash
nox -s tests
```

### Build Documentation

```bash
nox -s docs
```

### Generate Index Data Locally

```bash
export GCP_PROJECT=your-gcp-project-id
python scripts/python/idc_index_data_manager.py \
    --generate-parquet \
    --output-dir .
```

### Build Package

```bash
nox -s build
```

### Update to New IDC Version

```bash
nox -s bump
```

## Configuration Files Reference

| File                       | Purpose                                                     |
| -------------------------- | ----------------------------------------------------------- |
| `pyproject.toml`           | Package metadata, dependencies, build configuration         |
| `hatch_build.py`           | Custom build hook for data generation and file registration |
| `noxfile.py`               | Task automation (tests, docs, build, lint, bump)            |
| `.github/workflows/ci.yml` | Continuous integration pipeline                             |
| `.github/workflows/cd.yml` | Continuous deployment pipeline                              |

## Key Design Decisions

### SQL Comment Parsing

The system extracts documentation from SQL comments rather than maintaining
separate metadata files. This keeps documentation close to the query definition
and ensures they stay in sync.

### Parquet Exclusion

Large indexes are excluded from the PyPI package to keep it small (~15MB vs
~100MB+). The exclusion list is defined in `hatch_build.py`:

```{warning}
PyPI enforces a **100 MB maximum file size** for uploaded packages. Projects can request a size limit increase, but keeping packages small is preferred for faster installs and reduced storage costs. This is why large parquet files are excluded and distributed via GitHub releases instead.
```

```python
PARQUET_EXCLUDE_LIST = {
    "sm_index.parquet",
    "sm_instance_index.parquet",
    "clinical_index.parquet",
    "seg_index.parquet",
}
```

For excluded indexes:

- **Parquet files** are NOT in the PyPI package but ARE attached to GitHub
  releases
- **Schema JSON files** are included in the package (small, useful for column
  metadata)
- **SQL files** are included in the package (useful for reference)

The `INDEX_METADATA` dictionary will have `parquet_filepath: None` for excluded
indexes, signaling to consumers that the data must be fetched from GitHub
releases.

### Dynamic File Discovery

The build system dynamically discovers SQL files and generated artifacts rather
than maintaining explicit file lists. This makes adding new indexes
straightforward - just add the SQL file and register the index name.

## BigQuery Data Sources

All indexes query public BigQuery datasets:

- `bigquery-public-data.idc_current.dicom_metadata` - Current IDC DICOM metadata
- `bigquery-public-data.idc_current.dicom_all` - Current IDC DICOM data (all
  attributes)
- `bigquery-public-data.idc_v{N}.dicom_all` - Versioned IDC data (where N is
  version number)
- `bigquery-public-data.idc_v{N}_clinical.*` - Versioned clinical data tables

The `idc_current` views always point to the latest IDC release.
