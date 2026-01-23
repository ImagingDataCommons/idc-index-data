# Repository Overview

This document provides a comprehensive overview of the idc-index-data repository architecture, data flow, and key components to help contributors understand the codebase.

## Purpose

The `idc-index-data` package provides metadata indexes for the [NCI Imaging Data Commons (IDC)](https://imaging.datacommons.cancer.gov). It bundles pre-computed data indexes used by the [idc-index](https://pypi.org/project/idc-index/) Python package to enable users to query and download medical imaging data.

## What are "Indexes"?

Indexes are **metadata tables** queried from Google BigQuery that describe DICOM imaging data available in the IDC. Each index:

- Extracts and transforms DICOM metadata from BigQuery public datasets
- Provides attributes at different hierarchical levels (collection, patient, study, series)
- Includes download information (AWS S3 bucket locations, URLs)
- Is distributed as Parquet files for efficient data science workflows

### Available Indexes

| Index | Description | Granularity |
|-------|-------------|-------------|
| `idc_index` | Main index with core DICOM series metadata | One row per series |
| `collections_index` | Collection-level metadata | One row per collection |
| `prior_versions_index` | Historical version tracking | One row per prior version |
| `analysis_results_index` | Analysis results metadata | One row per analysis result |
| `clinical_index` | Clinical/tabular data metadata | One row per clinical table column |
| `sm_index` | Slide Microscopy series metadata | One row per SM series |
| `sm_instance_index` | Slide Microscopy instance metadata | One row per SM instance |
| `seg_index` | Segmentation series metadata | One row per SEG series |

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

SQL files define what data each index contains. They are located in two directories:

- **`scripts/sql/`** - Core indexes (idc_index, collections, prior_versions, analysis_results)
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

1. Removes large parquet files from package to reduce size (defined in `PARQUET_EXCLUDE_LIST`)
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

1. **generate-indices**: Executes all SQL queries and generates artifacts
2. **dist**: Downloads artifacts, builds wheel package, validates contents
3. **attach-release-assets**: Attaches parquet/schema/SQL files to GitHub releases
4. **publish**: Publishes wheel to PyPI (on release only)

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

| File | Purpose |
|------|---------|
| `pyproject.toml` | Package metadata, dependencies, build configuration |
| `hatch_build.py` | Custom build hook for data generation and file registration |
| `noxfile.py` | Task automation (tests, docs, build, lint, bump) |
| `.github/workflows/ci.yml` | Continuous integration pipeline |
| `.github/workflows/cd.yml` | Continuous deployment pipeline |

## Key Design Decisions

### SQL Comment Parsing

The system extracts documentation from SQL comments rather than maintaining separate metadata files. This keeps documentation close to the query definition and ensures they stay in sync.

### Parquet Exclusion

Large indexes (sm_index, sm_instance_index, clinical_index, seg_index) are excluded from the PyPI package to keep it small. These files are:
- Generated during CI/CD
- Attached to GitHub releases for direct download
- Available via the schema and SQL files in the package

### Dynamic File Discovery

The build system dynamically discovers SQL files and generated artifacts rather than maintaining explicit file lists. This makes adding new indexes straightforward - just add the SQL file and register the index name.

## BigQuery Data Sources

All indexes query public BigQuery datasets:

- `bigquery-public-data.idc_current.dicom_metadata` - Current IDC DICOM metadata
- `bigquery-public-data.idc_current.dicom_all` - Current IDC DICOM data (all attributes)
- `bigquery-public-data.idc_v{N}.dicom_all` - Versioned IDC data (where N is version number)
- `bigquery-public-data.idc_v{N}_clinical.*` - Versioned clinical data tables

The `idc_current` views always point to the latest IDC release.
