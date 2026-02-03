# Adding a New Index

This guide explains how to add a new metadata index to the idc-index-data
package.

## Overview

Each index is:

- Defined as a **BigQuery SQL query** with embedded documentation
- Generated as **Parquet files** and **JSON schema files**
- Registered in the Python package for programmatic access

## Prerequisites

- Access to Google BigQuery with the `bigquery-public-data` project
- Understanding of the DICOM metadata you want to index
- Familiarity with SQL (BigQuery dialect)

## Step 1: Create the SQL Query File

Create a new SQL file in the `assets/` directory following the naming convention
`{index_name}.sql` (e.g., `rt_index.sql` for a radiotherapy index).

### Required SQL Structure

Your SQL file **must** include documentation comments:

```sql
# table-description:
# Brief description of what this table contains.
# Explain the granularity (one row per series, per instance, etc.)
# and the key metadata captured.

SELECT
  # description:
  # Description of this column explaining what it represents
  column_name,

  # description:
  # Another column description. Multi-line descriptions
  # are supported by continuing with # on subsequent lines.
  another_column AS alias_name,

FROM
  `bigquery-public-data.idc_v23.dicom_metadata`
WHERE
  ...
```

### Documentation Rules

1. **Table description** - Must start with `# table-description:` at the top of
   the file
2. **Column descriptions** - Every column in the SELECT must be preceded by
   `# description:`
3. **No empty descriptions** - The build will fail if any column lacks a
   description
4. **DICOM provenance** - For columns that directly map to or are derived from
   DICOM attributes, include the source attribute name in the description (see
   examples below)

### Example

See `assets/seg_index.sql` or `assets/ann_index.sql` for complete examples showing:

- Complex CTEs (Common Table Expressions)
- Nested UNNEST operations
- Proper comment placement for all columns

## Step 2: Register the Index

Edit `src/idc_index_data/__init__.py` and add your index name to the
`_ALL_INDICES` list:

```python
_ALL_INDICES = [
    "idc_index",
    "prior_versions_index",
    "collections_index",
    "analysis_results_index",
    "clinical_index",
    "sm_index",
    "sm_instance_index",
    "seg_index",
    "ann_index",
    "ann_group_index",
    "your_new_index",  # <-- Add your index here
]
```

The index name must match the SQL filename without the `.sql` extension.

## Step 3: (Optional) Exclude Large Parquet Files

If your index generates a large Parquet file (>5MB), add it to the exclusion
list in `hatch_build.py` to reduce package size:

```{warning}
PyPI enforces a **100 MB maximum file size** for uploaded packages. If adding your index would push the total package size over this limit, you **must** add it to the exclusion list. Excluded files are still distributed via GitHub release assets.
```

```python
PARQUET_EXCLUDE_LIST: ClassVar[set[str]] = {
    "sm_index.parquet",
    "sm_instance_index.parquet",
    "clinical_index.parquet",
    "seg_index.parquet",
    "ann_index.parquet",
    "ann_group_index.parquet",
    "your_new_index.parquet",  # <-- Add if large
}
```

Excluded parquet files are still generated and attached to GitHub releases but
not bundled in the PyPI package.

## Step 4: Test Locally

### Generate the Index Data

```bash
# Set your GCP project (required for BigQuery access)
export GCP_PROJECT=your-gcp-project-id

# Generate all indexes including your new one
python scripts/python/idc_index_data_manager.py \
    --generate-parquet \
    --output-dir .
```

This will generate:

- `your_new_index.parquet` - The data file
- `your_new_index_schema.json` - Schema with column descriptions
- `your_new_index.sql` - Copy of your SQL query

### Run Tests

```bash
nox -s tests
```

Key tests to pass:

- `test_index_metadata_has_all_indices` - Verifies your index is in
  `INDEX_METADATA`
- `test_index_metadata_structure` - Validates the metadata structure
- `test_index_metadata_schema_content` - Checks schema is properly generated

## Step 5: Commit and Create PR

Follow the commit message convention:

```
enh: add {index_name} describing {what the index contains}

Brief explanation of the data source and any dependencies.
```

## Files Modified Summary

| File                             | Change                                                       |
| -------------------------------- | ------------------------------------------------------------ |
| `assets/{index_name}.sql`        | **Create** - SQL query with documentation                    |
| `src/idc_index_data/__init__.py` | **Edit** - Add to `_ALL_INDICES` list                        |
| `hatch_build.py`                 | **Edit** (optional) - Add to `PARQUET_EXCLUDE_LIST` if large |

## SQL Best Practices

### BigQuery Table References

Use versioned datasets (e.g., `idc_v23`) for reproducibility:

```sql
FROM `bigquery-public-data.idc_v23.dicom_metadata`
```

### Safe Array Access

Use `SAFE_OFFSET` to avoid errors on empty arrays:

```sql
array_column[SAFE_OFFSET(0)] AS first_element
```

### Aggregation Patterns

For series-level indexes, use aggregation functions:

```sql
SELECT
  SeriesInstanceUID,
  ANY_VALUE(Modality) AS Modality,
  COUNT(DISTINCT SOPInstanceUID) AS instance_count,
  STRING_AGG(DISTINCT column, ',') AS aggregated_values
FROM ...
GROUP BY SeriesInstanceUID
```

### Unsupported Types

Avoid these BigQuery types (they fail schema validation):

- `DATE`, `TIME`, `DATETIME` - Use `TIMESTAMP` or `STRING` instead
- `GEOGRAPHY`, `JSON` - Convert to `STRING`

### DICOM Attribute Provenance

For columns derived from DICOM attributes, document the source in the column
description. This helps users understand the data lineage and find related
documentation in the DICOM standard.

**Examples:**

```sql
# description:
# coordinate type (2D or 3D) as defined in DICOM AnnotationCoordinateType attribute
AnnotationCoordinateType,

# description:
# segmentation algorithm type as available in DICOM SegmentAlgorithmType attribute
SegmentAlgorithmType,

# description:
# pixel spacing in mm, derived from DICOM PixelSpacing attribute
min_PixelSpacing,

# description:
# anatomic location CodeMeaning from DICOM PrimaryAnatomicStructureSequence
# in SpecimenDescriptionSequence
primaryAnatomicStructure_CodeMeaning,

# description:
# algorithm name from DICOM AlgorithmName attribute in
# AnnotationGroupAlgorithmIdentificationSequence (when applicable)
AlgorithmName,
```

**Patterns to follow:**

- Direct attribute: `"as defined in DICOM {AttributeName} attribute"`
- Derived value: `"derived from DICOM {AttributeName} attribute"`
- Nested sequence: `"from DICOM {AttributeName} in {ParentSequence}"`
- Code sequence: `"CodeMeaning from DICOM {SequenceName}"`

## Generated Output Files

After successful build, your index produces:

### Parquet File (`{index_name}.parquet`)

Columnar data format with zstd compression containing the query results.

### Schema JSON (`{index_name}_schema.json`)

```json
{
  "table_description": "Your table description from comments",
  "columns": [
    {
      "name": "column_name",
      "type": "STRING",
      "mode": "NULLABLE",
      "description": "Your column description"
    }
  ]
}
```

### SQL File (`{index_name}.sql`)

Copy of your original query for reference.

## CI/CD Integration

Once merged, the GitHub Actions workflow automatically:

1. Executes your SQL query against BigQuery
2. Generates parquet and schema files
3. Attaches files to GitHub releases
4. Publishes updated package to PyPI

No additional configuration is needed - the build system discovers SQL files
automatically from both `scripts/sql/` and `assets/` directories.
