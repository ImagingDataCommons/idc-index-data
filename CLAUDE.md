# Claude Code Instructions

## Persistence

When asked to remember something or when learning reusable guidelines, save them
in this file — not in local memory. This repo is used across multiple machines,
so only repo-level persistence is reliable.

## Before committing

Always run `pre-commit run --files <staged files>` before committing to catch
formatting and linting issues. Fix any failures before creating the commit.

## After modifying SQL queries

Validate every changed SQL file against BigQuery with `--dry_run` before
committing:

```bash
bq query --project_id=idc-sandbox-000 --use_legacy_sql=false --dry_run < path/to/query.sql
```

Fix any query errors before creating the commit. Note that
`prior_versions_index.sql` uses procedural SQL (`EXECUTE IMMEDIATE`) and cannot
be validated with `--dry_run`.

## Before starting any task

Read the developer documentation in `docs/dev/` before exploring the codebase.
In particular, check if there is a guide that matches your task (e.g.,
`docs/dev/adding-new-index.md` when adding a new index). Use the guide as your
checklist.

## BigQuery access

Queries can be run locally via:

```bash
bq query --project_id=idc-sandbox-000 --use_legacy_sql=false < path/to/query.sql
```

## Column naming convention

- DICOM attributes use PascalCase (e.g., `SeriesInstanceUID`, `PatientID`)
- Derived/computed columns use snake_case (e.g., `sop_class_name`,
  `series_size_MB`, `valid_3d_volume`)

## Index SQL locations

- Core indexes: `scripts/sql/` (idc_index, collections_index,
  prior_versions_index, analysis_results_index)
- Specialized indexes: `assets/` (clinical_index, sm_index, sm_instance_index,
  seg_index, etc.)
