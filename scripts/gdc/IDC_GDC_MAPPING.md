# IDC-GDC Patient Mapping

## Approach

### gdc_parquet_generator.py

Checks which IDC patients from GDC-related collections exist as cases in GDC.

1. Runs the BigQuery query in `idc_gdc_selection.sql` against
   `bigquery-public-data.idc_current.dicom_all` to get all imaging studies from
   GDC-related collections (tcga, cptac, apollo, ccdi).

2. Groups the unique PatientIDs by their corresponding GDC project and
   batch-queries the GDC API (`https://api.gdc.cancer.gov/cases`) using POST
   requests with `(project.project_id, submitter_id)` filters.

3. Saves the results to `gdc_idc_mapping.parquet` (ZSTD compressed).

### IDC collection to GDC project mapping

PatientIDs are only unique within a project/collection, so the GDC lookup is
scoped by project:

- **TCGA**: direct 1:1 mapping (e.g. `tcga_brca` -> `TCGA-BRCA`)
- **CCDI**: direct mapping (`ccdi_mci` -> `CCDI-MCI`)
- **VAREPOP-APOLLO**: direct mapping (`varepop_apollo` -> `VAREPOP-APOLLO`)
- **CPTAC**: IDC has per-cancer collections (`cptac_brca`, `cptac_luad`, ...)
  but GDC groups all CPTAC cases under umbrella projects `CPTAC-2` and
  `CPTAC-3`, so all CPTAC patients are queried against both.

### Output schema

| Column             | Type   | Description                            |
| ------------------ | ------ | -------------------------------------- |
| `collection_id`    | string | IDC collection (e.g. `tcga_brca`)      |
| `PatientID`        | string | Patient identifier                     |
| `StudyInstanceUID` | string | DICOM study UID                        |
| `StudyDate`        | string | DICOM study date                       |
| `StudyDescription` | string | DICOM study description                |
| `study_type`       | string | `M` (microscopy/SM) or `R` (radiology) |
| `gdc_case_id`      | string | GDC case UUID, or null if not found    |

## Results (February 2025)

Overall: **15,699 / 17,649** unique (collection, PatientID) pairs found in GDC,
covering **18,186 / 20,454** study rows.

### Collections with unmatched patients

| Collection       | Total patients | Not in GDC | In GDC | Notes                                 |
| ---------------- | -------------- | ---------- | ------ | ------------------------------------- |
| `ccdi_mci`       | 4,407          | 1,363      | 3,044  | ~31% not registered in GDC            |
| `cptac_lscc`     | 212            | 104        | 108    | Partial coverage in CPTAC-3           |
| `cptac_cm`       | 95             | 95         | 0      | Not in GDC at all                     |
| `cptac_sar`      | 88             | 88         | 0      | Not in GDC at all                     |
| `cptac_brca`     | 198            | 75         | 123    | Matched ones are in CPTAC-2           |
| `cptac_coad`     | 178            | 72         | 106    | Matched ones are in CPTAC-2           |
| `varepop_apollo` | 41             | 34         | 7      | Most not in GDC                       |
| `cptac_pda`      | 195            | 42         | 153    | Partial coverage                      |
| `cptac_gbm`      | 178            | 20         | 158    | Partial coverage                      |
| `cptac_ucec`     | 254            | 16         | 238    | Partial coverage                      |
| `cptac_luad`     | 244            | 15         | 229    | Partial coverage                      |
| `tcga_coad`      | 464            | 4          | 460    | PatientID suffix mismatch (see below) |
| `tcga_ov`        | 591            | 1          | 590    | `TCGA-24-1932` not in GDC             |

All other TCGA collections have 100% match rate.

### Findings on unmatched patients

**Patients not in GDC at all.** Confirmed by querying the GDC API without any
project filter. These patients have imaging data in IDC but no corresponding
case record in GDC:

- **CPTAC-CM** (cutaneous melanoma) and **CPTAC-SAR** (sarcoma) are entirely
  absent from GDC. These appear to be imaging-only collections where data was
  submitted to IDC but never registered in GDC.
- **CCDI-MCI**: ~31% of patients not in GDC. The collection is actively growing
  and some patients may not yet have GDC records.
- **VAREPOP-APOLLO**: Most patients (34/41) are not in GDC.
- **Partial CPTAC coverage**: Several CPTAC sub-collections have patients in IDC
  that are not in GDC. The matched patients are typically in `CPTAC-2`; the
  unmatched ones are simply not registered in GDC.

**PatientID suffix mismatch (TCGA-COAD).** Four TCGA-COAD patients have a
trailing `A` in their IDC PatientID that GDC does not have:

| IDC PatientID   | GDC submitter_id |
| --------------- | ---------------- |
| `TCGA-AY-4070A` | `TCGA-AY-4070`   |
| `TCGA-AY-5543A` | `TCGA-AY-5543`   |
| `TCGA-AY-6197A` | `TCGA-AY-6197`   |
| `TCGA-AY-6386A` | `TCGA-AY-6386`   |

These patients exist in GDC but are not matched due to the ID discrepancy.

## Collection coverage analysis

GDC has 91 projects. We cross-referenced all IDC collection names against all
GDC project IDs and confirmed that the current SQL filters (`tcga%`, `%apollo%`,
`%cptac%`, `%ccdi%`) cover every IDC collection that has a corresponding GDC
project.

The remaining ~120 IDC collections (e.g. `acrin_*`, `cmb_*`, `htan_*`, `nlst`,
`rider_*`, `qin_*`, etc.) do not correspond to any GDC project. Other GDC
programs (TARGET, MATCH, CGCI, CMI, MMRF, HCMI, BEATAML, FM-AD, WCDT, etc.) do
not have imaging collections in IDC.
