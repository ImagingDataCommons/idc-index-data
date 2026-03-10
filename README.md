# idc-index-data

[![Actions Status][actions-badge]][actions-link]
[![Documentation Status][rtd-badge]][rtd-link]

[![PyPI version][pypi-version]][pypi-link]
[![PyPI platforms][pypi-platforms]][pypi-link]

[![Discourse Forum][discourse-forum-badge]][discourse-forum-link]

<!-- SPHINX-START -->

## About

`idc-index-data` is a Python package providing index files to query and download
data hosted by the
[NCI Imaging Data Commons (IDC)](https://imaging.datacommons.cancer.gov).

The PyPI package bundles a core set of index files (Parquet, JSON schemas, SQL
queries). Supplementary indices that are too large for PyPI distribution are
published as release artifacts on GitHub and uploaded to a public Google Cloud
Storage bucket on each release.

## Index files

The package provides metadata for the following indices via the `INDEX_METADATA`
dictionary:

| Index                    | In PyPI package | Description                                   |
| ------------------------ | :-------------: | --------------------------------------------- |
| `idc_index`              |       yes       | Core IDC DICOM study-level index              |
| `prior_versions_index`   |       yes       | Historical version tracking                   |
| `collections_index`      |       yes       | Collection-level metadata                     |
| `analysis_results_index` |       yes       | Analysis results metadata                     |
| `clinical_index`         |        -        | Clinical data (large)                         |
| `sm_index`               |        -        | Slide microscopy index (large)                |
| `sm_instance_index`      |        -        | Slide microscopy instance-level index (large) |
| `seg_index`              |        -        | Segmentation index                            |
| `ann_index`              |        -        | Annotation index                              |
| `ann_group_index`        |        -        | Annotation group index                        |
| `contrast_index`         |        -        | Contrast agent index                          |

Additionally, the following supplementary parquet files are generated and
published alongside the index files (not included in the PyPI package):

| File                      | Description                                                                            |
| ------------------------- | -------------------------------------------------------------------------------------- |
| `gdc_idc_mapping.parquet` | Mapping of IDC patients to [GDC](https://gdc.cancer.gov) cases                         |
| `tcia_idc_subset.parquet` | Subset of IDC index columns for [TCIA](https://www.cancerimagingarchive.net) workflows |

All index files (including supplementary ones) are available from:

- **GitHub Releases**: attached as release assets
- **Google Cloud Storage**: publicly readable via HTTPS

### Google Cloud Storage artifacts

Artifacts are uploaded to the `idc-index-data-artifacts` bucket on each release.
Two paths are maintained:

| Path                                       | Description                                      |
| ------------------------------------------ | ------------------------------------------------ |
| `gs://idc-index-data-artifacts/<version>/` | Artifacts for a specific release (e.g. `23.5.0`) |
| `gs://idc-index-data-artifacts/current/`   | Always points to the latest release              |

Individual files can be accessed via HTTPS at:

```
https://storage.googleapis.com/idc-index-data-artifacts/current/<filename>
```

For example:

| File                      | URL                                                                                     |
| ------------------------- | --------------------------------------------------------------------------------------- |
| `idc_index.parquet`       | https://storage.googleapis.com/idc-index-data-artifacts/current/idc_index.parquet       |
| `idc_index_schema.json`   | https://storage.googleapis.com/idc-index-data-artifacts/current/idc_index_schema.json   |
| `idc_index.sql`           | https://storage.googleapis.com/idc-index-data-artifacts/current/idc_index.sql           |
| `clinical_index.parquet`  | https://storage.googleapis.com/idc-index-data-artifacts/current/clinical_index.parquet  |
| `sm_index.parquet`        | https://storage.googleapis.com/idc-index-data-artifacts/current/sm_index.parquet        |
| `seg_index.parquet`       | https://storage.googleapis.com/idc-index-data-artifacts/current/seg_index.parquet       |
| `gdc_idc_mapping.parquet` | https://storage.googleapis.com/idc-index-data-artifacts/current/gdc_idc_mapping.parquet |
| `tcia_idc_subset.parquet` | https://storage.googleapis.com/idc-index-data-artifacts/current/tcia_idc_subset.parquet |

Replace `current` with a specific version tag (e.g. `23.5.0`) to pin to a
particular release.

## Usage

This package is intended to be used by the
[idc-index](https://pypi.org/project/idc-index/) Python package.

```python
import idc_index_data

# Access core index file paths
idc_index_data.IDC_INDEX_PARQUET_FILEPATH
idc_index_data.PRIOR_VERSIONS_INDEX_PARQUET_FILEPATH

# Access unified metadata for all indices
idc_index_data.INDEX_METADATA["idc_index"]["parquet_filepath"]
idc_index_data.INDEX_METADATA["idc_index"]["schema"]  # pre-loaded dict
idc_index_data.INDEX_METADATA["idc_index"]["sql"]  # pre-loaded string
```

## Acknowledgment

This software is maintained by the IDC team, which has been funded in whole or
in part with Federal funds from the NCI, NIH, under task order no. HHSN26110071
under contract no. HHSN261201500003l.

If this package helped your research, we would appreciate if you could cite IDC
paper below.

> Fedorov, A., Longabaugh, W. J. R., Pot, D., Clunie, D. A., Pieper, S. D.,
> Gibbs, D. L., Bridge, C., Herrmann, M. D., Homeyer, A., Lewis, R., Aerts, H.
> J. W., Krishnaswamy, D., Thiriveedhi, V. K., Ciausu, C., Schacherer, D. P.,
> Bontempi, D., Pihl, T., Wagner, U., Farahani, K., Kim, E. & Kikinis, R.
> _National Cancer Institute Imaging Data Commons: Toward Transparency,
> Reproducibility, and Scalability in Imaging Artificial Intelligence_.
> RadioGraphics (2023). https://doi.org/10.1148/rg.230180

<!-- prettier-ignore-start -->
[actions-badge]:            https://github.com/ImagingDataCommons/idc-index-data/workflows/CI/badge.svg
[actions-link]:             https://github.com/ImagingDataCommons/idc-index-data/actions
[discourse-forum-badge]: https://img.shields.io/discourse/https/discourse.canceridc.dev/status.svg
[discourse-forum-link]:  https://discourse.canceridc.dev/
[pypi-link]:                https://pypi.org/project/idc-index-data/
[pypi-platforms]:           https://img.shields.io/pypi/pyversions/idc-index-data
[pypi-version]:             https://img.shields.io/pypi/v/idc-index-data
[rtd-badge]:                https://readthedocs.org/projects/idc-index-data/badge/?version=latest
[rtd-link]:                 https://idc-index-data.readthedocs.io/en/latest/?badge=latest

<!-- prettier-ignore-end -->
