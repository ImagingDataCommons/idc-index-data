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

| Index | In PyPI package | Description |
|-------|:-:|---|
| `idc_index` | yes | Core IDC DICOM study-level index |
| `prior_versions_index` | yes | Historical version tracking |
| `collections_index` | yes | Collection-level metadata |
| `analysis_results_index` | yes | Analysis results metadata |
| `clinical_index` | - | Clinical data (large) |
| `sm_index` | - | Slide microscopy index (large) |
| `sm_instance_index` | - | Slide microscopy instance-level index (large) |
| `seg_index` | - | Segmentation index |
| `ann_index` | - | Annotation index |
| `ann_group_index` | - | Annotation group index |
| `contrast_index` | - | Contrast agent index |

Additionally, a `gdc_idc_mapping.parquet` file mapping IDC patients to the
[Genomic Data Commons (GDC)](https://gdc.cancer.gov) is generated and published
alongside the index files (not included in the PyPI package).

All index files (including supplementary ones) are available from:

- **GitHub Releases**: attached as release assets
- **Google Cloud Storage**: `gs://idc-index-data-artifacts/<version>/`
  (publicly readable, e.g.
  `https://storage.googleapis.com/idc-index-data-artifacts/23.5.0/idc_index.parquet`)

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
idc_index_data.INDEX_METADATA["idc_index"]["sql"]      # pre-loaded string
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
