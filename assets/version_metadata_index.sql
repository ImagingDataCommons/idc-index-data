# table-description:
# This table contains metadata about each IDC data release version. Each row
# corresponds to one IDC version and captures when that version was created.
# This index can be used to correlate data in other indexes (which include
# idc_version columns) with the corresponding release timestamps.

SELECT
  # description:
  # IDC version number identifying the data release
  idc_version,

  # description:
  # timestamp when this IDC version was created
  version_timestamp

FROM
  `bigquery-public-data.idc_v24.version_metadata`

ORDER BY
  idc_version
