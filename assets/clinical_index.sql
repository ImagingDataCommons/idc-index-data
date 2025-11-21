# table-description:
# This table contains metadata about the tabular data, including clinical data, accompanying images that
# is available in IDC. Think about this table as a dictionary containing information about the columns
# for all of the tabular data accompanying individual collections in IDC. Each row corresponds to a unique
# combination of collection, clinical data table that is available for that collection, and a column from that
# table. Individual tables referenced from this table can be retrieved using idc-index `get_clinical_table()`
# function.
SELECT
  # description:
  # unique identifier of the collection
  collection_id,
  # description:
  # full name of the table in which the column is stored
  table_name,
  # description:
  # short name of the table in which the column is stored
  SPLIT(table_name,'.')[SAFE_OFFSET(2)] AS short_table_name,
  # description:
  # name of the column in which the value is stored
  `column`,
  # description:
  # human readable name of the column
  column_label,
  # description:
  # values encountered in the column
  `values`
FROM
  `bigquery-public-data.idc_v23_clinical.column_metadata`
ORDER BY
  collection_id, table_name
