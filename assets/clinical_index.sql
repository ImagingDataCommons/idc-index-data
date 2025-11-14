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
  `bigquery-public-data.idc_v22_clinical.column_metadata`
ORDER BY
  collection_id, table_name
