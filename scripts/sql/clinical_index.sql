SELECT
  collection_id,
  table_name,
  SPLIT(table_name,'.')[SAFE_OFFSET(2)] AS short_table_name,
  `column`,
  column_label,
  `values`
FROM
  `bigquery-public-data.idc_v22_clinical.column_metadata`
ORDER BY
  collection_id, table_name
