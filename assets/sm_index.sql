SELECT
  * EXCEPT(Modality)
FROM
  `bigquery-public-data.idc_v18.dicom_metadata_curated_series_level`
WHERE
  Modality = "SM"
