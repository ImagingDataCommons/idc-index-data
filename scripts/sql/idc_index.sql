SELECT
  # collection level attributes
  ANY_VALUE(collection_id) AS collection_id,
  ANY_VALUE(analysis_result_id) AS analysis_result_id,
  ANY_VALUE(PatientID) AS PatientID,
  SeriesInstanceUID,
  ANY_VALUE(StudyInstanceUID) AS StudyInstanceUID,
  ANY_VALUE(source_DOI) AS source_DOI,
  # patient level attributes
  ANY_VALUE(PatientAge) AS PatientAge,
  ANY_VALUE(PatientSex) AS PatientSex,
  # study level attributes
  ANY_VALUE(StudyDate) AS StudyDate,
  ANY_VALUE(StudyDescription) AS StudyDescription,
  ANY_VALUE(dicom_curated.BodyPartExamined) AS BodyPartExamined,
  # series level attributes
  ANY_VALUE(Modality) AS Modality,
  ANY_VALUE(Manufacturer) AS Manufacturer,
  ANY_VALUE(ManufacturerModelName) AS ManufacturerModelName,
  ANY_VALUE(SAFE_CAST(SeriesDate AS STRING)) AS SeriesDate,
  ANY_VALUE(SeriesDescription) AS SeriesDescription,
  ANY_VALUE(SeriesNumber) AS SeriesNumber,
  COUNT(dicom_all.SOPInstanceUID) AS instanceCount,
  ANY_VALUE(license_short_name) as license_short_name,
  # download related attributes
  ANY_VALUE(aws_bucket)  AS aws_bucket,
  ANY_VALUE(crdc_series_uuid) AS crdc_series_uuid,
  # series_aws_url will be phased out in favor of constructing URL from bucket+UUID
  ANY_VALUE(CONCAT(series_aws_url,"*")) AS series_aws_url,
  ROUND(SUM(SAFE_CAST(instance_size AS float64))/1000000, 2) AS series_size_MB,
FROM
  `bigquery-public-data.idc_v21.dicom_all` AS dicom_all
JOIN
  `bigquery-public-data.idc_v21.dicom_metadata_curated` AS dicom_curated
ON
  dicom_all.SOPInstanceUID = dicom_curated.SOPInstanceUID
GROUP BY
  SeriesInstanceUID
