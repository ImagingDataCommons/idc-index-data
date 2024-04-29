WITH
  TEMP AS (
  SELECT
    -- collection level attributes 
    ANY_VALUE(collection_id) AS collection_id,
    ANY_VALUE(analysis_result_id) AS analysis_result_id,
    ANY_VALUE(PatientID) AS PatientID,
    SeriesInstanceUID,
    ANY_VALUE(StudyInstanceUID) AS StudyInstanceUID,
    ANY_VALUE(source_DOI) AS source_DOI,
    --patient level attributes 
    ANY_VALUE(PatientAge) AS PatientAge,
    ANY_VALUE(PatientSex) AS PatientSex,
    --study level attributes 
    ANY_VALUE(StudyDate) AS StudyDate,
    ANY_VALUE(StudyDescription) AS StudyDescription,
    ANY_VALUE(dicom_curated.BodyPartExamined) AS BodyPartExamined,
    -- series level attributes 
    ANY_VALUE(Modality) AS Modality,
    ANY_VALUE(Manufacturer) AS Manufacturer,
    ANY_VALUE(ManufacturerModelName) AS ManufacturerModelName,
    ANY_VALUE(SAFE_CAST(SeriesDate AS STRING)) AS SeriesDate,
    ANY_VALUE(SeriesDescription) AS SeriesDescription,
    ANY_VALUE(SeriesNumber) AS SeriesNumber,
    COUNT(dicom_all.SOPInstanceUID) AS instanceCount,
    ANY_VALUE(license_short_name) AS license_short_name,
    -- download related attributes 
    ANY_VALUE(CONCAT(series_aws_url,"*")) AS series_aws_url,
    ANY_VALUE(CONCAT(series_gcs_url,"*")) AS series_gcs_url,
    ROUND(SUM(SAFE_CAST(instance_size AS float64))/1000000, 2) AS series_size_MB,
  FROM
    `bigquery-public-data.idc_v18.dicom_all` AS dicom_all
  JOIN
    `bigquery-public-data.idc_v18.dicom_metadata_curated` AS dicom_curated
  ON
    dicom_all.SOPInstanceUID = dicom_curated.SOPInstanceUID
  GROUP BY
    SeriesInstanceUID),
  temp2 AS(
  SELECT
    collection_id,
    PatientID,
    StudyInstanceUID,
    StudyDate,
    StudyDescription,
    ARRAY_AGG(STRUCT(analysis_result_id,
        SeriesInstanceUID,
        source_DOI,
        PatientAge,
        PatientSex,
        BodyPartExamined,
        Modality,
        Manufacturer,
        ManufacturerModelName,
        SeriesDate,
        SeriesDescription,
        SeriesNumber,
        license_short_name,
        series_aws_url,
        series_gcs_url,
        series_size_MB
         )) AS seriesLevelAttributes
  FROM
    TEMP
  GROUP BY
    1,
    2,
    3,
    4,
    5 )
SELECT
  collection_id,
  PatientID,
  ARRAY_AGG(STRUCT(StudyInstanceUID,
      StudyDate,
      StudyDescription,
      seriesLevelAttributes)) studyLevelAttributes
FROM
  temp2
GROUP BY
  collection_id,
  PatientID
