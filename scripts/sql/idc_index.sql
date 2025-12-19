# table-description:
# This is the main metadata table provided by idc-index. Each row corresponds to a DICOM series, and contains
# attributes at the collection, patient, study, and series levels. The table also contains download-related
# attributes, such as the AWS S3 bucket and URL to download the series.
SELECT
  # collection level attributes
  # description:
  # short string with the identifier of the collection the series belongs to
  ANY_VALUE(collection_id) AS collection_id,
  # description:
  # this string is not empty if the specific series is
  # part of an analysis results collection; analysis results can be added to a
  #  given collection over time
  ANY_VALUE(analysis_result_id) AS analysis_result_id,
  # description:
  # identifier of the patient within the collection (DICOM attribute)
  ANY_VALUE(PatientID) AS PatientID,
  # description:
  # unique identifier of the DICOM series (DICOM attribute)
  SeriesInstanceUID,
  # description:
  # unique identifier of the DICOM study (DICOM attribute)
  ANY_VALUE(StudyInstanceUID) AS StudyInstanceUID,
  # description:
  # Digital Object Identifier of the dataset that contains the given
  # series; follow this DOI to learn more about the activity that produced
  # this series
  ANY_VALUE(source_DOI) AS source_DOI,
  # patient level attributes:
  # description:
  # age of the subject at the time of imaging (DICOM attribute)
  ANY_VALUE(PatientAge) AS PatientAge,
  # description:
  # subject sex (DICOM attribute)
  ANY_VALUE(PatientSex) AS PatientSex,
  # study level attributes
  # description:
  # date of the study (de-identified) (DICOM attribute)
  ANY_VALUE(StudyDate) AS StudyDate,
  # description:
  # textual description of the study content (DICOM attribute)
  ANY_VALUE(StudyDescription) AS StudyDescription,
  # description:
  # body part imaged (not applicable for SM series) (DICOM attribute)
  ANY_VALUE(dicom_curated.BodyPartExamined) AS BodyPartExamined,
  # series level attributes
  # description:
  # acquisition modality (DICOM attribute)
  ANY_VALUE(Modality) AS Modality,
  # description:
  # manufacturer of the equipment that produced the series (DICOM attribute)
  ANY_VALUE(Manufacturer) AS Manufacturer,
  # description:
  # model name of the equipment that produced the series (DICOM attribute)
  ANY_VALUE(ManufacturerModelName) AS ManufacturerModelName,
  # description:
  # date of the series (de-identified) (DICOM attribute)
  ANY_VALUE(SAFE_CAST(SeriesDate AS STRING)) AS SeriesDate,
  # description:
  # textual description of the series content (DICOM attribute)
  ANY_VALUE(SeriesDescription) AS SeriesDescription,
  # description:
  # series number (DICOM attribute)
  ANY_VALUE(SeriesNumber) AS SeriesNumber,
  # description:
  # number of instances in the series
  COUNT(dicom_all.SOPInstanceUID) AS instanceCount,
  # description:
  # short name of the license that applies to this series
  ANY_VALUE(license_short_name) as license_short_name,
  # download related attributes
  # description:
  # name of the AWS S3 bucket that contains the series
  ANY_VALUE(aws_bucket)  AS aws_bucket,
  # description:
  # unique identifier of the series within the IDC
  ANY_VALUE(crdc_series_uuid) AS crdc_series_uuid,
  # series_aws_url will be phased out in favor of constructing URL from bucket+UUID
  # description:
  # public AWS S3 URL to download the series in bulk (each instance is a separate file)
  ANY_VALUE(CONCAT(series_aws_url,"*")) AS series_aws_url,
  # description:
  # total size of the series in megabytes
  ROUND(SUM(SAFE_CAST(instance_size AS float64))/1000000, 2) AS series_size_MB,
FROM
  `bigquery-public-data.idc_v23.dicom_all` AS dicom_all
JOIN
  `bigquery-public-data.idc_v23.dicom_metadata_curated` AS dicom_curated
ON
  dicom_all.SOPInstanceUID = dicom_curated.SOPInstanceUID
GROUP BY
  SeriesInstanceUID
