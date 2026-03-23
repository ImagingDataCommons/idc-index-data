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
  ANY_VALUE(SAFE_CAST(StudyDate AS STRING)) AS StudyDate,
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
  # SOP Class UID identifying the type of DICOM object (e.g., CT Image Storage,
  # Segmentation Storage); more specific than Modality for distinguishing object types
  # (DICOM attribute)
  ANY_VALUE(SOPClassUID) AS SOPClassUID,
  # description:
  # human-readable name of the SOP Class (e.g., "CT Image Storage",
  # "Segmentation Storage"); derived from SOPClassUID
  ANY_VALUE(CASE SOPClassUID
    WHEN "1.2.840.10008.5.1.4.1.1.1" THEN "Computed Radiography Image Storage"
    WHEN "1.2.840.10008.5.1.4.1.1.1.1" THEN "Digital X-Ray Image Storage - For Presentation"
    WHEN "1.2.840.10008.5.1.4.1.1.1.1.1" THEN "Digital X-Ray Image Storage - For Processing"
    WHEN "1.2.840.10008.5.1.4.1.1.1.2" THEN "Digital Mammography X-Ray Image Storage - For Presentation"
    WHEN "1.2.840.10008.5.1.4.1.1.1.2.1" THEN "Digital Mammography X-Ray Image Storage - For Processing"
    WHEN "1.2.840.10008.5.1.4.1.1.104.3" THEN "Encapsulated STL Storage"
    WHEN "1.2.840.10008.5.1.4.1.1.11.1" THEN "Grayscale Softcopy Presentation State Storage"
    WHEN "1.2.840.10008.5.1.4.1.1.11.8" THEN "Advanced Blending Presentation State Storage"
    WHEN "1.2.840.10008.5.1.4.1.1.12.1" THEN "X-Ray Angiographic Image Storage"
    WHEN "1.2.840.10008.5.1.4.1.1.12.2" THEN "X-Ray Radiofluoroscopic Image Storage"
    WHEN "1.2.840.10008.5.1.4.1.1.128" THEN "Positron Emission Tomography Image Storage"
    WHEN "1.2.840.10008.5.1.4.1.1.13.1.3" THEN "Breast Tomosynthesis Image Storage"
    WHEN "1.2.840.10008.5.1.4.1.1.13.1.4" THEN "Breast Projection X-Ray Image Storage - For Presentation"
    WHEN "1.2.840.10008.5.1.4.1.1.13.1.5" THEN "Breast Projection X-Ray Image Storage - For Processing"
    WHEN "1.2.840.10008.5.1.4.1.1.2" THEN "CT Image Storage"
    WHEN "1.2.840.10008.5.1.4.1.1.20" THEN "Nuclear Medicine Image Storage"
    WHEN "1.2.840.10008.5.1.4.1.1.3.1" THEN "Ultrasound Multi-frame Image Storage"
    WHEN "1.2.840.10008.5.1.4.1.1.30" THEN "Parametric Map Storage"
    WHEN "1.2.840.10008.5.1.4.1.1.4" THEN "MR Image Storage"
    WHEN "1.2.840.10008.5.1.4.1.1.481.2" THEN "RT Dose Storage"
    WHEN "1.2.840.10008.5.1.4.1.1.481.3" THEN "RT Structure Set Storage"
    WHEN "1.2.840.10008.5.1.4.1.1.481.5" THEN "RT Plan Storage"
    WHEN "1.2.840.10008.5.1.4.1.1.6.1" THEN "Ultrasound Image Storage"
    WHEN "1.2.840.10008.5.1.4.1.1.66" THEN "Raw Data Storage"
    WHEN "1.2.840.10008.5.1.4.1.1.66.1" THEN "Spatial Registration Storage"
    WHEN "1.2.840.10008.5.1.4.1.1.66.4" THEN "Segmentation Storage"
    WHEN "1.2.840.10008.5.1.4.1.1.67" THEN "Real World Value Mapping Storage"
    WHEN "1.2.840.10008.5.1.4.1.1.7" THEN "Secondary Capture Image Storage"
    WHEN "1.2.840.10008.5.1.4.1.1.7.2" THEN "Multi-frame Grayscale Byte Secondary Capture Image Storage"
    WHEN "1.2.840.10008.5.1.4.1.1.77.1.6" THEN "VL Whole Slide Microscopy Image Storage"
    WHEN "1.2.840.10008.5.1.4.1.1.88.22" THEN "Enhanced SR Storage"
    WHEN "1.2.840.10008.5.1.4.1.1.88.33" THEN "Comprehensive SR Storage"
    WHEN "1.2.840.10008.5.1.4.1.1.88.34" THEN "Comprehensive 3D SR Storage"
    WHEN "1.2.840.10008.5.1.4.1.1.88.59" THEN "Key Object Selection Document Storage"
    WHEN "1.2.840.10008.5.1.4.1.1.88.67" THEN "X-Ray Radiation Dose SR Storage"
    WHEN "1.2.840.10008.5.1.4.1.1.88.71" THEN "Acquisition Context SR Storage"
    WHEN "1.2.840.10008.5.1.4.1.1.91.1" THEN "Microscopy Bulk Simple Annotations Storage"
    ELSE ERROR(CONCAT("Unmapped SOPClassUID: ", SOPClassUID, ". Please add a mapping."))
  END) AS sop_class_name,
  # description:
  # Transfer Syntax UID identifying the encoding of the stored instances (e.g.,
  # Explicit VR Little Endian, JPEG 2000, HTJ2K); comma-separated when a series
  # contains instances with different encodings, which is common for SM (DICOM attribute)
  STRING_AGG(DISTINCT TransferSyntaxUID, "," ORDER BY TransferSyntaxUID) AS TransferSyntaxUID,
  # description:
  # human-readable name of the Transfer Syntax (e.g., "JPEG 2000",
  # "Explicit VR Little Endian"); comma-separated when a series contains
  # instances with different encodings; derived from TransferSyntaxUID
  STRING_AGG(DISTINCT CASE TransferSyntaxUID
    WHEN "1.2.840.10008.1.2" THEN "Implicit VR Little Endian"
    WHEN "1.2.840.10008.1.2.1" THEN "Explicit VR Little Endian"
    WHEN "1.2.840.10008.1.2.2" THEN "Explicit VR Big Endian"
    WHEN "1.2.840.10008.1.2.4.50" THEN "JPEG Baseline"
    WHEN "1.2.840.10008.1.2.4.51" THEN "JPEG Extended"
    WHEN "1.2.840.10008.1.2.4.70" THEN "JPEG Lossless"
    WHEN "1.2.840.10008.1.2.4.80" THEN "JPEG-LS Lossless"
    WHEN "1.2.840.10008.1.2.4.90" THEN "JPEG 2000 Lossless"
    WHEN "1.2.840.10008.1.2.4.91" THEN "JPEG 2000"
    ELSE ERROR(CONCAT("Unmapped TransferSyntaxUID: ", TransferSyntaxUID, ". Please add a mapping."))
  END, "," ORDER BY CASE TransferSyntaxUID
    WHEN "1.2.840.10008.1.2" THEN "Implicit VR Little Endian"
    WHEN "1.2.840.10008.1.2.1" THEN "Explicit VR Little Endian"
    WHEN "1.2.840.10008.1.2.2" THEN "Explicit VR Big Endian"
    WHEN "1.2.840.10008.1.2.4.50" THEN "JPEG Baseline"
    WHEN "1.2.840.10008.1.2.4.51" THEN "JPEG Extended"
    WHEN "1.2.840.10008.1.2.4.70" THEN "JPEG Lossless"
    WHEN "1.2.840.10008.1.2.4.80" THEN "JPEG-LS Lossless"
    WHEN "1.2.840.10008.1.2.4.90" THEN "JPEG 2000 Lossless"
    WHEN "1.2.840.10008.1.2.4.91" THEN "JPEG 2000"
    ELSE ERROR(CONCAT("Unmapped TransferSyntaxUID: ", TransferSyntaxUID, ". Please add a mapping."))
  END) AS transfer_syntax_name,
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
  SUM(SAFE_CAST(instance_size AS float64))/1000000. AS series_size_MB,
FROM
  `bigquery-public-data.idc_v23.dicom_all` AS dicom_all
JOIN
  `bigquery-public-data.idc_v23.dicom_metadata_curated` AS dicom_curated
ON
  dicom_all.SOPInstanceUID = dicom_curated.SOPInstanceUID
GROUP BY
  SeriesInstanceUID
