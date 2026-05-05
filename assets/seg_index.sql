# table-description:
# This table contains one row per DICOM Segmentation
# SeriesInstanceUID available from IDC, and captures
# key metadata about the segmentation series including
# the number of segments, segmentation type, algorithm
# type and name, and the segmented image series.
# Note: multi-valued columns (AlgorithmType, AlgorithmName,
# SegmentedPropertyCategory_CodeMeanings, etc.) are aggregated
# with DISTINCT independently, so positional correspondence
# between columns is not preserved. For example, the first
# value in AlgorithmType does not necessarily pair with the
# first value in AlgorithmName.
WITH
  segmentations AS (
    WITH
      segmentations AS (
        WITH
          segs AS (
            SELECT
              PatientID,
              StudyInstanceUID,
              SeriesInstanceUID,
              SOPInstanceUID,
              FrameOfReferenceUID,
              SegmentSequence,
              SegmentationType
            FROM
              `bigquery-public-data.idc_v24.dicom_metadata`
            WHERE
              # more reliable than Modality = "SEG"
              SOPClassUID = "1.2.840.10008.5.1.4.1.1.66.4"
          )
        SELECT
          PatientID,
          StudyInstanceUID,
          SeriesInstanceUID,
          SOPInstanceUID,
          FrameOfReferenceUID,
          SegmentationType,
          CASE ARRAY_LENGTH(unnested.AnatomicRegionSequence)
            WHEN 0 THEN NULL
            ELSE
              STRUCT(
                unnested.AnatomicRegionSequence[OFFSET(0)].CodeValue
                  AS CodeValue,
                unnested.AnatomicRegionSequence[OFFSET(0)].CodingSchemeDesignator
                  AS CodingSchemeDesignator,
                unnested.AnatomicRegionSequence[OFFSET(0)].CodeMeaning
                  AS CodeMeaning)
            END
            AS AnatomicRegion,
          CASE
            (
              ARRAY_LENGTH(unnested.AnatomicRegionSequence) > 0
              AND ARRAY_LENGTH(
                unnested.AnatomicRegionSequence[OFFSET(0)].AnatomicRegionModifierSequence)
                > 0)
            WHEN TRUE
              THEN
                unnested.AnatomicRegionSequence[OFFSET(0)].AnatomicRegionModifierSequence[
                  OFFSET(
                    0)]  # unnested.AnatomicRegionSequence[OFFSET(0)].AnatomicRegionModifierSequence,
            ELSE
              NULL
            END
            AS AnatomicRegionModifier,
          CASE ARRAY_LENGTH(unnested.SegmentedPropertyCategoryCodeSequence)
            WHEN 0 THEN NULL
            ELSE
              unnested.SegmentedPropertyCategoryCodeSequence[
                OFFSET(0)]
            END
            AS SegmentedPropertyCategory,
          CASE ARRAY_LENGTH(unnested.SegmentedPropertyTypeCodeSequence)
            WHEN 0 THEN NULL
            ELSE
              unnested.SegmentedPropertyTypeCodeSequence[
                OFFSET(0)]
            END
            AS SegmentedPropertyType,
          # unnested.SegmentedPropertyTypeCodeSequence,
          # unnested.SegmentedPropertyTypeModifierCodeSequence,
          unnested.SegmentAlgorithmType,
          # for unknown reason, this attribute is REPEATED in our BigQuery schema
          unnested.SegmentAlgorithmName[SAFE_OFFSET(0)] as SegmentAlgorithmName,
          unnested.SegmentNumber,
          unnested.TrackingUID,
          unnested.TrackingID
        FROM
          segs
        CROSS JOIN
          UNNEST(SegmentSequence) AS unnested
      ),
      sampled_sops AS (
        SELECT
          SOPInstanceUID AS seg_SOPInstanceUID,
          ReferencedSeriesSequence[SAFE_OFFSET(0)].ReferencedInstanceSequence[SAFE_OFFSET(0)].ReferencedSOPInstanceUID
            AS rss_one,
          ReferencedImageSequence[SAFE_OFFSET(0)].ReferencedSOPInstanceUID
            AS ris_one,
          SourceImageSequence[SAFE_OFFSET(0)].ReferencedSOPInstanceUID
            AS sis_one
        FROM
          `bigquery-public-data.idc_v24.dicom_all`
        WHERE
          Modality = "SEG"
          AND SOPClassUID = "1.2.840.10008.5.1.4.1.1.66.4"
      ),
      coalesced_ref AS (
        SELECT
          *,
          COALESCE(rss_one, ris_one, sis_one) AS referenced_sop
        FROM
          sampled_sops
      )
    SELECT
      segmentations.*,
      dicom_all.SeriesInstanceUID AS segmented_SeriesInstanceUID,
      CONCAT(
        "https://viewer.imaging.datacommons.cancer.gov/viewer/",
        segmentations.StudyInstanceUID,
        "?seriesInstanceUID=",
        segmentations.SeriesInstanceUID,
        ",",
        dicom_all.SeriesInstanceUID) AS viewer_url,
    FROM
      coalesced_ref
    JOIN
      `bigquery-public-data.idc_v24.dicom_all` AS dicom_all
      ON
        coalesced_ref.referenced_sop = dicom_all.SOPInstanceUID
    RIGHT JOIN
      segmentations
      ON
        segmentations.SOPInstanceUID = coalesced_ref.seg_SOPInstanceUID
  )
SELECT
  # description:
  # DICOM SeriesInstanceUID identifier of the segmentation series
  SeriesInstanceUID,
  # description:
  # Type of segmentation as defined in DICOM SegmentationType attribute
  any_value(SegmentationType) AS SegmentationType,
  # description:
  # Number of segments in the segmentation series obtained by counting distinct DICOM SegmentNumber values in the DICOM SegmentatationSequence
  COUNT(DISTINCT (SegmentNumber)) total_segments,
  # description:
  # Segmentation algorithm type as available in DICOM SegmentAlgorithmType
  string_agg(DISTINCT (SegmentAlgorithmType), ',' ORDER BY SegmentAlgorithmType) AS AlgorithmType,
  # description:
  # Segmentation algorithm name as available in DICOM SegmentAlgorithmName
  string_agg(DISTINCT (SegmentAlgorithmName), ',' ORDER BY SegmentAlgorithmName) AS AlgorithmName,
  # description:
  # SeriesInstanceUID of the referenced image series that the segmentation applies to
  any_value(segmentations.segmented_SeriesInstanceUID)
    AS segmented_SeriesInstanceUID,
  # description:
  # Array of distinct CodeMeaning values from SegmentedPropertyCategoryCodeSequence across all segments
  # in the series, representing the broad category of the segmented property as defined in DICOM PS3.3 C.8.20.2,
  # e.g., ["Anatomical Structure"], ["Morphologically Altered Structure"]
  ARRAY_AGG(DISTINCT SegmentedPropertyCategory.CodeMeaning IGNORE NULLS ORDER BY SegmentedPropertyCategory.CodeMeaning)
    AS SegmentedPropertyCategory_CodeMeanings,
  # description:
  # Array of distinct CodeMeaning values from SegmentedPropertyTypeCodeSequence across all segments
  # in the series, representing the specific type of the segmented property as defined in DICOM PS3.3 C.8.20.2,
  # e.g., ["Liver", "Kidney", "Spleen"]
  ARRAY_AGG(DISTINCT SegmentedPropertyType.CodeMeaning IGNORE NULLS ORDER BY SegmentedPropertyType.CodeMeaning)
    AS SegmentedPropertyType_CodeMeanings,
  # description:
  # Array of distinct CodeMeaning values from AnatomicRegionSequence across all segments
  # in the series, representing the anatomic location of the segmented structure as defined in DICOM PS3.3 C.8.20.2,
  # e.g., ["Abdomen"], ["Thorax", "Head"]
  ARRAY_AGG(DISTINCT AnatomicRegion.CodeMeaning IGNORE NULLS ORDER BY AnatomicRegion.CodeMeaning)
    AS AnatomicRegion_CodeMeanings
FROM segmentations
GROUP BY SeriesInstanceUID
ORDER BY SegmentationType DESC, AlgorithmType, AlgorithmName
