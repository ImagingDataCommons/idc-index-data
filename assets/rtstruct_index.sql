# table-description:
# This table contains one row per DICOM RT Structure Set (RTSTRUCT)
# SeriesInstanceUID available from IDC, and captures key metadata
# about the structure set including the number of ROIs, ROI names,
# generation algorithms, interpreted types, and the referenced
# image series.
# Note: multi-valued columns (ROINames, ROIGenerationAlgorithms,
# RTROIInterpretedTypes) are aggregated with DISTINCT independently,
# so positional correspondence between columns is not preserved.
WITH
  rtstruct_rois AS (
    SELECT
      SeriesInstanceUID,
      SOPInstanceUID,
      roi.ROINumber,
      roi.ROIName,
      roi.ROIGenerationAlgorithm
    FROM
      `bigquery-public-data.idc_v24.dicom_all`
    CROSS JOIN
      UNNEST(StructureSetROISequence) AS roi
    WHERE
      # RT Structure Set Storage SOP Class UID - more reliable than Modality = "RTSTRUCT"
      SOPClassUID = "1.2.840.10008.5.1.4.1.1.481.3"
  ),
  rtstruct_observations AS (
    SELECT
      SOPInstanceUID,
      obs.ReferencedROINumber,
      obs.RTROIInterpretedType
    FROM
      `bigquery-public-data.idc_v24.dicom_all`
    CROSS JOIN
      UNNEST(RTROIObservationsSequence) AS obs
    WHERE
      SOPClassUID = "1.2.840.10008.5.1.4.1.1.481.3"
  ),
  referenced_series AS (
    SELECT
      SOPInstanceUID,
      rt_series.SeriesInstanceUID AS referenced_SeriesInstanceUID
    FROM
      `bigquery-public-data.idc_v24.dicom_all`
    CROSS JOIN
      UNNEST(ReferencedFrameOfReferenceSequence) AS ref_frame
    CROSS JOIN
      UNNEST(ref_frame.RTReferencedStudySequence) AS rt_study
    CROSS JOIN
      UNNEST(rt_study.RTReferencedSeriesSequence) AS rt_series
    WHERE
      SOPClassUID = "1.2.840.10008.5.1.4.1.1.481.3"
  ),
  joined AS (
    SELECT
      rtstruct_rois.SeriesInstanceUID,
      rtstruct_rois.ROINumber,
      rtstruct_rois.ROIName,
      rtstruct_rois.ROIGenerationAlgorithm,
      rtstruct_observations.RTROIInterpretedType,
      referenced_series.referenced_SeriesInstanceUID
    FROM
      rtstruct_rois
    LEFT JOIN
      rtstruct_observations
      ON
        rtstruct_rois.SOPInstanceUID = rtstruct_observations.SOPInstanceUID
        AND rtstruct_rois.ROINumber = rtstruct_observations.ReferencedROINumber
    LEFT JOIN
      referenced_series
      ON
        rtstruct_rois.SOPInstanceUID = referenced_series.SOPInstanceUID
  )
SELECT
  # description:
  # DICOM SeriesInstanceUID identifier of the RT Structure Set series
  SeriesInstanceUID,
  # description:
  # Number of ROIs in the structure set obtained by counting distinct
  # DICOM ROINumber values in the StructureSetROISequence
  COUNT(DISTINCT ROINumber) AS total_rois,
  # description:
  # Array of distinct ROI names from DICOM ROIName attribute in
  # StructureSetROISequence, e.g., ["GTV", "Heart", "Liver", "PTV"]
  ARRAY_AGG(DISTINCT ROIName IGNORE NULLS ORDER BY ROIName) AS ROINames,
  # description:
  # Array of distinct ROI generation algorithms from DICOM
  # ROIGenerationAlgorithm attribute in StructureSetROISequence,
  # e.g., ["AUTOMATIC", "MANUAL", "SEMIAUTOMATIC"]
  ARRAY_AGG(DISTINCT ROIGenerationAlgorithm IGNORE NULLS ORDER BY ROIGenerationAlgorithm)
    AS ROIGenerationAlgorithms,
  # description:
  # Array of distinct ROI interpreted types from DICOM
  # RTROIInterpretedType attribute in RTROIObservationsSequence,
  # e.g., ["GTV", "ORGAN", "PTV"]
  ARRAY_AGG(DISTINCT RTROIInterpretedType IGNORE NULLS ORDER BY RTROIInterpretedType)
    AS RTROIInterpretedTypes,
  # description:
  # SeriesInstanceUID of the referenced image series that the structure set
  # applies to, extracted from DICOM ReferencedFrameOfReferenceSequence
  # > RTReferencedStudySequence > RTReferencedSeriesSequence
  ANY_VALUE(referenced_SeriesInstanceUID)
    AS referenced_SeriesInstanceUID
FROM joined
GROUP BY SeriesInstanceUID
ORDER BY SeriesInstanceUID
