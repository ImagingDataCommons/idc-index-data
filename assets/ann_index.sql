# table-description:
# This table contains metadata about the Microscopy Bulk Simple Annotations (ANN) series
# available in IDC. Each row corresponds to a DICOM series containing annotations, and
# includes attributes such as the annotation coordinate type and references to the
# annotated image series. For detailed group-level information (counts, graphic types,
# property codes), join with ann_group_index using SeriesInstanceUID. This table can be
# joined with the main idc_index table using the SeriesInstanceUID column.
# Note: ANN series are assumed to contain a single instance.

SELECT
  # description:
  # DICOM SeriesInstanceUID identifier of the annotation series
  ann.SeriesInstanceUID,

  # description:
  # coordinate type of the annotations (2D or 3D) as defined in DICOM AnnotationCoordinateType attribute
  ann.AnnotationCoordinateType,

  # description:
  # SeriesInstanceUID of the referenced image series that the annotations apply to
  ReferencedSeriesSequence[SAFE_OFFSET(0)].SeriesInstanceUID AS referenced_SeriesInstanceUID

FROM
  `bigquery-public-data.idc_v23.dicom_all` AS ann
WHERE
  # Microscopy Bulk Simple Annotations SOP Class UID - more reliable than Modality = "ANN"
  SOPClassUID = "1.2.840.10008.5.1.4.1.1.91.1"
