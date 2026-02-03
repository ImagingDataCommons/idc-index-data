# table-description:
# This table contains metadata about the Microscopy Bulk Simple Annotations (ANN) series
# available in IDC. Each row corresponds to a DICOM series containing annotations, and
# includes attributes such as the annotation coordinate type, number of annotation groups,
# graphic types used, and references to the annotated image series. This table can be
# joined with the main index table using the SeriesInstanceUID column.

WITH
  -- Base ANN series data
  ann_base AS (
    SELECT
      SeriesInstanceUID,
      StudyInstanceUID,
      SOPInstanceUID,
      AnnotationCoordinateType,
      ContentLabel,
      ContentDescription,
      AnnotationGroupSequence,
      ReferencedSeriesSequence
    FROM
      `bigquery-public-data.idc_v23.dicom_all`
    WHERE
      # Microscopy Bulk Simple Annotations SOP Class UID - more reliable than Modality = "ANN"
      SOPClassUID = "1.2.840.10008.5.1.4.1.1.91.1"
  ),

  -- Unnest AnnotationGroupSequence to get group-level details
  annotation_groups AS (
    SELECT
      ann_base.SeriesInstanceUID,
      group_item.AnnotationGroupNumber,
      group_item.AnnotationGroupLabel,
      group_item.AnnotationGroupGenerationType,
      group_item.NumberOfAnnotations,
      group_item.GraphicType,
      group_item.AnnotationGroupAlgorithmIdentificationSequence[SAFE_OFFSET(0)].AlgorithmName AS AlgorithmName,
      CONCAT(
        group_item.AnnotationPropertyCategoryCodeSequence[SAFE_OFFSET(0)].CodingSchemeDesignator, ":",
        group_item.AnnotationPropertyCategoryCodeSequence[SAFE_OFFSET(0)].CodeValue
      ) AS PropertyCategory_code,
      group_item.AnnotationPropertyCategoryCodeSequence[SAFE_OFFSET(0)].CodeMeaning AS PropertyCategory_CodeMeaning,
      CONCAT(
        group_item.AnnotationPropertyTypeCodeSequence[SAFE_OFFSET(0)].CodingSchemeDesignator, ":",
        group_item.AnnotationPropertyTypeCodeSequence[SAFE_OFFSET(0)].CodeValue
      ) AS PropertyType_code,
      group_item.AnnotationPropertyTypeCodeSequence[SAFE_OFFSET(0)].CodeMeaning AS PropertyType_CodeMeaning
    FROM
      ann_base
    CROSS JOIN
      UNNEST(ann_base.AnnotationGroupSequence) AS group_item
  ),

  -- Get referenced series information
  referenced_series AS (
    SELECT
      ann_base.SeriesInstanceUID AS ann_SeriesInstanceUID,
      ref_series.SeriesInstanceUID AS referenced_SeriesInstanceUID,
      STRING_AGG(DISTINCT ref_instance.ReferencedSOPClassUID, ",") AS ReferencedSOPClassUIDs
    FROM
      ann_base
    CROSS JOIN
      UNNEST(ann_base.ReferencedSeriesSequence) AS ref_series
    CROSS JOIN
      UNNEST(ref_series.ReferencedInstanceSequence) AS ref_instance
    GROUP BY
      ann_base.SeriesInstanceUID, ref_series.SeriesInstanceUID
  ),

  -- Aggregate at series level
  series_aggregated AS (
    SELECT
      SeriesInstanceUID,
      COUNT(DISTINCT AnnotationGroupNumber) AS total_annotation_groups,
      SUM(NumberOfAnnotations) AS total_annotations,
      STRING_AGG(DISTINCT GraphicType, ",") AS GraphicTypes,
      STRING_AGG(DISTINCT AnnotationGroupGenerationType, ",") AS AnnotationGenerationTypes,
      ARRAY_AGG(DISTINCT AnnotationGroupLabel IGNORE NULLS) AS AnnotationGroupLabels,
      ARRAY_AGG(DISTINCT PropertyCategory_CodeMeaning IGNORE NULLS) AS AnnotationPropertyCategories,
      ARRAY_AGG(DISTINCT PropertyType_CodeMeaning IGNORE NULLS) AS AnnotationPropertyTypes,
      STRING_AGG(DISTINCT AlgorithmName, ",") AS AlgorithmNames
    FROM
      annotation_groups
    GROUP BY
      SeriesInstanceUID
  )

SELECT
  # description:
  # DICOM SeriesInstanceUID identifier of the annotation series
  ann_base.SeriesInstanceUID,

  # description:
  # coordinate type of the annotations (2D or 3D) as defined in DICOM AnnotationCoordinateType attribute
  ANY_VALUE(ann_base.AnnotationCoordinateType) AS AnnotationCoordinateType,

  # description:
  # total number of annotation groups in this series
  ANY_VALUE(series_aggregated.total_annotation_groups) AS total_annotation_groups,

  # description:
  # total number of individual annotations across all groups in this series
  ANY_VALUE(series_aggregated.total_annotations) AS total_annotations,

  # description:
  # comma-separated list of distinct graphic types used (POINT, POLYLINE, POLYGON, ELLIPSE, RECTANGLE),
  # aggregated from DICOM GraphicType attribute across all annotation groups
  ANY_VALUE(series_aggregated.GraphicTypes) AS GraphicTypes,

  # description:
  # comma-separated list of annotation generation types (MANUAL or AUTOMATIC),
  # aggregated from DICOM AnnotationGroupGenerationType attribute
  ANY_VALUE(series_aggregated.AnnotationGenerationTypes) AS AnnotationGenerationTypes,

  # description:
  # array of annotation group labels from DICOM AnnotationGroupLabel attribute
  ANY_VALUE(series_aggregated.AnnotationGroupLabels) AS AnnotationGroupLabels,

  # description:
  # array of annotation property category code meanings from DICOM AnnotationPropertyCategoryCodeSequence
  ANY_VALUE(series_aggregated.AnnotationPropertyCategories) AS AnnotationPropertyCategories,

  # description:
  # array of annotation property type code meanings from DICOM AnnotationPropertyTypeCodeSequence
  ANY_VALUE(series_aggregated.AnnotationPropertyTypes) AS AnnotationPropertyTypes,

  # description:
  # comma-separated algorithm names from DICOM AlgorithmName attribute
  # in AnnotationGroupAlgorithmIdentificationSequence (when applicable)
  ANY_VALUE(series_aggregated.AlgorithmNames) AS AlgorithmNames,

  # description:
  # content label as defined in DICOM ContentLabel attribute
  ANY_VALUE(ann_base.ContentLabel) AS ContentLabel,

  # description:
  # content description as defined in DICOM ContentDescription attribute
  ANY_VALUE(ann_base.ContentDescription) AS ContentDescription,

  # description:
  # comma-separated DICOM SOP Class UIDs of the referenced images
  ANY_VALUE(referenced_series.ReferencedSOPClassUIDs) AS ReferencedSOPClassUIDs,

  # description:
  # SeriesInstanceUID of the referenced image series that the annotations apply to
  ANY_VALUE(referenced_series.referenced_SeriesInstanceUID) AS referenced_SeriesInstanceUID

FROM
  ann_base
LEFT JOIN
  series_aggregated ON ann_base.SeriesInstanceUID = series_aggregated.SeriesInstanceUID
LEFT JOIN
  referenced_series ON ann_base.SeriesInstanceUID = referenced_series.ann_SeriesInstanceUID
GROUP BY
  ann_base.SeriesInstanceUID
