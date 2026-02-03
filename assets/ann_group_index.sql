# table-description:
# This table contains detailed metadata about individual annotation groups within
# Microscopy Bulk Simple Annotations (ANN) instances in IDC. Each row corresponds to
# a single annotation group, providing granular information about the graphic type,
# number of annotations, property codes, and algorithm details. This table can be
# joined with ann_index using SeriesInstanceUID for series-level context.

WITH
  ann_instances AS (
    SELECT
      SOPInstanceUID,
      SeriesInstanceUID,
      AnnotationCoordinateType,
      AnnotationGroupSequence,
      ReferencedSeriesSequence,
      instance_size,
      crdc_instance_uuid
    FROM
      `bigquery-public-data.idc_v23.dicom_all`
    WHERE
      Modality = "ANN"
  ),

  -- Get referenced SOP instance for each ANN instance
  referenced_instances AS (
    SELECT
      ann_instances.SOPInstanceUID AS ann_SOPInstanceUID,
      ref_instance.ReferencedSOPInstanceUID
    FROM
      ann_instances
    CROSS JOIN
      UNNEST(ann_instances.ReferencedSeriesSequence) AS ref_series
    CROSS JOIN
      UNNEST(ref_series.ReferencedInstanceSequence) AS ref_instance
  )

SELECT
  # description:
  # DICOM SOPInstanceUID identifier of the annotation instance
  ann_instances.SOPInstanceUID,

  # description:
  # DICOM SeriesInstanceUID for joining with ann_index and idc_index
  ann_instances.SeriesInstanceUID,

  # description:
  # sequential number identifying this annotation group as defined in DICOM AnnotationGroupNumber attribute
  group_item.AnnotationGroupNumber,

  # description:
  # unique identifier for this annotation group as defined in DICOM AnnotationGroupUID attribute
  group_item.AnnotationGroupUID,

  # description:
  # human-readable label as defined in DICOM AnnotationGroupLabel attribute
  group_item.AnnotationGroupLabel,

  # description:
  # how the annotations were generated (MANUAL or AUTOMATIC) as defined in DICOM AnnotationGroupGenerationType attribute
  group_item.AnnotationGroupGenerationType,

  # description:
  # total number of annotations in this group as defined in DICOM NumberOfAnnotations attribute
  group_item.NumberOfAnnotations,

  # description:
  # type of graphic used for annotations (POINT, POLYLINE, POLYGON, ELLIPSE, RECTANGLE) as defined in DICOM GraphicType attribute
  group_item.GraphicType,

  # description:
  # coordinate type (2D or 3D) as defined in DICOM AnnotationCoordinateType attribute
  ann_instances.AnnotationCoordinateType,

  # description:
  # annotation property category code tuple (CodingSchemeDesignator:CodeValue) from DICOM AnnotationPropertyCategoryCodeSequence
  CONCAT(
    group_item.AnnotationPropertyCategoryCodeSequence[SAFE_OFFSET(0)].CodingSchemeDesignator, ":",
    group_item.AnnotationPropertyCategoryCodeSequence[SAFE_OFFSET(0)].CodeValue
  ) AS AnnotationPropertyCategory_code,

  # description:
  # human-readable meaning of the annotation property category from DICOM AnnotationPropertyCategoryCodeSequence
  group_item.AnnotationPropertyCategoryCodeSequence[SAFE_OFFSET(0)].CodeMeaning AS AnnotationPropertyCategory_CodeMeaning,

  # description:
  # annotation property type code tuple (CodingSchemeDesignator:CodeValue) from DICOM AnnotationPropertyTypeCodeSequence
  CONCAT(
    group_item.AnnotationPropertyTypeCodeSequence[SAFE_OFFSET(0)].CodingSchemeDesignator, ":",
    group_item.AnnotationPropertyTypeCodeSequence[SAFE_OFFSET(0)].CodeValue
  ) AS AnnotationPropertyType_code,

  # description:
  # human-readable meaning of the annotation property type from DICOM AnnotationPropertyTypeCodeSequence
  group_item.AnnotationPropertyTypeCodeSequence[SAFE_OFFSET(0)].CodeMeaning AS AnnotationPropertyType_CodeMeaning,

  # description:
  # name of the algorithm from DICOM AlgorithmName attribute in AnnotationGroupAlgorithmIdentificationSequence
  # (when AnnotationGroupGenerationType is AUTOMATIC)
  group_item.AnnotationGroupAlgorithmIdentificationSequence[SAFE_OFFSET(0)].AlgorithmName AS AlgorithmName,

  # description:
  # SOPInstanceUID of a referenced image instance
  ANY_VALUE(referenced_instances.ReferencedSOPInstanceUID) AS ReferencedSOPInstanceUID,

  # description:
  # size of the annotation instance file in bytes
  ann_instances.instance_size,

  # description:
  # CRDC UUID for downloading this annotation instance
  ann_instances.crdc_instance_uuid

FROM
  ann_instances
CROSS JOIN
  UNNEST(ann_instances.AnnotationGroupSequence) AS group_item
LEFT JOIN
  referenced_instances ON ann_instances.SOPInstanceUID = referenced_instances.ann_SOPInstanceUID
GROUP BY
  ann_instances.SOPInstanceUID,
  ann_instances.SeriesInstanceUID,
  group_item.AnnotationGroupNumber,
  group_item.AnnotationGroupUID,
  group_item.AnnotationGroupLabel,
  group_item.AnnotationGroupGenerationType,
  group_item.NumberOfAnnotations,
  group_item.GraphicType,
  ann_instances.AnnotationCoordinateType,
  group_item.AnnotationPropertyCategoryCodeSequence,
  group_item.AnnotationPropertyTypeCodeSequence,
  group_item.AnnotationGroupAlgorithmIdentificationSequence,
  ann_instances.instance_size,
  ann_instances.crdc_instance_uuid
