# table-description:
# This table contains detailed metadata about individual annotation groups within
# Microscopy Bulk Simple Annotations (ANN) series in IDC. Each row corresponds to
# a single annotation group, providing granular information about the graphic type,
# number of annotations, property codes, and algorithm details. This table can be
# joined with ann_index using SeriesInstanceUID for series-level context.
# Note: ANN series are assumed to contain a single instance.

SELECT
  # description:
  # DICOM SeriesInstanceUID for joining with ann_index and idc_index
  ann.SeriesInstanceUID,

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
  group_item.AnnotationGroupAlgorithmIdentificationSequence[SAFE_OFFSET(0)].AlgorithmName AS AlgorithmName

FROM
  `bigquery-public-data.idc_v23.dicom_all` AS ann
CROSS JOIN
  UNNEST(ann.AnnotationGroupSequence) AS group_item
WHERE
  # Microscopy Bulk Simple Annotations SOP Class UID - more reliable than Modality = "ANN"
  SOPClassUID = "1.2.840.10008.5.1.4.1.1.91.1"
