# table-description:
# This table contains metadata about the slide microscopy (SM) series available in IDC. Each row
# corresponds to an instance from a DICOM Slide Microscopy series available from IDC, identified by
# `SOPInstanceUID`, and contains attributes specific to SM series, such as the pixel spacing at the maximum
# resolution layer, the power of the objective lens used to digitize the slide, and the anatomic location
# from where the imaged specimen was collected. This table can be joined with the main index table
# and/or with `sm_index` using the `SeriesInstanceUID` column.
WITH
  SpecimenPreparationSequence_unnested AS (
  SELECT
    SOPInstanceUID,
    concept_name_code_sequence.CodeMeaning AS cnc_cm,
    concept_name_code_sequence.CodingSchemeDesignator AS cnc_csd,
    concept_name_code_sequence.CodeValue AS cnc_val,
    concept_code_sequence.CodeMeaning AS ccs_cm,
    concept_code_sequence.CodingSchemeDesignator AS ccs_csd,
    concept_code_sequence.CodeValue AS ccs_val,
  FROM
    `bigquery-public-data.idc_v23.dicom_all`,
    UNNEST(SpecimenDescriptionSequence[SAFE_OFFSET(0)].SpecimenPreparationSequence) AS preparation_unnest_step1,
    UNNEST(preparation_unnest_step1.SpecimenPreparationStepContentItemSequence) AS preparation_unnest_step2,
    UNNEST(preparation_unnest_step2.ConceptNameCodeSequence) AS concept_name_code_sequence,
    UNNEST(preparation_unnest_step2.ConceptCodeSequence) AS concept_code_sequence ),
  slide_embedding AS (
  SELECT
    SOPInstanceUID,
    ARRAY_AGG(DISTINCT(CONCAT(ccs_cm,":",ccs_csd,":",ccs_val))) AS embeddingMedium_code_str
  FROM
    SpecimenPreparationSequence_unnested
  WHERE
    (cnc_csd = 'SCT'
      AND cnc_val = '430863003') -- CodeMeaning is 'Embedding medium'
  GROUP BY
    SOPInstanceUID ),
  slide_fixative AS (
  SELECT
    SOPInstanceUID,
    ARRAY_AGG(DISTINCT(CONCAT(ccs_cm, ":", ccs_csd,":",ccs_val))) AS tissueFixative_code_str
  FROM
    SpecimenPreparationSequence_unnested
  WHERE
    (cnc_csd = 'SCT'
      AND cnc_val = '430864009') -- CodeMeaning is 'Tissue Fixative'
  GROUP BY
    SOPInstanceUID ),
  slide_staining AS (
  SELECT
    SOPInstanceUID,
    ARRAY_AGG(DISTINCT(CONCAT(ccs_cm, ":", ccs_csd,":",ccs_val))) AS staining_usingSubstance_code_str,
  FROM
    SpecimenPreparationSequence_unnested
  WHERE
    (cnc_csd = 'SCT'
      AND cnc_val = '424361007') -- CodeMeaning is 'Using substance'
  GROUP BY
    SOPInstanceUID )
SELECT
  # description:
  # unique identifier of the instance
  dicom_all.SOPInstanceUID AS SOPInstanceUID,
  # description:
  # unique identifier of the series
  dicom_all.SeriesInstanceUID AS SeriesInstanceUID,
  -- Embedding Medium
  # description:
  # embedding medium used for the slide preparation
  ARRAY(
  SELECT
  IF
    (code IS NULL, NULL, SPLIT(code, ':')[SAFE_OFFSET(0)])
  FROM
    UNNEST(embeddingMedium_code_str) AS code ) AS embeddingMedium_CodeMeaning,
  # description:
  # embedding medium code tuple
  ARRAY(
  SELECT
  IF
    (code IS NULL, NULL,
    IF
      (STRPOS(code, ':') = 0, NULL, SUBSTR(code, STRPOS(code, ':') + 1)))
  FROM
    UNNEST(embeddingMedium_code_str) AS code ) AS embeddingMedium_code_designator_value_str,
  -- Tissue Fixative
  # description:
  # tissue fixative used for the slide preparation
  ARRAY(
  SELECT
  IF
    (code IS NULL, NULL, SPLIT(code, ':')[SAFE_OFFSET(0)])
  FROM
    UNNEST(tissueFixative_code_str) AS code ) AS tissueFixative_CodeMeaning,
  # description:
  # tissue fixative code tuple
  ARRAY(
  SELECT
  IF
    (code IS NULL, NULL,
    IF
      (STRPOS(code, ':') = 0, NULL, SUBSTR(code, STRPOS(code, ':') + 1)))
  FROM
    UNNEST(tissueFixative_code_str) AS code ) AS tissueFixative_code_designator_value_str,
  -- Staining using substance
  # description:
  # staining substances used for the slide preparation
  ARRAY(
  SELECT
  IF
    (code IS NULL, NULL, SPLIT(code, ':')[SAFE_OFFSET(0)])
  FROM
    UNNEST(staining_usingSubstance_code_str) AS code ) AS staining_usingSubstance_CodeMeaning,
  # description:
  # staining using substance code tuple
  ARRAY(
  SELECT
  IF
    (code IS NULL, NULL,
    IF
      (STRPOS(code, ':') = 0, NULL, SUBSTR(code, STRPOS(code, ':') + 1)))
  FROM
    UNNEST(staining_usingSubstance_code_str) AS code ) AS staining_usingSubstance_code_designator_value_str,
  -- instance-specific image attributes
  -- NB: there is a caveat that I think in general, we expect square pixels, but in htan_wustl and cptac_luad this assumption does not hold,
  -- and in htan_wustl, the difference is rather large (x2) - waiting to hear from David Clunie about this...
  # description:
  # pixel spacing in mm, rounded to 2 significant figures
  SAFE_CAST(SharedFunctionalGroupsSequence[SAFE_OFFSET(0)].PixelMeasuresSequence[SAFE_OFFSET(0)]. PixelSpacing[SAFE_OFFSET(0)] AS FLOAT64) AS PixelSpacing_0,
  # description:
  # DICOM ImageType attribute
  dicom_all.ImageType AS ImageType,
  # description:
  # DICOM TransferSyntaxUID attribute
  dicom_all.TransferSyntaxUID AS TransferSyntaxUID,
  # description:
  # size of the instance file in bytes
  dicom_all.instance_size AS instance_size,
  # description:
  # number of columns in the image
  dicom_all.TotalPixelMatrixColumns AS TotalPixelMatrixColumns,
  # description:
  # number of rows in the image
  dicom_all.TotalPixelMatrixRows AS TotalPixelMatrixRows,
  -- attributes needed to retrieve the selected instances/files
  # description:
  # unique identifier of the instance within the IDC
  dicom_all.crdc_instance_uuid AS crdc_instance_uuid
FROM
  `bigquery-public-data.idc_v23.dicom_all` AS dicom_all
LEFT JOIN
  slide_embedding
ON
  dicom_all.SOPInstanceUID = slide_embedding.SOPInstanceUID
LEFT JOIN
  slide_fixative
ON
  dicom_all.SOPInstanceUID = slide_fixative.SOPInstanceUID
LEFT JOIN
  slide_staining
ON
  dicom_all.SOPInstanceUID = slide_staining.SOPInstanceUID
WHERE
  dicom_all.Modality="SM"
ORDER BY
  SeriesInstanceUID DESC
