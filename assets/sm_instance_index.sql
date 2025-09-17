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
    `bigquery-public-data.idc_v22.dicom_all`,
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
  dicom_all.SOPInstanceUID,
  dicom_all.SeriesInstanceUID,
  -- Embedding Medium
  ARRAY(
  SELECT
  IF
    (code IS NULL, NULL, SPLIT(code, ':')[SAFE_OFFSET(0)])
  FROM
    UNNEST(embeddingMedium_code_str) AS code ) AS embeddingMedium_CodeMeaning,
  ARRAY(
  SELECT
  IF
    (code IS NULL, NULL,
    IF
      (STRPOS(code, ':') = 0, NULL, SUBSTR(code, STRPOS(code, ':') + 1)))
  FROM
    UNNEST(embeddingMedium_code_str) AS code ) AS embeddingMedium_code_designator_value_str,
  -- Tissue Fixative
  ARRAY(
  SELECT
  IF
    (code IS NULL, NULL, SPLIT(code, ':')[SAFE_OFFSET(0)])
  FROM
    UNNEST(tissueFixative_code_str) AS code ) AS tissueFixative_CodeMeaning,
  ARRAY(
  SELECT
  IF
    (code IS NULL, NULL,
    IF
      (STRPOS(code, ':') = 0, NULL, SUBSTR(code, STRPOS(code, ':') + 1)))
  FROM
    UNNEST(tissueFixative_code_str) AS code ) AS tissueFixative_code_designator_value_str,
  -- Staining using substance
  ARRAY(
  SELECT
  IF
    (code IS NULL, NULL, SPLIT(code, ':')[SAFE_OFFSET(0)])
  FROM
    UNNEST(staining_usingSubstance_code_str) AS code ) AS staining_usingSubstance_CodeMeaning,
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
  SAFE_CAST(SharedFunctionalGroupsSequence[SAFE_OFFSET(0)].PixelMeasuresSequence[SAFE_OFFSET(0)]. PixelSpacing[SAFE_OFFSET(0)] AS FLOAT64) AS PixelSpacing_0,
  dicom_all.ImageType,
  dicom_all.TransferSyntaxUID,
  dicom_all.instance_size,
  dicom_all.TotalPixelMatrixColumns,
  dicom_all.TotalPixelMatrixRows,
  -- attributes needed to retrieve the selected instances/files
  dicom_all.crdc_instance_uuid
FROM
  `bigquery-public-data.idc_v22.dicom_all` AS dicom_all
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
