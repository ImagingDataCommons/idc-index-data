-- Note that this query can be substituted with a much simpler one below
-- once this PR is merged and https://github.com/ImagingDataCommons/etl_flow/pull/104
-- the latter makes it to a public release
--
-- SELECT
--   * EXCEPT(Modality)
-- FROM
--   `bigquery-public-data.idc_v19.dicom_metadata_curated_series_level`
-- WHERE
--   Modality = "SM"

# table-description:
# This table contains metadata about the slide microscopy (SM) series available in IDC. Each row
# corresponds to a DICOM series, and contains attributes specific to SM series, such as the pixel spacing at the maximum
# resolution layer, the power of the objective lens used to digitize the slide, and the anatomic location
# from where the imaged specimen was collected. This table can be joined with the main index table using the
# `SeriesInstanceUID` column.
WITH
  temp_table AS (
  SELECT
    dicom_all.SeriesInstanceUID,
    ANY_VALUE(Modality) AS Modality,
    STRING_AGG(DISTINCT(collection_id),",") AS collection_id,
    ANY_VALUE(OpticalPathSequence[SAFE_OFFSET(0)].ObjectiveLensPower) AS ObjectiveLensPower,
    MAX(DISTINCT(TotalPixelMatrixColumns)) AS max_TotalPixelMatrixColumns,
    MAX(DISTINCT(TotalPixelMatrixRows)) AS max_TotalPixelMatrixRows,
    MAX(DISTINCT(`Columns`)) AS max_Columns,
    MAX(DISTINCT(`Rows`)) AS max_Rows,
    MIN(DISTINCT(SAFE_CAST(PixelSpacing[SAFE_OFFSET(0)] AS FLOAT64))) AS min_spacing_0,
    MIN(SAFE_CAST(SharedFunctionalGroupsSequence[SAFE_OFFSET(0)].PixelMeasuresSequence[SAFE_OFFSET(0)]. PixelSpacing[SAFE_OFFSET(0)] AS FLOAT64)) AS fg_min_spacing_0,
    ARRAY_AGG(DISTINCT(CONCAT(SpecimenDescriptionSequence[SAFE_OFFSET(0)].PrimaryAnatomicStructureSequence[SAFE_OFFSET(0)].CodingSchemeDesignator,":", SpecimenDescriptionSequence[SAFE_OFFSET(0)].PrimaryAnatomicStructureSequence[SAFE_OFFSET(0)].CodeValue, ":", SpecimenDescriptionSequence[SAFE_OFFSET(0)].PrimaryAnatomicStructureSequence[SAFE_OFFSET(0)].CodeMeaning)) IGNORE NULLS)[SAFE_OFFSET(0)] AS primaryAnatomicStructure_code_str,
    ARRAY_AGG(DISTINCT(CONCAT(SpecimenDescriptionSequence[SAFE_OFFSET(0)].PrimaryAnatomicStructureSequence[SAFE_OFFSET(0)].PrimaryAnatomicStructureModifierSequence[SAFE_OFFSET(0)].CodingSchemeDesignator,":", SpecimenDescriptionSequence[SAFE_OFFSET(0)].PrimaryAnatomicStructureSequence[SAFE_OFFSET(0)].PrimaryAnatomicStructureModifierSequence[SAFE_OFFSET(0)].CodeValue, ":", SpecimenDescriptionSequence[SAFE_OFFSET(0)].PrimaryAnatomicStructureSequence[SAFE_OFFSET(0)].PrimaryAnatomicStructureModifierSequence[SAFE_OFFSET(0)].CodeMeaning)) IGNORE NULLS)[SAFE_OFFSET(0)] AS primaryAnatomicStructureModifier_code_str,

    ARRAY_AGG(DISTINCT(CONCAT(OpticalPathSequence[SAFE_OFFSET(0)].IlluminationTypeCodeSequence[SAFE_OFFSET(0)].CodingSchemeDesignator,":", OpticalPathSequence[SAFE_OFFSET(0)].IlluminationTypeCodeSequence[SAFE_OFFSET(0)].CodeValue, ":", OpticalPathSequence[SAFE_OFFSET(0)].IlluminationTypeCodeSequence[SAFE_OFFSET(0)].CodeMeaning)) IGNORE NULLS)[SAFE_OFFSET(0)] AS illuminationType_code_str,

    ARRAY_AGG(DISTINCT(CONCAT(AdmittingDiagnosesCodeSequence[SAFE_OFFSET(0)].CodingSchemeDesignator,":", AdmittingDiagnosesCodeSequence[SAFE_OFFSET(0)].CodeValue, ":", AdmittingDiagnosesCodeSequence[SAFE_OFFSET(0)].CodeMeaning)) IGNORE NULLS)[SAFE_OFFSET(0)] AS admittingDiagnosis_code_str


  FROM
    `bigquery-public-data.idc_v23.dicom_all` AS dicom_all
  GROUP BY
    SeriesInstanceUID
  ),

SpecimenPreparationSequence_unnested AS (
      SELECT
        SeriesInstanceUID,
        concept_name_code_sequence.CodeMeaning AS cnc_cm,
        concept_name_code_sequence.CodingSchemeDesignator AS cnc_csd,
        concept_name_code_sequence.CodeValue AS cnc_val,
        concept_code_sequence.CodeMeaning AS ccs_cm,
        concept_code_sequence.CodingSchemeDesignator AS ccs_csd,
        concept_code_sequence.CodeValue AS ccs_val,
      FROM `bigquery-public-data.idc_v23.dicom_all`,
      UNNEST(SpecimenDescriptionSequence[SAFE_OFFSET(0)].SpecimenPreparationSequence) as preparation_unnest_step1,
      UNNEST(preparation_unnest_step1.SpecimenPreparationStepContentItemSequence) as preparation_unnest_step2,
      UNNEST(preparation_unnest_step2.ConceptNameCodeSequence) as concept_name_code_sequence,
      UNNEST(preparation_unnest_step2.ConceptCodeSequence) as concept_code_sequence
    ),

    slide_embedding AS (
    SELECT
      SeriesInstanceUID,
      ARRAY_AGG(DISTINCT(CONCAT(ccs_cm,":",ccs_csd,":",ccs_val))) as embeddingMedium_code_str
    FROM SpecimenPreparationSequence_unnested
    WHERE (cnc_csd = 'SCT' and cnc_val = '430863003') -- CodeMeaning is 'Embedding medium'
    GROUP BY SeriesInstanceUID
    ),

    slide_fixative AS (
    SELECT
      SeriesInstanceUID,
      ARRAY_AGG(DISTINCT(CONCAT(ccs_cm, ":", ccs_csd,":",ccs_val))) as tissueFixative_code_str
    FROM SpecimenPreparationSequence_unnested
    WHERE (cnc_csd = 'SCT' and cnc_val = '430864009') -- CodeMeaning is 'Tissue Fixative'
    GROUP BY SeriesInstanceUID
    ),

    slide_staining AS (
    SELECT
      SeriesInstanceUID,
      ARRAY_AGG(DISTINCT(CONCAT(ccs_cm, ":", ccs_csd,":",ccs_val))) as staining_usingSubstance_code_str,
    FROM SpecimenPreparationSequence_unnested
    WHERE (cnc_csd = 'SCT' and cnc_val = '424361007') -- CodeMeaning is 'Using substance'
    GROUP BY SeriesInstanceUID
    )

SELECT
  # description:
  # DICOM SeriesInstanceUID identifier of the series
  temp_table.SeriesInstanceUID,
  -- Embedding Medium
  # description:
  # embedding medium used for the slide preparation
  ARRAY(
    SELECT IF(code IS NULL, NULL, SPLIT(code, ':')[SAFE_OFFSET(0)])
    FROM UNNEST(embeddingMedium_code_str) AS code
  ) AS embeddingMedium_CodeMeaning,
  # description:
  # embedding medium code tuple
  ARRAY(
    SELECT IF(code IS NULL, NULL,
              IF(STRPOS(code, ':') = 0, NULL,
                 SUBSTR(code, STRPOS(code, ':') + 1)))
    FROM UNNEST(embeddingMedium_code_str) AS code
  ) AS embeddingMedium_code_designator_value_str,
  -- Tissue Fixative
  # description:
  # tissue fixative used for the slide preparation
  ARRAY(
    SELECT IF(code IS NULL, NULL, SPLIT(code, ':')[SAFE_OFFSET(0)])
    FROM UNNEST(tissueFixative_code_str) AS code
  ) AS tissueFixative_CodeMeaning,
  # description:
  # tissue fixative code tuple
  ARRAY(
    SELECT IF(code IS NULL, NULL,
              IF(STRPOS(code, ':') = 0, NULL,
                 SUBSTR(code, STRPOS(code, ':') + 1)))
    FROM UNNEST(tissueFixative_code_str) AS code
  ) AS tissueFixative_code_designator_value_str,
  -- Staining using substance
  # description:
  # staining substances used for the slide preparation
  ARRAY(
    SELECT IF(code IS NULL, NULL, SPLIT(code, ':')[SAFE_OFFSET(0)])
    FROM UNNEST(staining_usingSubstance_code_str) AS code
  ) AS staining_usingSubstance_CodeMeaning,
  # description:
  # staining using substance code tuple
  ARRAY(
    SELECT IF(code IS NULL, NULL,
              IF(STRPOS(code, ':') = 0, NULL,
                 SUBSTR(code, STRPOS(code, ':') + 1)))
    FROM UNNEST(staining_usingSubstance_code_str) AS code
  ) AS staining_usingSubstance_code_designator_value_str,
  # description:
  # pixel spacing in mm at the maximum resolution layer, rounded to 2 significant figures
  if(COALESCE(min_spacing_0, fg_min_spacing_0) = 0, 0,
    round(COALESCE(min_spacing_0, fg_min_spacing_0) ,CAST(2 -1-floor(log10(abs(COALESCE(min_spacing_0, fg_min_spacing_0) ))) AS INT64))) AS min_PixelSpacing_2sf,
  # description:
  # width of the image at the maximum resolution
  COALESCE(max_TotalPixelMatrixColumns, max_Columns) AS max_TotalPixelMatrixColumns,
  # description:
  # height of the image at the maximum resolution
  COALESCE(max_TotalPixelMatrixRows, max_Rows) AS max_TotalPixelMatrixRows,
  # description:
  # power of the objective lens of the equipment used to digitize the slide
  SAFE_CAST(ObjectiveLensPower as INT) as ObjectiveLensPower,
  # description:
  # anatomic location from where the imaged specimen was collected
  CONCAT(SPLIT(primaryAnatomicStructure_code_str,":")[SAFE_OFFSET(0)],":",SPLIT(primaryAnatomicStructure_code_str,":")[SAFE_OFFSET(1)]) as primaryAnatomicStructure_code_designator_value_str,
  # description:
  # code tuple for the anatomic location from where the imaged specimen was collected
  SPLIT(primaryAnatomicStructure_code_str,":")[SAFE_OFFSET(2)] as primaryAnatomicStructure_CodeMeaning,
  # description:
  # additional characteristics of the specimen, such as whether it is a tumor or normal tissue (when available)
  CONCAT(SPLIT(primaryAnatomicStructureModifier_code_str,":")[SAFE_OFFSET(0)],":",SPLIT(primaryAnatomicStructureModifier_code_str,":")[SAFE_OFFSET(1)]) as primaryAnatomicStructureModifier_code_designator_value_str,
  # description:
  # code tuple for additional characteristics of the specimen, such as whether it is a tumor or normal tissue (when available)
  SPLIT(primaryAnatomicStructureModifier_code_str,":")[SAFE_OFFSET(2)] as primaryAnatomicStructureModifier_CodeMeaning,
  # description:
  # illumination type used during slide digitization
  CONCAT(SPLIT(illuminationType_code_str,":")[SAFE_OFFSET(0)],":",SPLIT(illuminationType_code_str,":")[SAFE_OFFSET(1)]) as illuminationType_code_designator_value_str,
  # description:
  # code tuple for the illumination type used during slide digitization
  SPLIT(illuminationType_code_str,":")[SAFE_OFFSET(2)] as illuminationType_CodeMeaning,
  # description:
  # admitting diagnosis associated with the specimen imaged on the slide (when available)
  CONCAT(SPLIT(admittingDiagnosis_code_str,":")[SAFE_OFFSET(0)],":",SPLIT(admittingDiagnosis_code_str,":")[SAFE_OFFSET(1)]) as admittingDiagnosis_code_designator_value_str,
  # description:
  # code tuple for the admitting diagnosis associated with the specimen imaged on the slide (when available)
  SPLIT(admittingDiagnosis_code_str,":")[SAFE_OFFSET(2)] as admittingDiagnosis_CodeMeaning,
FROM
  temp_table
LEFT JOIN slide_embedding on temp_table.SeriesInstanceUID = slide_embedding.SeriesInstanceUID
LEFT JOIN slide_fixative on temp_table.SeriesInstanceUID = slide_fixative.SeriesInstanceUID
LEFT JOIN slide_staining on temp_table.SeriesInstanceUID = slide_staining.SeriesInstanceUID
WHERE
  Modality = "SM"
