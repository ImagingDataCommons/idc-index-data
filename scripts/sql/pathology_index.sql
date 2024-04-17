SELECT
  collection_id,
  source_DOI,
  PatientID,
  PatientAge,
  PatientSex,
  StudyInstanceUID,
  StudyDate,
  StudyDescription,
  BodyPartExamined,
  SeriesInstanceUID,
  Modality,
  Manufacturer,
  ManufacturerModelName,
  SeriesDate,
  SeriesDescription,
  SeriesNumber,
  license_short_name,
  CONCAT(series_aws_url,'*')series_aws_url,
  sum(ROUND((SAFE_CAST(instance_size AS float64))/1000000, 2)) AS series_size_MB,
  ARRAY_AGG(
  STRUCT(
  FrameOfReferenceUID,
  crdc_instance_uuid,
  ContainerIdentifier,
  pms.PixelSpacing[0] AS PixelSpacing,
  `Rows`,
  `Columns`,
  TotalPixelMatrixRows,
  TotalPixelMatrixColumns,
  it AS ImageType,
  TransferSyntaxUID,
  pass.CodeValue AS PrimaryAnatomicStructureSequence_CodeValue,
  pass.CodeMeaning AS PrimaryAnatomicStructureSequence_CodeMeaning,
  pass.CodingSchemeDesignator AS PrimaryAnatomicStructureSequence_CodingSchemeDesignator,
  pasms.CodeValue AS PrimaryAnatomicStructureModifierSequence_CodeValue,
  pasms.CodeMeaning AS PrimaryAnatomicStructureModifierSequence_CodeMeaning,
  pasms.CodingSchemeDesignator AS PrimaryAnatomicStructureModifierSequence_CodingSchemeDesignator,
  sds.SpecimenUID,
  spscis.ValueType AS SpecimenPreparationStepContentItemSequence_ValueType,
  spscis_cncs.CodeValue AS SpecimenPreparationStepContentItemSequence_ConceptNameCodeSequence_CodeValue,
  spscis_cncs.CodeMeaning AS SpecimenPreparationStepContentItemSequence_ConceptNameCodeSequence_CodeMeaning,
  spscis_cncs.CodingSchemeDesignator AS SpecimenPreparationStepContentItemSequence_ConceptNameCodeSequence_CodingSchemeDesignator,
  spscis_ccs.CodeValue AS SpecimenPreparationStepContentItemSequence_ConceptCodeSequence_CodeValue,
  spscis_ccs.CodeMeaning AS SpecimenPreparationStepContentItemSequence_ConceptCodeSequence_CodeMeaning,
  spscis_ccs.CodingSchemeDesignator AS SpecimenPreparationStepContentItemSequence_ConceptCodeSequence_CodingSchemeDesignator,
  ops.LightPathFilterPassThroughWavelength,
  ops.IlluminationWavelength,
  ops_itcs.CodeValue AS OpticalPathSequence_IlluminationTypeCodeSequence_CodeValue,
  ops_itcs.CodeMeaning AS OpticalPathSequence_IlluminationTypeCodeSequence_CodeMeaning,
  ops_itcs.CodingSchemeDesignator AS OpticalPathSequence_IlluminationTypeCodeSequence_CodingSchemeDesignator,
  ops_iccs.CodeValue AS OpticalPathSequence_IlluminationColorCodeSequence_CodeValue,
  ops_iccs.CodeMeaning AS OpticalPathSequence_IlluminationColorCodeSequence_CodeMeaning,
  ops_iccs.CodingSchemeDesignator AS OpticalPathSequence_IlluminationColorCodeSequence_CodingSchemeDesignator)) AS Attributes
FROM
  `bigquery-public-data.idc_current.dicom_all` idc
LEFT JOIN
  unnest (ImageType) AS it
LEFT JOIN
  unnest (idc.SharedFunctionalGroupsSequence) AS sfgs
LEFT JOIN
  unnest (sfgs.PixelMeasuresSequence) AS pms
LEFT JOIN
  unnest (SpecimenDescriptionSequence) AS sds
LEFT JOIN
  unnest (sds.PrimaryAnatomicStructureSequence) AS pass
LEFT JOIN
  unnest (pass.PrimaryAnatomicStructureModifierSequence) AS pasms
LEFT JOIN
  unnest (sds.SpecimenPreparationSequence) AS sps
LEFT JOIN
  unnest (sps.SpecimenPreparationStepContentItemSequence) AS spscis
LEFT JOIN
  unnest (spscis.ConceptNameCodeSequence) AS spscis_cncs
LEFT JOIN
  unnest (spscis.ConceptCodeSequence) AS spscis_ccs
LEFT JOIN
  unnest (OpticalPathSequence) AS ops
LEFT JOIN
  unnest (ops.IlluminationTypeCodeSequence) AS ops_itcs
LEFT JOIN
  unnest (ops.IlluminationColorCodeSequence) AS ops_iccs
WHERE
  Modality in ('SM')
GROUP BY
  collection_id,
  source_DOI,
  PatientID,
  PatientAge,
  PatientSex,
  StudyInstanceUID,
  StudyDate,
  StudyDescription,
  BodyPartExamined,
  SeriesInstanceUID,
  Modality,
  Manufacturer,
  ManufacturerModelName,
  SeriesDate,
  SeriesDescription,
  SeriesNumber,
  license_short_name,
  series_aws_url
