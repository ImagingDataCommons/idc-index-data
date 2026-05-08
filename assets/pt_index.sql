# table-description:
# This table contains one row per Positron Emission Tomography Image Storage
# (SOPClassUID 1.2.840.10008.5.1.4.1.1.128) DICOM series in IDC, capturing
# PET acquisition, reconstruction and radiopharmaceutical parameters
# that are not included in the main idc_index table.
# The index can be joined to idc_index on SeriesInstanceUID to combine
# universal series metadata with PET-specific acquisition parameters.
# ActualFrameDuration is reported as an array of all distinct per-instance values
# because it legitimately varies across frames in dynamic (multi-frame) PET acquisitions.
# All other attributes are constant within a series and are aggregated with ANY_VALUE.

WITH pt_data AS (
  SELECT
    SeriesInstanceUID,
    ANY_VALUE(ARRAY_TO_STRING(SeriesType, '/')) AS SeriesType,
    ANY_VALUE(Units) AS Units,
    ANY_VALUE(DecayCorrection) AS DecayCorrection,
    ANY_VALUE(CorrectedImage) AS CorrectedImage,
    ANY_VALUE(RandomsCorrectionMethod) AS RandomsCorrectionMethod,
    ANY_VALUE(ReconstructionMethod) AS ReconstructionMethod,
    ARRAY_AGG(DISTINCT SAFE_CAST(ActualFrameDuration AS FLOAT64) IGNORE NULLS
              ORDER BY SAFE_CAST(ActualFrameDuration AS FLOAT64)) AS ActualFrameDuration,
    ANY_VALUE(ScatterCorrectionMethod) AS ScatterCorrectionMethod,
    ANY_VALUE(AttenuationCorrectionMethod) AS AttenuationCorrectionMethod,
    ANY_VALUE(RadiopharmaceuticalInformationSequence[SAFE_OFFSET(0)].RadionuclideCodeSequence[SAFE_OFFSET(0)].CodeMeaning) AS RadionuclideCodeMeaning,
    ANY_VALUE(SAFE_CAST(RadiopharmaceuticalInformationSequence[SAFE_OFFSET(0)].RadionuclideTotalDose AS FLOAT64)) AS RadionuclideTotalDose,
    CAST(ANY_VALUE(RadiopharmaceuticalInformationSequence[SAFE_OFFSET(0)].RadiopharmaceuticalStartTime) AS STRING) AS RadiopharmaceuticalStartTime,
    ANY_VALUE(RadiopharmaceuticalInformationSequence[SAFE_OFFSET(0)].Radiopharmaceutical) AS Radiopharmaceutical,
    ANY_VALUE(SAFE_CAST(PixelSpacing[SAFE_OFFSET(0)] AS FLOAT64)) AS PixelSpacing_row_mm,
    ANY_VALUE(SAFE_CAST(PixelSpacing[SAFE_OFFSET(1)] AS FLOAT64)) AS PixelSpacing_col_mm,
    ANY_VALUE(`Rows`) AS `Rows`,
    ANY_VALUE(`Columns`) AS `Columns`,
    ANY_VALUE(SAFE_CAST(SliceThickness AS FLOAT64)) AS SliceThickness,
    ANY_VALUE(NumberOfSlices) AS NumberOfSlices,
    ANY_VALUE(NumberOfTimeSlices) AS NumberOfTimeSlices
  FROM `bigquery-public-data.idc_v24.dicom_all`
  WHERE SOPClassUID = '1.2.840.10008.5.1.4.1.1.128'
  GROUP BY SeriesInstanceUID
)
SELECT
  # description:
  # DICOM SeriesInstanceUID — unique identifier of the PET series; use to join with idc_index
  SeriesInstanceUID,

  # description:
  # acquisition type of the series as defined in DICOM SeriesType attribute,
  # encoded as a slash-separated string of the two type values
  # (e.g., STATIC/IMAGE, DYNAMIC/IMAGE, GATED/IMAGE, WHOLE BODY/IMAGE);
  # constant across instances — aggregated with ANY_VALUE(ARRAY_TO_STRING(SeriesType, '/'))
  SeriesType,

  # description:
  # pixel value units as defined in DICOM Units attribute
  # (e.g., BQML = Bq/mL, CNTS = counts, CPS = counts/s, GML = g/mL);
  # constant across instances — aggregated with ANY_VALUE
  Units,

  # description:
  # type of decay correction applied as defined in DICOM DecayCorrection attribute
  # (START = corrected to scan start time, ADMIN = corrected to radiopharmaceutical
  # administration time, NONE = no correction);
  # constant across instances — aggregated with ANY_VALUE
  DecayCorrection,

  # description:
  # list of corrections applied to the image as defined in DICOM CorrectedImage attribute
  # (e.g., ATTN = attenuation, SCAT = scatter, DECY = decay, RAN = randoms);
  # may contain multiple values; constant across instances — aggregated with ANY_VALUE
  CorrectedImage,

  # description:
  # method used for randoms correction as defined in DICOM RandomsCorrectionMethod attribute;
  # constant across instances — aggregated with ANY_VALUE
  RandomsCorrectionMethod,

  # description:
  # reconstruction algorithm as defined in DICOM ReconstructionMethod attribute
  # (e.g., OSEM, FBP);
  # constant across instances — aggregated with ANY_VALUE
  ReconstructionMethod,

  # description:
  # distinct actual frame durations in ms present in the series,
  # derived from DICOM ActualFrameDuration attribute;
  # aggregated as ARRAY_AGG(DISTINCT) across all instances because ActualFrameDuration
  # legitimately varies across frames in dynamic PET acquisitions;
  # single-element array for static PET, multi-element for dynamic PET with
  # variable frame durations
  ActualFrameDuration,

  # description:
  # scatter correction method as defined in DICOM ScatterCorrectionMethod attribute;
  # constant across instances — aggregated with ANY_VALUE
  ScatterCorrectionMethod,

  # description:
  # attenuation correction method as defined in DICOM AttenuationCorrectionMethod attribute;
  # constant across instances — aggregated with ANY_VALUE
  AttenuationCorrectionMethod,

  # description:
  # code meaning of the radionuclide used, from
  # RadiopharmaceuticalInformationSequence[0].RadionuclideCodeSequence[0].CodeMeaning;
  # (e.g., Fluorine F18, Gallium Ga-68);
  # constant across instances — aggregated with ANY_VALUE
  RadionuclideCodeMeaning,

  # description:
  # total administered dose of the radionuclide in Bq, from
  # RadiopharmaceuticalInformationSequence[0].RadionuclideTotalDose;
  # constant across instances — aggregated with ANY_VALUE
  RadionuclideTotalDose,

  # description:
  # time of radiopharmaceutical administration (injection time), from
  # RadiopharmaceuticalInformationSequence[0].RadiopharmaceuticalStartTime;
  # stored as STRING (HH:MM:SS.FFFFFF) because DICOM TIME type is not supported
  # in parquet output; constant across instances — aggregated with ANY_VALUE
  RadiopharmaceuticalStartTime,

  # description:
  # free-text name of the radiopharmaceutical as defined in
  # RadiopharmaceuticalInformationSequence[0].Radiopharmaceutical
  # (e.g., Fluorodeoxyglucose F^18^); values are not standardized across sites;
  # see RadionuclideCodeMeaning for a more consistent alternative;
  # constant across instances — aggregated with ANY_VALUE
  Radiopharmaceutical,

  # description:
  # in-plane pixel spacing along the row direction in mm, derived from DICOM PixelSpacing[0];
  # PET pixel spacing is isotropic in almost all series in IDC;
  # aggregated with ANY_VALUE — constant across instances within a series
  PixelSpacing_row_mm,

  # description:
  # in-plane pixel spacing along the column direction in mm, derived from DICOM PixelSpacing[1];
  # PET pixel spacing is isotropic in almost all series in IDC;
  # aggregated with ANY_VALUE — constant across instances within a series
  PixelSpacing_col_mm,

  # description:
  # number of pixel rows per image slice as defined in DICOM Rows attribute;
  # constant across instances — aggregated with ANY_VALUE
  `Rows`,

  # description:
  # number of pixel columns per image slice as defined in DICOM Columns attribute;
  # constant across instances — aggregated with ANY_VALUE
  `Columns`,

  # description:
  # nominal slice thickness in mm as defined in DICOM SliceThickness attribute;
  # constant across instances — aggregated with ANY_VALUE
  SliceThickness,

  # description:
  # total number of slices in the series as defined in DICOM NumberOfSlices attribute;
  # constant across instances — aggregated with ANY_VALUE
  NumberOfSlices,

  # description:
  # number of time frames in the series as defined in DICOM NumberOfTimeSlices attribute;
  # populated only for dynamic (multi-frame) PET series, NULL for static PET;
  # constant across instances — aggregated with ANY_VALUE
  NumberOfTimeSlices

FROM pt_data
-- Sort by low-cardinality acquisition parameters first to improve parquet compression
ORDER BY
  Units NULLS LAST,
  RadionuclideCodeMeaning NULLS LAST,
  SeriesInstanceUID
