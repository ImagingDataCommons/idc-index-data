# table-description:
# This table contains one row per CT Image Storage (SOPClassUID 1.2.840.10008.5.1.4.1.1.2)
# DICOM series in IDC, capturing acquisition and reconstruction parameters
# that are not included in the main idc_index table.
# The index can be joined to idc_index on SeriesInstanceUID to combine
# universal series metadata with CT-specific acquisition parameters.
# For XRayTubeCurrent, Exposure, and ExposureTime — which vary across instances within
# a series due to dose modulation — both min and max values are reported.
# All other attributes are aggregated with ANY_VALUE (one representative instance).

WITH ct_data AS (
  SELECT
    SeriesInstanceUID,
    ANY_VALUE(ImageType) AS ImageType,
    ANY_VALUE(SAFE_CAST(PixelSpacing[SAFE_OFFSET(0)] AS FLOAT64)) AS PixelSpacing_row_mm,
    ANY_VALUE(SAFE_CAST(PixelSpacing[SAFE_OFFSET(1)] AS FLOAT64)) AS PixelSpacing_col_mm,
    ANY_VALUE(`Rows`) AS `Rows`,
    ANY_VALUE(`Columns`) AS `Columns`,
    ANY_VALUE(SAFE_CAST(SliceThickness AS FLOAT64)) AS SliceThickness,
    ANY_VALUE(SAFE_CAST(KVP AS FLOAT64)) AS KVP,
    ANY_VALUE(ScanOptions) AS ScanOptions,
    ANY_VALUE(ConvolutionKernel) AS ConvolutionKernel,
    ANY_VALUE(SAFE_CAST(GantryDetectorTilt AS FLOAT64)) AS GantryDetectorTilt,
    MIN(SAFE_CAST(XRayTubeCurrent AS FLOAT64)) AS XRayTubeCurrent_min,
    MAX(SAFE_CAST(XRayTubeCurrent AS FLOAT64)) AS XRayTubeCurrent_max,
    ANY_VALUE(FilterType) AS FilterType,
    MIN(SAFE_CAST(Exposure AS FLOAT64)) AS Exposure_min,
    MAX(SAFE_CAST(Exposure AS FLOAT64)) AS Exposure_max,
    MIN(SAFE_CAST(ExposureTime AS FLOAT64)) AS ExposureTime_min,
    MAX(SAFE_CAST(ExposureTime AS FLOAT64)) AS ExposureTime_max,
    ANY_VALUE(SAFE_CAST(DataCollectionDiameter AS FLOAT64)) AS DataCollectionDiameter,
    ANY_VALUE(SAFE_CAST(ReconstructionDiameter AS FLOAT64)) AS ReconstructionDiameter,
    ANY_VALUE(SpiralPitchFactor) AS SpiralPitchFactor
  FROM `bigquery-public-data.idc_v24.dicom_all`
  WHERE SOPClassUID = '1.2.840.10008.5.1.4.1.1.2'
  GROUP BY SeriesInstanceUID
)
SELECT
  # description:
  # DICOM SeriesInstanceUID — unique identifier of the CT series; use to join with idc_index
  SeriesInstanceUID,

  # description:
  # image type values as defined in DICOM ImageType attribute
  # (e.g., ORIGINAL/DERIVED, PRIMARY/SECONDARY, AXIAL/LOCALIZER);
  # aggregated with ANY_VALUE — constant across instances within a series
  ImageType,

  # description:
  # in-plane pixel spacing along the row direction in mm, derived from DICOM PixelSpacing[0];
  # a subset of CT series have anisotropic spacing where this differs from PixelSpacing_col_mm;
  # aggregated with ANY_VALUE — constant across instances within a series
  PixelSpacing_row_mm,

  # description:
  # in-plane pixel spacing along the column direction in mm, derived from DICOM PixelSpacing[1];
  # a subset of CT series have anisotropic spacing where this differs from PixelSpacing_row_mm;
  # aggregated with ANY_VALUE — constant across instances within a series
  PixelSpacing_col_mm,

  # description:
  # number of pixel rows per image slice as defined in DICOM Rows attribute;
  # aggregated with ANY_VALUE — constant across instances within a series
  `Rows`,

  # description:
  # number of pixel columns per image slice as defined in DICOM Columns attribute;
  # aggregated with ANY_VALUE — constant across instances within a series
  `Columns`,

  # description:
  # nominal reconstructed slice thickness in mm as defined in DICOM SliceThickness attribute;
  # aggregated with ANY_VALUE — constant across instances within a series
  SliceThickness,

  # description:
  # peak kilovoltage of the X-ray tube in kV as defined in DICOM KVP attribute;
  # constant across instances — aggregated with ANY_VALUE
  KVP,

  # description:
  # acquisition scan options as defined in DICOM ScanOptions attribute
  # (e.g., HELICAL MODE, AXIAL MODE, SCOUT MODE); may contain multiple values;
  # aggregated with ANY_VALUE — constant across instances within a series
  ScanOptions,

  # description:
  # reconstruction convolution kernel as defined in DICOM ConvolutionKernel attribute;
  # vendor-specific string (e.g., B30f, STANDARD, LUNG); may contain multiple values;
  # aggregated with ANY_VALUE — constant across instances within a series
  ConvolutionKernel,

  # description:
  # nominal angle of the scanning gantry in degrees as defined in DICOM GantryDetectorTilt
  # attribute; non-zero for gantry-tilted acquisitions;
  # constant across instances — aggregated with ANY_VALUE
  GantryDetectorTilt,

  # description:
  # minimum X-ray tube current in mA across all instances in the series,
  # derived from DICOM XRayTubeCurrent attribute;
  # equals XRayTubeCurrent_max for fixed-current acquisitions;
  # lower than XRayTubeCurrent_max for dose-modulated acquisitions (MIN across all instances)
  XRayTubeCurrent_min,

  # description:
  # maximum X-ray tube current in mA across all instances in the series,
  # derived from DICOM XRayTubeCurrent attribute;
  # equals XRayTubeCurrent_min for fixed-current acquisitions;
  # higher than XRayTubeCurrent_min for dose-modulated acquisitions (MAX across all instances)
  XRayTubeCurrent_max,

  # description:
  # type of filter used in the acquisition as defined in DICOM FilterType attribute
  # (e.g., WEDGE, BUTTERFLY, FLAT);
  # constant across instances — aggregated with ANY_VALUE
  FilterType,

  # description:
  # minimum exposure in mAs across all instances in the series,
  # derived from DICOM Exposure attribute;
  # equals Exposure_max for fixed-exposure acquisitions;
  # lower than Exposure_max for dose-modulated acquisitions (MIN across all instances)
  Exposure_min,

  # description:
  # maximum exposure in mAs across all instances in the series,
  # derived from DICOM Exposure attribute;
  # equals Exposure_min for fixed-exposure acquisitions;
  # higher than Exposure_min for dose-modulated acquisitions (MAX across all instances)
  Exposure_max,

  # description:
  # minimum duration of the X-ray exposure in ms across all instances in the series,
  # derived from DICOM ExposureTime attribute (MIN across all instances)
  ExposureTime_min,

  # description:
  # maximum duration of the X-ray exposure in ms across all instances in the series,
  # derived from DICOM ExposureTime attribute (MAX across all instances)
  ExposureTime_max,

  # description:
  # diameter of the region over which data were collected in mm as defined in
  # DICOM DataCollectionDiameter attribute;
  # constant across instances — aggregated with ANY_VALUE
  DataCollectionDiameter,

  # description:
  # diameter of the reconstruction field of view in mm as defined in
  # DICOM ReconstructionDiameter attribute;
  # aggregated with ANY_VALUE — constant across instances within a series
  ReconstructionDiameter,

  # description:
  # ratio of the beam pitch for helical CT as defined in DICOM SpiralPitchFactor attribute;
  # NULL for non-helical (sequential/axial) acquisitions;
  # constant across instances — aggregated with ANY_VALUE
  SpiralPitchFactor

FROM ct_data
-- Sort by low-cardinality acquisition parameters first to improve parquet compression
ORDER BY
  KVP NULLS LAST,
  ConvolutionKernel[SAFE_OFFSET(0)] NULLS LAST,
  SliceThickness NULLS LAST,
  SeriesInstanceUID
