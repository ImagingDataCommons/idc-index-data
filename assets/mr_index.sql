# table-description:
# This table contains one row per MR Image Storage (SOPClassUID 1.2.840.10008.5.1.4.1.1.4)
# DICOM series in IDC, capturing MR acquisition and sequence parameters
# that are not included in the main idc_index table.
# The index can be joined to idc_index on SeriesInstanceUID to combine
# universal series metadata with MR-specific acquisition parameters.
# EchoTime and DiffusionBValue are reported as arrays of all distinct per-instance values
# because they legitimately differ across instances within multi-echo and
# diffusion-weighted series respectively.
# All other attributes are aggregated with ANY_VALUE (one representative instance).

WITH mr_data AS (
  SELECT
    SeriesInstanceUID,
    ANY_VALUE(SAFE_CAST(MagneticFieldStrength AS FLOAT64)) AS MagneticFieldStrength,
    ANY_VALUE(ScanningSequence) AS ScanningSequence,
    ANY_VALUE(SequenceVariant) AS SequenceVariant,
    ANY_VALUE(MRAcquisitionType) AS MRAcquisitionType,
    ARRAY_AGG(DISTINCT SAFE_CAST(EchoTime AS FLOAT64) IGNORE NULLS
              ORDER BY SAFE_CAST(EchoTime AS FLOAT64)) AS EchoTime,
    ANY_VALUE(SAFE_CAST(RepetitionTime AS FLOAT64)) AS RepetitionTime,
    ANY_VALUE(SAFE_CAST(EchoTrainLength AS INT64)) AS EchoTrainLength,
    ANY_VALUE(SAFE_CAST(FlipAngle AS FLOAT64)) AS FlipAngle,
    ANY_VALUE(SAFE_CAST(PixelBandwidth AS FLOAT64)) AS PixelBandwidth,
    ANY_VALUE(SAFE_CAST(ImagingFrequency AS FLOAT64)) AS ImagingFrequency,
    ANY_VALUE(ImagedNucleus) AS ImagedNucleus,
    ANY_VALUE(SAFE_CAST(PixelSpacing[SAFE_OFFSET(0)] AS FLOAT64)) AS PixelSpacing_row_mm,
    ANY_VALUE(SAFE_CAST(PixelSpacing[SAFE_OFFSET(1)] AS FLOAT64)) AS PixelSpacing_col_mm,
    ANY_VALUE(`Rows`) AS `Rows`,
    ANY_VALUE(`Columns`) AS `Columns`,
    ANY_VALUE(SAFE_CAST(SliceThickness AS FLOAT64)) AS SliceThickness,
    ANY_VALUE(SAFE_CAST(InversionTime AS FLOAT64)) AS InversionTime,
    ANY_VALUE(ReceiveCoilName) AS ReceiveCoilName,
    ANY_VALUE(SequenceName) AS SequenceName,
    ARRAY_AGG(DISTINCT DiffusionBValue IGNORE NULLS
              ORDER BY DiffusionBValue) AS DiffusionBValue,
    ANY_VALUE(SAFE_CAST(NumberOfTemporalPositions AS INT64)) AS NumberOfTemporalPositions
  FROM `bigquery-public-data.idc_v24.dicom_all`
  WHERE SOPClassUID = '1.2.840.10008.5.1.4.1.1.4'
  GROUP BY SeriesInstanceUID
)
SELECT
  # description:
  # DICOM SeriesInstanceUID — unique identifier of the MR series; use to join with idc_index
  SeriesInstanceUID,

  # description:
  # static magnetic field strength in Tesla as defined in DICOM MagneticFieldStrength attribute;
  # constant across instances within a series — aggregated with ANY_VALUE
  MagneticFieldStrength,

  # description:
  # pulse sequence type as defined in DICOM ScanningSequence attribute
  # (SE = Spin Echo, GR = Gradient Recalled, IR = Inversion Recovery, EP = Echo Planar);
  # may contain multiple values; constant across instances — aggregated with ANY_VALUE
  ScanningSequence,

  # description:
  # variant of the scanning sequence as defined in DICOM SequenceVariant attribute
  # (SK = Segmented k-Space, MTC = Magnetization Transfer Contrast, SS = Steady State,
  # TRSS = Time Reversed Steady State, SP = Spoiled, MP = MAG Prepared, OSP = Oversampling
  # Phase, NONE = No sequence variant);
  # may contain multiple values; constant across instances — aggregated with ANY_VALUE
  SequenceVariant,

  # description:
  # whether the acquisition is 2D or 3D as defined in DICOM MRAcquisitionType attribute;
  # constant across instances — aggregated with ANY_VALUE
  MRAcquisitionType,

  # description:
  # distinct echo times in ms present in the series, derived from DICOM EchoTime attribute;
  # aggregated as ARRAY_AGG(DISTINCT) across all instances because EchoTime legitimately
  # varies in multi-echo sequences; single-element array for single-echo series,
  # multi-element array for multi-echo series (e.g., [2.46, 4.92, 7.38])
  EchoTime,

  # description:
  # repetition time in ms as defined in DICOM RepetitionTime attribute;
  # aggregated with ANY_VALUE — constant across instances within a series
  RepetitionTime,

  # description:
  # number of echoes in the echo train as defined in DICOM EchoTrainLength attribute;
  # constant across instances — aggregated with ANY_VALUE
  EchoTrainLength,

  # description:
  # flip angle in degrees as defined in DICOM FlipAngle attribute;
  # aggregated with ANY_VALUE — constant across instances within a series
  FlipAngle,

  # description:
  # receiver bandwidth per pixel in Hz as defined in DICOM PixelBandwidth attribute;
  # aggregated with ANY_VALUE — constant across instances within a series
  PixelBandwidth,

  # description:
  # Larmor resonance frequency in MHz as defined in DICOM ImagingFrequency attribute;
  # proportional to MagneticFieldStrength (42.577 MHz/T for proton);
  # constant across instances — aggregated with ANY_VALUE
  ImagingFrequency,

  # description:
  # nucleus used for imaging as defined in DICOM ImagedNucleus attribute
  # (e.g., 1H for proton, 31P, 23Na);
  # constant across instances — aggregated with ANY_VALUE
  ImagedNucleus,

  # description:
  # in-plane pixel spacing along the row direction in mm, derived from DICOM PixelSpacing[0];
  # MR pixel spacing is isotropic in almost all series in IDC;
  # aggregated with ANY_VALUE — constant across instances within a series
  PixelSpacing_row_mm,

  # description:
  # in-plane pixel spacing along the column direction in mm, derived from DICOM PixelSpacing[1];
  # MR pixel spacing is isotropic in almost all series in IDC;
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
  # nominal slice thickness in mm as defined in DICOM SliceThickness attribute;
  # aggregated with ANY_VALUE — constant across instances within a series
  SliceThickness,

  # description:
  # inversion time in ms as defined in DICOM InversionTime attribute;
  # populated only for inversion recovery sequences, NULL otherwise;
  # constant across instances — aggregated with ANY_VALUE
  InversionTime,

  # description:
  # name of the receiver coil used as defined in DICOM ReceiveCoilName attribute;
  # aggregated with ANY_VALUE — constant across instances within a series
  ReceiveCoilName,

  # description:
  # manufacturer-specific pulse sequence name as defined in DICOM SequenceName attribute
  # (e.g., *tfl* for Siemens FLASH, *ep_b1000* for Siemens EPI diffusion);
  # aggregated with ANY_VALUE — constant across instances within a series
  SequenceName,

  # description:
  # distinct diffusion b-values in s/mm² present in the series,
  # derived from DICOM DiffusionBValue attribute;
  # aggregated as ARRAY_AGG(DISTINCT) across all instances because DiffusionBValue legitimately
  # varies across instances in diffusion-weighted series;
  # empty array for non-DWI series; multi-element for DWI (e.g., [0.0, 1000.0])
  DiffusionBValue,

  # description:
  # number of temporal positions (time frames) in the series as defined in
  # DICOM NumberOfTemporalPositions attribute; populated for dynamic (DCE-MRI) series;
  # NULL otherwise; constant across instances — aggregated with ANY_VALUE
  NumberOfTemporalPositions

FROM mr_data
-- Sort by low-cardinality acquisition parameters first to improve parquet compression
ORDER BY
  MagneticFieldStrength NULLS LAST,
  ScanningSequence[SAFE_OFFSET(0)] NULLS LAST,
  MRAcquisitionType NULLS LAST,
  EchoTime[SAFE_OFFSET(0)] NULLS LAST,
  SeriesInstanceUID
