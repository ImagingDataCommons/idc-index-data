# table-description:
# This table contains one row per DICOM series that has contrast agent information.
# It captures contrast bolus metadata from CT, MR, PT, XA, and RF imaging modalities,
# including the agent name, ingredient, and administration route. Only series with
# at least one non-null contrast attribute are included. This table can be joined
# with the main idc_index table using the SeriesInstanceUID column.

WITH contrast_data AS (
  SELECT
    SeriesInstanceUID,
    ARRAY_AGG(DISTINCT ContrastBolusAgent IGNORE NULLS) AS ContrastBolusAgent,
    ARRAY_AGG(DISTINCT ContrastBolusIngredient IGNORE NULLS) AS ContrastBolusIngredient,
    ARRAY_AGG(DISTINCT ContrastBolusRoute IGNORE NULLS) AS ContrastBolusRoute
  FROM `bigquery-public-data.idc_v23.dicom_all`
  WHERE Modality IN ('CT', 'MR', 'PT', 'XA', 'RF')
  GROUP BY SeriesInstanceUID
)
SELECT
  # description:
  # DICOM SeriesInstanceUID identifier of the imaging series
  SeriesInstanceUID,

  # description:
  # distinct contrast agent names used in the series as defined in DICOM ContrastBolusAgent attribute
  ContrastBolusAgent,

  # description:
  # distinct contrast agent ingredients used in the series as defined in DICOM ContrastBolusIngredient attribute
  ContrastBolusIngredient,

  # description:
  # distinct contrast administration routes used in the series as defined in DICOM ContrastBolusRoute attribute
  ContrastBolusRoute

FROM contrast_data
WHERE
  ARRAY_LENGTH(ContrastBolusAgent) > 0
  OR ARRAY_LENGTH(ContrastBolusIngredient) > 0
  OR ARRAY_LENGTH(ContrastBolusRoute) > 0
