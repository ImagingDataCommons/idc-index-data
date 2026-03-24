#standardSQL

# table-description:
# This table contains one row per DICOM series from IDC
# for single-frame CT, MR, and PT SOP classes, with boolean
# columns characterizing the geometric properties of each series.
# The checks determine whether the series forms a regularly-spaced
# rectilinear 3D volume (consistent orientation, spacing, dimensions,
# and slice positions). Series that do not pass all checks may still
# be usable with additional processing such as resampling or
# acquisition geometry correction (e.g., for variable-spacing or
# gantry-tilted acquisitions). Oblique-aware: uses projection-based
# slice position computation, which handles gantry-tilted CT, oblique
# MR, and axial PET uniformly.

# To use a specific IDC version instead of idc_current, replace
# `bigquery-public-data.idc_current.dicom_all` with e.g. `bigquery-public-data.idc_v18.dicom_all`

# Configurable parameters
DECLARE sliceIntervalTolerance FLOAT64 DEFAULT 0.2;   # max allowed variation in slice spacing (mm);
                                                       # matches kSliceTolerance in dcm2niix
                                                       # (https://github.com/rordenlab/dcm2niix/blob/f6d7a001/console/nii_dicom_batch.cpp#L64)
DECLARE inPlaneTolerance FLOAT64 DEFAULT 0.1;          # max in-plane position jitter (mm)
DECLARE orientationTolerance FLOAT64 DEFAULT 0.01;     # cross-product magnitude deviation from 1.0

WITH
# CTE 1: Select per-instance data for single-frame CT, MR, PT series, excluding localizers
rawData AS (
  SELECT
    SeriesInstanceUID,
    SOPInstanceUID,
    Modality,
    # Extract all three components of ImagePositionPatient
    SAFE_CAST(ImagePositionPatient[SAFE_OFFSET(0)] AS FLOAT64) AS ippX,
    SAFE_CAST(ImagePositionPatient[SAFE_OFFSET(1)] AS FLOAT64) AS ippY,
    SAFE_CAST(ImagePositionPatient[SAFE_OFFSET(2)] AS FLOAT64) AS ippZ,
    # ImageOrientationPatient as string for consistency check
    ARRAY_TO_STRING(ImageOrientationPatient, '/') AS iop,
    # Extract row direction cosines (first 3 elements)
    (
      SELECT ARRAY_AGG(SAFE_CAST(part AS FLOAT64) ORDER BY index)
      FROM UNNEST(ImageOrientationPatient) part WITH OFFSET index
      WHERE index BETWEEN 0 AND 2
    ) AS x_vector,
    # Extract column direction cosines (last 3 elements)
    (
      SELECT ARRAY_AGG(SAFE_CAST(part AS FLOAT64) ORDER BY index)
      FROM UNNEST(ImageOrientationPatient) part WITH OFFSET index
      WHERE index BETWEEN 3 AND 5
    ) AS y_vector,
    ARRAY_TO_STRING(PixelSpacing, '/') AS pixelSpacing,
    `Rows` AS pixelRows,
    `Columns` AS pixelColumns
  FROM
    `bigquery-public-data.idc_current.dicom_all` bid
  WHERE
    # Single-frame SOP Classes: CT Image Storage, MR Image Storage, PET Image Storage
    SOPClassUID IN ('1.2.840.10008.5.1.4.1.1.2', '1.2.840.10008.5.1.4.1.1.4', '1.2.840.10008.5.1.4.1.1.128')
    AND SeriesInstanceUID NOT IN (
      SELECT SeriesInstanceUID
      FROM `bigquery-public-data.idc_current.dicom_all`, UNNEST(ImageType) image_type
      WHERE image_type = 'LOCALIZER' or image_type LIKE "%MIP%"
    )
),

# CTE 2: Compute cross product of row and column orientation vectors
crossProduct AS (
  SELECT
    SOPInstanceUID,
    SeriesInstanceUID,
    (SELECT AS STRUCT
      (x_vector[OFFSET(1)] * y_vector[OFFSET(2)] - x_vector[OFFSET(2)] * y_vector[OFFSET(1)]) AS x,
      (x_vector[OFFSET(2)] * y_vector[OFFSET(0)] - x_vector[OFFSET(0)] * y_vector[OFFSET(2)]) AS y,
      (x_vector[OFFSET(0)] * y_vector[OFFSET(1)] - x_vector[OFFSET(1)] * y_vector[OFFSET(0)]) AS z
    ) AS cp
  FROM rawData
),

# CTE 3: Project IPP onto slice normal for orientation-independent slice position
sliceProjection AS (
  SELECT
    r.*,
    cp.cp,
    # Cross product magnitude (should be ~1 for orthogonal unit vectors)
    SQRT(cp.cp.x * cp.cp.x + cp.cp.y * cp.cp.y + cp.cp.z * cp.cp.z) AS crossProductMagnitude,
    # Project IPP onto slice normal to get the true slice coordinate
    (r.ippX * cp.cp.x + r.ippY * cp.cp.y + r.ippZ * cp.cp.z) AS slicePosition,
    # In-plane coordinates: project IPP onto row and column directions
    # For a geometrically consistent series, these should be constant across all instances
    (r.ippX * r.x_vector[OFFSET(0)] + r.ippY * r.x_vector[OFFSET(1)] + r.ippZ * r.x_vector[OFFSET(2)]) AS inPlaneRow,
    (r.ippX * r.y_vector[OFFSET(0)] + r.ippY * r.y_vector[OFFSET(1)] + r.ippZ * r.y_vector[OFFSET(2)]) AS inPlaneCol,
    # Adjacent slice interval via LEAD on projected position
    LEAD(r.ippX * cp.cp.x + r.ippY * cp.cp.y + r.ippZ * cp.cp.z)
      OVER (PARTITION BY r.SeriesInstanceUID
            ORDER BY (r.ippX * cp.cp.x + r.ippY * cp.cp.y + r.ippZ * cp.cp.z))
      - (r.ippX * cp.cp.x + r.ippY * cp.cp.y + r.ippZ * cp.cp.z) AS slice_interval,
    # Expected spacing derived from the full span (first-to-last) to minimize
    # numerical issues, per the approach used in 3D Slicer:
    # https://github.com/Slicer/Slicer/commit/3328b81211cb2e9ae16a0b49097744171c8c71c0
    SAFE_DIVIDE(
      MAX(r.ippX * cp.cp.x + r.ippY * cp.cp.y + r.ippZ * cp.cp.z)
        OVER (PARTITION BY r.SeriesInstanceUID)
      - MIN(r.ippX * cp.cp.x + r.ippY * cp.cp.y + r.ippZ * cp.cp.z)
        OVER (PARTITION BY r.SeriesInstanceUID),
      COUNT(*) OVER (PARTITION BY r.SeriesInstanceUID) - 1
    ) AS expected_spacing
  FROM rawData r
  JOIN crossProduct cp USING (SOPInstanceUID, SeriesInstanceUID)
),

# CTE 4: Aggregate per series and compute boolean check columns
geometryChecks AS (
  SELECT
    SeriesInstanceUID,
    ANY_VALUE(Modality) AS Modality,
    # Individual check results
    COUNT(DISTINCT iop) = 1 AS single_orientation,
    COUNT(DISTINCT pixelSpacing) = 1 AS consistent_pixel_spacing,
    COUNT(DISTINCT SOPInstanceUID) = COUNT(DISTINCT slicePosition) AS unique_slice_positions,
    COUNT(DISTINCT pixelRows) = 1 AND COUNT(DISTINCT pixelColumns) = 1 AS consistent_image_dimensions,
    MIN(crossProductMagnitude) BETWEEN (1 - orientationTolerance) AND (1 + orientationTolerance)
      AND MAX(crossProductMagnitude) BETWEEN (1 - orientationTolerance) AND (1 + orientationTolerance) AS orthogonal_orientation,
    MAX(inPlaneRow) - MIN(inPlaneRow) < inPlaneTolerance AS consistent_in_plane_row,
    MAX(inPlaneCol) - MIN(inPlaneCol) < inPlaneTolerance AS consistent_in_plane_col,
    # Compare each adjacent interval against expected spacing derived from
    # the full span; MAX of the absolute deviation must be within tolerance.
    # slice_interval is NULL for the last slice (from LEAD), and MAX ignores NULLs.
    MAX(ABS(slice_interval - expected_spacing)) < sliceIntervalTolerance AS uniform_slice_spacing
  FROM sliceProjection
  GROUP BY
    SeriesInstanceUID
)

SELECT
  # description:
  # unique identifier of the DICOM series
  SeriesInstanceUID,
  # description:
  # TRUE if all instances share the same ImageOrientationPatient
  single_orientation,
  # description:
  # TRUE if the cross product of row and column orientation vectors has unit magnitude
  # (within orientationTolerance), confirming orthogonal direction cosines
  orthogonal_orientation,
  # description:
  # TRUE if every instance has a distinct slice position along the volume normal,
  # i.e. no duplicate or overlapping slices
  unique_slice_positions,
  # description:
  # TRUE if the projection of ImagePositionPatient onto the row direction is constant
  # across all instances (within inPlaneTolerance)
  consistent_in_plane_row,
  # description:
  # TRUE if the projection of ImagePositionPatient onto the column direction is constant
  # across all instances (within inPlaneTolerance)
  consistent_in_plane_col,
  # description:
  # TRUE if all instances share the same PixelSpacing
  consistent_pixel_spacing,
  # description:
  # TRUE if all instances share the same Rows and Columns values
  consistent_image_dimensions,
  # description:
  # TRUE if the spacing between consecutive slices is constant
  # (within sliceIntervalTolerance)
  uniform_slice_spacing,
  # description:
  # TRUE if all individual checks pass, indicating the series forms a regularly-spaced
  # rectilinear 3D volume that can be loaded directly into a 3D array without resampling
  single_orientation AND orthogonal_orientation AND unique_slice_positions
    AND consistent_in_plane_row AND consistent_in_plane_col AND consistent_pixel_spacing
    AND consistent_image_dimensions AND uniform_slice_spacing AS regularly_spaced_3d_volume
FROM geometryChecks
ORDER BY regularly_spaced_3d_volume DESC, SeriesInstanceUID
