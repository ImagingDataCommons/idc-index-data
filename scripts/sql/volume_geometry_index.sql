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
#
# Approach overview:
#
# Each DICOM instance (slice) carries its 3D position in patient space
# (ImagePositionPatient, abbreviated IPP) and the orientation of its pixel
# grid (ImageOrientationPatient, abbreviated IOP). IOP provides two unit
# vectors — the row direction and the column direction — that define the
# image plane. Their cross product gives the slice normal, i.e. the
# direction perpendicular to the image plane.
#
# To check whether slices are regularly spaced along the volume axis, we
# project each instance's IPP onto the slice normal. This yields a single
# scalar "slice position" for each instance, regardless of whether the
# acquisition is axial, oblique, or gantry-tilted. The expected spacing
# is computed from the full span (first-to-last slice position divided by
# N-1), and each adjacent pair is compared against it — an approach that
# minimizes floating-point accumulation errors (see 3D Slicer reference
# below). The slice spacing tolerance is relative (a fraction of expected
# spacing) rather than absolute, so it scales correctly for both human
# imaging (~1-5mm spacing) and preclinical/small-animal imaging (~0.1mm).
#
# Similarly, projecting IPP onto the row and column directions gives
# in-plane coordinates that should be constant across all slices if the
# slices are properly aligned (no lateral shift between slices).
#
# Key SQL patterns used:
#
#   WITH ... AS (...) — Common Table Expression (CTE): defines a named
#       temporary result set, like a subquery you can reference by name.
#       The query is structured as a chain of CTEs that progressively
#       transform the data.
#
#   SAFE_CAST(x AS FLOAT64) — converts x to a floating-point number,
#       returning NULL instead of an error if the conversion fails.
#
#   ARRAY[OFFSET(i)] — accesses the i-th element of an array (0-based).
#       SAFE_OFFSET returns NULL instead of an error for out-of-bounds.
#
#   LEAD(value) OVER (PARTITION BY key ORDER BY value) — a window function
#       that returns the value from the next row within the same partition
#       (group), ordered by the specified column. Used here to compute the
#       distance between each slice and the next one. Returns NULL for the
#       last row in each partition (no next row).
#
#   MAX/MIN(...) OVER (PARTITION BY key) — window aggregates that compute
#       the max/min across all rows sharing the same key, without collapsing
#       rows. Used to compute per-series statistics while keeping per-instance
#       rows intact.
#
#   SAFE_DIVIDE(a, b) — returns a/b, or NULL if b is zero (avoids
#       division-by-zero errors for single-instance series).
#
#   COUNT(DISTINCT x) — counts the number of unique values of x.
#
#   ANY_VALUE(x) — returns an arbitrary value of x from the group;
#       used when all values in the group are expected to be the same.

# To use a specific IDC version instead of idc_current, replace
# `bigquery-public-data.idc_current.dicom_all` with e.g. `bigquery-public-data.idc_v24.dicom_all`

# Configurable parameters
DECLARE relativeSliceTolerance FLOAT64 DEFAULT 0.01;   # max allowed variation in slice spacing as a fraction
                                                       # of expected spacing (1%); matches the default
                                                       # _DEFAULT_SPACING_RELATIVE_TOLERANCE in highdicom
                                                       # (https://github.com/ImagingDataCommons/highdicom/blob/9750a6f9/src/highdicom/spatial.py#L19);
                                                       # relative tolerance scales correctly for both human
                                                       # imaging (~1-5mm spacing) and preclinical/small-animal
                                                       # imaging (~0.1mm spacing)
DECLARE inPlaneTolerance FLOAT64 DEFAULT 0.1;          # max in-plane position jitter (mm)
DECLARE orientationTolerance FLOAT64 DEFAULT 0.01;     # cross-product magnitude deviation from 1.0

WITH

# ---------------------------------------------------------------------------
# CTE 1 — rawData: extract per-instance geometry from DICOM headers
# ---------------------------------------------------------------------------
# Selects one row per DICOM instance (slice) for single-frame CT, MR, and PT
# series, excluding localizer/scout images and MIP reconstructions.
# Extracts the position (IPP), orientation vectors (IOP row and column
# direction cosines), pixel spacing, and matrix dimensions for each instance.
rawData AS (
  SELECT
    SeriesInstanceUID,
    SOPInstanceUID,
    Modality,

    # ImagePositionPatient (0020,0032): the x, y, z coordinates (in mm) of
    # the upper-left corner of this slice in the patient coordinate system.
    SAFE_CAST(ImagePositionPatient[SAFE_OFFSET(0)] AS FLOAT64) AS ippX,
    SAFE_CAST(ImagePositionPatient[SAFE_OFFSET(1)] AS FLOAT64) AS ippY,
    SAFE_CAST(ImagePositionPatient[SAFE_OFFSET(2)] AS FLOAT64) AS ippZ,

    # ImageOrientationPatient (0020,0037) as a single string for equality
    # comparison — if two instances have different IOP strings, the
    # orientation changed within the series.
    ARRAY_TO_STRING(ImageOrientationPatient, '/') AS iop,

    # Row direction cosines: first 3 elements of IOP.
    # This is the unit vector along the row (horizontal) direction of the
    # image pixel grid in patient coordinates.
    (
      SELECT ARRAY_AGG(SAFE_CAST(part AS FLOAT64) ORDER BY index)
      FROM UNNEST(ImageOrientationPatient) part WITH OFFSET index
      WHERE index BETWEEN 0 AND 2
    ) AS x_vector,

    # Column direction cosines: last 3 elements of IOP.
    # This is the unit vector along the column (vertical) direction of the
    # image pixel grid in patient coordinates.
    (
      SELECT ARRAY_AGG(SAFE_CAST(part AS FLOAT64) ORDER BY index)
      FROM UNNEST(ImageOrientationPatient) part WITH OFFSET index
      WHERE index BETWEEN 3 AND 5
    ) AS y_vector,

    # PixelSpacing (0028,0030) as a string for equality comparison
    ARRAY_TO_STRING(PixelSpacing, '/') AS pixelSpacing,

    # Image matrix dimensions
    `Rows` AS pixelRows,
    `Columns` AS pixelColumns

  FROM
    `bigquery-public-data.idc_current.dicom_all` bid
  WHERE
    # Restrict to single-frame SOP Classes:
    #   1.2.840.10008.5.1.4.1.1.2   = CT Image Storage
    #   1.2.840.10008.5.1.4.1.1.4   = MR Image Storage
    #   1.2.840.10008.5.1.4.1.1.128 = Positron Emission Tomography Image Storage
    SOPClassUID IN ('1.2.840.10008.5.1.4.1.1.2', '1.2.840.10008.5.1.4.1.1.4', '1.2.840.10008.5.1.4.1.1.128')
    # Exclude localizer (scout) images and MIP reconstructions, which are
    # not part of the volumetric acquisition
    AND SeriesInstanceUID NOT IN (
      SELECT SeriesInstanceUID
      FROM `bigquery-public-data.idc_current.dicom_all`, UNNEST(ImageType) image_type
      WHERE image_type = 'LOCALIZER' or image_type LIKE "%MIP%"
    )
),

# ---------------------------------------------------------------------------
# CTE 2 — crossProduct: compute slice normal from orientation vectors
# ---------------------------------------------------------------------------
# The cross product of the row and column direction cosines gives the slice
# normal vector. For a well-formed DICOM instance, the row and column vectors
# are orthogonal unit vectors, so their cross product should also be a unit
# vector (magnitude = 1.0). The cross product formula for vectors (a,b,c)
# and (d,e,f) is: (bf-ce, cd-af, ae-bd).
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

# ---------------------------------------------------------------------------
# CTE 3 — sliceProjection: compute per-instance geometric coordinates
# ---------------------------------------------------------------------------
# For each instance, computes:
#   - crossProductMagnitude: length of the slice normal (should be ~1.0)
#   - slicePosition: the instance's position projected onto the slice normal
#     (a single scalar that represents "how far along the volume axis" this
#     slice is — this works regardless of oblique orientation or gantry tilt)
#   - inPlaneRow/Col: the instance's position projected onto the row/column
#     directions (should be constant across all slices if they are aligned)
#   - slice_interval: distance to the next slice along the volume axis
#   - expected_spacing: the ideal uniform spacing computed from the full
#     span divided by (N-1), which is more numerically stable than comparing
#     adjacent pairs directly
sliceProjection AS (
  SELECT
    r.*,
    cp.cp,

    # Magnitude of the cross product vector = sqrt(x² + y² + z²).
    # Should be ~1.0 for orthogonal unit vectors.
    SQRT(cp.cp.x * cp.cp.x + cp.cp.y * cp.cp.y + cp.cp.z * cp.cp.z) AS crossProductMagnitude,

    # Dot product of IPP with the slice normal gives the scalar slice
    # position along the volume axis.
    (r.ippX * cp.cp.x + r.ippY * cp.cp.y + r.ippZ * cp.cp.z) AS slicePosition,

    # Dot product of IPP with the row direction. If all slices are aligned
    # (no lateral shift), this value should be the same for every instance
    # in the series.
    (r.ippX * r.x_vector[OFFSET(0)] + r.ippY * r.x_vector[OFFSET(1)] + r.ippZ * r.x_vector[OFFSET(2)]) AS inPlaneRow,

    # Dot product of IPP with the column direction (same logic as above).
    (r.ippX * r.y_vector[OFFSET(0)] + r.ippY * r.y_vector[OFFSET(1)] + r.ippZ * r.y_vector[OFFSET(2)]) AS inPlaneCol,

    # LEAD returns the slice position of the next instance when rows are
    # sorted by slice position within each series. Subtracting the current
    # position gives the spacing to the next slice. The last instance in
    # each series gets NULL (no next slice).
    LEAD(r.ippX * cp.cp.x + r.ippY * cp.cp.y + r.ippZ * cp.cp.z)
      OVER (PARTITION BY r.SeriesInstanceUID
            ORDER BY (r.ippX * cp.cp.x + r.ippY * cp.cp.y + r.ippZ * cp.cp.z))
      - (r.ippX * cp.cp.x + r.ippY * cp.cp.y + r.ippZ * cp.cp.z) AS slice_interval,

    # Expected uniform spacing = total span / (number of slices - 1).
    # Derived from the first-to-last slice distance rather than any single
    # adjacent pair, which minimizes floating-point accumulation errors.
    # Reference: 3D Slicer acquisition geometry modeling
    # (https://github.com/Slicer/Slicer/commit/3328b81211cb2e9ae16a0b49097744171c8c71c0)
    # SAFE_DIVIDE returns NULL instead of error when dividing by zero
    # (handles the edge case of a single-instance series where N-1 = 0).
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

# ---------------------------------------------------------------------------
# CTE 4 — geometryChecks: aggregate per-instance data into per-series checks
# ---------------------------------------------------------------------------
# Collapses all instances within each series into a single row with boolean
# columns indicating whether each geometric property holds. Each check uses
# aggregate functions (COUNT, MIN, MAX) across all instances in the series.
geometryChecks AS (
  SELECT
    SeriesInstanceUID,
    ANY_VALUE(Modality) AS Modality,

    # single_orientation: TRUE if every instance has the same IOP string.
    # Multiple orientations would mean the image plane rotated mid-series.
    COUNT(DISTINCT iop) = 1 AS single_orientation,

    # consistent_pixel_spacing: TRUE if every instance has the same
    # PixelSpacing. Different spacings would mean the in-plane resolution
    # changed mid-series.
    COUNT(DISTINCT pixelSpacing) = 1 AS consistent_pixel_spacing,

    # unique_slice_positions: TRUE if no two instances share the same
    # projected slice position. Duplicate positions indicate overlapping
    # slices (e.g., repeated acquisitions at the same location).
    COUNT(DISTINCT SOPInstanceUID) = COUNT(DISTINCT slicePosition) AS unique_slice_positions,

    # consistent_image_dimensions: TRUE if all instances have the same
    # Rows and Columns. Different dimensions would mean the pixel matrix
    # size changed mid-series.
    COUNT(DISTINCT pixelRows) = 1 AND COUNT(DISTINCT pixelColumns) = 1 AS consistent_image_dimensions,

    # orthogonal_orientation: TRUE if the cross product magnitude is ~1.0
    # for all instances (within orientationTolerance). A magnitude far from
    # 1.0 means the row and column direction cosines are not orthogonal
    # unit vectors.
    MIN(crossProductMagnitude) BETWEEN (1 - orientationTolerance) AND (1 + orientationTolerance)
      AND MAX(crossProductMagnitude) BETWEEN (1 - orientationTolerance) AND (1 + orientationTolerance) AS orthogonal_orientation,

    # consistent_in_plane_row: TRUE if the IPP projection onto the row
    # direction varies by less than inPlaneTolerance across all instances.
    # Large variation means slices are laterally shifted relative to each
    # other (not stacked along a straight line).
    MAX(inPlaneRow) - MIN(inPlaneRow) < inPlaneTolerance AS consistent_in_plane_row,

    # consistent_in_plane_col: same check for the column direction.
    MAX(inPlaneCol) - MIN(inPlaneCol) < inPlaneTolerance AS consistent_in_plane_col,

    # uniform_slice_spacing: TRUE if every adjacent slice interval is
    # within relativeSliceTolerance of the expected uniform spacing.
    # The tolerance is relative (a fraction of expected_spacing) so it
    # scales correctly for both human imaging (~1-5mm spacing) and
    # preclinical/small-animal imaging (~0.1mm spacing).
    # The expected spacing is derived from the full first-to-last span
    # (see CTE 3 above). MAX ignores NULLs, so the last slice (which has
    # NULL slice_interval from LEAD) is automatically excluded.
    MAX(ABS(slice_interval - expected_spacing))
      < relativeSliceTolerance * ABS(ANY_VALUE(expected_spacing)) AS uniform_slice_spacing,

    # obliquity_degrees: the angle (in degrees) between the slice normal
    # and the nearest cardinal axis (X, Y, or Z in patient coordinates).
    # 0° means pure axial, sagittal, or coronal. Values > 0° indicate
    # oblique acquisition or gantry tilt (e.g., ~15° for tilted head CT).
    # Computed as ACOS of the largest absolute component of the normalized
    # slice normal vector — the largest component corresponds to the
    # nearest cardinal axis.
    ROUND(ACOS(
      GREATEST(
        ABS(ANY_VALUE(cp.x)),
        ABS(ANY_VALUE(cp.y)),
        ABS(ANY_VALUE(cp.z))
      ) / ANY_VALUE(crossProductMagnitude)
    ) * 180 / ACOS(-1), 2) AS obliquity_degrees

  FROM sliceProjection
  GROUP BY
    SeriesInstanceUID
)

# ---------------------------------------------------------------------------
# Final SELECT: output one row per series with all check results
# ---------------------------------------------------------------------------
SELECT
  # description:
  # unique identifier of the DICOM series
  SeriesInstanceUID,
  # description:
  # TRUE if all instances share the same ImageOrientationPatient (DICOM attribute)
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
  # TRUE if all instances share the same PixelSpacing (DICOM attribute)
  consistent_pixel_spacing,
  # description:
  # TRUE if all instances share the same Rows and Columns values (DICOM attributes)
  consistent_image_dimensions,
  # description:
  # TRUE if the spacing between consecutive slices is constant
  # (within relativeSliceTolerance, a relative fraction of expected spacing)
  uniform_slice_spacing,
  # description:
  # angle in degrees between the slice normal and the nearest cardinal axis
  # (X, Y, or Z in patient coordinates); 0 means pure axial, sagittal, or coronal;
  # values above 0 indicate oblique acquisition or gantry tilt
  obliquity_degrees,
  # description:
  # TRUE if all individual checks pass, indicating the series forms a regularly-spaced
  # rectilinear 3D volume that can be loaded directly into a 3D array without resampling
  single_orientation AND orthogonal_orientation AND unique_slice_positions
    AND consistent_in_plane_row AND consistent_in_plane_col AND consistent_pixel_spacing
    AND consistent_image_dimensions AND uniform_slice_spacing AS regularly_spaced_3d_volume
FROM geometryChecks
ORDER BY regularly_spaced_3d_volume DESC, SeriesInstanceUID
