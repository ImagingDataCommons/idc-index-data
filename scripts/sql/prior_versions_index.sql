# table-description:
# Versions of IDC series that an earlier IDC release served and the current release no longer
# serves, each with the range of releases that served it. A version is identified by
# crdc_series_uuid. A SeriesInstanceUID that is also in the main index was revised (its current
# version is in the main index); one that is not was removed from IDC. A version can span more
# than one row, since a new row starts whenever its storage bucket changed between releases,
# even though its content did not. This table is not a record of what is new in a release: it
# never lists added series, and a row ending at a release does not mean that release changed the
# series. To find the series added or revised in a release, use series_init_idc_version and
# series_revised_idc_version in the main index. The main use of this table is to resolve and
# download series listed in manifests made from earlier releases.

-- For details on the syntax, see
-- https://cloud.google.com/bigquery/docs/reference/standard-sql/procedural-language
--
-- The script runs in two parts. EXECUTE IMMEDIATE collects every past series version into a
-- temporary table, since the list of releases to read is only known at run time. The final
-- SELECT is static, so its column descriptions are parsed like those of any other index.
--
-- Step 1: Declare variables
DECLARE idc_versions ARRAY<INT64>;
DECLARE latest_idc_version INT64 DEFAULT 25;
DECLARE union_all_query STRING;

--Step 2
--SET latest_idc_version = (
--SELECT max(idc_version)
--FROM
--bigquery-public-data.idc_current.version_metadata
--);

-- Step 3: Get all idc_versions
SET idc_versions = (
  SELECT GENERATE_ARRAY(1, latest_idc_version)
  -- SELECT [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18]
  --SELECT ARRAY_AGG(idc_version)
  --FROM
  --`bigquery-public-data.idc_current.version_metadata`
);

-- Step 4: Generate the UNION ALL query dynamically
SET union_all_query = (
  SELECT STRING_AGG(
    FORMAT("""
    SELECT
    %d AS idc_version,
    collection_id,
    PatientID,
    SeriesInstanceUID,
    StudyInstanceUID,
    Modality,
    regexp_extract(gcs_url, 'gs://([^/]+)/') as gcs_bucket,
    crdc_series_uuid,
    ROUND(SUM(SAFE_CAST(instance_size AS float64))/1000000, 2) AS series_size_MB,
  FROM
  `bigquery-public-data.idc_v%d.dicom_all` AS dicom_all
  where crdc_series_uuid not in (select distinct crdc_series_uuid from `bigquery-public-data.idc_v%d.dicom_all`)
  GROUP BY
  1,2,3,4,5,6,7,8

  """,
  version, version, latest_idc_version),
    " UNION ALL "
    ORDER BY version
  )
  FROM UNNEST(idc_versions) AS version
);

-- Step 5: Collect the series versions of every past release into a temporary table
EXECUTE IMMEDIATE FORMAT("""
CREATE TEMP TABLE all_versions AS
%s
""",
  union_all_query
);

-- Step 6: One row per past series version and storage bucket, with the releases that served it
WITH versions AS (
  SELECT
    collection_id,
    PatientID,
    SeriesInstanceUID,
    StudyInstanceUID,
    Modality,
    gcs_bucket,
    crdc_series_uuid,
    series_size_MB,
    CASE

    # map GCS bucket to AWS bucket, since for idc-index we prefer AWS
    # if new buckets are included in IDC, this will need to be updated!

    WHEN gcs_bucket='public-datasets-idc' THEN 'idc-open-data'
    WHEN gcs_bucket='idc-open-idc1' THEN 'idc-open-data-two'
    WHEN gcs_bucket='idc-open-cr' THEN 'idc-open-data-cr'
    WHEN gcs_bucket='idc-open-data' THEN 'idc-open-data'
    ELSE ERROR(CONCAT('Unmapped GCS bucket: ', gcs_bucket, '. Please add a mapping.'))
      END AS aws_bucket,
    MIN(idc_version) AS min_idc_version,
    MAX(idc_version) AS max_idc_version
  FROM all_versions

  where gcs_bucket not in ('idc-open-idc')

  #per @bcli4d:idc-open-idc was our public bucket before we moved most data to the Google owned public-datasets-idc.
  #We decided at the time to not touch BQ. To deal with this and other cases where some metadata can change (Licences),
  #we include the mutable_metadata table which maps crdc_instance_uuid to current gcs_url, aws_url, license, doi.

  GROUP BY
   1,2,3,4,5,6,7,8
)
SELECT
  # description:
  # unique identifier of the collection the series belonged to
  collection_id,
  # description:
  # identifier of the patient within the collection (DICOM attribute PatientID)
  PatientID,
  # description:
  # DICOM SeriesInstanceUID of the series; the same for every version of a series, so it also
  # matches the current version in the main index if the series was revised rather than removed
  SeriesInstanceUID,
  # description:
  # DICOM StudyInstanceUID of the study the series belongs to
  StudyInstanceUID,
  # description:
  # DICOM Modality of the series
  Modality,
  # description:
  # name of the Google Cloud Storage bucket that held this version of the series in the releases
  # min_idc_version through max_idc_version
  gcs_bucket,
  # description:
  # unique identifier of this version of the series within IDC; changes whenever the content of
  # the series changes, so it never matches a crdc_series_uuid in the main index. The same value
  # on several rows is one version that moved between storage buckets
  crdc_series_uuid,
  # description:
  # total size of this version of the series in megabytes
  series_size_MB,
  # description:
  # public AWS S3 URL to download this version of the series in bulk (each instance is a
  # separate file)
  CONCAT('s3://', aws_bucket, '/', crdc_series_uuid, '/*') AS series_aws_url,
  # description:
  # name of the AWS S3 bucket that contains this version of the series
  aws_bucket,
  # description:
  # first IDC release (as an integer) in this row's range: this version, in this storage bucket,
  # was served from this release on
  min_idc_version,
  # description:
  # last IDC release (as an integer) in this row's range. The next release revised the series,
  # removed it, or moved this version to another storage bucket (which starts a new row)
  max_idc_version
FROM versions
ORDER BY
 collection_id, min_idc_version, Modality
