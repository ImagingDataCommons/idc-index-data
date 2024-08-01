-- Step 1: Declare variables
DECLARE idc_versions ARRAY<INT64>;
DECLARE latest_idc_version INT64;
DECLARE union_all_query STRING;

-- Step 2: Get all idc_versions
SET idc_versions = (
  SELECT [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18]
  --SELECT ARRAY_AGG(idc_version)
  --FROM 
  --`bigquery-public-data.idc_current.version_metadata`
);

SET latest_idc_version = (
  SELECT 18
  --SELECT max(idc_version)
  --FROM 
  --`bigquery-public-data.idc_current.version_metadata`
);

-- Step 3: Generate the UNION ALL query dynamically
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
  where SeriesInstanceUID not in (select distinct seriesInstanceUID from `bigquery-public-data.idc_v%d.dicom_all`)
  GROUP BY
  1,2,3,4,5,6,7,8

""", version, version, latest_idc_version),
    " UNION ALL "
  )
  FROM UNNEST(idc_versions) AS version
);

-- Step 4: Execute the complete query
EXECUTE IMMEDIATE FORMAT("""
WITH all_versions AS (
  %s
)
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
  WHEN gcs_bucket='public-datasets-idc' THEN CONCAT('s3://','idc-open-data/',crdc_series_uuid, '/*')
  WHEN gcs_bucket='idc-open-idc1' THEN CONCAT('s3://','idc-open-data/',crdc_series_uuid, '/*')
  WHEN gcs_bucket='idc-open-cr' THEN CONCAT('s3://','idc-open-data-cr/',crdc_series_uuid, '/*')
    END AS series_aws_url,
  MIN(idc_version) AS min_idc_version,
  MAX(idc_version) AS max_idc_version
FROM all_versions
where gcs_bucket not in ('idc-open-idc')
--and seriesInstanceUID not in (select
--distinct series_instance_uid from `idc-dev-etl.idc_v18_dev.all_joined_public`
--where
--series_instance_uid not in (select distinct seriesInstanceUID from bigquery-public-data.idc_current.dicom_all))
GROUP BY   
 1,2,3,4,5,6,7,8
""", union_all_query
);
