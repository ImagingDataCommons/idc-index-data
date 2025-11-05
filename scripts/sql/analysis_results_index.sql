SELECT
  ID AS analysis_result_id,
  Title AS analysis_result_title,
  source_doi,
  source_url,
  Subjects,
  Collections,
  AnalysisArtifacts,
  Updated,
  license_url,
  license_long_name,
  license_short_name,
  Description,
  Citation
FROM
  `bigquery-public-data.idc_v22.analysis_results_metadata`
