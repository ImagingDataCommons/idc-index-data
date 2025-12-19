# table-description:
# This table contains metadata about the analysis results collections available in IDC. Each row corresponds to an
# analysis results collection, and contains attributes such as the collection name, types of cancer represented,
# number of subjects, and pointers to the resources to learn more about the content of the collection
SELECT
  # description:
  # unique identifier of the analysis results collection
  ID AS analysis_result_id,
  # description:
  # name of the analysis results collection
  Title AS analysis_result_title,
  # description:
  # Digital Object Identifier (DOI) of the analysis results collection
  source_doi AS source_DOI,
  # description:
  # URL for the location of additional information about the analysis results collection
  source_url,
  # description:
  # number of subjects analyzed in the analysis results collection
  Subjects,
  # description:
  # collections analyzed in the analysis results collection
  Collections,
  # description:
  # modalities corresponding to the analysis artifacts included in the analysis results collection
  Modalities,
  # description:
  # timestamp of the last update to the analysis results collection
  Updated,
  # description:
  # license URL for the analysis results collection
  license_url,
  # description:
  # license name for the analysis results collection
  license_long_name,
  # description:
  # short name for the license of the analysis results collection
  license_short_name,
  # description:
  # detailed description of the analysis results collection
  Description,
  # description:
  # citation for the analysis results collection that should be used for acknowledgment
  Citation
FROM
  `bigquery-public-data.idc_v23.analysis_results_metadata`
