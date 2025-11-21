# table-description:
# This table contains metadata about the collections available in IDC. Each row corresponds to a collection,
# and contains attributes such as the collection name, types of cancer represented, number of subjects,
# and pointers to the resources to learn more about the content of the collection.
SELECT
  # description:
  # name of the collection
  collection_name,
  # description:
  # unique identifier of the collection
  collection_id,
  # description:
  # types of cancer represented in the collection
  CancerTypes,
  # description:
  # locations of tumors represented in the collection
  TumorLocations,
  # description:
  # number of subjects in the collection
  Subjects,
  # description:
  # species represented in the collection
  Species,
  # description:
  # sources of data for the collection
  Sources,
  # description:
  # additional data supporting the collection available in IDC
  SupportingData,
  # description:
  # broader initiative/category under which this collection is being shared
  Program,
  # description:
  # status of the collection (Completed or Ongoing)
  Status,
  # description:
  # timestamp of the last update to the collection
  Updated,
  # description:
  # detailed information about the collection
  Description
FROM
  `bigquery-public-data.idc_v23.original_collections_metadata`
