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
  cancer_types,
  # description:
  # locations of tumors represented in the collection
  tumor_locations,
  # description:
  # number of subjects in the collection
  subjects,
  # description:
  # species represented in the collection
  species,
  # description:
  # sources of data for the collection
  sources,
  # description:
  # additional data supporting the collection available in IDC
  supporting_data,
  # description:
  # broader initiative/category under which this collection is being shared
  program_id,
  # description:
  # status of the collection (Completed or Ongoing)
  status,
  # description:
  # date of the last update to the collection
  updated,
  # description:
  # detailed information about the collection
  description
FROM
  `bigquery-public-data.idc_v24.original_collections_metadata`
ORDER BY
  collection_id
