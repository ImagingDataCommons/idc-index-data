WITH
  gdc_image_studies AS (
    SELECT
      ANY_VALUE(collection_id) AS collection_id,
      ANY_VALUE(PatientID) AS PatientID,
      StudyInstanceUID,
      ANY_VALUE(StudyDate) AS StudyDate,
      ANY_VALUE(StudyDescription) AS StudyDescription,
      ARRAY_AGG(DISTINCT Modality ORDER BY Modality) AS Modalities
    FROM
      `bigquery-public-data.idc_current.dicom_all`
    WHERE
      collection_id LIKE "tcga%" OR
      collection_id LIKE "%apollo%" OR
      collection_id LIKE "%cptac%" OR
      collection_id LIKE "%ccdi%" OR
      collection_id LIKE "cgci%" OR
      collection_id LIKE "cddp_eagle%" OR
      collection_id LIKE "hcmi%"
    GROUP BY
      StudyInstanceUID)
SELECT
  * EXCEPT (Modalities),
  CASE
    WHEN "SM" IN UNNEST(Modalities) THEN "M" # microscopy
    ELSE "R" # radiology
  END AS study_type
FROM gdc_image_studies
