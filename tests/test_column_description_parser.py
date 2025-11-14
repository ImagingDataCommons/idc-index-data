from __future__ import annotations

from scripts.python.idc_index_data_manager import IDCIndexDataManager


class TestColumnDescriptionParser:
    """Tests for parsing column descriptions from SQL comments."""

    def test_simple_column_description(self):
        """Test parsing a simple column with description."""
        sql_query = """
SELECT
  # description:
  # name of the collection
  collection_name,
FROM table
"""
        descriptions = IDCIndexDataManager.parse_column_descriptions(sql_query)
        assert "collection_name" in descriptions
        assert descriptions["collection_name"] == "name of the collection"

    def test_multiline_description(self):
        """Test parsing a column with multi-line description."""
        sql_query = """
SELECT
  # description:
  # this string is not empty if the specific series is
  # part of an analysis results collection; analysis results can be added to a
  # given collection over time
  ANY_VALUE(analysis_result_id) AS analysis_result_id,
FROM table
"""
        descriptions = IDCIndexDataManager.parse_column_descriptions(sql_query)
        assert "analysis_result_id" in descriptions
        expected = (
            "this string is not empty if the specific series is "
            "part of an analysis results collection; analysis results can be added to a "
            "given collection over time"
        )
        assert descriptions["analysis_result_id"] == expected

    def test_column_with_as_clause(self):
        """Test parsing a column with AS clause."""
        sql_query = """
SELECT
  # description:
  # unique identifier of the DICOM series
  SeriesInstanceUID AS series_id,
FROM table
"""
        descriptions = IDCIndexDataManager.parse_column_descriptions(sql_query)
        assert "series_id" in descriptions
        assert descriptions["series_id"] == "unique identifier of the DICOM series"

    def test_column_with_function(self):
        """Test parsing a column with function call."""
        sql_query = """
SELECT
  # description:
  # age of the subject at the time of imaging
  ANY_VALUE(PatientAge) AS PatientAge,
FROM table
"""
        descriptions = IDCIndexDataManager.parse_column_descriptions(sql_query)
        assert "PatientAge" in descriptions
        assert descriptions["PatientAge"] == "age of the subject at the time of imaging"

    def test_multiple_columns(self):
        """Test parsing multiple columns with descriptions."""
        sql_query = """
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
FROM table
"""
        descriptions = IDCIndexDataManager.parse_column_descriptions(sql_query)
        assert len(descriptions) == 3
        assert descriptions["collection_name"] == "name of the collection"
        assert descriptions["collection_id"] == "unique identifier of the collection"
        assert (
            descriptions["CancerTypes"]
            == "types of cancer represented in the collection"
        )

    def test_column_without_description(self):
        """Test that columns without descriptions are not in the result."""
        sql_query = """
SELECT
  # description:
  # name of the collection
  collection_name,
  collection_id,
FROM table
"""
        descriptions = IDCIndexDataManager.parse_column_descriptions(sql_query)
        assert "collection_name" in descriptions
        assert "collection_id" not in descriptions

    def test_extract_column_name_simple(self):
        """Test extracting column name from simple definition."""
        assert (
            IDCIndexDataManager._extract_column_name("collection_name,")
            == "collection_name"
        )
        assert (
            IDCIndexDataManager._extract_column_name("collection_name")
            == "collection_name"
        )

    def test_extract_column_name_with_as(self):
        """Test extracting column name with AS clause."""
        assert (
            IDCIndexDataManager._extract_column_name(
                "ANY_VALUE(collection_id) AS collection_id,"
            )
            == "collection_id"
        )
        assert IDCIndexDataManager._extract_column_name("column AS alias,") == "alias"

    def test_extract_column_name_complex(self):
        """Test extracting column name from complex expressions."""
        assert (
            IDCIndexDataManager._extract_column_name(
                "ROUND(SUM(SAFE_CAST(instance_size AS float64))/1000000, 2) AS series_size_MB,"
            )
            == "series_size_MB"
        )
        assert (
            IDCIndexDataManager._extract_column_name("COUNT(SOPInstanceUID) AS count,")
            == "count"
        )

    def test_complex_multiline_select(self):
        """Test parsing a complex multi-line SELECT statement."""
        sql_query = """
SELECT
  # description:
  # total size of the series in megabytes
  ROUND(SUM(SAFE_CAST(instance_size AS float64))/1000000, 2) AS series_size_MB,
FROM table
"""
        descriptions = IDCIndexDataManager.parse_column_descriptions(sql_query)
        assert "series_size_MB" in descriptions
        assert descriptions["series_size_MB"] == "total size of the series in megabytes"

    def test_no_descriptions(self):
        """Test SQL query with no descriptions."""
        sql_query = """
SELECT
  collection_name,
  collection_id,
  CancerTypes
FROM table
"""
        descriptions = IDCIndexDataManager.parse_column_descriptions(sql_query)
        assert len(descriptions) == 0

    def test_empty_description_lines(self):
        """Test handling of empty comment lines in descriptions."""
        sql_query = """
SELECT
  # description:
  # name of the collection
  #
  # additional info
  collection_name,
FROM table
"""
        descriptions = IDCIndexDataManager.parse_column_descriptions(sql_query)
        assert "collection_name" in descriptions
        # Empty comment lines should be skipped
        assert (
            descriptions["collection_name"] == "name of the collection additional info"
        )

    def test_nested_array_select_with_if(self):
        """Test parsing complex nested ARRAY/SELECT/IF statements."""
        sql_query = """
SELECT
  # description:
  # embedding medium used for the slide preparation
  ARRAY(
  SELECT
  IF
    (code IS NULL, NULL, SPLIT(code, ':')[SAFE_OFFSET(0)])
  FROM
    UNNEST(embeddingMedium_code_str) AS code ) AS embeddingMedium_CodeMeaning,
  # description:
  # embedding medium code tuple
  ARRAY(
  SELECT
  IF
    (code IS NULL, NULL,
    IF
      (STRPOS(code, ':') = 0, NULL, SUBSTR(code, STRPOS(code, ':') + 1)))
  FROM
    UNNEST(embeddingMedium_code_str) AS code ) AS embeddingMedium_code_designator_value_str,
FROM table
"""
        descriptions = IDCIndexDataManager.parse_column_descriptions(sql_query)
        assert len(descriptions) == 2
        assert "embeddingMedium_CodeMeaning" in descriptions
        assert (
            descriptions["embeddingMedium_CodeMeaning"]
            == "embedding medium used for the slide preparation"
        )
        assert "embeddingMedium_code_designator_value_str" in descriptions
        assert (
            descriptions["embeddingMedium_code_designator_value_str"]
            == "embedding medium code tuple"
        )
