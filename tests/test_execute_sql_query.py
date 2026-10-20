from __future__ import annotations

from unittest.mock import MagicMock

from scripts.python.idc_index_data_manager import IDCIndexDataManager


def test_query_runs_once(tmp_path):
    """Each index query is billed per run, so it must be sent to BigQuery exactly once."""
    sql_file = tmp_path / "some_index.sql"
    sql_file.write_text("SELECT 1 AS x")

    # Skip __init__, which would create a real BigQuery client.
    manager = IDCIndexDataManager.__new__(IDCIndexDataManager)
    manager.client = MagicMock()

    _, basename, schema, sql = manager.execute_sql_query(str(sql_file))

    manager.client.query.assert_called_once_with("SELECT 1 AS x")
    result = manager.client.query.return_value.result.return_value
    assert schema is result.schema
    assert basename == "some_index"
    assert sql == "SELECT 1 AS x"
