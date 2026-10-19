from __future__ import annotations

import hashlib

from scripts.python import idc_index_data_manager
from scripts.python.idc_index_data_manager import IDCIndexDataManager


class TestCacheKey:
    """Tests for the key of the GCS cache of generated index artifacts."""

    def test_key_changes_with_sql(self, tmp_path):
        sql_file = tmp_path / "some_index.sql"
        sql_file.write_text("SELECT 1")
        key_before = IDCIndexDataManager._compute_cache_key(str(sql_file))
        sql_file.write_text("SELECT 2")
        assert IDCIndexDataManager._compute_cache_key(str(sql_file)) != key_before

    def test_key_changes_with_generator(self, tmp_path, monkeypatch):
        """A generator change (e.g., to the description parser) must not restore
        _schema.json files built by the previous generator from the same SQL."""
        sql_file = tmp_path / "some_index.sql"
        sql_file.write_text("SELECT 1")
        key_before = IDCIndexDataManager._compute_cache_key(str(sql_file))
        monkeypatch.setattr(idc_index_data_manager, "_GENERATOR_HASH", "changed")
        assert IDCIndexDataManager._compute_cache_key(str(sql_file)) != key_before

    def test_key_is_not_the_sql_hash_alone(self, tmp_path):
        """Keys must not collide with entries cached under the SQL-only key."""
        sql_file = tmp_path / "some_index.sql"
        sql_file.write_text("SELECT 1")
        sql_only = hashlib.sha256(b"SELECT 1").hexdigest()
        assert IDCIndexDataManager._compute_cache_key(str(sql_file)) != sql_only
