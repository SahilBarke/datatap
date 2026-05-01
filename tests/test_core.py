"""Unit tests for core modules."""
import sys
sys.path.insert(0, ".")

import pytest
from core.flattener import flatten, flatten_records, extract_results, apply_transform
from core.schema_inferrer import infer_schema
from core.config_loader import load_config, SourceConfig
from core.loader import get_engine, load_records


# ── Flattener Tests ─────────────────────────────────────────────────────────

def test_flatten_simple():
    assert flatten({"a": 1, "b": "x"}) == {"a": 1, "b": "x"}

def test_flatten_nested_dict():
    result = flatten({"trainer": {"name": "Ash", "badges": 8}})
    assert result == {"trainer_name": "Ash", "trainer_badges": 8}

def test_flatten_nested_list():
    result = flatten({"team": [{"name": "pikachu"}, {"name": "charizard"}]})
    assert result == {"team_0_name": "pikachu", "team_1_name": "charizard"}

def test_flatten_deeply_nested():
    result = flatten({"a": {"b": {"c": 42}}})
    assert result == {"a_b_c": 42}

def test_flatten_null_value():
    result = flatten({"name": None})
    assert result == {"name": None}

def test_extract_results_list():
    assert extract_results([1, 2, 3], ".") == [1, 2, 3]

def test_extract_results_dot_path():
    data = {"data": {"items": [{"id": 1}, {"id": 2}]}}
    assert extract_results(data, "data.items") == [{"id": 1}, {"id": 2}]

def test_extract_results_missing_path():
    data = {"results": [{"id": 1}]}
    # Missing path returns the whole response as single record
    result = extract_results(data, "missing.path")
    assert result == [data]


# ── Schema Inferrer Tests ────────────────────────────────────────────────────

from sqlalchemy import Integer, Float, Text, Boolean

def test_infer_schema_basic():
    records = [{"id": 1, "name": "ash", "score": 9.5, "active": True}]
    schema = infer_schema(records)
    assert schema["id"] == Integer
    assert schema["name"] == Text
    assert schema["score"] == Float
    assert schema["active"] == Boolean

def test_infer_schema_int_float_conflict():
    records = [{"val": 1}, {"val": 2.5}]
    schema = infer_schema(records)
    assert schema["val"] == Float  # resolves to Float

def test_infer_schema_type_conflict_falls_back_to_text():
    records = [{"val": 1}, {"val": "string"}]
    schema = infer_schema(records)
    assert schema["val"] == Text


# ── Transform Tests ──────────────────────────────────────────────────────────

from core.config_loader import TransformConfig

def test_transform_rename():
    from core.flattener import apply_transform
    record = {"userId": 1, "title": "hello"}
    cfg = TransformConfig(rename={"userId": "user_id"})
    result = apply_transform(record, cfg)
    assert "user_id" in result
    assert "userId" not in result

def test_transform_exclude():
    record = {"id": 1, "secret": "xyz", "name": "ash"}
    cfg = TransformConfig(exclude=["secret"])
    result = apply_transform(record, cfg)
    assert "secret" not in result
    assert "id" in result

def test_transform_include():
    record = {"id": 1, "name": "ash", "url": "http://..."}
    cfg = TransformConfig(include=["id", "name"])
    result = apply_transform(record, cfg)
    assert set(result.keys()) == {"id", "name"}


# ── DB Loader Tests ──────────────────────────────────────────────────────────

def test_load_records_creates_table(tmp_path):
    from core.config_loader import SourceConfig, StorageConfig
    engine = get_engine(f"sqlite:///{tmp_path}/test.db")
    config = SourceConfig(
        name="test",
        url="http://example.com",
        storage=StorageConfig(table_name="test_table")
    )
    records = [{"id": 1, "name": "pikachu"}, {"id": 2, "name": "bulbasaur"}]
    written = load_records(config, records, engine)
    assert written == 2

def test_upsert_does_not_duplicate(tmp_path):
    from core.config_loader import SourceConfig, StorageConfig
    from sqlalchemy import text
    engine = get_engine(f"sqlite:///{tmp_path}/upsert.db")
    config = SourceConfig(
        name="upsert_test",
        url="http://example.com",
        storage=StorageConfig(table_name="items", upsert_key="id")
    )
    records = [{"id": 1, "name": "pikachu"}]
    load_records(config, records, engine)
    # Run again with same id — should not duplicate
    load_records(config, records, engine)
    with engine.connect() as conn:
        count = conn.execute(text("SELECT COUNT(*) FROM items")).scalar()
    assert count == 1


if __name__ == "__main__":
    pytest.main([__file__, "-v"])