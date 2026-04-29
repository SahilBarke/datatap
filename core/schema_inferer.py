"""
Schema Inferrer
Looks at flat dicts, infers SQL column types, and creates/migrates
database tables automatically using SQLAlchemy.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, Type

from sqlalchemy import (
    MetaData,
    Table,
    Column,
    Text,
    Integer,
    Float,
    Boolean,
    inspect,
    text,
)
from sqlalchemy.engine import Engine


# Logging setup
logger = logging.getLogger(__name__)


# Python → SQLAlchemy type mapping
TYPE_MAP: Dict[Type[Any], Type] = {
    int: Integer,
    float: Float,
    bool: Boolean,
    str: Text,
    type(None): Text,  # NULL defaults to TEXT (safe fallback)
}


# Type inference helpers
def normalize_type(value: Any) -> Type:
    """
    Normalize Python value to a base type.

    Important:
    - bool must be checked before int (since bool is subclass of int)
    """
    if isinstance(value, bool):
        return bool
    if isinstance(value, int):
        return int
    if isinstance(value, float):
        return float
    if isinstance(value, str):
        return str
    if value is None:
        return type(None)

    # Unknown types fallback
    return str


def resolve_type_conflict(existing: Type, new: Type) -> Type:
    """
    Resolve conflicts between two Python types.

    Rules:
    - int + float → float
    - anything else → str (TEXT)
    """
    if existing == new:
        return existing

    if {existing, new} == {int, float}:
        return float

    logger.warning(f"Type conflict detected: {existing} vs {new}, defaulting to TEXT")
    return str


# Schema inference
def infer_schema(records: list[dict]) -> dict[str, Type]:
    """
    Infer schema from records.

    Returns:
        {column_name: SQLAlchemy type class}
    """
    schema: Dict[str, Type] = {}

    for record in records:
        for key, value in record.items():
            new_type = normalize_type(value)

            if key not in schema:
                schema[key] = new_type
                continue

            schema[key] = resolve_type_conflict(schema[key], new_type)

    # Convert Python types → SQLAlchemy column types
    return {col: TYPE_MAP.get(t, Text) for col, t in schema.items()}


# Identifier sanitization (basic safety)
def sanitize_identifier(name: str) -> str:
    """
    Basic sanitization for SQL identifiers.
    """
    return "".join(c for c in name if c.isalnum() or c == "_")


# Table creation / migration
def ensure_table(engine: Engine, table_name: str, schema: dict[str, Type]) -> Table:
    """
    Ensure table exists and matches schema.
    - Creates table if missing
    - Adds new columns if needed
    """
    metadata = MetaData()
    inspector = inspect(engine)

    safe_table_name = sanitize_identifier(table_name)

    if safe_table_name not in inspector.get_table_names():
        # Create new table
        columns = [Column("_id", Integer, primary_key=True, autoincrement=True)]

        for col_name, col_type in schema.items():
            safe_col = sanitize_identifier(col_name)
            columns.append(Column(safe_col, col_type()))

        table = Table(safe_table_name, metadata, *columns)
        metadata.create_all(engine)

        logger.info(f"Created table '{safe_table_name}' with {len(schema)} columns")
        return table

    # Existing table → check for missing columns
    existing_cols = {c["name"] for c in inspector.get_columns(safe_table_name)}

    new_cols = {
        k: v for k, v in schema.items() if sanitize_identifier(k) not in existing_cols
    }

    if new_cols:
        # Use transaction-safe execution
        with engine.begin() as conn:
            for col_name, col_type in new_cols.items():
                safe_col = sanitize_identifier(col_name)

                col_sql = col_type().compile(dialect=engine.dialect)

                conn.execute(
                    text(
                        f'ALTER TABLE "{safe_table_name}" '
                        f'ADD COLUMN "{safe_col}" {col_sql}'
                    )
                )

                logger.info(f"Added column '{safe_col}' to '{safe_table_name}'")

    # Reflect updated table
    metadata.reflect(bind=engine, only=[safe_table_name])
    return metadata.tables[safe_table_name]
