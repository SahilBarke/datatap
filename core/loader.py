"""
DB Loader
Handles database connections (SQLite / PostgreSQL) and
record insertion with optional upsert support.
"""

from __future__ import annotations
import os
import logging
import re
from typing import Dict, Any
from threading import Lock

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from core.config_loader import SourceConfig
from core.schema_inferrer import ensure_table, infer_schema

logger = logging.getLogger(__name__)

# Cache engines by database URL to reuse connections across calls
_engines: dict[str, Engine] = {}
_engine_lock = Lock()  # ensures thread-safe engine creation

# Regex to validate SQL identifiers
_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _validate_identifier(name: str) -> str:
    """Validate table/column names to avoid SQL injection."""
    if not _IDENTIFIER_RE.match(name):
        raise ValueError(f"Invalid SQL identifier: {name}")
    return name


def get_engine(db_url: str | None = None) -> Engine:
    """
    Get or create a SQLAlchemy engine for the given database URL.
    Uses caching to reuse connections across calls.
    """
    if db_url is None:
        # Default DB URL can be set via environment variable or defaults to SQLite file
        db_url = os.getenv("DATABASE_URL", "sqlite:///datatap.db")

    # Ensure only one thread creates the engine
    with _engine_lock:
        if db_url not in _engines:
            connect_args = (
                {"check_same_thread": False} if db_url.startswith("sqlite") else {}
            )

            # pool_pre_ping ensures dead connections are automatically refreshed
            _engines[db_url] = create_engine(
                db_url,
                connect_args=connect_args,
                pool_pre_ping=True,
                future=True,
            )

            logger.info("Connected to database: %s", db_url)

    return _engines[db_url]


def load_records(
    config: SourceConfig,
    flat_records: list[Dict[str, Any]],
    engine: Engine | None = None,
) -> int:
    """
    Load records into the database with optional upsert logic.

    Flow:
    - Infer schema
    - Ensure table exists
    - For each record:
        - Clean fields
        - If upsert_key exists:
            - UPDATE if exists
            - INSERT if not
        - Else:
            - INSERT
    """
    if not flat_records:
        logger.info("No records to load for '%s'", config.name)
        return 0

    if engine is None:
        engine = get_engine()

    # Validate identifiers before using in raw SQL
    table_name = _validate_identifier(config.storage.table_name)
    upsert_key = config.storage.upsert_key
    if upsert_key:
        upsert_key = _validate_identifier(upsert_key)

    # Infer schema dynamically from incoming records
    schema = infer_schema(flat_records)

    # Create or update table structure if needed
    table = ensure_table(engine, table_name, schema)

    written = 0

    # Transaction block:
    # - commits automatically on success
    # - rolls back automatically on failure
    with engine.begin() as conn:

        # If upsert key is defined, load existing keys into memory for fast lookup
        existing_keys = set()
        if upsert_key:
            result = conn.execute(text(f'SELECT "{upsert_key}" FROM "{table_name}"'))
            existing_keys = {row[0] for row in result}

        for record in flat_records:
            # Keep only fields that exist in schema
            clean = {k: v for k, v in record.items() if k in schema}

            dropped = set(record) - set(clean)
            if dropped:
                logger.debug("Dropped fields for '%s': %s", table_name, dropped)

            if upsert_key and upsert_key in clean:
                # Check existence using in-memory set for performance
                if clean[upsert_key] in existing_keys:

                    # Build dynamic UPDATE clause excluding the upsert key
                    set_clause = ", ".join(
                        f'"{k}" = :{k}' for k in clean if k != upsert_key
                    )

                    # Only run UPDATE if there are fields to update
                    if set_clause:
                        conn.execute(
                            text(
                                f'UPDATE "{table_name}" '
                                f"SET {set_clause} "
                                f'WHERE "{upsert_key}" = :_upsert_val'
                            ),
                            {**clean, "_upsert_val": clean[upsert_key]},
                        )
                else:
                    # INSERT new record
                    conn.execute(table.insert(), clean)
                    written += 1

                    # Keep in-memory cache updated to avoid duplicate inserts in same batch
                    existing_keys.add(clean[upsert_key])
            else:
                # No upsert key → always insert
                conn.execute(table.insert(), clean)
                written += 1

    logger.info("Wrote %d new records to '%s'", written, table_name)
    return written
