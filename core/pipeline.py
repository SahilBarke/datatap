"""
Pipeline
The main orchestrator. Ties together: fetch → flatten → transform → infer → load.
Also used by the scheduler and the web UI's "Run Now" button.
"""

from __future__ import annotations
import logging
from datetime import datetime
from dataclasses import dataclass, field

from core.config_loader import SourceConfig
from core.fetcher import fetch_all
from core.flattener import flatten_records, apply_transform
from core.loader import load_records, get_engine
from sqlalchemy.engine import Engine

# Logger setup (production-friendly instead of print)
logger = logging.getLogger(__name__)


@dataclass
class RunResult:
    source_name: str
    started_at: datetime
    finished_at: datetime | None = None
    records_fetched: int = 0
    records_written: int = 0
    success: bool = False
    error: str | None = None
    sample: list[dict] = field(default_factory=list)  # first 5 records for preview


def run_pipeline(config: SourceConfig, engine: Engine | None = None) -> RunResult:
    """Run the full pipeline for a single source config."""
    result = RunResult(source_name=config.name, started_at=datetime.utcnow())

    try:
        logger.info("Pipeline started for source='%s'", config.name)

        # 1. Fetch raw JSON records
        raw_records = fetch_all(config)
        result.records_fetched = len(raw_records)
        logger.info("Fetched %d records for '%s'", result.records_fetched, config.name)

        # 2. Flatten nested JSON
        flat_records = flatten_records(raw_records)
        logger.info("Flattened to %d records for '%s'", len(flat_records), config.name)

        # 3. Apply transforms (rename, include, exclude)
        flat_records = [apply_transform(r, config.transform) for r in flat_records]

        # 4. Store sample for preview (before loading)
        result.sample = flat_records[:5]

        # 5. Load into database
        if engine is None:
            engine = get_engine()

        result.records_written = load_records(config, flat_records, engine)
        logger.info("Wrote %d records for '%s'", result.records_written, config.name)

        result.success = True
        logger.info("Pipeline SUCCESS for '%s'", config.name)

    except Exception:
        result.success = False
        result.error = "Pipeline failed"
        logger.exception("Pipeline ERROR for source='%s'", config.name)

    finally:
        result.finished_at = datetime.utcnow()

    duration = (result.finished_at - result.started_at).total_seconds()
    status = "SUCCESS" if result.success else "FAILED"

    logger.info(
        "Pipeline %s | source='%s' | written=%d | duration=%.2fs",
        status,
        config.name,
        result.records_written,
        duration,
    )

    return result
