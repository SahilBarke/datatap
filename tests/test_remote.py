# Test script to poke at the RemoteOK API and verify end-to-end flow
import sys
sys.path.insert(0, '.')

from core.config_loader import load_config
from core.fetcher import fetch_all
from core.flattener import flatten_records
from core.schema_inferrer import infer_schema
from core.loader import get_engine, load_records


def main():
    print("Loading config...")
    config = load_config("configs/remote.yaml")

    print("Fetching data...")
    records = fetch_all(config)

    print(f"Fetched: {len(records)} records")

    if not records:
        print("No data")
        return

    print("\nSample record:")
    print(records[0])

    # Flatten (safe even if already flat)
    flat = flatten_records(records)

    schema = infer_schema(flat)
    print("\nSchema:")
    print({k: v.__name__ for k, v in schema.items()})

    engine = get_engine("sqlite:///remoteok.db")

    written = load_records(config, flat, engine)
    print(f"\nWritten: {written} rows")

    from sqlalchemy import text
    with engine.connect() as conn:
        rows = conn.execute(text("SELECT * FROM jobs")).fetchall()
        print("Rows:", len(rows))
        print("First:", dict(rows[0]._mapping))


if __name__ == "__main__":
    main()