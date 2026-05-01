# Test script to poke at the PokeAPI and verify end-to-end flow

import sys
sys.path.insert(0, '.')

from core.flattener import flatten, flatten_records
from core.schema_inferrer import infer_schema
from core.loader import get_engine, load_records
from core.config_loader import load_config
from sqlalchemy import text


def main():
    # Simulated fetched data
    mock_records = [
        {'id': 1, 'name': 'bulbasaur', 'url': 'https://pokeapi.co/api/v2/pokemon/1/'},
        {'id': 2, 'name': 'ivysaur', 'url': 'https://pokeapi.co/api/v2/pokemon/2/'},
        {'id': 3, 'name': 'venusaur', 'url': 'https://pokeapi.co/api/v2/pokemon/3/'},
    ]

    # Test flattening nested JSON
    nested = {
        'id': 1,
        'trainer': {'name': 'Ash', 'badges': 8},
        'team': [{'name': 'pikachu'}, {'name': 'charizard'}]
    }

    flat = flatten(nested)

    print('Flattened nested JSON:')
    for k, v in flat.items():
        print(f'  {k}: {v}')

    # Test schema inference
    flat_records = flatten_records(mock_records)
    schema = infer_schema(flat_records)

    print()
    print('Inferred schema:', {k: v.__name__ for k, v in schema.items()})

    # Test DB load
    config = load_config('configs/pokemon.yaml')
    engine = get_engine('sqlite:///test_datatap.db')

    written = load_records(config, flat_records, engine)
    print(f'Written: {written} rows')

    # Verify DB contents
    with engine.connect() as conn:
        rows = conn.execute(text('SELECT * FROM pokemon')).fetchall()
        print(f'Rows in DB: {len(rows)}')
        print('First row:', dict(rows[0]._mapping))


if __name__ == "__main__":
    main()