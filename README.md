# DataTap

**A config-driven, generic API → Database pipeline.**  
Drop a YAML file, get a fully automated data collection pipeline.

---

## What it does

DataTap lets you collect JSON data from any REST API and store it in a database — without writing any code per data source. You describe the API in a YAML config file, and DataTap handles fetching, flattening nested JSON, inferring the database schema, creating tables automatically, and running on a schedule.

```YAML Config → Fetch → Flatten → Infer Schema → Store → Repeat on schedule```

---

## Quickstart

```bash
# 1. Clone and install
git clone https://github.com/sahilbarke/datatap
cd datatap
pip install -r requirements.txt

# 2. Copy env config
cp .env.example .env

# 3. Add a data source config
cp configs/pokemon.yaml configs/myapi.yaml
# Edit myapi.yaml with your API details
# configs/remote.yaml is there for example

# 4. Run
python main.py
# → Open http://localhost:8000
# or 
uvicorn web.app:app
# -> Open http://127.0.0.1:8000
```

---

## YAML Config Reference

Every data source is described by a single YAML file in the `configs/` folder.

```yaml
name: pokemon                          # unique slug (lowercase, underscores)
url: https://pokeapi.co/api/v2/pokemon
method: GET                            # GET or POST

auth:
  type: none                           # none | api_key | bearer
  token: YOUR_TOKEN                    # for bearer
  api_key: YOUR_KEY                    # for api_key
  api_key_header: X-API-Key            # header name for api_key

params:                                # static query params appended to every request
  lang: en

pagination:
  type: offset                         # none | offset | page | cursor
  results_path: results                # dot-path to list in response e.g. "data.items"
  limit_param: limit
  offset_param: offset
  limit: 100
  max_pages: 10                        # safety cap

schedule:
  interval_minutes: 60                 # how often to auto-fetch

storage:
  table: pokemon                       # DB table name (auto-created)
  upsert_key: id                       # optional: field to use for dedup/update

transform:
  rename:                              # rename columns
    userId: user_id
  exclude:                             # drop these fields
    - internal_code
  include: []                          # if set, keep ONLY these fields
```

---

## Web UI

| Page | What it shows |
|---|---|
| **Dashboard** | All sources, row counts, last run status, Run Now / Export buttons |
| **Source detail** | Data preview table, schema columns, run history log |
| **Add source** | YAML editor with validation, example configs |

---

## Database Support

**SQLite** (default — no setup needed):
```env
DATABASE_URL=sqlite:///datatap.db
```

**PostgreSQL**:
```env
DATABASE_URL=postgresql://user:password@localhost:5432/datatap
```

Switch by changing the `DATABASE_URL` in your `.env` file. No other code changes needed.

---

## How schema inference works

DataTap automatically creates and migrates your database tables:

1. After fetching, all JSON records are **flattened** — nested objects become `parent_child` columns, arrays become `field_0_key`, `field_1_key`, etc.
2. Python types are mapped to SQL types: `int→INTEGER`, `float→REAL`, `str→TEXT`, `bool→BOOLEAN`
3. On first run: `CREATE TABLE` with all inferred columns
4. On subsequent runs: if new fields appear, `ALTER TABLE ADD COLUMN` runs automatically

You never write SQL or migrations.

---

## Project Structure

```
datatap/
  core/
    config_loader.py    → Pydantic YAML validation models
    fetcher.py          → HTTP requests, auth, pagination, retries
    flattener.py        → nested JSON → flat dict + transforms
    schema_inferrer.py  → infer SQL types, CREATE/ALTER TABLE
    loader.py           → INSERT / UPSERT into database
    pipeline.py         → orchestrates the full fetch→store flow
    run_log.py          → stores run history in _datatap_runs table
    scheduler.py        → APScheduler background jobs per source
  web/
    app.py              → FastAPI routes + lifespan
    templates/          → Jinja2 HTML templates
  configs/              → your YAML source configs live here
  main.py               → app entry point
  tests/
    test_core.py        → unit tests (16 tests)
```

---

## Example: Collect Pokemon data

```yaml
# configs/pokemon.yaml
name: pokemon
url: https://pokeapi.co/api/v2/pokemon
method: GET
auth:
  type: none
pagination:
  type: offset
  results_path: results
  limit: 100
  max_pages: 5
schedule:
  interval_minutes: 1440
storage:
  table: pokemon
  upsert_key: name
```

After running, query your data directly:
```sql
SELECT name, url FROM pokemon LIMIT 10;
```

Or export to CSV from the dashboard and load into a notebook:
```python
import pandas as pd
df = pd.read_csv("pokemon.csv")
```

---

## Stack

- **FastAPI** — web framework
- **SQLAlchemy** — database ORM (SQLite + PostgreSQL)
- **APScheduler** — background job scheduling
- **httpx** — async HTTP client
- **Pydantic** — config validation
- **PyYAML** — YAML parsing
- **Jinja2** — HTML templating
