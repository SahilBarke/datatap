"""
DataTap Web App
FastAPI backend serving the dashboard, API routes, and static files.
"""

from __future__ import annotations
import sys
import os
import io
import csv
from pathlib import Path
from contextlib import asynccontextmanager

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from fastapi import FastAPI, HTTPException, Form
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.templating import Jinja2Templates
from fastapi.requests import Request
import yaml
from sqlalchemy import text, inspect

from core.config_loader import load_all_configs, load_config, SourceConfig
from core.pipeline import run_pipeline
from core.loader import get_engine
from core.run_log import init_log_db, save_run, get_runs, get_source_stats
from core.scheduler import start_scheduler, stop_scheduler, register_source

BASE_DIR = Path(__file__).parent.parent
CONFIGS_DIR = BASE_DIR / "configs"
DB_URL = os.getenv("DATABASE_URL", f"sqlite:///{BASE_DIR}/datatap.db")


@asynccontextmanager
async def lifespan(app: FastAPI):
    engine = get_engine(DB_URL)
    init_log_db(engine)
    configs = load_all_configs(CONFIGS_DIR)
    start_scheduler(configs, engine)
    yield
    stop_scheduler()


app = FastAPI(title="DataTap", lifespan=lifespan)

TEMPLATES_DIR = Path(__file__).parent / "templates"
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))


# ── Helpers ───────────────────────────────────────────────────────────────────


def _engine():
    return get_engine(DB_URL)


def _get_table_count(table_name: str) -> int:
    try:
        with _engine().connect() as conn:
            return (
                conn.execute(text(f'SELECT COUNT(*) FROM "{table_name}"')).scalar() or 0
            )
    except Exception:
        return 0


def _configs() -> list[SourceConfig]:
    return load_all_configs(CONFIGS_DIR)


def _render(request: Request, template: str, context: dict):
    """Compatibility wrapper — works with both old and new Starlette versions."""
    try:
        # Starlette 0.28+ new signature
        return templates.TemplateResponse(
            request=request, name=template, context=context
        )
    except TypeError:
        # Older Starlette — request must be inside context dict
        return templates.TemplateResponse(template, {"request": request, **context})


# ── Pages ─────────────────────────────────────────────────────────────────────


@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    configs = _configs()
    stats = get_source_stats(_engine())
    sources = []
    for cfg in configs:
        s = stats.get(cfg.name, {})
        sources.append(
            {
                "name": cfg.name,
                "table": cfg.storage.table_name,
                "interval_mins": cfg.schedule.interval_mins,
                "row_count": _get_table_count(cfg.storage.table_name),
                "last_run": s.get("last_run"),
                "last_success": s.get("last_success", None),
                "run_count": s.get("run_count", 0),
                "last_error": s.get("last_error"),
            }
        )
    return _render(
        request,
        "dashboard.html",
        {
            "sources": sources,
            "total_sources": len(sources),
        },
    )


@app.get("/source/{name}", response_class=HTMLResponse)
async def source_detail(request: Request, name: str):
    configs = {c.name: c for c in _configs()}
    if name not in configs:
        raise HTTPException(404, "Source not found")
    cfg = configs[name]
    runs = get_runs(_engine(), source_name=name, limit=20)
    row_count = _get_table_count(cfg.storage.table_name)

    columns = []
    try:
        insp = inspect(_engine())
        columns = [
            c["name"]
            for c in insp.get_columns(cfg.storage.table_name)
            if c["name"] != "_id"
        ]
    except Exception:
        pass

    sample_rows = []
    try:
        with _engine().connect() as conn:
            rows = conn.execute(
                text(f'SELECT * FROM "{cfg.storage.table_name}" LIMIT 10')
            ).fetchall()
            sample_rows = [dict(r._mapping) for r in rows]
    except Exception:
        pass

    return _render(
        request,
        "source_detail.html",
        {
            "cfg": cfg,
            "runs": runs,
            "row_count": row_count,
            "columns": columns,
            "sample_rows": sample_rows,
        },
    )


@app.get("/add", response_class=HTMLResponse)
async def add_source_page(request: Request):
    return _render(request, "add_source.html", {"error": None, "success": None})


# ── API Routes ────────────────────────────────────────────────────────────────


@app.post("/api/run/{name}")
async def run_now(name: str):
    configs = {c.name: c for c in _configs()}
    if name not in configs:
        raise HTTPException(404, "Source not found")
    cfg = configs[name]
    result = run_pipeline(cfg, _engine())
    save_run(_engine(), result)
    return {
        "success": result.success,
        "records_fetched": result.records_fetched,
        "records_written": result.records_written,
        "error": result.error,
        "duration_s": (result.finished_at - result.started_at).total_seconds(),
    }


@app.post("/api/add-source")
async def add_source(yaml_content: str = Form(...)):
    try:
        raw = yaml.safe_load(yaml_content)
        cfg = SourceConfig(**raw)
    except Exception as e:
        raise HTTPException(400, f"Invalid config: {e}")

    config_path = CONFIGS_DIR / f"{cfg.name}.yaml"
    if config_path.exists():
        raise HTTPException(409, f"Source '{cfg.name}' already exists")

    config_path.write_text(yaml_content)
    register_source(cfg, _engine())
    return {"message": f"Source '{cfg.name}' added successfully"}


@app.get("/api/export/{name}")
async def export_csv(name: str):
    configs = {c.name: c for c in _configs()}
    if name not in configs:
        raise HTTPException(404, "Source not found")
    table = configs[name].storage.table_name
    try:
        with _engine().connect() as conn:
            rows = conn.execute(text(f'SELECT * FROM "{table}"')).fetchall()
    except Exception as e:
        raise HTTPException(500, f"Could not read table: {e}")

    if not rows:
        raise HTTPException(404, "No data to export")

    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=rows[0]._mapping.keys())
    writer.writeheader()
    for row in rows:
        writer.writerow(dict(row._mapping))

    output.seek(0)
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={name}.csv"},
    )


@app.get("/api/preview/{name}")
async def preview_data(name: str, limit: int = 20, offset: int = 0):
    configs = {c.name: c for c in _configs()}
    if name not in configs:
        raise HTTPException(404, "Source not found")
    table = configs[name].storage.table_name
    try:
        with _engine().connect() as conn:
            rows = conn.execute(
                text(f'SELECT * FROM "{table}" LIMIT :limit OFFSET :offset'),
                {"limit": limit, "offset": offset},
            ).fetchall()
            total = conn.execute(text(f'SELECT COUNT(*) FROM "{table}"')).scalar()
        return {"rows": [dict(r._mapping) for r in rows], "total": total}
    except Exception as e:
        raise HTTPException(500, str(e))


@app.get("/api/logs/{name}")
async def get_logs(name: str):
    runs = get_runs(_engine(), source_name=name, limit=30)
    return [
        {
            "id": r.id,
            "started_at": r.started_at.isoformat() if r.started_at else None,
            "finished_at": r.finished_at.isoformat() if r.finished_at else None,
            "records_fetched": r.records_fetched,
            "records_written": r.records_written,
            "success": r.success,
            "error": r.error,
        }
        for r in runs
    ]


@app.delete("/api/source/{name}")
async def delete_source(name: str):
    config_path = CONFIGS_DIR / f"{name}.yaml"
    if not config_path.exists():
        raise HTTPException(404, "Source not found")
    config_path.unlink()
    return {"message": f"Source '{name}' deleted"}
