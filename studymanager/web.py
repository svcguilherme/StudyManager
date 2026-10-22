"""Quadro kanban web (FastAPI). Cada ação vira um evento no Kafka, igual à CLI."""
import time
from datetime import datetime
from pathlib import Path
from urllib.parse import urlencode

from fastapi import FastAPI, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from studymanager import commands, config, db
from studymanager.domain import COLUMNS, column_of, current_streak
from studymanager.events import StudyEvent
from studymanager.kafka_io import EventProducer

app = FastAPI(title="StudyManager", docs_url=None, redoc_url=None)
templates = Jinja2Templates(directory=Path(__file__).parent / "templates")
_producer: EventProducer | None = None


def _get_producer() -> EventProducer:
    global _producer
    _producer = _producer or EventProducer()
    return _producer


def _wait_projected(event: StudyEvent, timeout: float = 3) -> None:
    # Consistência eventual: espera um instante o projector gravar o evento,
    # para o quadro já aparecer atualizado depois do redirect.
    deadline = time.monotonic() + timeout
    with db.connect() as conn:
        while time.monotonic() < deadline and not db.event_recorded(conn, event.event_id):
            time.sleep(0.1)


def _act(action, *args) -> RedirectResponse:
    try:
        event = action(*args, producer=_get_producer())
    except commands.CommandError as exc:
        return RedirectResponse("/?" + urlencode({"error": str(exc)}), status_code=303)
    _wait_projected(event)
    return RedirectResponse("/", status_code=303)


def _fmt_minutes(minutes: int) -> str:
    return f"{minutes // 60}h{minutes % 60:02d}"


templates.env.filters["minutes"] = _fmt_minutes
templates.env.filters["local"] = lambda value: value.astimezone(config.TIMEZONE).strftime("%d/%m %H:%M") if value else ""


@app.get("/", response_class=HTMLResponse)
def board(request: Request, error: str | None = None):
    now = datetime.now(config.TIMEZONE)
    with db.connect() as conn:
        items = db.all_items(conn)
        by_tag = db.minutes_by_tag(conn)
        days = db.study_days(conn)
        version = db.board_version(conn)
    columns: dict[str, list[dict]] = {key: [] for key in COLUMNS}
    for item in items:
        item["elapsed"] = int((now - item["started_at"]).total_seconds() // 60) if item["started_at"] else 0
        columns[column_of(item, now)].append(item)
    return templates.TemplateResponse(request, "board.html", {
        "columns": columns,
        "labels": COLUMNS,
        "wip_limit": config.WIP_LIMIT,
        "total_minutes": sum(item["total_minutes"] for item in items),
        "streak": current_streak(days, now.date()),
        "by_tag": by_tag[:6],
        "version": version,
        "site_enabled": bool(config.SITE_URL),
        "error": error,
    })


@app.get("/api/version")
def version() -> dict:
    with db.connect() as conn:
        return {"version": db.board_version(conn)}


@app.post("/items")
def add(title: str = Form(...), tags: str = Form("")):
    return _act(commands.add_item, title, tags.split(","))


@app.post("/items/{item_id}/start")
def start(item_id: str, force: bool = Form(False)):
    return _act(commands.start_item, item_id, force)


@app.post("/items/{item_id}/pause")
def pause(item_id: str, note: str = Form("")):
    return _act(commands.pause_item, item_id, note.strip())


@app.post("/items/{item_id}/done")
def done(item_id: str, confidence: int = Form(...), note: str = Form("")):
    return _act(commands.complete_item, item_id, confidence, note.strip())
