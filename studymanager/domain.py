"""Regras de negócio puras (sem Kafka nem banco), fáceis de testar.

`apply` é o "reducer" do Event Sourcing: estado_atual + evento -> novo estado.
É usado tanto pelo projector (que grava no Postgres) quanto pelo replay em
memória (viagem no tempo e rebuild).
"""
from datetime import date, datetime, timedelta

from studymanager import config
from studymanager.events import (
    ReviewScheduled,
    StudyCompleted,
    StudyEvent,
    StudyItemCreated,
    StudyPaused,
    StudyStarted,
)

COLUMNS = {"todo": "To Do", "doing": "Doing", "review": "To Review", "done": "Done"}


def _elapsed_minutes(item: dict, until: datetime) -> int:
    if item["started_at"] is None:
        return 0
    return round((until - item["started_at"]).total_seconds() / 60)


def apply(item: dict | None, event: StudyEvent) -> dict:
    if isinstance(event, StudyItemCreated):
        return {
            "id": event.item_id,
            "title": event.title,
            "tags": list(event.tags),
            "status": "todo",
            "created_at": event.occurred_at,
            "started_at": None,
            "total_minutes": 0,
            "review_round": 0,
            "next_review_at": None,
            "last_confidence": None,
            "last_note": None,
        }

    if item is None:
        raise ValueError(f"{event.type} para item inexistente: {event.item_id}")
    item = dict(item)

    match event:
        case StudyStarted():
            item["status"] = "doing"
            item["started_at"] = event.occurred_at
        case StudyPaused():
            item["total_minutes"] += _elapsed_minutes(item, event.occurred_at)
            item["status"] = "todo"
            item["started_at"] = None
            item["last_note"] = event.note or item["last_note"]
        case StudyCompleted():
            item["total_minutes"] += _elapsed_minutes(item, event.occurred_at)
            item["status"] = "done"
            item["started_at"] = None
            item["last_confidence"] = event.confidence
            item["last_note"] = event.note or item["last_note"]
        case ReviewScheduled():
            item["review_round"] = event.review_round
            item["next_review_at"] = event.due_at
    return item


def replay(events: list[StudyEvent]) -> dict[str, dict]:
    items: dict[str, dict] = {}
    for event in events:
        try:
            items[event.item_id] = apply(items.get(event.item_id), event)
        except ValueError:
            continue
    return items


def column_of(item: dict, now: datetime) -> str:
    if item["status"] == "done" and item["next_review_at"] and item["next_review_at"] <= now:
        return "review"
    return item["status"]


def current_streak(study_days: set[date], today: date) -> int:
    """Dias seguidos estudando até hoje (ou até ontem, se hoje ainda não estudou)."""
    day = today if today in study_days else today - timedelta(days=1)
    streak = 0
    while day in study_days:
        streak += 1
        day -= timedelta(days=1)
    return streak


def schedule_next_review(previous_round: int, confidence: int) -> tuple[int, int]:
    """Retorna (nova_rodada, intervalo) da revisão espaçada.

    Confiança baixa (1-2) reinicia o ciclo; caso contrário avança um degrau.
    """
    if confidence <= 2:
        return 1, config.REVIEW_INTERVALS[0]
    rnd = previous_round + 1
    return rnd, config.REVIEW_INTERVALS[min(rnd, len(config.REVIEW_INTERVALS)) - 1]
