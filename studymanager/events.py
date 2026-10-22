"""Contrato dos eventos publicados no tópico do Kafka.

Cada evento é um fato imutável sobre um item de estudo. O estado do quadro
(todo/doing/done) nunca é gravado diretamente: ele é derivado destes eventos.
"""
from datetime import datetime, timezone
from typing import Annotated, Literal, Union
from uuid import uuid4

from pydantic import BaseModel, Field, TypeAdapter


def _now() -> datetime:
    return datetime.now(timezone.utc)


class BaseEvent(BaseModel):
    event_id: str = Field(default_factory=lambda: uuid4().hex)
    item_id: str
    occurred_at: datetime = Field(default_factory=_now)


class StudyItemCreated(BaseEvent):
    type: Literal["StudyItemCreated"] = "StudyItemCreated"
    title: str
    tags: list[str] = []


class StudyStarted(BaseEvent):
    type: Literal["StudyStarted"] = "StudyStarted"


class StudyPaused(BaseEvent):
    type: Literal["StudyPaused"] = "StudyPaused"
    note: str | None = None


class StudyCompleted(BaseEvent):
    type: Literal["StudyCompleted"] = "StudyCompleted"
    confidence: int = Field(ge=1, le=5)
    note: str | None = None


class ReviewScheduled(BaseEvent):
    type: Literal["ReviewScheduled"] = "ReviewScheduled"
    review_round: int
    due_at: datetime


StudyEvent = Annotated[
    Union[StudyItemCreated, StudyStarted, StudyPaused, StudyCompleted, ReviewScheduled],
    Field(discriminator="type"),
]

_adapter = TypeAdapter(StudyEvent)


def parse_event(raw: bytes | str) -> StudyEvent:
    return _adapter.validate_json(raw)


def event_from_dict(data: dict) -> StudyEvent:
    return _adapter.validate_python(data)
