from datetime import datetime, timedelta, timezone

import pytest

from studymanager.domain import apply, column_of, replay, schedule_next_review
from studymanager.events import (
    ReviewScheduled,
    StudyCompleted,
    StudyItemCreated,
    StudyPaused,
    StudyStarted,
    parse_event,
)

T0 = datetime(2026, 10, 22, 19, 0, tzinfo=timezone.utc)


def at(minutes: int) -> datetime:
    return T0 + timedelta(minutes=minutes)


def test_full_lifecycle_accumulates_time():
    events = [
        StudyItemCreated(item_id="a1", title="Kafka", tags=["kafka"], occurred_at=at(0)),
        StudyStarted(item_id="a1", occurred_at=at(0)),
        StudyPaused(item_id="a1", note="parei em offsets", occurred_at=at(25)),
        StudyStarted(item_id="a1", occurred_at=at(60)),
        StudyCompleted(item_id="a1", confidence=4, occurred_at=at(90)),
    ]
    item = replay(events)["a1"]
    assert item["status"] == "done"
    assert item["total_minutes"] == 55
    assert item["last_confidence"] == 4
    assert item["last_note"] == "parei em offsets"


def test_event_for_unknown_item_is_rejected():
    with pytest.raises(ValueError):
        apply(None, StudyStarted(item_id="x"))


def test_done_item_moves_to_review_when_due():
    item = replay([
        StudyItemCreated(item_id="a1", title="Kafka", occurred_at=at(0)),
        StudyStarted(item_id="a1", occurred_at=at(0)),
        StudyCompleted(item_id="a1", confidence=3, occurred_at=at(30)),
        ReviewScheduled(item_id="a1", review_round=1, due_at=at(30) + timedelta(days=1)),
    ])["a1"]
    assert column_of(item, at(60)) == "done"
    assert column_of(item, at(30) + timedelta(days=2)) == "review"


@pytest.mark.parametrize(
    ("previous", "confidence", "expected"),
    [(0, 4, (1, 1)), (1, 4, (2, 3)), (2, 5, (3, 7)), (9, 5, (10, 30)), (3, 2, (1, 1))],
)
def test_schedule_next_review(previous, confidence, expected):
    assert schedule_next_review(previous, confidence) == expected


def test_event_roundtrip_through_json():
    event = StudyCompleted(item_id="a1", confidence=5, note="ok")
    assert parse_event(event.model_dump_json()) == event


def test_current_streak_counts_until_yesterday_if_not_studied_today():
    from datetime import date

    from studymanager.domain import current_streak

    today = date(2026, 10, 22)
    days = {date(2026, 10, 21), date(2026, 10, 20), date(2026, 10, 18)}
    assert current_streak(days, today) == 2
    assert current_streak(days | {today}, today) == 3
    assert current_streak(set(), today) == 0
