"""Consumer `review-scheduler`: reage a StudyCompleted e publica ReviewScheduled
(revisão espaçada). Tem o próprio estado (`review_state`), sem depender da
projeção do quadro, como um microsserviço independente."""
from datetime import timedelta

from studymanager import config, db
from studymanager.domain import schedule_next_review
from studymanager.events import ReviewScheduled, StudyCompleted, StudyEvent
from studymanager.kafka_io import EventProducer


class ReviewScheduler:
    group_id = "review-scheduler"

    def __init__(self) -> None:
        self.conn = db.connect()
        self.producer = EventProducer()

    def handle(self, event: StudyEvent) -> None:
        if not isinstance(event, StudyCompleted):
            return
        with self.conn.transaction():
            if not db.mark_processed(self.conn, self.group_id, event.event_id):
                return
            previous = db.get_review_round(self.conn, event.item_id)
            rnd, interval = schedule_next_review(previous, event.confidence)
            db.set_review_round(self.conn, event.item_id, rnd)
            # event_id derivado do evento de origem: se publicarmos de novo após
            # uma falha, o projector descarta a duplicata.
            self.producer.publish(ReviewScheduled(
                event_id=f"review-{event.event_id}",
                item_id=event.item_id,
                review_round=rnd,
                due_at=event.occurred_at + timedelta(**{config.REVIEW_UNIT: interval}),
            ))
            self.producer.flush()
