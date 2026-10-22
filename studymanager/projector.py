"""Consumer `board-projector`: grava cada evento no event store e atualiza a
projeção `study_items`, que é o que o quadro exibe."""
from rich.console import Console

from studymanager import db
from studymanager.domain import apply
from studymanager.events import StudyEvent

console = Console()


class Projector:
    group_id = "board-projector"

    def __init__(self) -> None:
        self.conn = db.connect()

    def handle(self, event: StudyEvent) -> None:
        with self.conn.transaction():
            if not db.record_event(self.conn, event):
                return  # já processado antes (redelivery do Kafka)
            item = db.get_item(self.conn, event.item_id, for_update=True)
            try:
                new_item = apply(item, event)
            except ValueError as exc:
                console.print(f"[yellow]{self.group_id}: {exc}[/]")
                return
            db.upsert_item(self.conn, new_item)
