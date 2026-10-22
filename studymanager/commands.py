"""Casos de uso compartilhados pela CLI e pelo quadro web: validam contra a
projeção e publicam o evento correspondente no Kafka."""
from uuid import uuid4

from studymanager import config, db
from studymanager.events import (
    StudyCompleted,
    StudyEvent,
    StudyItemCreated,
    StudyPaused,
    StudyStarted,
)
from studymanager.kafka_io import EventProducer


class CommandError(Exception):
    pass


def _publish(event: StudyEvent, producer: EventProducer | None) -> StudyEvent:
    producer = producer or EventProducer()
    producer.publish(event)
    producer.flush()
    return event


def find_item(conn, id_prefix: str) -> dict:
    matches = db.find_items(conn, id_prefix)
    if not matches:
        raise CommandError(f"Nenhum item com id '{id_prefix}'. O worker está rodando?")
    if len(matches) > 1:
        raise CommandError(f"Id ambíguo: {', '.join(m['id'] for m in matches)}")
    return matches[0]


def add_item(title: str, tags: list[str], producer: EventProducer | None = None) -> StudyEvent:
    title = title.strip()
    if not title:
        raise CommandError("Informe o que você quer estudar.")
    tags = list(dict.fromkeys(t.strip().lower() for t in tags if t.strip()))
    return _publish(StudyItemCreated(item_id=uuid4().hex[:6], title=title, tags=tags), producer)


def start_item(id_prefix: str, force: bool = False, producer: EventProducer | None = None) -> StudyEvent:
    with db.connect() as conn:
        item = find_item(conn, id_prefix)
        doing = db.count_doing(conn)
    if item["status"] == "doing":
        raise CommandError("Esse item já está em Doing.")
    if doing >= config.WIP_LIMIT and not force:
        raise CommandError(
            f"Limite de WIP atingido ({doing}/{config.WIP_LIMIT}). Termine ou pause algo antes."
        )
    return _publish(StudyStarted(item_id=item["id"]), producer)


def pause_item(id_prefix: str, note: str | None = None, producer: EventProducer | None = None) -> StudyEvent:
    with db.connect() as conn:
        item = find_item(conn, id_prefix)
    if item["status"] != "doing":
        raise CommandError("Só dá para pausar um item que está em Doing.")
    return _publish(StudyPaused(item_id=item["id"], note=note or None), producer)


def complete_item(
    id_prefix: str, confidence: int, note: str | None = None, producer: EventProducer | None = None
) -> StudyEvent:
    with db.connect() as conn:
        item = find_item(conn, id_prefix)
    if item["status"] != "doing":
        raise CommandError("Comece o item antes de concluí-lo.")
    if not 1 <= confidence <= 5:
        raise CommandError("A confiança vai de 1 a 5.")
    return _publish(StudyCompleted(item_id=item["id"], confidence=confidence, note=note or None), producer)
