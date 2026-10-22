"""Integração com o site (PHP + MySQL) via API HTTP.

- site-importer: de tempos em tempos busca no site os itens novos cadastrados
  no admin e publica StudyItemCreated no Kafka (marca d'água em sync_state).
- site-sync: consumer group que envia cada evento + o estado atual do item para
  o site. Se o site estiver fora do ar, o offset não é confirmado e a mensagem
  fica guardada no Kafka até a entrega funcionar.
"""
import threading
from datetime import datetime

import httpx
from rich.console import Console

from studymanager import config, db
from studymanager.events import StudyEvent, StudyItemCreated
from studymanager.kafka_io import EventProducer

console = Console()

WATERMARK_KEY = "site_items_after"


class NotReadyYet(Exception):
    """O evento ainda não foi projetado no Postgres; tenta de novo em seguida."""


class SiteClient:
    def __init__(self) -> None:
        self._http = httpx.Client(
            base_url=config.SITE_URL,
            headers={"X-StudyManager-Key": config.SITE_API_KEY},
            timeout=15,
        )

    def fetch_items(self, after: int) -> list[dict]:
        response = self._http.get("/api/study/items", params={"after": after})
        response.raise_for_status()
        return response.json()["items"]

    def push_event(self, event: StudyEvent, item: dict) -> str:
        item_json = {k: v.isoformat() if isinstance(v, datetime) else v for k, v in item.items()}
        response = self._http.post(
            "/api/study/events", json={"event": event.model_dump(mode="json"), "item": item_json}
        )
        response.raise_for_status()
        return response.json()["status"]


class SiteSync:
    group_id = "site-sync"

    def __init__(self) -> None:
        self.conn = db.connect()
        self.client = SiteClient()

    def handle(self, event: StudyEvent) -> None:
        # Envia o estado projetado pelo board-projector; como os consumer groups
        # andam em ritmos diferentes, espera o projector alcançar este evento.
        if not db.event_recorded(self.conn, event.event_id):
            raise NotReadyYet(f"{event.event_id} ainda não projetado")
        item = db.get_item(self.conn, event.item_id)
        if item is None:
            return
        self.client.push_event(event, item)


def run_site_importer(stop: threading.Event) -> None:
    client = SiteClient()
    producer = EventProducer()
    conn = db.connect()
    console.print(f"[cyan]site-importer[/] lendo {config.SITE_URL} a cada {config.SITE_POLL_SECONDS}s...")
    while not stop.is_set():
        try:
            after = int(db.get_sync_state(conn, WATERMARK_KEY) or 0)
            items = client.fetch_items(after)
            for item in items:
                # event_id fixo por item do site: importar de novo não duplica nada.
                producer.publish(StudyItemCreated(
                    event_id=f"site-item-{item['id']}",
                    item_id=f"site-{item['id']}",
                    title=item["title"],
                    tags=item["tags"],
                    occurred_at=item["created_at"],
                ))
            if items:
                producer.flush()
                db.set_sync_state(conn, WATERMARK_KEY, str(items[-1]["id"]))
                console.print(f"[cyan]site-importer[/] {len(items)} item(ns) importado(s) do site")
        except (httpx.HTTPError, RuntimeError) as exc:
            console.print(f"[yellow]site-importer: site indisponível ({exc}); tento de novo depois[/]")
        stop.wait(config.SITE_POLL_SECONDS)
