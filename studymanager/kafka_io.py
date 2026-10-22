"""Infraestrutura Kafka: producer, criação do tópico e loop genérico de consumer."""
import threading
from collections.abc import Callable

from confluent_kafka import Consumer, KafkaError, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic
from pydantic import ValidationError
from rich.console import Console

from studymanager import config
from studymanager.events import StudyEvent, parse_event

console = Console()


class EventProducer:
    def __init__(self) -> None:
        # acks=all + idempotência: o broker não grava a mesma mensagem duas vezes em retries.
        self._producer = Producer({
            "bootstrap.servers": config.KAFKA_BOOTSTRAP,
            "enable.idempotence": True,
            "acks": "all",
        })
        self._errors: list[KafkaError] = []

    def _on_delivery(self, err, _msg) -> None:
        if err is not None:
            self._errors.append(err)

    def publish(self, event: StudyEvent) -> None:
        # A chave é o item_id: todos os eventos de um item caem na mesma partição,
        # o que garante a ordem deles para os consumers.
        self._producer.produce(
            config.TOPIC,
            key=event.item_id,
            value=event.model_dump_json(),
            on_delivery=self._on_delivery,
        )

    def flush(self, timeout: float = 10) -> None:
        pending = self._producer.flush(timeout)
        if pending or self._errors:
            raise RuntimeError(f"Falha ao publicar no Kafka: pendentes={pending} erros={self._errors}")


def ensure_topic(partitions: int = 3) -> bool:
    admin = AdminClient({"bootstrap.servers": config.KAFKA_BOOTSTRAP})
    if config.TOPIC in admin.list_topics(timeout=10).topics:
        return False
    futures = admin.create_topics([NewTopic(config.TOPIC, num_partitions=partitions, replication_factor=1)])
    futures[config.TOPIC].result()
    return True


def run_consumer(group_id: str, handle: Callable[[StudyEvent], None], stop: threading.Event) -> None:
    """Consome o tópico com commit manual: o offset só avança depois que `handle`
    terminou com sucesso (entrega at-least-once; os handlers são idempotentes)."""
    consumer = Consumer({
        "bootstrap.servers": config.KAFKA_BOOTSTRAP,
        "group.id": group_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([config.TOPIC])
    console.print(f"[cyan]{group_id}[/] ouvindo '{config.TOPIC}'...")
    backoff = 1
    try:
        while not stop.is_set():
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                console.print(f"[red]{group_id}: {msg.error()}[/]")
                continue
            try:
                event = parse_event(msg.value())
            except ValidationError as exc:
                # Mensagem inválida ("poison pill"): registra e segue em frente.
                console.print(f"[red]{group_id}: mensagem inválida ignorada (offset {msg.offset()}): {exc}[/]")
            else:
                try:
                    handle(event)
                except Exception as exc:
                    # Falha temporária (banco, site fora do ar...): volta o consumer para esta
                    # mensagem, sem confirmar o offset, e tenta de novo com espera crescente.
                    console.print(f"[yellow]{group_id}: {exc!r}; nova tentativa em {backoff}s[/]")
                    consumer.seek(TopicPartition(msg.topic(), msg.partition(), msg.offset()))
                    stop.wait(backoff)
                    backoff = min(backoff * 2, 60)
                    continue
                backoff = 1
                console.print(
                    f"[dim]{group_id}[/] {event.type} item={event.item_id} "
                    f"[dim]p{msg.partition()}@{msg.offset()}[/]"
                )
            consumer.commit(message=msg, asynchronous=False)
    finally:
        consumer.close()
