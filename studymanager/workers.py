"""Sobe os consumers do Kafka (e a integração com o site) em threads."""
import threading

from studymanager import config
from studymanager.kafka_io import run_consumer
from studymanager.projector import Projector
from studymanager.scheduler import ReviewScheduler
from studymanager.site_sync import SiteSync, run_site_importer

AVAILABLE = {"projector": [Projector], "scheduler": [ReviewScheduler], "site": [SiteSync]}


def start_workers(stop: threading.Event, name: str = "all") -> list[threading.Thread]:
    if name != "all" and name not in AVAILABLE:
        raise ValueError(f"Worker desconhecido: {name}")
    use_site = name == "site" or (name == "all" and bool(config.SITE_URL))
    if use_site and not config.SITE_URL:
        raise ValueError("Configure SITE_URL e SITE_API_KEY no .env para sincronizar com o site.")
    if name == "all":
        selected = [Projector, ReviewScheduler] + ([SiteSync] if use_site else [])
    else:
        selected = AVAILABLE[name]

    threads = []
    for cls in selected:
        handler = cls()
        threads.append(threading.Thread(
            target=run_consumer, args=(cls.group_id, handler.handle, stop), name=cls.group_id, daemon=True
        ))
    if use_site:
        threads.append(threading.Thread(target=run_site_importer, args=(stop,), name="site-importer", daemon=True))
    for thread in threads:
        thread.start()
    return threads
