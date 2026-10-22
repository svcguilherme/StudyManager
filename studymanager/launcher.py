"""Ponto de entrada do app: garante Docker + Kafka + Postgres no ar, sobe os
workers e o quadro web, e abre o navegador. É o que roda na inicialização do Windows."""
import shutil
import socket
import subprocess
import sys
import threading
import time
import webbrowser
from pathlib import Path

from studymanager import config

CREATE_NO_WINDOW = getattr(subprocess, "CREATE_NO_WINDOW", 0)
INFRA_TIMEOUT = 600


def _redirect_output() -> None:
    # No .exe sem console, stdout/stderr são None: manda tudo para um log.
    if sys.stdout is not None and sys.stderr is not None:
        return
    log_dir = config.APP_DIR / "logs"
    log_dir.mkdir(exist_ok=True)
    log = open(log_dir / "studymanager.log", "a", encoding="utf-8", buffering=1)
    sys.stdout = sys.stderr = log


def _log(message: str) -> None:
    print(f"{time.strftime('%Y-%m-%d %H:%M:%S')} {message}", flush=True)


def _port_in_use(port: int) -> bool:
    with socket.socket() as sock:
        return sock.connect_ex(("127.0.0.1", port)) == 0


def _docker() -> str:
    found = shutil.which("docker")
    if found:
        return found
    default = Path.home() / "AppData/Local/Programs/DockerDesktop/resources/bin/docker.exe"
    for candidate in (default, Path(r"C:\Program Files\Docker\Docker\resources\bin\docker.exe")):
        if candidate.exists():
            return str(candidate)
    raise RuntimeError("Docker não encontrado. Instale o Docker Desktop.")


def _run(*args: str) -> subprocess.CompletedProcess:
    return subprocess.run(args, capture_output=True, text=True, creationflags=CREATE_NO_WINDOW)


def _start_docker_desktop(docker: str) -> None:
    # docker.exe fica em <instalação>/resources/bin; o Docker Desktop.exe na raiz da instalação.
    desktop = Path(docker).parents[2] / "Docker Desktop.exe"
    if desktop.exists():
        _log("Docker parado; iniciando o Docker Desktop...")
        subprocess.Popen([str(desktop)], creationflags=CREATE_NO_WINDOW)


def ensure_infra() -> None:
    from studymanager import db
    from studymanager.kafka_io import ensure_topic

    docker = _docker()
    deadline = time.monotonic() + INFRA_TIMEOUT
    if _run(docker, "info").returncode != 0:
        _start_docker_desktop(docker)

    compose = ["compose", "-p", "studymanager", "-f", str(config.APP_DIR / "docker-compose.yml"), "up", "-d"]
    while (result := _run(docker, *compose)).returncode != 0:
        if time.monotonic() > deadline:
            raise RuntimeError(f"docker compose falhou: {result.stderr.strip()}")
        _log("Aguardando o Docker ficar pronto...")
        time.sleep(5)

    while True:
        try:
            with db.connect() as conn:
                db.create_schema(conn)
            ensure_topic()
            return
        except Exception as exc:
            if time.monotonic() > deadline:
                raise
            _log(f"Aguardando Postgres/Kafka: {exc}")
            time.sleep(3)


def _open_when_ready(url: str) -> None:
    for _ in range(100):
        if _port_in_use(config.WEB_PORT):
            webbrowser.open(url)
            return
        time.sleep(0.3)


def main(open_browser: bool = True) -> None:
    _redirect_output()
    url = f"http://localhost:{config.WEB_PORT}"
    if _port_in_use(config.WEB_PORT):
        _log("StudyManager já está rodando.")
        if open_browser:
            webbrowser.open(url)
        return

    _log("Iniciando StudyManager...")
    ensure_infra()

    import uvicorn

    from studymanager.web import app
    from studymanager.workers import start_workers

    stop = threading.Event()
    start_workers(stop)
    if open_browser:
        threading.Thread(target=_open_when_ready, args=(url,), daemon=True).start()
    _log(f"Quadro em {url}")
    uvicorn.run(app, host="127.0.0.1", port=config.WEB_PORT, log_level="warning")
    stop.set()
