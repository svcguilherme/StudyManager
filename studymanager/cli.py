"""CLI `study`: publica comandos como eventos no Kafka e lê o quadro do Postgres."""
import threading
from datetime import date, datetime, time
from typing import Annotated

import typer
from rich.console import Console
from rich.table import Table

from studymanager import commands, config, db
from studymanager.domain import COLUMNS, column_of, current_streak, replay
from studymanager.events import StudyEvent, event_from_dict
from studymanager.kafka_io import ensure_topic

app = typer.Typer(help="StudyManager: kanban de estudos orientado a eventos.", no_args_is_help=True)
console = Console()


def _run(action, *args, **kwargs) -> StudyEvent:
    try:
        event = action(*args, **kwargs)
    except commands.CommandError as exc:
        console.print(f"[red]{exc}[/]")
        raise typer.Exit(1)
    console.print(f"[green]✓[/] {event.type} publicado em '{config.TOPIC}' (item [bold]{event.item_id}[/])")
    return event


def _fmt_dt(value: datetime | None) -> str:
    return value.astimezone(config.TIMEZONE).strftime("%d/%m %H:%M") if value else "-"


@app.command()
def init() -> None:
    """Cria as tabelas no Postgres e o tópico no Kafka."""
    with db.connect() as conn:
        db.create_schema(conn)
    console.print("[green]✓[/] tabelas criadas no Postgres")
    created = ensure_topic()
    console.print(f"[green]✓[/] tópico '{config.TOPIC}' " + ("criado" if created else "já existia"))


@app.command()
def add(
    title: str,
    tag: Annotated[list[str] | None, typer.Option("--tag", "-t", help="Pode repetir: -t kafka -t java")] = None,
) -> None:
    """Adiciona um assunto ao backlog (To Do)."""
    _run(commands.add_item, title, tag or [])


@app.command()
def start(
    item_id: str,
    force: Annotated[bool, typer.Option("--force", help="Ignora o limite de WIP")] = False,
) -> None:
    """Move um item para Doing (também usado para começar uma revisão)."""
    _run(commands.start_item, item_id, force)


@app.command()
def pause(
    item_id: str,
    note: Annotated[str | None, typer.Option("--note", "-n", help="Onde você parou")] = None,
) -> None:
    """Pausa o estudo e devolve o item para To Do, somando o tempo estudado."""
    _run(commands.pause_item, item_id, note)


@app.command()
def done(
    item_id: str,
    confidence: Annotated[int, typer.Option("--confidence", "-c", min=1, max=5, help="1 = não entendi, 5 = domino")],
    note: Annotated[str | None, typer.Option("--note", "-n")] = None,
) -> None:
    """Conclui o estudo; o review-scheduler agenda a próxima revisão."""
    _run(commands.complete_item, item_id, confidence, note)


@app.command()
def board(
    at: Annotated[str | None, typer.Option("--at", help="Viagem no tempo: quadro no fim do dia AAAA-MM-DD")] = None,
) -> None:
    """Mostra o quadro kanban."""
    with db.connect() as conn:
        if at:
            ref = datetime.combine(date.fromisoformat(at), time.max, tzinfo=config.TIMEZONE)
            events = [event_from_dict(row["payload"]) for row in db.load_events(conn, until=ref)]
            items = list(replay(events).values())
        else:
            ref = datetime.now(config.TIMEZONE)
            items = db.all_items(conn)

    columns: dict[str, list[dict]] = {key: [] for key in COLUMNS}
    for item in items:
        columns[column_of(item, ref)].append(item)

    title = f"Quadro em {at}" if at else "Quadro de estudos"
    table = Table(title=title, expand=True, show_lines=False)
    for key, label in COLUMNS.items():
        count = f"{len(columns[key])}/{config.WIP_LIMIT}" if key == "doing" else str(len(columns[key]))
        table.add_column(f"{label} ({count})", ratio=1)

    def card(item: dict, key: str) -> str:
        tags = " ".join(f"#{t}" for t in item["tags"])
        extra = f"{item['total_minutes']}min"
        if key == "review":
            extra += f" · revisão {item['review_round']}"
        elif key == "done" and item["next_review_at"]:
            extra += f" · revisar {_fmt_dt(item['next_review_at'])}"
        return f"[bold cyan]{item['id']}[/] {item['title']}\n[dim]{tags} {extra}[/]"

    table.add_row(*("\n\n".join(card(i, key) for i in columns[key]) for key in COLUMNS))
    console.print(table)


@app.command()
def history(item_id: str) -> None:
    """Lista todos os eventos de um item (o event store)."""
    with db.connect() as conn:
        try:
            item = commands.find_item(conn, item_id)
        except commands.CommandError as exc:
            console.print(f"[red]{exc}[/]")
            raise typer.Exit(1)
        rows = db.load_events(conn, item_id=item["id"])
    table = Table(title=f"{item['id']} · {item['title']}")
    table.add_column("Quando")
    table.add_column("Evento")
    table.add_column("Dados")
    base_fields = {"event_id", "item_id", "occurred_at", "type"}
    for row in rows:
        data = {k: v for k, v in row["payload"].items() if k not in base_fields and v not in (None, [])}
        table.add_row(_fmt_dt(row["occurred_at"]), row["type"], ", ".join(f"{k}={v}" for k, v in data.items()))
    console.print(table)


@app.command()
def stats() -> None:
    """Horas por tag e sequência de dias estudando."""
    with db.connect() as conn:
        by_tag = db.minutes_by_tag(conn)
        days = db.study_days(conn)

    table = Table(title="Tempo por tag")
    table.add_column("Tag")
    table.add_column("Itens", justify="right")
    table.add_column("Tempo", justify="right")
    for row in by_tag:
        hours, minutes = divmod(row["minutes"], 60)
        table.add_row(row["tag"], str(row["items"]), f"{hours}h{minutes:02d}")
    console.print(table)

    streak = current_streak(days, datetime.now(config.TIMEZONE).date())
    console.print(f"Sequência atual: [bold]{streak}[/] dia(s) · total de dias estudados: {len(days)}")


@app.command()
def rebuild() -> None:
    """Apaga a projeção do quadro e a reconstrói a partir do event store."""
    with db.connect() as conn:
        with conn.transaction():
            events = [event_from_dict(row["payload"]) for row in db.load_events(conn)]
            items = replay(events)
            db.truncate_items(conn)
            for item in items.values():
                db.upsert_item(conn, item)
    console.print(f"[green]✓[/] {len(items)} itens reconstruídos a partir de {len(events)} eventos")


@app.command()
def worker(
    name: Annotated[str, typer.Argument(help="projector | scheduler | site | all")] = "all",
) -> None:
    """Roda os consumers do Kafka (deixe em um terminal separado).

    Com SITE_URL configurado, `all` também sincroniza com o site.
    """
    from studymanager.workers import start_workers

    stop = threading.Event()
    try:
        threads = start_workers(stop, name)
    except ValueError as exc:
        console.print(f"[red]{exc}[/]")
        raise typer.Exit(1)
    try:
        while any(t.is_alive() for t in threads):
            for t in threads:
                t.join(0.5)
    except KeyboardInterrupt:
        console.print("Encerrando workers...")
        stop.set()
        for t in threads:
            t.join()


@app.command(name="app")
def run_app(
    no_browser: Annotated[bool, typer.Option("--no-browser", help="Não abre o navegador")] = False,
) -> None:
    """Sobe tudo (Docker, workers e quadro web em http://localhost:8092) e abre o navegador."""
    from studymanager.launcher import main

    main(open_browser=not no_browser)


if __name__ == "__main__":
    app()
