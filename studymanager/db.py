"""Acesso ao PostgreSQL.

Tabelas:
- events:           event store (todo evento que passou pelo Kafka, imutável)
- study_items:      projeção do quadro (read model), reconstruível a partir de events
- review_state:     estado próprio do review-scheduler
- processed_events: controle de idempotência por consumer
"""
import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from studymanager import config
from studymanager.events import StudyEvent

SCHEMA = """
CREATE TABLE IF NOT EXISTS events (
    seq         BIGSERIAL PRIMARY KEY,
    event_id    TEXT UNIQUE NOT NULL,
    item_id     TEXT NOT NULL,
    type        TEXT NOT NULL,
    payload     JSONB NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS events_item_idx ON events (item_id, seq);

CREATE TABLE IF NOT EXISTS study_items (
    id              TEXT PRIMARY KEY,
    title           TEXT NOT NULL,
    tags            TEXT[] NOT NULL DEFAULT '{}',
    status          TEXT NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL,
    started_at      TIMESTAMPTZ,
    total_minutes   INT NOT NULL DEFAULT 0,
    review_round    INT NOT NULL DEFAULT 0,
    next_review_at  TIMESTAMPTZ,
    last_confidence INT,
    last_note       TEXT
);

CREATE TABLE IF NOT EXISTS review_state (
    item_id TEXT PRIMARY KEY,
    round   INT NOT NULL
);

CREATE TABLE IF NOT EXISTS processed_events (
    consumer TEXT NOT NULL,
    event_id TEXT NOT NULL,
    PRIMARY KEY (consumer, event_id)
);

CREATE TABLE IF NOT EXISTS sync_state (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
"""

ITEM_COLUMNS = [
    "id", "title", "tags", "status", "created_at", "started_at", "total_minutes",
    "review_round", "next_review_at", "last_confidence", "last_note",
]


def connect() -> psycopg.Connection:
    return psycopg.connect(config.DATABASE_URL, autocommit=True, row_factory=dict_row)


def create_schema(conn: psycopg.Connection) -> None:
    conn.execute(SCHEMA)


# ---------- event store ----------

def record_event(conn: psycopg.Connection, event: StudyEvent) -> bool:
    """Grava o evento; retorna False se ele já existia (mensagem duplicada)."""
    row = conn.execute(
        """
        INSERT INTO events (event_id, item_id, type, payload, occurred_at)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (event_id) DO NOTHING
        RETURNING event_id
        """,
        (event.event_id, event.item_id, event.type, Jsonb(event.model_dump(mode="json")), event.occurred_at),
    ).fetchone()
    return row is not None


def event_recorded(conn: psycopg.Connection, event_id: str) -> bool:
    return conn.execute("SELECT 1 FROM events WHERE event_id = %s", (event_id,)).fetchone() is not None


def board_version(conn: psycopg.Connection) -> int:
    """Último evento gravado: muda sempre que o quadro muda."""
    return conn.execute("SELECT coalesce(max(seq), 0) AS v FROM events").fetchone()["v"]


def load_events(conn: psycopg.Connection, until=None, item_id: str | None = None) -> list[dict]:
    sql = "SELECT * FROM events WHERE TRUE"
    params: list = []
    if until is not None:
        sql += " AND occurred_at <= %s"
        params.append(until)
    if item_id is not None:
        sql += " AND item_id = %s"
        params.append(item_id)
    return conn.execute(sql + " ORDER BY seq", params).fetchall()


# ---------- projeção do quadro ----------

def get_item(conn: psycopg.Connection, item_id: str, for_update: bool = False) -> dict | None:
    sql = "SELECT * FROM study_items WHERE id = %s" + (" FOR UPDATE" if for_update else "")
    return conn.execute(sql, (item_id,)).fetchone()


def find_items(conn: psycopg.Connection, id_prefix: str) -> list[dict]:
    return conn.execute(
        "SELECT * FROM study_items WHERE id LIKE %s ORDER BY id", (id_prefix + "%",)
    ).fetchall()


def all_items(conn: psycopg.Connection) -> list[dict]:
    return conn.execute("SELECT * FROM study_items ORDER BY created_at").fetchall()


def count_doing(conn: psycopg.Connection) -> int:
    return conn.execute("SELECT count(*) AS n FROM study_items WHERE status = 'doing'").fetchone()["n"]


def upsert_item(conn: psycopg.Connection, item: dict) -> None:
    cols = ", ".join(ITEM_COLUMNS)
    placeholders = ", ".join(f"%({c})s" for c in ITEM_COLUMNS)
    updates = ", ".join(f"{c} = EXCLUDED.{c}" for c in ITEM_COLUMNS if c != "id")
    conn.execute(
        f"INSERT INTO study_items ({cols}) VALUES ({placeholders}) "
        f"ON CONFLICT (id) DO UPDATE SET {updates}",
        {c: item[c] for c in ITEM_COLUMNS},
    )


def truncate_items(conn: psycopg.Connection) -> None:
    conn.execute("TRUNCATE study_items")


def minutes_by_tag(conn: psycopg.Connection) -> list[dict]:
    return conn.execute(
        """
        SELECT tag, sum(total_minutes) AS minutes, count(*) AS items
        FROM study_items,
             unnest(CASE WHEN tags = '{}' THEN ARRAY['(sem tag)'] ELSE tags END) AS tag
        GROUP BY tag ORDER BY minutes DESC
        """
    ).fetchall()


def study_days(conn: psycopg.Connection) -> set:
    rows = conn.execute(
        """
        SELECT DISTINCT (occurred_at AT TIME ZONE %s)::date AS day
        FROM events WHERE type IN ('StudyStarted', 'StudyPaused', 'StudyCompleted')
        """,
        (str(config.TIMEZONE),),
    ).fetchall()
    return {r["day"] for r in rows}


# ---------- estado do review-scheduler ----------

def mark_processed(conn: psycopg.Connection, consumer: str, event_id: str) -> bool:
    row = conn.execute(
        "INSERT INTO processed_events (consumer, event_id) VALUES (%s, %s) "
        "ON CONFLICT DO NOTHING RETURNING event_id",
        (consumer, event_id),
    ).fetchone()
    return row is not None


def get_review_round(conn: psycopg.Connection, item_id: str) -> int:
    row = conn.execute("SELECT round FROM review_state WHERE item_id = %s", (item_id,)).fetchone()
    return row["round"] if row else 0


def set_review_round(conn: psycopg.Connection, item_id: str, rnd: int) -> None:
    conn.execute(
        "INSERT INTO review_state (item_id, round) VALUES (%s, %s) "
        "ON CONFLICT (item_id) DO UPDATE SET round = EXCLUDED.round",
        (item_id, rnd),
    )


# ---------- integração com o site ----------

def get_sync_state(conn: psycopg.Connection, key: str) -> str | None:
    row = conn.execute("SELECT value FROM sync_state WHERE key = %s", (key,)).fetchone()
    return row["value"] if row else None


def set_sync_state(conn: psycopg.Connection, key: str, value: str) -> None:
    conn.execute(
        "INSERT INTO sync_state (key, value) VALUES (%s, %s) "
        "ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
        (key, value),
    )
