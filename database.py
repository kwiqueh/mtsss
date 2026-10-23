from __future__ import annotations
import asyncio
from contextlib import contextmanager
from datetime import datetime, timedelta
import json
import os
from pathlib import Path
import re
import sqlite3
import tempfile
from typing import Any
from zoneinfo import ZoneInfo
from aiogram.fsm.state import State
from aiogram.fsm.storage.base import BaseStorage, StateType, StorageKey
from exports import write_private_csv

MOSCOW_TZ = ZoneInfo("Europe/Moscow")
DB_TIMEOUT = 10.0
STALE_DELIVERY_MINUTES = 10
MAX_RETRY_ATTEMPTS = 10
BUSINESS_STATUSES = {
    "новая",
    "повторная",
    "в работе",
    "связались",
    "назначено",
    "подключено",
    "отказ",
    "не дозвонились",
}
AGENTS_MAP = {
    "alex": "Александр",
    "alexander": "Александр",
    "sasha": "Александр",
    "sergey": "Сергей",
    "serg": "Сергей",
}


@contextmanager
def connect(path, *, readonly=False, **kwargs):
    if readonly:
        target = Path(path).resolve().as_uri() + "?mode=ro"
        connection = sqlite3.connect(target, uri=True, timeout=DB_TIMEOUT)
        connection.execute("PRAGMA query_only=ON")
    else:
        connection = sqlite3.connect(path, timeout=DB_TIMEOUT)
    connection.execute("PRAGMA busy_timeout=10000")
    connection.execute("PRAGMA foreign_keys=ON")
    try:
        with connection:
            yield connection
    finally:
        connection.close()


def parse_ref_tag(raw_tag: str | None) -> tuple[str, str, str, str]:
    """
    Возвращает:
    ref_tag, agent_code, agent_name, house_tag.

    Разрешённые примеры:
    alex
    alex_dom12
    sasha
    sasha_dom12
    sergey
    sergey_dom12

    Все неизвестные значения считаются прямым переходом.
    """
    if not raw_tag or not re.fullmatch(r"[A-Za-z0-9_-]{1,64}", raw_tag):
        return "direct", "direct", "Прямой переход", ""
    code, _, campaign = raw_tag.partition("_")
    code = code.lower()
    known_sources = {"vk", "tg", "telegram", "ads", "qr", "direct"}
    if code not in AGENTS_MAP and code not in known_sources:
        return "direct", "direct", "Прямой переход", ""
    name = AGENTS_MAP.get(
        code,
        {
            "vk": "VK",
            "tg": "Telegram",
            "telegram": "Telegram",
            "ads": "Реклама",
            "qr": "QR",
            "direct": "Прямой переход",
        }.get(code, code),
    )
    return raw_tag, code, name, campaign


class SQLiteStorage(BaseStorage):
    """
    SQLite storage для FSM Aiogram.

    Все соединения создаются на короткое время в том потоке, где
    выполняется конкретная синхронная операция.
    """

    def __init__(self, db_path: str):
        self.db_path = db_path
        self._lock = asyncio.Lock()

    def _connect(self):
        return connect(self.db_path)

    @staticmethod
    def _key(storage_key: StorageKey) -> str:
        return (
            f"{storage_key.bot_id}:"
            f"{storage_key.chat_id}:"
            f"{storage_key.user_id}:"
            f"{storage_key.thread_id}:"
            f"{storage_key.business_connection_id}:"
            f"{storage_key.destiny}"
        )

    async def set_state(
        self,
        key: StorageKey,
        state: StateType = None,
    ) -> None:
        state_name = state.state if isinstance(state, State) else state

        async with self._lock:
            await asyncio.to_thread(
                self._set_state_sync,
                self._key(key),
                state_name,
            )

    def _set_state_sync(
        self,
        key: str,
        state_name: str | None,
    ) -> None:
        with self._connect() as connection:
            if state_name is None:
                connection.execute(
                    "DELETE FROM fsm_storage WHERE storage_key = ?",
                    (key,),
                )
                return

            connection.execute(
                """
                INSERT INTO fsm_storage(storage_key, state, data, updated_at)
                VALUES (?, ?, '{}', datetime('now','+3 hours'))
                ON CONFLICT(storage_key) DO UPDATE SET
                    state = excluded.state, updated_at=excluded.updated_at
                """,
                (key, state_name),
            )

    async def get_state(self, key: StorageKey) -> str | None:
        async with self._lock:
            return await asyncio.to_thread(
                self._get_state_sync,
                self._key(key),
            )

    def _get_state_sync(self, key: str) -> str | None:
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT state
                FROM fsm_storage
                WHERE storage_key = ?
                """,
                (key,),
            ).fetchone()

        return row[0] if row else None

    async def set_data(
        self,
        key: StorageKey,
        data: dict[str, Any],
    ) -> None:
        encoded = json.dumps(
            data,
            ensure_ascii=False,
        )

        async with self._lock:
            await asyncio.to_thread(
                self._set_data_sync,
                self._key(key),
                encoded,
            )

    def _set_data_sync(
        self,
        key: str,
        encoded: str,
    ) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO fsm_storage(storage_key, state, data, updated_at)
                VALUES (?, NULL, ?, datetime('now','+3 hours'))
                ON CONFLICT(storage_key) DO UPDATE SET
                    data = excluded.data, updated_at=excluded.updated_at
                """,
                (key, encoded),
            )

    async def get_data(
        self,
        key: StorageKey,
    ) -> dict[str, Any]:
        async with self._lock:
            encoded = await asyncio.to_thread(
                self._get_data_sync,
                self._key(key),
            )

        if not encoded:
            return {}

        try:
            result = json.loads(encoded)
        except json.JSONDecodeError:
            return {}

        return result if isinstance(result, dict) else {}

    def _get_data_sync(self, key: str) -> str | None:
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT data
                FROM fsm_storage
                WHERE storage_key = ?
                """,
                (key,),
            ).fetchone()

        return row[0] if row else None

    async def update_data(
        self, key: StorageKey, data: dict[str, Any]
    ) -> dict[str, Any]:
        async with self._lock:
            return await asyncio.to_thread(self._update_data_sync, self._key(key), data)

    def _update_data_sync(self, key: str, data: dict[str, Any]) -> dict[str, Any]:
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT data FROM fsm_storage WHERE storage_key=?", (key,)
            ).fetchone()
            current = json.loads(row[0]) if row else {}
            current.update(data)
            connection.execute(
                "INSERT INTO fsm_storage(storage_key, state, data, updated_at) VALUES (?, NULL, ?, datetime('now','+3 hours')) ON CONFLICT(storage_key) DO UPDATE SET data=excluded.data, updated_at=excluded.updated_at",
                (key, json.dumps(current, ensure_ascii=False)),
            )
            return current

    async def close(self) -> None:
        # Соединения не удерживаются объектом storage.
        return None


def now_moscow() -> str:
    return datetime.now(MOSCOW_TZ).strftime("%Y-%m-%d %H:%M:%S")


def init_db(db_path: str) -> None:
    Path(db_path).resolve().parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    path = Path(db_path)
    if not path.exists():
        try:
            descriptor = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
            os.close(descriptor)
        except FileExistsError:
            pass
    if path.exists() and path.stat().st_size:
        with connect(db_path, readonly=True) as check:
            version = check.execute("PRAGMA user_version").fetchone()[0]
        if version > 2:
            raise RuntimeError("База создана более новой версией бота")
        if version < 2:
            backup_database(db_path)

    with connect(
        db_path,
        timeout=DB_TIMEOUT,
    ) as connection:
        connection.execute("PRAGMA journal_mode = WAL")
        connection.execute("PRAGMA synchronous = NORMAL")
        connection.execute("PRAGMA busy_timeout = 10000")
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("BEGIN IMMEDIATE")

        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS leads (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ref_tag TEXT NOT NULL DEFAULT 'direct',
                agent_code TEXT NOT NULL DEFAULT 'direct',
                agent_name TEXT NOT NULL DEFAULT 'Прямой переход',
                house_tag TEXT NOT NULL DEFAULT '',
                user_id INTEGER,
                username TEXT,
                address TEXT NOT NULL,
                service TEXT NOT NULL,
                action_type TEXT NOT NULL DEFAULT 'Подключение',
                name TEXT NOT NULL,
                phone TEXT NOT NULL,
                call_time TEXT NOT NULL,
                created_at TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'новая',
                notification_status TEXT NOT NULL DEFAULT 'ожидает',
                claimed_at TEXT,
                delivered_at TEXT,
                delivery_attempts INTEGER NOT NULL DEFAULT 0,
                last_delivery_error TEXT,
                crm_chat_id INTEGER,
                crm_message_id INTEGER,
                claimed_by_user_id INTEGER,
                claimed_by_username TEXT,
                manager_note TEXT,
                consent_accepted_at TEXT,
                consent_policy_version TEXT,
                marketing_consent INTEGER NOT NULL DEFAULT 0
            )
            """
        )

        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS funnel_sessions (
                user_id INTEGER PRIMARY KEY,
                ref_tag TEXT NOT NULL,
                current_step TEXT NOT NULL,
                started_at TEXT NOT NULL,
                last_activity_at TEXT NOT NULL,
                followup_sent INTEGER NOT NULL DEFAULT 0,
                completed INTEGER NOT NULL DEFAULT 0,
                cancelled INTEGER NOT NULL DEFAULT 0
            )
            """
        )

        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS fsm_storage (
                storage_key TEXT PRIMARY KEY,
                state TEXT,
                data TEXT NOT NULL DEFAULT '{}'
            )
            """
        )

        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS funnel_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                ref_tag TEXT NOT NULL,
                event_name TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """
        )

        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS lead_status_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                lead_id INTEGER NOT NULL,
                old_status TEXT,
                new_status TEXT NOT NULL,
                changed_by INTEGER NOT NULL,
                changed_at TEXT NOT NULL
            )
            """
        )

        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS lead_activity (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                lead_id INTEGER NOT NULL,
                actor_user_id INTEGER NOT NULL,
                actor_username TEXT,
                action TEXT NOT NULL,
                payload TEXT,
                created_at TEXT NOT NULL
            )
            """
        )

        columns = {
            row[1] for row in connection.execute("PRAGMA table_info(leads)").fetchall()
        }

        if (
            not {"id", "address", "service", "name", "phone", "call_time", "created_at"}
            <= columns
        ):
            raise RuntimeError("Неподдерживаемая схема leads; нужна отдельная миграция")
        migrations = {
            "ref_tag": "TEXT NOT NULL DEFAULT 'direct'",
            "username": "TEXT",
            "user_id": "INTEGER",
            "notification_status": "TEXT NOT NULL DEFAULT 'ожидает'",
            "status": "TEXT NOT NULL DEFAULT 'новая'",
            "submission_id": "TEXT",
            "source": "TEXT NOT NULL DEFAULT 'unknown'",
            "campaign": "TEXT NOT NULL DEFAULT ''",
            "category": "TEXT NOT NULL DEFAULT 'unknown'",
            "next_attempt_at": "TEXT",
            "agent_code": ("TEXT NOT NULL DEFAULT 'direct'"),
            "agent_name": ("TEXT NOT NULL DEFAULT 'Прямой переход'"),
            "house_tag": ("TEXT NOT NULL DEFAULT ''"),
            "action_type": ("TEXT NOT NULL DEFAULT 'Подключение'"),
            "crm_chat_id": "INTEGER",
            "crm_message_id": "INTEGER",
            "claimed_by_user_id": "INTEGER",
            "claimed_by_username": "TEXT",
            "manager_note": "TEXT",
            "claimed_at": "TEXT",
            "delivered_at": "TEXT",
            "delivery_attempts": ("INTEGER NOT NULL DEFAULT 0"),
            "last_delivery_error": "TEXT",
            "consent_accepted_at": "TEXT",
            "consent_policy_version": "TEXT",
            "marketing_consent": ("INTEGER NOT NULL DEFAULT 0"),
        }

        for column, definition in migrations.items():
            if column not in columns:
                connection.execute(
                    f'ALTER TABLE leads ADD COLUMN "{column}" {definition}'
                )

        fsm_columns = {
            row[1] for row in connection.execute("PRAGMA table_info(fsm_storage)")
        }
        if "updated_at" not in fsm_columns:
            connection.execute("ALTER TABLE fsm_storage ADD COLUMN updated_at TEXT")
            connection.execute("UPDATE fsm_storage SET updated_at=?", (now_moscow(),))
        connection.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_submission_id ON leads(submission_id) WHERE submission_id IS NOT NULL"
        )
        connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_leads_phone_date ON leads(phone, created_at)"
        )
        connection.execute(
            "CREATE TABLE IF NOT EXISTS user_attribution(user_id INTEGER PRIMARY KEY, ref_tag TEXT NOT NULL, source TEXT NOT NULL, campaign TEXT NOT NULL, created_at TEXT NOT NULL)"
        )
        connection.execute(
            "CREATE TABLE IF NOT EXISTS erasure_requests(user_id INTEGER PRIMARY KEY, requested_at TEXT NOT NULL)"
        )
        # Existing agent/ref tags are evidence of a source; no historical ad attribution is invented.
        for lead_id, tag in connection.execute(
            "SELECT id, ref_tag FROM leads WHERE source='unknown'"
        ).fetchall():
            parsed, code, _, campaign = parse_ref_tag(tag)
            if tag == parsed:
                connection.execute(
                    "UPDATE leads SET source=?, campaign=? WHERE id=?",
                    (code, campaign, lead_id),
                )
        connection.execute("PRAGMA user_version=2")

        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_leads_agent
            ON leads(agent_code, created_at)
            """
        )

        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_leads_status
            ON leads(status)
            """
        )

        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_leads_notification
            ON leads(notification_status, delivery_attempts)
            """
        )

        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_funnel_sessions
            ON funnel_sessions(
                completed,
                cancelled,
                followup_sent,
                last_activity_at
            )
            """
        )

        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_lead_activity_lead
            ON lead_activity(lead_id, created_at)
            """
        )


def normalize_phone(value: str) -> str | None:
    if (
        not isinstance(value, str)
        or len(value) > 40
        or not re.fullmatch(r"[+0-9() .-]+", value)
    ):
        return None
    digits = re.sub(r"\D", "", value or "")

    if len(digits) == 10 and digits.startswith("9"):
        digits = "7" + digits
    elif len(digits) == 11 and digits.startswith("8"):
        digits = "7" + digits[1:]
    elif len(digits) == 11 and digits.startswith("7"):
        pass
    else:
        return None

    if not re.fullmatch(r"79\d{9}", digits):
        return None

    return f"+{digits}"


def update_funnel_session_sync(
    db_path: str,
    user_id: int,
    ref_tag: str,
    step: str,
    completed: bool = False,
    cancelled: bool = False,
) -> None:
    """
    Обновляет текущую сессию, сохраняя исходное состояние
    completed/cancelled, если текущий переход не меняет его.
    """
    now = now_moscow()

    with connect(
        db_path,
        timeout=DB_TIMEOUT,
    ) as connection:
        connection.execute("PRAGMA busy_timeout = 10000")

        connection.execute(
            """
            INSERT INTO funnel_sessions (
                user_id,
                ref_tag,
                current_step,
                started_at,
                last_activity_at,
                followup_sent,
                completed,
                cancelled
            )
            VALUES (?, ?, ?, ?, ?, 0, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET
                ref_tag = excluded.ref_tag,
                current_step = excluded.current_step,
                last_activity_at = excluded.last_activity_at,
                completed = CASE
                    WHEN excluded.completed = 1 THEN 1
                    ELSE funnel_sessions.completed
                END,
                cancelled = CASE
                    WHEN excluded.cancelled = 1 THEN 1
                    ELSE funnel_sessions.cancelled
                END
            """,
            (
                user_id,
                ref_tag,
                step,
                now,
                now,
                int(completed),
                int(cancelled),
            ),
        )


async def update_funnel_session(
    db_path: str,
    user_id: int,
    ref_tag: str,
    step: str,
    completed: bool = False,
    cancelled: bool = False,
) -> None:
    await asyncio.to_thread(
        update_funnel_session_sync,
        db_path,
        user_id,
        ref_tag,
        step,
        completed,
        cancelled,
    )


def reset_funnel_session_sync(
    db_path: str,
    user_id: int,
    ref_tag: str,
    step: str,
) -> None:
    """
    Начинает новый сценарий пользователя.

    В отличие от обычного update сбрасывает completed, cancelled
    и followup_sent, чтобы повторный /start снова участвовал
    в follow-up и аналитике.
    """
    now = now_moscow()

    with connect(
        db_path,
        timeout=DB_TIMEOUT,
    ) as connection:
        connection.execute("PRAGMA busy_timeout = 10000")

        connection.execute(
            """
            INSERT INTO funnel_sessions (
                user_id,
                ref_tag,
                current_step,
                started_at,
                last_activity_at,
                followup_sent,
                completed,
                cancelled
            )
            VALUES (?, ?, ?, ?, ?, 0, 0, 0)
            ON CONFLICT(user_id) DO UPDATE SET
                ref_tag = excluded.ref_tag,
                current_step = excluded.current_step,
                started_at = excluded.started_at,
                last_activity_at = excluded.last_activity_at,
                followup_sent = 0,
                completed = 0,
                cancelled = 0
            """,
            (
                user_id,
                ref_tag,
                step,
                now,
                now,
            ),
        )


async def reset_funnel_session(
    db_path: str,
    user_id: int,
    ref_tag: str,
    step: str,
) -> None:
    await asyncio.to_thread(
        reset_funnel_session_sync,
        db_path,
        user_id,
        ref_tag,
        step,
    )


def cancel_funnel_session_sync(
    db_path: str,
    user_id: int,
) -> None:
    """
    Отменяет текущую сессию, не меняя ref_tag.
    """
    with connect(
        db_path,
        timeout=DB_TIMEOUT,
    ) as connection:
        connection.execute("PRAGMA busy_timeout = 10000")

        connection.execute(
            """
            UPDATE funnel_sessions
            SET current_step = 'cancelled',
                last_activity_at = ?,
                cancelled = 1
            WHERE user_id = ?
            """,
            (
                now_moscow(),
                user_id,
            ),
        )


async def cancel_funnel_session(
    db_path: str,
    user_id: int,
) -> None:
    await asyncio.to_thread(
        cancel_funnel_session_sync,
        db_path,
        user_id,
    )


def record_event_sync(
    db_path: str,
    user_id: int,
    ref_tag: str,
    event_name: str,
) -> None:
    with connect(
        db_path,
        timeout=DB_TIMEOUT,
    ) as connection:
        connection.execute(
            """
            INSERT INTO funnel_events(
                user_id,
                ref_tag,
                event_name,
                created_at
            )
            VALUES (?, ?, ?, ?)
            """,
            (
                user_id,
                ref_tag,
                event_name,
                now_moscow(),
            ),
        )


async def record_event(
    db_path: str,
    user_id: int,
    ref_tag: str,
    event_name: str,
) -> None:
    await asyncio.to_thread(
        record_event_sync,
        db_path,
        user_id,
        ref_tag,
        event_name,
    )


def save_lead_sync(
    db_path: str,
    data: dict[str, Any],
) -> tuple[int, bool, int | None]:
    phone = normalize_phone(data.get("phone", ""))
    if not phone:
        raise ValueError("Некорректный номер телефона")
    for key, minimum, maximum in (
        ("address", 0, 300),
        ("service", 1, 100),
        ("name", 2, 100),
        ("call_time", 1, 100),
    ):
        value = data.get(key, "")
        if not isinstance(value, str) or not minimum <= len(value.strip()) <= maximum:
            raise ValueError("Некорректные данные заявки: " + key)
    submission_id = data.get("submission_id")
    if not submission_id or not re.fullmatch(r"[A-Za-z0-9_-]{1,64}", submission_id):
        raise ValueError("Отсутствует ключ отправки формы")
    now = datetime.now(MOSCOW_TZ)
    now_str = now.strftime("%Y-%m-%d %H:%M:%S")
    duplicate_since = (now - timedelta(days=30)).strftime("%Y-%m-%d %H:%M:%S")

    ref_tag, agent_code, agent_name, house_tag = parse_ref_tag(data.get("ref_tag"))

    with connect(
        db_path,
        timeout=DB_TIMEOUT,
    ) as connection:
        connection.execute("PRAGMA busy_timeout = 10000")
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("BEGIN IMMEDIATE")

        existing = connection.execute(
            "SELECT id FROM leads WHERE submission_id=?", (submission_id,)
        ).fetchone()
        if existing:
            return existing[0], True, existing[0]
        # A new form with identical customer/service/address within 5 min is an accidental repeat.
        recent = connection.execute(
            "SELECT id FROM leads WHERE user_id=? AND phone=? AND service=? AND address=? AND action_type=? AND created_at>=? ORDER BY id DESC LIMIT 1",
            (
                data.get("user_id"),
                phone,
                data["service"],
                data.get("address", ""),
                data.get("action_type", "Подключение"),
                (now - timedelta(minutes=5)).strftime("%Y-%m-%d %H:%M:%S"),
            ),
        ).fetchone()
        if recent:
            connection.execute(
                "UPDATE funnel_sessions SET completed=1, current_step='completed' WHERE user_id=?",
                (data.get("user_id"),),
            )
            return recent[0], True, recent[0]
        previous = connection.execute(
            """
            SELECT id
            FROM leads
            WHERE phone = ?
              AND created_at >= ?
            ORDER BY id DESC
            LIMIT 1
            """,
            (
                phone,
                duplicate_since,
            ),
        ).fetchone()

        is_duplicate = previous is not None
        previous_id = previous[0] if previous else None
        status = "повторная" if is_duplicate else "новая"

        cursor = connection.execute(
            """
            INSERT INTO leads (
                ref_tag,
                agent_code,
                agent_name,
                house_tag,
                user_id,
                username,
                address,
                service,
                action_type,
                name,
                phone,
                call_time,
                created_at,
                status,
                notification_status,
                consent_accepted_at,
                consent_policy_version,
                marketing_consent
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                ref_tag,
                agent_code,
                agent_name,
                house_tag,
                data.get("user_id"),
                data.get("username"),
                str(data["address"])[:300],
                str(data["service"])[:100],
                data.get("action_type", "Подключение"),
                str(data["name"])[:100],
                phone,
                str(data["call_time"])[:100],
                now_str,
                status,
                "ожидает",
                data.get("consent_accepted_at"),
                data.get("consent_policy_version"),
                int(bool(data.get("marketing_consent"))),
            ),
        )

        lead_id = int(cursor.lastrowid)
        attribution = connection.execute(
            "SELECT source, campaign, ref_tag FROM user_attribution WHERE user_id=?",
            (data.get("user_id"),),
        ).fetchone()
        source = attribution[0] if attribution else agent_code
        campaign = attribution[1] if attribution else house_tag
        tag = attribution[2] if attribution else ref_tag
        connection.execute(
            "UPDATE leads SET submission_id=?, source=?, campaign=?, category=?, ref_tag=? WHERE id=?",
            (
                submission_id,
                source,
                campaign,
                data.get("selected_category", "unknown"),
                tag,
                lead_id,
            ),
        )
        connection.execute(
            "UPDATE funnel_sessions SET completed=1, current_step='completed', last_activity_at=? WHERE user_id=?",
            (now_str, data.get("user_id")),
        )
        connection.execute(
            "INSERT INTO funnel_events(user_id, ref_tag, event_name, created_at) VALUES (?, ?, 'lead_created', ?)",
            (data.get("user_id") or 0, tag, now_str),
        )

    return lead_id, is_duplicate, previous_id


async def save_lead(
    db_path: str,
    data: dict[str, Any],
) -> tuple[int, bool, int | None]:
    return await asyncio.to_thread(
        save_lead_sync,
        db_path,
        data,
    )


def claim_lead_sync(
    db_path: str,
    lead_id: int,
) -> bool:
    """
    Атомарно резервирует заявку для delivery worker.
    """
    with connect(
        db_path,
        timeout=DB_TIMEOUT,
    ) as connection:
        connection.execute("PRAGMA busy_timeout = 10000")

        cursor = connection.execute(
            """
            UPDATE leads
            SET notification_status = 'отправляется',
                claimed_at = ?,
                delivery_attempts = delivery_attempts + 1,
                last_delivery_error = NULL
            WHERE id = ?
              AND notification_status IN ('ожидает', 'ошибка')
              AND delivery_attempts < ?
            """,
            (
                now_moscow(),
                lead_id,
                MAX_RETRY_ATTEMPTS,
            ),
        )

        return cursor.rowcount == 1


async def claim_lead(
    db_path: str,
    lead_id: int,
) -> bool:
    return await asyncio.to_thread(
        claim_lead_sync,
        db_path,
        lead_id,
    )


def mark_delivered_sync(
    db_path: str,
    lead_id: int,
    chat_id: int,
    message_id: int,
) -> bool:
    with connect(
        db_path,
        timeout=DB_TIMEOUT,
    ) as connection:
        connection.execute("PRAGMA busy_timeout = 10000")

        cursor = connection.execute(
            """
            UPDATE leads
            SET notification_status = 'доставлено',
                delivered_at = ?,
                claimed_at = NULL,
                crm_chat_id = ?,
                crm_message_id = ?
            WHERE id = ?
              AND notification_status = 'отправляется'
            """,
            (
                now_moscow(),
                chat_id,
                message_id,
                lead_id,
            ),
        )

        return cursor.rowcount == 1


async def mark_delivered(
    db_path: str,
    lead_id: int,
    chat_id: int,
    message_id: int,
) -> bool:
    return await asyncio.to_thread(
        mark_delivered_sync,
        db_path,
        lead_id,
        chat_id,
        message_id,
    )


def mark_delivery_error_sync(
    db_path: str,
    lead_id: int,
    error_text: str,
) -> bool:
    with connect(
        db_path,
        timeout=DB_TIMEOUT,
    ) as connection:
        connection.execute("PRAGMA busy_timeout = 10000")

        cursor = connection.execute(
            """
            UPDATE leads
            SET notification_status = ?,
                claimed_at = NULL,
                last_delivery_error = ?, next_attempt_at = ?
            WHERE id = ?
              AND notification_status = 'отправляется'
            """,
            (
                "проверить"
                if error_text in {"TelegramNetworkError", "UnknownDeliveryError"}
                else (
                    "недоступно"
                    if error_text in {"TelegramForbiddenError", "TelegramBadRequest"}
                    else "ошибка"
                ),
                str(error_text).split(":", 1)[0][:80],
                (
                    datetime.now(MOSCOW_TZ)
                    + timedelta(
                        seconds=max(
                            60,
                            float(error_text.partition(":")[2])
                            if error_text.startswith("RetryAfter:")
                            else 60,
                        )
                    )
                ).strftime("%Y-%m-%d %H:%M:%S"),
                lead_id,
            ),
        )

        return cursor.rowcount == 1


async def mark_delivery_error(
    db_path: str,
    lead_id: int,
    error_text: str,
) -> bool:
    return await asyncio.to_thread(
        mark_delivery_error_sync,
        db_path,
        lead_id,
        error_text,
    )


def get_delivery_queue_sync(
    db_path: str,
    limit: int = 50,
) -> list[tuple]:
    with connect(
        db_path,
        readonly=True,
    ) as connection:
        connection.execute("PRAGMA busy_timeout = 10000")

        return connection.execute(
            """
            SELECT
                id,
                ref_tag,
                address,
                service,
                name,
                phone,
                call_time,
                created_at,
                username,
                user_id,
                status,
                action_type,
                agent_name,
                house_tag,
                claimed_by_username,
                manager_note,
                crm_chat_id,
                crm_message_id
            FROM leads
            WHERE notification_status IN ('ожидает', 'ошибка')
              AND delivery_attempts < ?
              AND (next_attempt_at IS NULL OR next_attempt_at <= datetime('now', '+3 hours'))
            ORDER BY id ASC
            LIMIT ?
            """,
            (
                MAX_RETRY_ATTEMPTS,
                max(1, min(limit, 100)),
            ),
        ).fetchall()


async def get_delivery_queue(
    db_path: str,
    limit: int = 50,
) -> list[tuple]:
    return await asyncio.to_thread(
        get_delivery_queue_sync,
        db_path,
        limit,
    )


def recover_stale_deliveries_sync(
    db_path: str,
) -> int:
    threshold = (
        datetime.now(MOSCOW_TZ) - timedelta(minutes=STALE_DELIVERY_MINUTES)
    ).strftime("%Y-%m-%d %H:%M:%S")

    with connect(
        db_path,
        timeout=DB_TIMEOUT,
    ) as connection:
        connection.execute("PRAGMA busy_timeout = 10000")

        cursor = connection.execute(
            """
            UPDATE leads
            SET notification_status = 'проверить',
                claimed_at = NULL, last_delivery_error = 'InterruptedDelivery'
            WHERE notification_status = 'отправляется'
              AND claimed_at IS NOT NULL
              AND claimed_at < ?
            """,
            (threshold,),
        )

        return cursor.rowcount


async def recover_stale_deliveries(
    db_path: str,
) -> int:
    return await asyncio.to_thread(
        recover_stale_deliveries_sync,
        db_path,
    )


def record_lead_activity_sync(
    db_path: str,
    lead_id: int,
    actor_user_id: int,
    actor_username: str | None,
    action: str,
    payload: str | None = None,
) -> None:
    with connect(
        db_path,
        timeout=DB_TIMEOUT,
    ) as connection:
        connection.execute("PRAGMA busy_timeout = 10000")

        connection.execute(
            """
            INSERT INTO lead_activity (
                lead_id,
                actor_user_id,
                actor_username,
                action,
                payload,
                created_at
            )
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                lead_id,
                actor_user_id,
                actor_username,
                action,
                payload,
                now_moscow(),
            ),
        )


async def record_lead_activity(
    db_path: str,
    lead_id: int,
    actor_user_id: int,
    actor_username: str | None,
    action: str,
    payload: str | None = None,
) -> None:
    await asyncio.to_thread(
        record_lead_activity_sync,
        db_path,
        lead_id,
        actor_user_id,
        actor_username,
        action,
        payload,
    )


def assign_lead_to_manager_sync(
    db_path: str,
    lead_id: int,
    user_id: int,
    username: str,
) -> bool:
    manager_username = f"@{username}" if username else f"id:{user_id}"

    with connect(
        db_path,
        timeout=DB_TIMEOUT,
    ) as connection:
        connection.execute("PRAGMA busy_timeout = 10000")
        connection.execute("BEGIN IMMEDIATE")

        old = connection.execute(
            "SELECT status FROM leads WHERE id=?", (lead_id,)
        ).fetchone()
        if old is None or old[0] in {"подключено", "отказ"}:
            return False
        cursor = connection.execute(
            """
            UPDATE leads
            SET claimed_by_user_id = ?,
                claimed_by_username = ?,
                status = 'в работе'
            WHERE id = ?
              AND claimed_by_user_id IS NULL
            """,
            (
                user_id,
                manager_username,
                lead_id,
            ),
        )

        if cursor.rowcount != 1:
            return False

        connection.execute(
            "INSERT INTO lead_status_history(lead_id,old_status,new_status,changed_by,changed_at) VALUES (?,?,?,?,?)",
            (lead_id, old[0], "в работе", user_id, now_moscow()),
        )

        return True


async def assign_lead_to_manager(
    db_path: str,
    lead_id: int,
    user_id: int,
    username: str,
) -> bool:
    return await asyncio.to_thread(
        assign_lead_to_manager_sync,
        db_path,
        lead_id,
        user_id,
        username,
    )


def update_lead_note_sync(
    db_path: str,
    lead_id: int,
    note: str,
) -> bool:
    normalized_note = " ".join(str(note).split()).strip()[:500]

    with connect(
        db_path,
        timeout=DB_TIMEOUT,
    ) as connection:
        connection.execute("PRAGMA busy_timeout = 10000")

        cursor = connection.execute(
            """
            UPDATE leads
            SET manager_note = ?
            WHERE id = ?
            """,
            (
                normalized_note,
                lead_id,
            ),
        )

        return cursor.rowcount == 1


async def update_lead_note(
    db_path: str,
    lead_id: int,
    note: str,
) -> bool:
    return await asyncio.to_thread(
        update_lead_note_sync,
        db_path,
        lead_id,
        note,
    )


def update_lead_status_sync(
    db_path: str,
    lead_id: int,
    new_status: str,
    changed_by: int,
) -> bool:
    if new_status not in BUSINESS_STATUSES:
        return False

    with connect(
        db_path,
        timeout=DB_TIMEOUT,
    ) as connection:
        connection.execute("PRAGMA busy_timeout = 10000")
        connection.execute("BEGIN IMMEDIATE")

        old_row = connection.execute(
            """
            SELECT status
            FROM leads
            WHERE id = ?
            """,
            (lead_id,),
        ).fetchone()

        if old_row is None:
            return False

        old_status = old_row[0]

        if old_status == new_status:
            return True

        connection.execute(
            """
            UPDATE leads
            SET status = ?
            WHERE id = ?
            """,
            (
                new_status,
                lead_id,
            ),
        )

        connection.execute(
            """
            INSERT INTO lead_status_history (
                lead_id,
                old_status,
                new_status,
                changed_by,
                changed_at
            )
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                lead_id,
                old_status,
                new_status,
                changed_by,
                now_moscow(),
            ),
        )

        return True


async def update_lead_status(
    db_path: str,
    lead_id: int,
    new_status: str,
    changed_by: int,
) -> bool:
    return await asyncio.to_thread(
        update_lead_status_sync,
        db_path,
        lead_id,
        new_status,
        changed_by,
    )


def get_lead_by_id_sync(
    db_path: str,
    lead_id: int,
) -> tuple | None:
    with connect(
        db_path,
        readonly=True,
    ) as connection:
        connection.execute("PRAGMA busy_timeout = 10000")

        return connection.execute(
            """
            SELECT
                id,
                ref_tag,
                address,
                service,
                name,
                phone,
                call_time,
                created_at,
                username,
                user_id,
                status,
                action_type,
                agent_name,
                house_tag,
                claimed_by_username,
                manager_note,
                crm_chat_id,
                crm_message_id
            FROM leads
            WHERE id = ?
            """,
            (lead_id,),
        ).fetchone()


def generate_export_csv_sync(
    db_path: str,
) -> tuple[str, int]:
    with connect(
        db_path,
        readonly=True,
    ) as connection:
        connection.execute("PRAGMA busy_timeout = 10000")

        rows = connection.execute(
            """
            SELECT
                l.id,
                l.created_at,
                l.agent_code,
                l.agent_name,
                l.house_tag,
                l.ref_tag,
                l.address,
                l.service,
                l.action_type,
                l.name,
                l.phone,
                l.call_time,
                l.status,
                l.claimed_by_username,
                l.manager_note,
                l.consent_accepted_at,
                l.consent_policy_version,
                l.source, l.campaign, l.category, l.notification_status,
                COALESCE(
                    (
                        SELECT GROUP_CONCAT(
                            old_status || ' -> ' || new_status
                            || ' [' || changed_at || ']',
                            ' | '
                        )
                        FROM lead_status_history h
                        WHERE h.lead_id = l.id
                    ),
                    ''
                ) AS status_history
            FROM leads l
            ORDER BY l.id ASC
            """
        ).fetchall()

    headers = [
        "ID",
        "Дата заявки",
        "Код агента",
        "Агент",
        "Метка дома",
        "QR-метка",
        "Адрес",
        "Тариф",
        "Цель",
        "Имя",
        "Телефон",
        "Время связи",
        "Статус",
        "Менеджер",
        "Заметка",
        "Согласие принято",
        "Версия политики",
        "Источник",
        "Кампания",
        "Категория",
        "Статус доставки",
        "История статусов",
    ]

    descriptor, temporary_path = tempfile.mkstemp(suffix=".csv")
    os.close(descriptor)
    try:
        write_private_csv(Path(temporary_path), headers, rows, overwrite=True)
    except BaseException:
        Path(temporary_path).unlink(missing_ok=True)
        raise

    return temporary_path, len(rows)


def fetch_stats_sync(
    db_path: str,
) -> tuple[
    int,
    int,
    list[tuple[str, int]],
    list[tuple[str, int]],
    list[tuple[str, int]],
]:
    with connect(
        db_path,
        readonly=True,
    ) as connection:
        connection.execute("PRAGMA busy_timeout = 10000")

        today = datetime.now(MOSCOW_TZ).strftime("%Y-%m-%d")

        today_count = connection.execute(
            """
            SELECT COUNT(*)
            FROM leads
            WHERE date(created_at) = ?
            """,
            (today,),
        ).fetchone()[0]

        total_count = connection.execute("SELECT COUNT(*) FROM leads").fetchone()[0]

        agents = connection.execute(
            """
            SELECT
                agent_name,
                COUNT(*)
            FROM leads
            GROUP BY agent_name
            ORDER BY COUNT(*) DESC
            """
        ).fetchall()

        statuses = connection.execute(
            """
            SELECT
                status,
                COUNT(*)
            FROM leads
            GROUP BY status
            ORDER BY COUNT(*) DESC
            """
        ).fetchall()

        houses = connection.execute(
            """
            SELECT
                COALESCE(NULLIF(house_tag, ''), 'без метки дома'),
                COUNT(*)
            FROM leads
            GROUP BY house_tag
            ORDER BY COUNT(*) DESC
            """
        ).fetchall()

    return (
        today_count,
        total_count,
        agents,
        statuses,
        houses,
    )


def get_abandoned_sessions_sync(
    db_path: str,
    threshold: str,
    limit: int = 20,
) -> list[tuple[int, str]]:
    with connect(
        db_path,
        readonly=True,
    ) as connection:
        connection.execute("PRAGMA busy_timeout = 10000")

        return connection.execute(
            """
            SELECT user_id, ref_tag, last_activity_at
            FROM funnel_sessions
            WHERE completed = 0
              AND cancelled = 0
              AND followup_sent = 0
              AND current_step IN (
                  'OrderFSM:waiting_for_address',
                  'OrderFSM:choosing_category',
                  'OrderFSM:waiting_for_service',
                  'OrderFSM:viewing_tariff',
                  'waiting_for_address',
                  'choosing_category',
                  'waiting_for_service',
                  'viewing_tariff'
              )
              AND last_activity_at <= ?
            ORDER BY last_activity_at ASC
            LIMIT ?
            """,
            (
                threshold,
                max(1, min(limit, 100)),
            ),
        ).fetchall()


async def get_abandoned_sessions(
    db_path: str,
    threshold: str,
    limit: int = 20,
) -> list[tuple[int, str]]:
    return await asyncio.to_thread(
        get_abandoned_sessions_sync,
        db_path,
        threshold,
        limit,
    )


def mark_followup_sent_sync(
    db_path: str,
    user_id: int,
) -> bool:
    with connect(
        db_path,
        timeout=DB_TIMEOUT,
    ) as connection:
        connection.execute("PRAGMA busy_timeout = 10000")

        cursor = connection.execute(
            """
            UPDATE funnel_sessions
            SET followup_sent = 1,
                last_activity_at = ?
            WHERE user_id = ?
              AND completed = 0
              AND cancelled = 0
              AND followup_sent = 0
            """,
            (
                now_moscow(),
                user_id,
            ),
        )

        return cursor.rowcount == 1


async def mark_followup_sent(
    db_path: str,
    user_id: int,
) -> bool:
    return await asyncio.to_thread(
        mark_followup_sent_sync,
        db_path,
        user_id,
    )


def mark_funnel_blocked_sync(
    db_path: str,
    user_id: int,
) -> None:
    with connect(
        db_path,
        timeout=DB_TIMEOUT,
    ) as connection:
        connection.execute(
            """
            UPDATE funnel_sessions
            SET cancelled = 1,
                current_step = 'blocked',
                last_activity_at = ?
            WHERE user_id = ?
            """,
            (
                now_moscow(),
                user_id,
            ),
        )


async def mark_funnel_blocked(
    db_path: str,
    user_id: int,
) -> None:
    await asyncio.to_thread(
        mark_funnel_blocked_sync,
        db_path,
        user_id,
    )


def backup_database(db_path: str) -> str:
    """Consistent online backup includes committed WAL, never copy a live .db alone."""
    directory = Path(db_path).resolve().parent / "backups"
    directory.mkdir(mode=0o700, exist_ok=True)
    descriptor, filename = tempfile.mkstemp(
        prefix="leads-", suffix=".db", dir=directory
    )
    os.close(descriptor)
    try:
        with connect(db_path, readonly=True) as source:
            target = sqlite3.connect(filename)
            try:
                source.backup(target)
                if target.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
                    raise RuntimeError("Не прошла проверка резервной копии")
            finally:
                target.close()
    except BaseException:
        os.unlink(filename)
        raise
    return filename


def remember_attribution_sync(
    db_path: str, user_id: int, raw_tag: str | None
) -> tuple[str, str, str, str]:
    tag, code, _, campaign = parse_ref_tag(raw_tag)
    with connect(db_path) as connection:
        connection.execute(
            "INSERT INTO user_attribution(user_id,ref_tag,source,campaign,created_at) VALUES (?,?,?,?,?) ON CONFLICT(user_id) DO NOTHING",
            (user_id, tag, code, campaign, now_moscow()),
        )
        row = connection.execute(
            "SELECT ref_tag FROM user_attribution WHERE user_id=?", (user_id,)
        ).fetchone()
    return parse_ref_tag(row[0])


def claim_followup_sync(
    db_path: str, user_id: int, threshold: str, expected_activity: str
) -> bool:
    with connect(db_path) as connection:
        cursor = connection.execute(
            """UPDATE funnel_sessions SET followup_sent=1
            WHERE user_id=? AND followup_sent=0 AND completed=0 AND cancelled=0
            AND last_activity_at=? AND last_activity_at<=?
            AND current_step IN ('OrderFSM:waiting_for_address','OrderFSM:choosing_category',
                'OrderFSM:waiting_for_service','OrderFSM:viewing_tariff',
                'waiting_for_address','choosing_category','waiting_for_service','viewing_tariff')""",
            (user_id, expected_activity, threshold),
        )
        return cursor.rowcount == 1


def fetch_analytics_sync(db_path: str) -> dict:
    with connect(db_path, readonly=True) as connection:
        total = connection.execute("SELECT COUNT(*) FROM leads").fetchone()[0]
        connected = connection.execute(
            "SELECT COUNT(*) FROM leads WHERE status='подключено'"
        ).fetchone()[0]
        result = dict(
            total=total,
            connected=connected,
            conversion=100 * connected / total if total else 0.0,
        )
        for key, column in [
            ("categories", "category"),
            ("sources", "source"),
            ("campaigns", "campaign"),
            ("dates", "substr(created_at,1,10)"),
            ("delivery", "notification_status"),
        ]:
            # Columns are fixed constants, never input.
            result[key] = connection.execute(
                f"SELECT {column}, COUNT(*) FROM leads GROUP BY {column} ORDER BY COUNT(*) DESC LIMIT 15"
            ).fetchall()
        return result


def request_erasure_sync(db_path: str, user_id: int) -> None:
    with connect(db_path) as connection:
        connection.execute(
            "INSERT INTO erasure_requests(user_id,requested_at) VALUES (?,?) ON CONFLICT(user_id) DO UPDATE SET requested_at=excluded.requested_at",
            (user_id, now_moscow()),
        )
        _clear_user_fsm(connection, {user_id})
        connection.execute("DELETE FROM funnel_sessions WHERE user_id=?", (user_id,))


def retention_cleanup_sync(
    db_path: str, retention_days: int, *, apply: bool = False
) -> int:
    """Explicit anonymization, never automatic deletion of leads; backup before applying."""
    threshold = (datetime.now(MOSCOW_TZ) - timedelta(days=retention_days)).strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    with connect(db_path, readonly=True) as connection:
        count = connection.execute(
            "SELECT COUNT(*) FROM leads WHERE user_id IS NOT NULL AND (created_at<? OR user_id IN (SELECT user_id FROM erasure_requests))",
            (threshold,),
        ).fetchone()[0]
    if not apply:
        return count
    backup_database(db_path)
    with connect(db_path) as connection:
        connection.execute("BEGIN IMMEDIATE")
        condition = "created_at<? OR user_id IN (SELECT user_id FROM erasure_requests)"
        connection.execute(
            f"DELETE FROM lead_activity WHERE lead_id IN (SELECT id FROM leads WHERE {condition})",
            (threshold,),
        )
        connection.execute(
            f"UPDATE leads SET name='Удалено', phone='', address='', username=NULL, user_id=NULL, manager_note=NULL, last_delivery_error=NULL, notification_status=CASE WHEN notification_status='доставлено' THEN notification_status ELSE 'удалено' END WHERE {condition}",
            (threshold,),
        )
        requested_ids = {
            row[0] for row in connection.execute("SELECT user_id FROM erasure_requests")
        }
        _clear_user_fsm(connection, requested_ids)
        connection.execute("DELETE FROM fsm_storage WHERE updated_at<?", (threshold,))
        connection.execute(
            "DELETE FROM funnel_events WHERE created_at<? OR user_id IN (SELECT user_id FROM erasure_requests)",
            (threshold,),
        )
        connection.execute(
            "DELETE FROM funnel_sessions WHERE last_activity_at<? OR user_id IN (SELECT user_id FROM erasure_requests)",
            (threshold,),
        )
        connection.execute(
            "DELETE FROM user_attribution WHERE created_at<? OR user_id IN (SELECT user_id FROM erasure_requests)",
            (threshold,),
        )
        connection.execute("DELETE FROM erasure_requests")
    return count


def _clear_user_fsm(connection, user_ids):
    keys = [row[0] for row in connection.execute("SELECT storage_key FROM fsm_storage")]
    for key in keys:
        parts = key.split(":")
        if len(parts) >= 3 and parts[2].isdigit() and int(parts[2]) in user_ids:
            connection.execute("DELETE FROM fsm_storage WHERE storage_key=?", (key,))


def list_leads_sync(db_path: str, *, delivery_only: bool = False) -> list[tuple]:
    with connect(db_path, readonly=True) as connection:
        condition = (
            "WHERE notification_status IN ('проверить','недоступно','ошибка')"
            if delivery_only
            else ""
        )
        return connection.execute(
            f"SELECT id,status,notification_status,created_at FROM leads {condition} ORDER BY id DESC LIMIT 20"
        ).fetchall()


def retry_delivery_sync(db_path: str, lead_id: int) -> bool:
    with connect(db_path) as connection:
        cursor = connection.execute(
            "UPDATE leads SET notification_status='ожидает', delivery_attempts=0, next_attempt_at=NULL, claimed_at=NULL WHERE id=? AND notification_status IN ('проверить','недоступно','ошибка')",
            (lead_id,),
        )
        return cursor.rowcount == 1
