from __future__ import annotations

import asyncio
from collections import defaultdict
import csv
from datetime import datetime, timedelta
import html
import json
import logging
import os
from pathlib import Path
import re
import sqlite3
import tempfile
from typing import Any, Awaitable, Callable, Dict
from zoneinfo import ZoneInfo

from aiogram import BaseMiddleware, Bot, Dispatcher, F, Router
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.exceptions import (
    TelegramBadRequest,
    TelegramForbiddenError,
    TelegramNetworkError,
    TelegramRetryAfter,
)
from aiogram.filters import Command, CommandObject, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.base import BaseStorage, StateType, StorageKey
from aiogram.types import (
    CallbackQuery,
    FSInputFile,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    Message,
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove,
    TelegramObject,
)

from tariffs import (
    CATEGORIES_INFO,
    TARIFFS,
    get_categories_kb,
    get_tariffs_by_category_kb,
    get_variants_kb,
    get_tariff_actions_kb,
)


LOGGER = logging.getLogger(__name__)


try:
    MOSCOW_TZ = ZoneInfo("Europe/Moscow")
except Exception:
    from datetime import timezone

    MOSCOW_TZ = timezone(timedelta(hours=3))


DB_TIMEOUT = 10.0
STALE_DELIVERY_MINUTES = 10
MAX_RETRY_ATTEMPTS = 10


BUSINESS_STATUSES = {
    "новая",
    "в работе",
    "связались",
    "назначено",
    "подключено",
    "отказ",
    "не дозвонились",
}


TIME_SLOTS = {
    "time_morning": "Утро (09:00–12:00)",
    "time_day": "День (12:00–17:00)",
    "time_evening": "Вечер (17:00–21:00)",
    "time_any": "В любое время",
}


AGENTS_MAP = {
    "alex": "Александр",
    "alexander": "Александр",
    "sasha": "Александр",
    "sergey": "Сергей",
    "serg": "Сергей",
}


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
    if not raw_tag:
        return "direct", "direct", "Прямой переход", ""

    clean = re.sub(
        r"[^a-zA-Z0-9_\-а-яА-Я]",
        "",
        str(raw_tag),
    )[:64]

    if not clean:
        return "direct", "direct", "Прямой переход", ""

    parts = clean.split("_", 1)
    code = parts[0].lower()
    house_tag = parts[1][:48] if len(parts) > 1 else ""

    agent_name = AGENTS_MAP.get(code)
    if agent_name is None:
        return "direct", "direct", "Прямой переход", ""

    normalized_ref_tag = code
    if house_tag:
        normalized_ref_tag = f"{code}_{house_tag}"

    return (
        normalized_ref_tag,
        code,
        agent_name,
        house_tag,
    )


class ThrottlingMiddleware(BaseMiddleware):
    """
    Простая защита от частых повторных сообщений и callback-запросов.

    Middleware работает внутри одного процесса. Отдельная защита от
    повторного создания заявки обеспечивается SQL/FSM-блокировкой.
    """

    def __init__(self, latency: float = 0.4):
        self.latency = latency
        self.user_timestamps: dict[int, float] = {}

    async def __call__(
        self,
        handler: Callable[
            [TelegramObject, Dict[str, Any]],
            Awaitable[Any],
        ],
        event: TelegramObject,
        data: Dict[str, Any],
    ) -> Any:
        user = getattr(event, "from_user", None)

        if user is None:
            return await handler(event, data)

        now = asyncio.get_running_loop().time()
        last_time = self.user_timestamps.get(user.id, 0.0)

        if now - last_time < self.latency:
            if isinstance(event, CallbackQuery):
                try:
                    await event.answer(
                        "⚠️ Не так быстро!",
                        show_alert=False,
                    )
                except Exception:
                    pass
            return None

        self.user_timestamps[user.id] = now

        if len(self.user_timestamps) > 10000:
            threshold = now - max(self.latency * 10, 10.0)
            self.user_timestamps = {
                user_id: timestamp
                for user_id, timestamp in self.user_timestamps.items()
                if timestamp >= threshold
            }

        return await handler(event, data)


def sanitize_excel_cell(value: Any) -> str:
    """
    Защита CSV от формульных инъекций при открытии в Excel.
    """
    if value is None:
        return ""

    text = str(value)
    text = text.replace("\r", " ").replace("\n", " ")

    if text.startswith(("=", "+", "-", "@")):
        return "'" + text

    return text


class SQLiteStorage(BaseStorage):
    """
    SQLite storage для FSM Aiogram.

    Все соединения создаются на короткое время в том потоке, где
    выполняется конкретная синхронная операция.
    """

    def __init__(self, db_path: str):
        self.db_path = db_path
        self._lock = asyncio.Lock()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(
            self.db_path,
            timeout=DB_TIMEOUT,
        )
        connection.execute("PRAGMA busy_timeout = 10000")
        connection.execute("PRAGMA foreign_keys = ON")
        return connection

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
        state_name = state.state if state else None

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
                INSERT INTO fsm_storage(storage_key, state, data)
                VALUES (?, ?, '{}')
                ON CONFLICT(storage_key) DO UPDATE SET
                    state = excluded.state
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
                INSERT INTO fsm_storage(storage_key, state, data)
                VALUES (?, NULL, ?)
                ON CONFLICT(storage_key) DO UPDATE SET
                    data = excluded.data
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

    async def close(self) -> None:
        # Соединения не удерживаются объектом storage.
        return None


def now_moscow() -> str:
    return datetime.now(MOSCOW_TZ).strftime(
        "%Y-%m-%d %H:%M:%S"
    )


def init_db(db_path: str) -> None:
    Path(db_path).resolve().parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    with sqlite3.connect(
        db_path,
        timeout=DB_TIMEOUT,
    ) as connection:
        connection.execute("PRAGMA journal_mode = WAL")
        connection.execute("PRAGMA synchronous = NORMAL")
        connection.execute("PRAGMA busy_timeout = 10000")
        connection.execute("PRAGMA foreign_keys = ON")

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
            row[1]
            for row in connection.execute(
                "PRAGMA table_info(leads)"
            ).fetchall()
        }

        migrations = {
            "agent_code": (
                "TEXT NOT NULL DEFAULT 'direct'"
            ),
            "agent_name": (
                "TEXT NOT NULL DEFAULT 'Прямой переход'"
            ),
            "house_tag": (
                "TEXT NOT NULL DEFAULT ''"
            ),
            "action_type": (
                "TEXT NOT NULL DEFAULT 'Подключение'"
            ),
            "crm_chat_id": "INTEGER",
            "crm_message_id": "INTEGER",
            "claimed_by_user_id": "INTEGER",
            "claimed_by_username": "TEXT",
            "manager_note": "TEXT",
            "claimed_at": "TEXT",
            "delivered_at": "TEXT",
            "delivery_attempts": (
                "INTEGER NOT NULL DEFAULT 0"
            ),
            "last_delivery_error": "TEXT",
            "consent_accepted_at": "TEXT",
            "consent_policy_version": "TEXT",
            "marketing_consent": (
                "INTEGER NOT NULL DEFAULT 0"
            ),
        }

        for column, definition in migrations.items():
            if column not in columns:
                connection.execute(
                    f'ALTER TABLE leads ADD COLUMN "{column}" {definition}'
                )

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

    with sqlite3.connect(
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

    with sqlite3.connect(
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
    with sqlite3.connect(
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
    with sqlite3.connect(
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
    phone = data["phone"]
    now = datetime.now(MOSCOW_TZ)
    now_str = now.strftime("%Y-%m-%d %H:%M:%S")
    duplicate_since = (
        now - timedelta(days=30)
    ).strftime("%Y-%m-%d %H:%M:%S")

    ref_tag, agent_code, agent_name, house_tag = parse_ref_tag(
        data.get("ref_tag")
    )

    with sqlite3.connect(
        db_path,
        timeout=DB_TIMEOUT,
    ) as connection:
        connection.execute("PRAGMA busy_timeout = 10000")
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("BEGIN IMMEDIATE")

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
    with sqlite3.connect(
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
    with sqlite3.connect(
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
    with sqlite3.connect(
        db_path,
        timeout=DB_TIMEOUT,
    ) as connection:
        connection.execute("PRAGMA busy_timeout = 10000")

        cursor = connection.execute(
            """
            UPDATE leads
            SET notification_status = 'ошибка',
                claimed_at = NULL,
                last_delivery_error = ?
            WHERE id = ?
              AND notification_status = 'отправляется'
            """,
            (
                str(error_text)[:1000],
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
    with sqlite3.connect(
        db_path,
        timeout=DB_TIMEOUT,
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
        datetime.now(MOSCOW_TZ)
        - timedelta(minutes=STALE_DELIVERY_MINUTES)
    ).strftime("%Y-%m-%d %H:%M:%S")

    with sqlite3.connect(
        db_path,
        timeout=DB_TIMEOUT,
    ) as connection:
        connection.execute("PRAGMA busy_timeout = 10000")

        cursor = connection.execute(
            """
            UPDATE leads
            SET notification_status = 'ожидает',
                claimed_at = NULL
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
    with sqlite3.connect(
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
    manager_username = (
        f"@{username}"
        if username
        else f"id:{user_id}"
    )

    with sqlite3.connect(
        db_path,
        timeout=DB_TIMEOUT,
    ) as connection:
        connection.execute("PRAGMA busy_timeout = 10000")
        connection.execute("BEGIN IMMEDIATE")

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
            """
            INSERT INTO lead_status_history (
                lead_id,
                old_status,
                new_status,
                changed_by,
                changed_at
            )
            SELECT
                id,
                status,
                'в работе',
                ?,
                ?
            FROM leads
            WHERE id = ?
            """,
            (
                user_id,
                now_moscow(),
                lead_id,
            ),
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
    normalized_note = " ".join(
        str(note).split()
    ).strip()[:500]

    with sqlite3.connect(
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

    with sqlite3.connect(
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
    with sqlite3.connect(
        db_path,
        timeout=DB_TIMEOUT,
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


def build_agent_text(
    row: tuple,
    delivery_prefix: str = "🚨 <b>НОВАЯ ЗАЯВКА",
) -> str:
    (
        lead_id,
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
        claimed_by,
        manager_note,
        crm_chat_id,
        crm_message_id,
    ) = row

    safe_agent_name = html.escape(
        str(agent_name or "Прямой переход")
    )
    safe_house_tag = html.escape(
        str(house_tag or "")
    )
    safe_ref_tag = html.escape(
        str(ref_tag or "direct")
    )
    safe_address = html.escape(
        str(address or "")
    )
    safe_service = html.escape(
        str(service or "")
    )
    safe_name = html.escape(
        str(name or "")
    )
    safe_phone = html.escape(
        str(phone or "")
    )
    safe_call_time = html.escape(
        str(call_time or "")
    )
    safe_username = html.escape(
        str(username or "отсутствует")
    )
    safe_created_at = html.escape(
        str(created_at or "")
    )
    safe_status = html.escape(
        str(status or "")
    )
    safe_action_type = html.escape(
        str(action_type or "Подключение")
    )

    house_info = (
        f" (дом: {safe_house_tag})"
        if safe_house_tag
        else ""
    )

    claimed_text = (
        html.escape(str(claimed_by))
        if claimed_by
        else "<i>не назначен</i>"
    )

    note_text = (
        "\n📝 <b>Заметка:</b> "
        f"<i>{html.escape(str(manager_note))}</i>"
        if manager_note
        else ""
    )

    return (
        f"{delivery_prefix} №{lead_id}</b>\n"
        "━━━━━━━━━━━━━━━━━━\n"
        f"🎯 <b>Цель:</b> "
        f"<b>{safe_action_type}</b>\n"
        f"💼 <b>Агент:</b> "
        f"<b>{safe_agent_name}</b>{house_info}\n"
        f"🏷 <b>Метка QR:</b> "
        f"<code>{safe_ref_tag}</code>\n"
        f"🏠 <b>Адрес:</b> {safe_address}\n"
        f"📦 <b>Тариф:</b> {safe_service}\n"
        f"👤 <b>Имя:</b> {safe_name}\n"
        f'📞 <b>Телефон:</b> '
        f'<a href="tel:{safe_phone}">{safe_phone}</a>\n'
        f"⏰ <b>Время звонка:</b> "
        f"{safe_call_time}\n"
        f"💬 <b>Telegram:</b> {safe_username}\n"
        f"📅 <b>Создана:</b> {safe_created_at}\n"
        f"📌 <b>Статус:</b> "
        f"<b>{safe_status}</b>\n"
        f"👨‍💼 <b>В работе у:</b> "
        f"<b>{claimed_text}</b>"
        f"{note_text}"
    )


def lead_actions_kb(
    lead_id: int,
    claimed_by: str | None = None,
) -> InlineKeyboardMarkup:
    if claimed_by:
        claim_button = InlineKeyboardButton(
            text=f"👤 Взял: {claimed_by}",
            callback_data="noop",
        )
    else:
        claim_button = InlineKeyboardButton(
            text="🙋‍♂️ Взять в работу",
            callback_data=f"lead_claim:{lead_id}",
        )

    return InlineKeyboardMarkup(
        inline_keyboard=[
            [claim_button],
            [
                InlineKeyboardButton(
                    text="📞 Связался",
                    callback_data=(
                        f"lead_status:{lead_id}:связались"
                    ),
                ),
                InlineKeyboardButton(
                    text="📅 Назначено",
                    callback_data=(
                        f"lead_status:{lead_id}:назначено"
                    ),
                ),
            ],
            [
                InlineKeyboardButton(
                    text="✅ Подключён",
                    callback_data=(
                        f"lead_status:{lead_id}:подключено"
                    ),
                ),
                InlineKeyboardButton(
                    text="❌ Отказ",
                    callback_data=(
                        f"lead_status:{lead_id}:отказ"
                    ),
                ),
            ],
            [
                InlineKeyboardButton(
                    text="📝 Добавить заметку",
                    callback_data=f"lead_note:{lead_id}",
                )
            ],
        ]
    )


def get_start_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="🚀 Подобрать тариф и подключение",
                    callback_data="start_process",
                )
            ]
        ]
    )


def get_phone_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [
                KeyboardButton(
                    text="📱 Поделиться контактом",
                    request_contact=True,
                )
            ]
        ],
        resize_keyboard=True,
        one_time_keyboard=True,
        input_field_placeholder="Введите номер телефона",
    )


def get_time_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="🌅 Утро (09:00–12:00)",
                    callback_data="time_morning",
                )
            ],
            [
                InlineKeyboardButton(
                    text="☀️ День (12:00–17:00)",
                    callback_data="time_day",
                )
            ],
            [
                InlineKeyboardButton(
                    text="🌙 Вечер (17:00–21:00)",
                    callback_data="time_evening",
                )
            ],
            [
                InlineKeyboardButton(
                    text="⚡ В любое время",
                    callback_data="time_any",
                )
            ],
        ]
    )


def get_consent_kb(
    privacy_policy_url: str,
) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="✅ Отправить заявку",
                    callback_data="consent_accept",
                )
            ],
            [
                InlineKeyboardButton(
                    text="✏️ Изменить данные",
                    callback_data="consent_edit",
                )
            ],
            [
                InlineKeyboardButton(
                    text="📄 Политика обработки ПДн",
                    url=privacy_policy_url,
                )
            ],
        ]
    )


def cancel_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="❌ Отменить",
                    callback_data="cancel_order",
                )
            ]
        ]
    )
def consent_text(
    data: dict[str, Any],
    personal_data_operator: str,
) -> str:
    return (
        "🔎 <b>Проверьте данные заявки</b>\n\n"
        f"🎯 Тип: <b>{html.escape(str(data.get('action_type', 'Подключение')))}</b>\n"
        f"🏠 Адрес: <b>{html.escape(str(data.get('address', '')))}</b>\n"
        f"📦 Услуга: <b>{html.escape(str(data.get('service', '')))}</b>\n"
        f"👤 Имя: <b>{html.escape(str(data.get('name', '')))}</b>\n"
        f"📞 Телефон: <code>{html.escape(str(data.get('phone', '')))}</code>\n"
        f"⏰ Время звонка: <b>{html.escape(str(data.get('call_time', '')))}</b>\n\n"
        "Нажимая кнопку «Отправить заявку», вы даёте согласие "
        "на обработку указанных персональных данных для связи "
        "по вопросу подключения услуг.\n\n"
        f"Оператор: <i>{html.escape(personal_data_operator)}</i>"
    )


async def verify_manager_access(
    callback: CallbackQuery,
    admin_ids: list[int],
    manager_ids: list[int],
    crm_chat_id: int,
) -> bool:
    user_id = callback.from_user.id
    message = callback.message

    if message is None:
        await callback.answer(
            "⛔ Сообщение CRM не найдено.",
            show_alert=True,
        )
        return False

    allowed_ids = set(admin_ids) | set(manager_ids)

    if user_id not in allowed_ids:
        await callback.answer(
            "⛔ У вас нет прав для управления заявкой.",
            show_alert=True,
        )
        return False

    if crm_chat_id and message.chat.id != crm_chat_id:
        await callback.answer(
            "⛔ Действие доступно только в CRM-чате.",
            show_alert=True,
        )
        return False

    return True


class OrderFSM(StatesGroup):
    waiting_for_address = State()
    choosing_category = State()
    waiting_for_service = State()
    viewing_tariff = State()
    waiting_for_name = State()
    waiting_for_phone = State()
    waiting_for_call_time = State()
    waiting_for_consent = State()


class ManagerNoteFSM(StatesGroup):
    waiting_for_note_text = State()


router = Router()
router.message.middleware(ThrottlingMiddleware(0.4))
router.callback_query.middleware(ThrottlingMiddleware(0.3))

_submission_locks: defaultdict[int, asyncio.Lock] = defaultdict(
    asyncio.Lock
)


@router.message(Command("cancel", "отмена"))
async def cmd_cancel(
    message: Message,
    state: FSMContext,
    db_path: str,
) -> None:
    await state.clear()
    await cancel_funnel_session(
        db_path=db_path,
        user_id=message.from_user.id,
    )

    await message.answer(
        "❌ Оформление отменено.\n\n"
        "Чтобы начать заново, отправьте /start.",
        reply_markup=ReplyKeyboardRemove(),
    )


@router.callback_query(F.data == "cancel_order")
async def callback_cancel_order(
    callback: CallbackQuery,
    state: FSMContext,
    db_path: str,
) -> None:
    await callback.answer("Оформление отменено")

    await state.clear()
    await cancel_funnel_session(
        db_path=db_path,
        user_id=callback.from_user.id,
    )

    if callback.message:
        await callback.message.answer(
            "❌ Оформление отменено.\n\n"
            "Чтобы начать заново, отправьте /start.",
            reply_markup=ReplyKeyboardRemove(),
        )


@router.callback_query(F.data == "noop")
async def process_noop(callback: CallbackQuery) -> None:
    await callback.answer(
        "Заявка уже взята в работу.",
        show_alert=False,
    )


@router.message(CommandStart())
async def cmd_start(
    message: Message,
    state: FSMContext,
    command: CommandObject,
    db_path: str,
) -> None:
    user_id = message.from_user.id

    ref_tag, agent_code, agent_name, house_tag = parse_ref_tag(
        command.args
    )

    await state.clear()

    await state.update_data(
        ref_tag=ref_tag,
        agent_code=agent_code,
        agent_name=agent_name,
        house_tag=house_tag,
        started_at=now_moscow(),
    )

    await reset_funnel_session(
        db_path=db_path,
        user_id=user_id,
        ref_tag=ref_tag,
        step="start",
    )

    await record_event(
        db_path=db_path,
        user_id=user_id,
        ref_tag=ref_tag,
        event_name="start",
    )

    welcome_agent = ""

    if agent_code != "direct":
        welcome_agent = (
            f"\nВаш персональный консультант: "
            f"<b>{html.escape(agent_name)}</b>."
        )

    await message.answer(
        f"👋 <b>Здравствуйте!</b>{welcome_agent}\n\n"
        "Я официальный онлайн-помощник по подключению услуг "
        "<b>МТС</b> по вашему району.\n\n"
        "Подберём выгодный тариф и передадим заявку специалисту "
        "за 1 минуту без ожидания оператора.",
        reply_markup=get_start_kb(),
        parse_mode="HTML",
    )


@router.callback_query(F.data == "resume_order")
async def process_resume_order(
    callback: CallbackQuery,
    state: FSMContext,
    db_path: str,
    personal_data_operator: str,
    privacy_policy_url: str,
) -> None:
    if callback.message is None:
        await callback.answer(
            "Сообщение недоступно.",
            show_alert=True,
        )
        return

    current_state = await state.get_state()
    data = await state.get_data()
    user_id = callback.from_user.id
    ref_tag = data.get("ref_tag", "direct")

    await callback.answer()

    if not current_state:
        await state.set_state(OrderFSM.waiting_for_address)
        await update_funnel_session(
            db_path=db_path,
            user_id=user_id,
            ref_tag=ref_tag,
            step=OrderFSM.waiting_for_address.state,
        )

        await callback.message.answer(
            "📍 Давайте продолжим.\n\n"
            "Напишите город, улицу и номер дома.\n"
            "Например: <i>ул. Ленина, 45</i>",
            reply_markup=cancel_kb(),
            parse_mode="HTML",
        )
        return

    if current_state == OrderFSM.waiting_for_address.state:
        await callback.message.answer(
            "📍 Давайте продолжим.\n\n"
            "Напишите город, улицу и номер дома.\n"
            "Например: <i>ул. Ленина, 45</i>",
            reply_markup=cancel_kb(),
            parse_mode="HTML",
        )
        return

    if current_state == OrderFSM.choosing_category.state:
        address = html.escape(str(data.get("address", "")))

        await callback.message.answer(
            f"🏠 Адрес: <b>{address}</b>\n\n"
            "📋 <b>ВЫБЕРИТЕ КАТЕГОРИЮ УСЛУГ:</b>",
            reply_markup=get_categories_kb(),
            parse_mode="HTML",
        )
        return

    if current_state == OrderFSM.waiting_for_service.state:
        category_key = data.get("selected_category", "combo")
        category_info = CATEGORIES_INFO.get(
            category_key,
            {
                "name": "Тарифы",
                "desc": "",
            },
        )

        await callback.message.answer(
            f"📂 <b>{html.escape(category_info['name'])}</b>\n"
            f"<i>{html.escape(category_info['desc'])}</i>\n\n"
            "👇 Выберите подходящий тариф:",
            reply_markup=get_tariffs_by_category_kb(category_key),
            parse_mode="HTML",
        )
        return

    if current_state == OrderFSM.viewing_tariff.state:
        service_key = data.get("service_key")
        tariff = TARIFFS.get(service_key)

        if tariff:
            await callback.message.answer(
                tariff["description"],
                reply_markup=get_tariff_actions_kb(),
                parse_mode="HTML",
            )
        else:
            await state.set_state(OrderFSM.choosing_category)
            await callback.message.answer(
                "📋 Выберите категорию услуг:",
                reply_markup=get_categories_kb(),
            )
        return

    if current_state == OrderFSM.waiting_for_name.state:
        await callback.message.answer(
            "👤 Напишите, как к вам обращаться.",
            reply_markup=cancel_kb(),
        )
        return

    if current_state == OrderFSM.waiting_for_phone.state:
        name = html.escape(str(data.get("name", "")))

        await callback.message.answer(
            f"👤 Имя: <b>{name}</b>\n\n"
            "📞 Отправьте свой контакт или введите "
            "номер телефона вручную.",
            reply_markup=get_phone_kb(),
            parse_mode="HTML",
        )
        return

    if current_state == OrderFSM.waiting_for_call_time.state:
        await callback.message.answer(
            "⏰ Выберите удобное время для звонка.",
            reply_markup=get_time_kb(),
        )
        return

    if current_state == OrderFSM.waiting_for_consent.state:
        await callback.message.answer(
            consent_text(
                data=data,
                personal_data_operator=personal_data_operator,
            ),
            reply_markup=get_consent_kb(privacy_policy_url),
            parse_mode="HTML",
        )
        return

    await state.set_state(OrderFSM.choosing_category)
    await callback.message.answer(
        "📋 Выберите категорию услуг:",
        reply_markup=get_categories_kb(),
    )


@router.callback_query(F.data == "start_process")
async def process_start(
    callback: CallbackQuery,
    state: FSMContext,
    db_path: str,
) -> None:
    if callback.message is None:
        await callback.answer(
            "Сообщение недоступно.",
            show_alert=True,
        )
        return

    await callback.answer()

    data = await state.get_data()
    ref_tag = data.get("ref_tag", "direct")
    user_id = callback.from_user.id

    await state.set_state(OrderFSM.waiting_for_address)

    await update_funnel_session(
        db_path=db_path,
        user_id=user_id,
        ref_tag=ref_tag,
        step=OrderFSM.waiting_for_address.state,
    )

    await record_event(
        db_path=db_path,
        user_id=user_id,
        ref_tag=ref_tag,
        event_name="funnel_started",
    )

    await callback.message.answer(
        "📍 <b>Шаг 1 из 6. Ваш адрес</b>\n\n"
        "Напишите город, улицу и номер дома.\n"
        "Например: <i>ул. Ленина, 45</i>\n\n"
        "Адрес нужен для проверки технической возможности подключения.",
        reply_markup=cancel_kb(),
        parse_mode="HTML",
    )


@router.message(OrderFSM.waiting_for_address, F.text)
async def process_address(
    message: Message,
    state: FSMContext,
    db_path: str,
) -> None:
    address = " ".join(message.text.split()).strip()

    if len(address) < 4:
        await message.answer(
            "⚠️ Адрес слишком короткий.\n"
            "Укажите город, улицу и номер дома."
        )
        return

    if len(address) > 300:
        await message.answer(
            "⚠️ Слишком длинный адрес. "
            "Максимум 300 символов."
        )
        return

    await state.update_data(address=address)
    data = await state.get_data()
    ref_tag = data.get("ref_tag", "direct")
    user_id = message.from_user.id

    await state.set_state(OrderFSM.choosing_category)

    await update_funnel_session(
        db_path=db_path,
        user_id=user_id,
        ref_tag=ref_tag,
        step=OrderFSM.choosing_category.state,
    )

    await record_event(
        db_path=db_path,
        user_id=user_id,
        ref_tag=ref_tag,
        event_name="address_entered",
    )

    await message.answer(
        f"✅ Адрес принят:\n"
        f"<b>{html.escape(address)}</b>\n\n"
        "📋 <b>ВЫБЕРИТЕ КАТЕГОРИЮ УСЛУГ:</b>",
        reply_markup=get_categories_kb(),
        parse_mode="HTML",
    )


@router.message(OrderFSM.waiting_for_address)
async def process_address_invalid(message: Message) -> None:
    await message.answer(
        "⚠️ Пожалуйста, отправьте адрес обычным текстовым сообщением."
    )


@router.callback_query(
    OrderFSM.choosing_category,
    F.data.startswith("cat:"),
)
async def process_category(
    callback: CallbackQuery,
    state: FSMContext,
) -> None:
    if callback.message is None:
        await callback.answer(
            "Сообщение недоступно.",
            show_alert=True,
        )
        return

    category_key = callback.data.split(":", 1)[1]
    category_info = CATEGORIES_INFO.get(category_key)

    if not category_info:
        await callback.answer(
            "Категория не найдена.",
            show_alert=True,
        )
        return

    await callback.answer()
    await state.update_data(selected_category=category_key)
    await state.set_state(OrderFSM.waiting_for_service)

    await callback.message.edit_text(
        f"📂 <b>{html.escape(category_info['name'])}</b>\n"
        f"<i>{html.escape(category_info['desc'])}</i>\n\n"
        "👇 Выберите интересующий тариф:",
        reply_markup=get_tariffs_by_category_kb(category_key),
        parse_mode="HTML",
    )


@router.callback_query(F.data == "action_back_categories")
async def process_back_to_categories(
    callback: CallbackQuery,
    state: FSMContext,
) -> None:
    if callback.message is None:
        await callback.answer(
            "Сообщение недоступно.",
            show_alert=True,
        )
        return

    await callback.answer()
    await state.set_state(OrderFSM.choosing_category)

    await callback.message.edit_text(
        "📋 <b>ВЫБЕРИТЕ КАТЕГОРИЮ УСЛУГ:</b>",
        reply_markup=get_categories_kb(),
        parse_mode="HTML",
    )


@router.callback_query(
    OrderFSM.waiting_for_service,
    F.data.startswith("tariff_item:"),
)
async def process_tariff_item(
    callback: CallbackQuery,
) -> None:
    if callback.message is None:
        await callback.answer(
            "Сообщение недоступно.",
            show_alert=True,
        )
        return

    item_id = callback.data.split(":", 1)[1]
    variants = get_variants_kb(item_id)

    if not variants:
        await callback.answer(
            "Варианты не найдены.",
            show_alert=True,
        )
        return

    prompt_text, keyboard = variants

    await callback.answer()
    await callback.message.edit_text(
        prompt_text,
        reply_markup=keyboard,
        parse_mode="HTML",
    )


@router.callback_query(
    OrderFSM.waiting_for_service,
    F.data.startswith("service:"),
)
async def process_service(
    callback: CallbackQuery,
    state: FSMContext,
    db_path: str,
) -> None:
    if callback.message is None:
        await callback.answer(
            "Сообщение недоступно.",
            show_alert=True,
        )
        return

    service_key = callback.data.split(":", 1)[1]
    tariff = TARIFFS.get(service_key)

    if tariff is None:
        await callback.answer(
            "Услуга не найдена.",
            show_alert=True,
        )
        return

    await callback.answer()

    await state.update_data(
        service=tariff["title"],
        service_key=service_key,
    )
    data = await state.get_data()
    ref_tag = data.get("ref_tag", "direct")
    user_id = callback.from_user.id

    await state.set_state(OrderFSM.viewing_tariff)

    await update_funnel_session(
        db_path=db_path,
        user_id=user_id,
        ref_tag=ref_tag,
        step=OrderFSM.viewing_tariff.state,
    )

    await record_event(
        db_path=db_path,
        user_id=user_id,
        ref_tag=ref_tag,
        event_name="service_selected",
    )

    await callback.message.edit_text(
        tariff["description"],
        reply_markup=get_tariff_actions_kb(),
        parse_mode="HTML",
    )
@router.callback_query(F.data == "action_back_tariffs")
async def process_back_tariffs(
    callback: CallbackQuery,
    state: FSMContext,
) -> None:
    if callback.message is None:
        await callback.answer(
            "Сообщение недоступно.",
            show_alert=True,
        )
        return

    data = await state.get_data()
    category_key = data.get("selected_category", "combo")
    category_info = CATEGORIES_INFO.get(
        category_key,
        {
            "name": "Тарифы",
            "desc": "",
        },
    )

    await callback.answer()
    await state.set_state(OrderFSM.waiting_for_service)

    await callback.message.edit_text(
        f"📂 <b>{html.escape(category_info['name'])}</b>\n"
        f"<i>{html.escape(category_info['desc'])}</i>\n\n"
        "👇 Выберите интересующий тариф:",
        reply_markup=get_tariffs_by_category_kb(category_key),
        parse_mode="HTML",
    )


@router.callback_query(
    OrderFSM.viewing_tariff,
    F.data.in_({"action_order", "action_call"}),
)
async def process_action(
    callback: CallbackQuery,
    state: FSMContext,
) -> None:
    if callback.message is None:
        await callback.answer(
            "Сообщение недоступно.",
            show_alert=True,
        )
        return

    action_type = (
        "Подключение"
        if callback.data == "action_order"
        else "Заказ звонка"
    )

    await callback.answer()
    await state.update_data(action_type=action_type)
    await state.set_state(OrderFSM.waiting_for_name)

    await callback.message.answer(
        "👤 <b>Шаг 3 из 6. Как к вам обращаться?</b>\n\n"
        "Напишите имя.",
        reply_markup=cancel_kb(),
        parse_mode="HTML",
    )


@router.message(OrderFSM.waiting_for_name, F.text)
async def process_name(
    message: Message,
    state: FSMContext,
) -> None:
    name = " ".join(message.text.split()).strip()

    if len(name) < 2:
        await message.answer(
            "⚠️ Имя слишком короткое.\n"
            "Напишите, как к вам обращаться."
        )
        return

    if len(name) > 100:
        await message.answer(
            "⚠️ Имя слишком длинное. Максимум 100 символов."
        )
        return

    await state.update_data(name=name)
    await state.set_state(OrderFSM.waiting_for_phone)

    await message.answer(
        f"Приятно познакомиться, "
        f"<b>{html.escape(name)}</b>!\n\n"
        "📞 <b>Шаг 4 из 6. Номер телефона</b>\n\n"
        "Номер нужен только для связи по заявке.\n"
        "Можно нажать кнопку внизу или ввести номер вручную.",
        reply_markup=get_phone_kb(),
        parse_mode="HTML",
    )


@router.message(OrderFSM.waiting_for_name)
async def process_name_invalid(message: Message) -> None:
    await message.answer(
        "⚠️ Пожалуйста, напишите имя текстом."
    )


@router.message(OrderFSM.waiting_for_phone)
async def process_phone(
    message: Message,
    state: FSMContext,
) -> None:
    phone: str | None = None

    if message.contact:
        contact_user_id = message.contact.user_id

        if (
            contact_user_id is not None
            and contact_user_id != message.from_user.id
        ):
            await message.answer(
                "⚠️ Пожалуйста, отправьте свой собственный номер "
                "или введите его вручную."
            )
            return

        phone = normalize_phone(
            message.contact.phone_number
        )

    elif message.text:
        phone = normalize_phone(message.text)

    if phone is None:
        await message.answer(
            "❌ Не удалось распознать номер.\n\n"
            "Введите мобильный номер РФ, например:\n"
            "<code>+7 925 123-45-67</code>",
            reply_markup=get_phone_kb(),
            parse_mode="HTML",
        )
        return

    await state.update_data(phone=phone)
    await state.set_state(OrderFSM.waiting_for_call_time)

    await message.answer(
        "✅ Номер принят.\n\n"
        "⏰ <b>Шаг 5 из 6. Когда удобно принять звонок?</b>",
        reply_markup=ReplyKeyboardRemove(),
        parse_mode="HTML",
    )

    await message.answer(
        "Выберите удобный диапазон:",
        reply_markup=get_time_kb(),
    )


@router.callback_query(
    OrderFSM.waiting_for_call_time,
    F.data.startswith("time_"),
)
async def process_call_time(
    callback: CallbackQuery,
    state: FSMContext,
    personal_data_operator: str,
    privacy_policy_url: str,
) -> None:
    if callback.message is None:
        await callback.answer(
            "Сообщение недоступно.",
            show_alert=True,
        )
        return

    selected_time = TIME_SLOTS.get(callback.data)

    if selected_time is None:
        await callback.answer(
            "Диапазон не найден.",
            show_alert=True,
        )
        return

    await callback.answer()
    await state.update_data(call_time=selected_time)
    await state.set_state(OrderFSM.waiting_for_consent)

    data = await state.get_data()

    await callback.message.answer(
        consent_text(
            data=data,
            personal_data_operator=personal_data_operator,
        ),
        reply_markup=get_consent_kb(privacy_policy_url),
        parse_mode="HTML",
    )


@router.callback_query(
    OrderFSM.waiting_for_consent,
    F.data == "consent_edit",
)
async def process_consent_edit(
    callback: CallbackQuery,
    state: FSMContext,
) -> None:
    if callback.message is None:
        await callback.answer(
            "Сообщение недоступно.",
            show_alert=True,
        )
        return

    await callback.answer()
    await state.set_state(OrderFSM.waiting_for_name)

    await callback.message.answer(
        "✏️ Введите ваше имя заново.",
        reply_markup=cancel_kb(),
    )


@router.callback_query(
    OrderFSM.waiting_for_consent,
    F.data == "consent_accept",
)
async def process_consent_accept(
    callback: CallbackQuery,
    state: FSMContext,
    db_path: str,
    agent_name: str,
    policy_version: str,
) -> None:
    if callback.message is None:
        await callback.answer(
            "Сообщение недоступно.",
            show_alert=True,
        )
        return

    user_id = callback.from_user.id

    async with _submission_locks[user_id]:
        await callback.answer()
        data = await state.get_data()

        required_fields = (
            "address",
            "service",
            "name",
            "phone",
            "call_time",
        )

        if not all(data.get(field) for field in required_fields):
            await state.clear()

            await callback.message.answer(
                "⚠️ Сессия заявки устарела.\n"
                "Начните заново: /start"
            )
            return

        data["user_id"] = user_id
        data["username"] = (
            f"@{callback.from_user.username}"
            if callback.from_user.username
            else "отсутствует"
        )
        data["consent_accepted_at"] = now_moscow()
        data["consent_policy_version"] = policy_version
        data["marketing_consent"] = False

        try:
            lead_id, is_duplicate, previous_id = await save_lead(
                db_path=db_path,
                data=data,
            )
        except sqlite3.Error:
            LOGGER.exception(
                "Ошибка сохранения заявки пользователя %s",
                user_id,
            )

            await callback.message.answer(
                "⚠️ Не удалось сохранить заявку из-за ошибки базы данных.\n"
                "Попробуйте ещё раз через минуту."
            )
            return

        ref_tag = data.get("ref_tag", "direct")

        await update_funnel_session(
            db_path=db_path,
            user_id=user_id,
            ref_tag=ref_tag,
            step="completed",
            completed=True,
        )

        await record_event(
            db_path=db_path,
            user_id=user_id,
            ref_tag=ref_tag,
            event_name="consent_accepted",
        )

        await record_event(
            db_path=db_path,
            user_id=user_id,
            ref_tag=ref_tag,
            event_name="lead_created",
        )

        await state.clear()

        try:
            await callback.message.edit_reply_markup(
                reply_markup=None,
            )
        except TelegramBadRequest:
            pass

        consultant_name = data.get("agent_name") or agent_name

        if consultant_name == "Прямой переход":
            consultant_name = agent_name

        duplicate_text = ""

        if is_duplicate and previous_id:
            duplicate_text = (
                "\n\nℹ️ Похоже, вы уже оставляли заявку недавно. "
                "Мы всё равно передали новую заявку специалисту."
            )

        await callback.message.answer(
            f"🎉 <b>Заявка №{lead_id} принята!</b>\n\n"
            f"Специалист <b>{html.escape(str(consultant_name))}</b> "
            "свяжется с вами в указанное время:\n"
            f"<b>{html.escape(str(data['call_time']))}</b>\n\n"
            f"Номер для связи: "
            f"<code>{html.escape(str(data['phone']))}</code>"
            f"{duplicate_text}",
            parse_mode="HTML",
            reply_markup=ReplyKeyboardRemove(),
        )


async def manager_access_or_answer(
    callback: CallbackQuery,
    admin_ids: list[int],
    manager_ids: list[int],
    crm_chat_id: int,
) -> bool:
    return await verify_manager_access(
        callback=callback,
        admin_ids=admin_ids,
        manager_ids=manager_ids,
        crm_chat_id=crm_chat_id,
    )


@router.callback_query(F.data.startswith("lead_claim:"))
async def process_lead_claim(
    callback: CallbackQuery,
    db_path: str,
    admin_ids: list[int],
    manager_ids: list[int],
    crm_chat_id: int,
) -> None:
    if not await manager_access_or_answer(
        callback=callback,
        admin_ids=admin_ids,
        manager_ids=manager_ids,
        crm_chat_id=crm_chat_id,
    ):
        return

    try:
        lead_id = int(
            callback.data.split(":", 1)[1]
        )
    except (ValueError, AttributeError):
        await callback.answer(
            "Некорректный идентификатор заявки.",
            show_alert=True,
        )
        return

    username = callback.from_user.username or ""

    assigned = await assign_lead_to_manager(
        db_path=db_path,
        lead_id=lead_id,
        user_id=callback.from_user.id,
        username=username,
    )

    if not assigned:
        await callback.answer(
            "⚠️ Заявку уже взял другой менеджер.",
            show_alert=True,
        )
        return

    await record_lead_activity(
        db_path=db_path,
        lead_id=lead_id,
        actor_user_id=callback.from_user.id,
        actor_username=callback.from_user.username,
        action="claim",
    )

    await callback.answer(
        "✅ Вы взяли заявку в работу."
    )

    row = await asyncio.to_thread(
        get_lead_by_id_sync,
        db_path,
        lead_id,
    )

    if not row or callback.message is None:
        return

    try:
        await callback.message.edit_text(
            build_agent_text(
                row,
                delivery_prefix="📋 <b>ЗАЯВКА В РАБОТЕ",
            ),
            parse_mode="HTML",
            reply_markup=lead_actions_kb(
                lead_id,
                row[14],
            ),
        )
    except TelegramBadRequest as exc:
        LOGGER.warning(
            "Не удалось обновить карточку #%s: %s",
            lead_id,
            exc,
        )


@router.callback_query(F.data.startswith("lead_status:"))
async def process_lead_status(
    callback: CallbackQuery,
    db_path: str,
    admin_ids: list[int],
    manager_ids: list[int],
    crm_chat_id: int,
) -> None:
    if not await manager_access_or_answer(
        callback=callback,
        admin_ids=admin_ids,
        manager_ids=manager_ids,
        crm_chat_id=crm_chat_id,
    ):
        return

    try:
        _, lead_id_raw, new_status = callback.data.split(
            ":",
            2,
        )
        lead_id = int(lead_id_raw)
    except (ValueError, AttributeError):
        await callback.answer(
            "Некорректные данные кнопки.",
            show_alert=True,
        )
        return

    updated = await update_lead_status(
        db_path=db_path,
        lead_id=lead_id,
        new_status=new_status,
        changed_by=callback.from_user.id,
    )

    if not updated:
        await callback.answer(
            "Заявка не найдена или статус недопустим.",
            show_alert=True,
        )
        return

    await record_lead_activity(
        db_path=db_path,
        lead_id=lead_id,
        actor_user_id=callback.from_user.id,
        actor_username=callback.from_user.username,
        action="status_change",
        payload=new_status,
    )

    await callback.answer(
        f"Статус изменён: {new_status}"
    )

    row = await asyncio.to_thread(
        get_lead_by_id_sync,
        db_path,
        lead_id,
    )

    if not row or callback.message is None:
        return

    try:
        await callback.message.edit_text(
            build_agent_text(
                row,
                delivery_prefix="📋 <b>КАРТОЧКА ЗАЯВКИ",
            ),
            parse_mode="HTML",
            reply_markup=lead_actions_kb(
                lead_id,
                row[14],
            ),
        )
    except TelegramBadRequest as exc:
        LOGGER.warning(
            "Не удалось обновить карточку #%s: %s",
            lead_id,
            exc,
        )


@router.callback_query(F.data.startswith("lead_note:"))
async def process_start_note(
    callback: CallbackQuery,
    state: FSMContext,
    admin_ids: list[int],
    manager_ids: list[int],
    crm_chat_id: int,
) -> None:
    if not await manager_access_or_answer(
        callback=callback,
        admin_ids=admin_ids,
        manager_ids=manager_ids,
        crm_chat_id=crm_chat_id,
    ):
        return

    if callback.message is None:
        await callback.answer(
            "Сообщение недоступно.",
            show_alert=True,
        )
        return

    try:
        lead_id = int(
            callback.data.split(":", 1)[1]
        )
    except (ValueError, AttributeError):
        await callback.answer(
            "Некорректный идентификатор заявки.",
            show_alert=True,
        )
        return

    await state.set_state(
        ManagerNoteFSM.waiting_for_note_text
    )

    await state.update_data(
        note_lead_id=lead_id,
        crm_chat_id=callback.message.chat.id,
        crm_msg_id=callback.message.message_id,
    )

    await callback.answer()

    await callback.message.reply(
        f"📝 Напишите заметку для заявки №{lead_id}.\n"
        "Для отмены отправьте /cancel.",
    )


@router.message(
    ManagerNoteFSM.waiting_for_note_text,
    F.text,
)
async def process_save_note(
    message: Message,
    state: FSMContext,
    db_path: str,
    admin_ids: list[int],
    manager_ids: list[int],
    crm_chat_id: int,
) -> None:
    allowed_ids = set(admin_ids) | set(manager_ids)

    if message.from_user.id not in allowed_ids:
        await state.clear()
        return

    if message.chat.id != crm_chat_id:
        await message.answer(
            "⛔ Заметки можно добавлять только из CRM-чата."
        )
        return

    data = await state.get_data()
    raw_lead_id = data.get("note_lead_id")
    target_chat_id = data.get("crm_chat_id")
    target_message_id = data.get("crm_msg_id")

    try:
        lead_id = int(raw_lead_id)
    except (TypeError, ValueError):
        await state.clear()
        await message.answer(
            "⚠️ Сессия заметки устарела."
        )
        return

    note = " ".join(message.text.split()).strip()

    if not note:
        await message.answer(
            "⚠️ Заметка не может быть пустой."
        )
        return

    if len(note) > 500:
        await message.answer(
            "⚠️ Заметка слишком длинная. "
            "Максимум 500 символов."
        )
        return

    author = message.from_user.username

    if author:
        author_name = f"@{author}"
    else:
        author_name = message.from_user.first_name or "Менеджер"

    note_text = f"{author_name}: {note}"

    updated = await update_lead_note(
        db_path=db_path,
        lead_id=lead_id,
        note=note_text,
    )

    if not updated:
        await state.clear()
        await message.answer(
            "⚠️ Заявка не найдена."
        )
        return

    await record_lead_activity(
        db_path=db_path,
        lead_id=lead_id,
        actor_user_id=message.from_user.id,
        actor_username=message.from_user.username,
        action="note",
        payload=note,
    )

    await state.clear()

    await message.reply(
        f"✅ Заметка к заявке №{lead_id} сохранена."
    )

    row = await asyncio.to_thread(
        get_lead_by_id_sync,
        db_path,
        lead_id,
    )

    if not row:
        return

    actual_chat_id = target_chat_id or row[16]
    actual_message_id = target_message_id or row[17]

    if not actual_chat_id or not actual_message_id:
        return

    try:
        await message.bot.edit_message_text(
            chat_id=actual_chat_id,
            message_id=actual_message_id,
            text=build_agent_text(
                row,
                delivery_prefix="📋 <b>КАРТОЧКА ЗАЯВКИ",
            ),
            parse_mode="HTML",
            reply_markup=lead_actions_kb(
                lead_id,
                row[14],
            ),
        )
    except TelegramBadRequest as exc:
        LOGGER.warning(
            "Не удалось обновить карточку #%s после заметки: %s",
            lead_id,
            exc,
        )


def generate_export_csv_sync(
    db_path: str,
) -> tuple[str, int]:
    with sqlite3.connect(
        db_path,
        timeout=DB_TIMEOUT,
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
        "История статусов",
    ]

    with tempfile.NamedTemporaryFile(
        mode="w",
        delete=False,
        suffix=".csv",
        encoding="utf-8-sig",
        newline="",
    ) as temporary_file:
        writer = csv.writer(
            temporary_file,
            delimiter=";",
        )

        writer.writerow(headers)

        for row in rows:
            writer.writerow(
                [
                    sanitize_excel_cell(value)
                    for value in row
                ]
            )

        temporary_path = temporary_file.name

    return temporary_path, len(rows)


@router.message(Command("export"))
async def cmd_export(
    message: Message,
    admin_ids: list[int],
    db_path: str,
) -> None:
    if message.from_user.id not in admin_ids:
        return

    temporary_path: str | None = None

    try:
        temporary_path, count = await asyncio.to_thread(
            generate_export_csv_sync,
            db_path,
        )

        document = FSInputFile(
            temporary_path,
            filename=(
                f"leads_"
                f"{datetime.now().strftime('%Y%m%d_%H%M')}.csv"
            ),
        )

        await message.reply_document(
            document,
            caption=f"📊 Экспорт заявок: {count} шт.",
        )
    except Exception:
        LOGGER.exception("Ошибка формирования экспорта")

        await message.answer(
            "⚠️ Не удалось сформировать экспорт."
        )
    finally:
        if temporary_path:
            try:
                os.remove(temporary_path)
            except OSError:
                pass


def fetch_stats_sync(
    db_path: str,
) -> tuple[
    int,
    int,
    list[tuple[str, int]],
    list[tuple[str, int]],
    list[tuple[str, int]],
]:
    with sqlite3.connect(
        db_path,
        timeout=DB_TIMEOUT,
    ) as connection:
        connection.execute("PRAGMA busy_timeout = 10000")

        today = datetime.now(MOSCOW_TZ).strftime(
            "%Y-%m-%d"
        )

        today_count = connection.execute(
            """
            SELECT COUNT(*)
            FROM leads
            WHERE date(created_at) = ?
            """,
            (today,),
        ).fetchone()[0]

        total_count = connection.execute(
            "SELECT COUNT(*) FROM leads"
        ).fetchone()[0]

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


@router.message(Command("stats"))
async def cmd_stats(
    message: Message,
    admin_ids: list[int],
    db_path: str,
) -> None:
    if message.from_user.id not in admin_ids:
        return

    (
        today_count,
        total_count,
        agents,
        statuses,
        houses,
    ) = await asyncio.to_thread(
        fetch_stats_sync,
        db_path,
    )

    agents_text = "\n".join(
        f"• <b>{html.escape(str(agent_name))}</b>: "
        f"{count}"
        for agent_name, count in agents
    )

    statuses_text = "\n".join(
        f"• <b>{html.escape(str(status))}</b>: "
        f"{count}"
        for status, count in statuses
    )

    houses_text = "\n".join(
        f"• <code>{html.escape(str(house))}</code>: "
        f"{count}"
        for house, count in houses
    )

    await message.answer(
        "📊 <b>Статистика CRM МТС</b>\n\n"
        f"📅 Сегодня: <b>{today_count}</b>\n"
        f"📦 Всего заявок: <b>{total_count}</b>\n\n"
        f"<b>По агентам:</b>\n"
        f"{agents_text or 'нет данных'}\n\n"
        f"<b>По статусам:</b>\n"
        f"{statuses_text or 'нет данных'}\n\n"
        f"<b>По меткам домов:</b>\n"
        f"{houses_text or 'нет данных'}",
        parse_mode="HTML",
    )


async def delivery_worker(
    bot: Bot,
    db_path: str,
    crm_chat_id: int,
    crm_thread_id: int | None,
    stop_event: asyncio.Event,
) -> None:
    LOGGER.info("Delivery worker запущен")

    while not stop_event.is_set():
        try:
            await recover_stale_deliveries(db_path)

            queue = await get_delivery_queue(
                db_path=db_path,
                limit=10,
            )

            for lead in queue:
                if stop_event.is_set():
                    break

                lead_id = lead[0]

                claimed = await claim_lead(
                    db_path=db_path,
                    lead_id=lead_id,
                )

                if not claimed:
                    continue

                try:
                    send_kwargs: dict[str, Any] = {
                        "chat_id": crm_chat_id,
                        "text": build_agent_text(lead),
                        "parse_mode": "HTML",
                        "reply_markup": lead_actions_kb(
                            lead_id,
                            lead[14],
                        ),
                    }

                    if crm_thread_id is not None:
                        send_kwargs["message_thread_id"] = (
                            crm_thread_id
                        )

                    sent_message = await bot.send_message(
                        **send_kwargs
                    )

                except TelegramRetryAfter as exc:
                    await mark_delivery_error(
                        db_path=db_path,
                        lead_id=lead_id,
                        error_text=(
                            f"RetryAfter: {exc.retry_after}"
                        ),
                    )

                    await asyncio.sleep(
                        float(exc.retry_after)
                    )

                except TelegramForbiddenError as exc:
                    await mark_delivery_error(
                        db_path=db_path,
                        lead_id=lead_id,
                        error_text=(
                            "TelegramForbiddenError: "
                            f"{exc}"
                        ),
                    )

                    LOGGER.error(
                        "Нет доступа к CRM-чату для заявки #%s",
                        lead_id,
                    )

                except TelegramNetworkError as exc:
                    await mark_delivery_error(
                        db_path=db_path,
                        lead_id=lead_id,
                        error_text=str(exc),
                    )

                    LOGGER.warning(
                        "Сетевая ошибка доставки заявки #%s: %s",
                        lead_id,
                        exc,
                    )

                except TelegramBadRequest as exc:
                    await mark_delivery_error(
                        db_path=db_path,
                        lead_id=lead_id,
                        error_text=str(exc),
                    )

                    LOGGER.error(
                        "Ошибка Telegram API для заявки #%s: %s",
                        lead_id,
                        exc,
                    )

                except Exception as exc:
                    await mark_delivery_error(
                        db_path=db_path,
                        lead_id=lead_id,
                        error_text=str(exc),
                    )

                    LOGGER.exception(
                        "Неизвестная ошибка доставки заявки #%s",
                        lead_id,
                    )

                else:
                    await mark_delivered(
                        db_path=db_path,
                        lead_id=lead_id,
                        chat_id=sent_message.chat.id,
                        message_id=sent_message.message_id,
                    )

            await asyncio.sleep(4)

        except asyncio.CancelledError:
            break

        except Exception:
            LOGGER.exception(
                "Критическая ошибка delivery worker"
            )
            await asyncio.sleep(10)

    LOGGER.info("Delivery worker остановлен")


def get_abandoned_sessions_sync(
    db_path: str,
    threshold: str,
    limit: int = 20,
) -> list[tuple[int, str]]:
    with sqlite3.connect(
        db_path,
        timeout=DB_TIMEOUT,
    ) as connection:
        connection.execute("PRAGMA busy_timeout = 10000")

        return connection.execute(
            """
            SELECT user_id, ref_tag
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
    with sqlite3.connect(
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
    with sqlite3.connect(
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


async def followup_worker(
    bot: Bot,
    db_path: str,
    followup_minutes: int,
    stop_event: asyncio.Event,
) -> None:
    LOGGER.info("Follow-up worker запущен")

    while not stop_event.is_set():
        try:
            threshold = (
                datetime.now(MOSCOW_TZ)
                - timedelta(minutes=followup_minutes)
            ).strftime("%Y-%m-%d %H:%M:%S")

            abandoned = await get_abandoned_sessions(
                db_path=db_path,
                threshold=threshold,
                limit=20,
            )

            for user_id, ref_tag in abandoned:
                if stop_event.is_set():
                    break

                keyboard = InlineKeyboardMarkup(
                    inline_keyboard=[
                        [
                            InlineKeyboardButton(
                                text="🚀 Продолжить выбор",
                                callback_data="resume_order",
                            )
                        ],
                        [
                            InlineKeyboardButton(
                                text="🔕 Не напоминать",
                                callback_data="cancel_order",
                            )
                        ],
                    ]
                )

                try:
                    await bot.send_message(
                        chat_id=user_id,
                        text=(
                            "👋 <b>Здравствуйте!</b>\n\n"
                            "Вы начали подбирать услуги МТС, "
                            "но не завершили заявку.\n\n"
                            "Можно продолжить с того же шага. "
                            "Если нужна помощь с тарифом или "
                            "проверкой адреса, специалист подскажет."
                        ),
                        reply_markup=keyboard,
                        parse_mode="HTML",
                    )

                    marked = await mark_followup_sent(
                        db_path=db_path,
                        user_id=user_id,
                    )

                    if not marked:
                        LOGGER.info(
                            "Follow-up для пользователя %s "
                            "уже был обработан",
                            user_id,
                        )

                except TelegramForbiddenError:
                    await mark_funnel_blocked(
                        db_path=db_path,
                        user_id=user_id,
                    )

                except TelegramBadRequest as exc:
                    error_text = str(exc).lower()

                    if (
                        "chat not found" in error_text
                        or "deactivated" in error_text
                        or "blocked" in error_text
                    ):
                        await mark_funnel_blocked(
                            db_path=db_path,
                            user_id=user_id,
                        )
                    else:
                        LOGGER.warning(
                            "TelegramBadRequest follow-up "
                            "для %s: %s",
                            user_id,
                            exc,
                        )

                except TelegramRetryAfter as exc:
                    LOGGER.warning(
                        "RetryAfter follow-up для %s: %s",
                        user_id,
                        exc.retry_after,
                    )
                    await asyncio.sleep(
                        float(exc.retry_after)
                    )

                except TelegramNetworkError as exc:
                    LOGGER.warning(
                        "Сетевая ошибка follow-up для %s: %s",
                        user_id,
                        exc,
                    )

                except Exception as exc:
                    LOGGER.exception(
                        "Ошибка follow-up для пользователя %s: %s",
                        user_id,
                        exc,
                    )

            await asyncio.sleep(60)

        except asyncio.CancelledError:
            break

        except Exception:
            LOGGER.exception(
                "Критическая ошибка follow-up worker"
            )
            await asyncio.sleep(60)

    LOGGER.info("Follow-up worker остановлен")


async def start_agent_bot(
    token: str,
    admin_id: int | None = None,
    admin_ids: list[int] | None = None,
    manager_ids: list[int] | None = None,
    crm_chat_id: int | None = None,
    crm_thread_id: int | None = None,
    proxy: str | None = None,
    agent_name: str = "МТС Экспресс",
    db_path: str = "leads.db",
    privacy_policy_url: str = "",
    personal_data_operator: str = "",
    policy_version: str = "2026-01",
    followup_minutes: int = 25,
    **kwargs: Any,
) -> None:
    del kwargs

    if admin_ids is None:
        admin_ids = (
            [admin_id]
            if admin_id
            else [929398815]
        )

    admin_ids = list(
        dict.fromkeys(
            int(value)
            for value in admin_ids
            if isinstance(value, int)
        )
    )

    if not admin_ids:
        admin_ids = [929398815]

    if manager_ids is None:
        manager_ids = []

    manager_ids = list(
        dict.fromkeys(
            int(value)
            for value in manager_ids
            if isinstance(value, int)
        )
    )

    for current_admin_id in admin_ids:
        if current_admin_id not in manager_ids:
            manager_ids.append(current_admin_id)

    if crm_chat_id is None:
        crm_chat_id = admin_ids[0]

    if not proxy:
        proxy = os.getenv(
            "TELEGRAM_PROXY",
            "",
        ).strip() or None

    init_db(db_path)

    storage = SQLiteStorage(db_path)
    dispatcher = Dispatcher(storage=storage)
    dispatcher.include_router(router)

    stop_event = asyncio.Event()

    session = (
        AiohttpSession(proxy=proxy)
        if proxy
        else AiohttpSession()
    )

    async with Bot(
        token=token,
        session=session,
    ) as bot:
        delivery_task = asyncio.create_task(
            delivery_worker(
                bot=bot,
                db_path=db_path,
                crm_chat_id=crm_chat_id,
                crm_thread_id=crm_thread_id,
                stop_event=stop_event,
            )
        )

        followup_task = asyncio.create_task(
            followup_worker(
                bot=bot,
                db_path=db_path,
                followup_minutes=followup_minutes,
                stop_event=stop_event,
            )
        )

        LOGGER.info(
            "Бот запущен. CRM-чат: %s; прокси: %s",
            crm_chat_id,
            proxy or "отключен",
        )

        try:
            await dispatcher.start_polling(
                bot,
                admin_ids=admin_ids,
                manager_ids=manager_ids,
                crm_chat_id=crm_chat_id,
                db_path=db_path,
                agent_name=agent_name,
                privacy_policy_url=privacy_policy_url,
                personal_data_operator=personal_data_operator,
                policy_version=policy_version,
            )
        finally:
            stop_event.set()

            delivery_task.cancel()
            followup_task.cancel()

            await asyncio.gather(
                delivery_task,
                followup_task,
                return_exceptions=True,
            )

            await storage.close()
            LOGGER.info("Бот корректно остановлен")
