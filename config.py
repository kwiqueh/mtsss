"""Validated configuration. Environment variables take precedence over bot.env."""

from __future__ import annotations

from dataclasses import dataclass, field
import logging
import os
from pathlib import Path
import re
from urllib.parse import urlsplit

BASE_DIR = Path(__file__).resolve().parent
TOKEN_PATTERN = re.compile(r"^\d{8,12}:[A-Za-z0-9_-]{35}$")
DEFAULT_AGENT_NAME = "МТС Экспресс"


def clean_env_value(value: str) -> str:
    value = value.strip()
    if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
        value = value[1:-1]
    return value.strip()


def load_env_file(path: Path) -> None:
    if not path.is_file():
        return
    try:
        text = path.read_text(encoding="utf-8-sig")
    except UnicodeDecodeError:
        text = path.read_text(encoding="cp1251")
    for number, raw in enumerate(text.splitlines(), 1):
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        key, separator, value = line.partition("=")
        key = key.strip()
        if not separator or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", key):
            raise ValueError(f"Некорректная строка bot.env: {number}")
        os.environ.setdefault(key, clean_env_value(value))


def integer(name: str, default: str = "", *, minimum: int | None = None) -> int:
    raw = os.getenv(name, default).strip()
    if not re.fullmatch(r"-?\d+", raw):
        raise ValueError(f"{name}: требуется целое число")
    result = int(raw)
    if result == 0 or (minimum is not None and result < minimum):
        raise ValueError(f"{name}: недопустимое значение")
    return result


def ids(name: str) -> list[int]:
    raw = os.getenv(name, "").strip()
    if not raw:
        return []
    result = []
    for item in raw.split(","):
        if not item.strip().isdigit() or int(item.strip()) <= 0:
            raise ValueError(f"{name}: нужны положительные ID через запятую")
        result.append(int(item.strip()))
    return list(dict.fromkeys(result))


@dataclass(frozen=True)
class Config:
    token: str = field(repr=False)
    admin_ids: list[int]
    manager_ids: list[int]
    crm_chat_id: int
    crm_thread_id: int | None
    followup_minutes: int
    agent_name: str
    db_path: str
    privacy_policy_url: str
    personal_data_operator: str
    policy_version: str
    retention_days: int
    proxy: str | None = field(default=None, repr=False)

    def bot_kwargs(self) -> dict:
        return dict(
            token=self.token,
            admin_ids=self.admin_ids,
            manager_ids=self.manager_ids,
            crm_chat_id=self.crm_chat_id,
            crm_thread_id=self.crm_thread_id,
            followup_minutes=self.followup_minutes,
            agent_name=self.agent_name,
            db_path=self.db_path,
            privacy_policy_url=self.privacy_policy_url,
            personal_data_operator=self.personal_data_operator,
            policy_version=self.policy_version,
            retention_days=self.retention_days,
            proxy=self.proxy,
        )


def get_config() -> Config:
    token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    if not TOKEN_PATTERN.fullmatch(token):
        raise ValueError("TELEGRAM_BOT_TOKEN отсутствует или имеет неверный формат")
    admins = list(
        dict.fromkeys([integer("TELEGRAM_ADMIN_ID", minimum=1)] + ids("ADMIN_IDS"))
    )
    managers = list(dict.fromkeys(admins + ids("MANAGER_IDS")))
    crm = (
        integer("CRM_CHAT_ID", str(admins[0]))
        if os.getenv("CRM_CHAT_ID", "").strip()
        else admins[0]
    )
    thread = (
        integer("CRM_THREAD_ID", minimum=1)
        if os.getenv("CRM_THREAD_ID", "").strip()
        else None
    )
    if thread and crm > 0:
        raise ValueError("CRM_THREAD_ID допустим только для группового CRM_CHAT_ID")
    followup_raw = os.getenv("FOLLOWUP_MINUTES", "25").strip()
    if not followup_raw.isdigit() or not 0 <= int(followup_raw) <= 10080:
        raise ValueError("FOLLOWUP_MINUTES: целое число от 0 до 10080 (0 — отключить)")
    policy = os.getenv("PRIVACY_POLICY_URL", "").strip()
    url = urlsplit(policy)
    if url.scheme != "https" or not url.hostname or url.username or url.password:
        raise ValueError(
            "PRIVACY_POLICY_URL: укажите HTTPS-ссылку на вашу политику обработки данных"
        )
    operator = os.getenv("PERSONAL_DATA_OPERATOR", "").strip()
    if not operator or len(operator) > 300:
        raise ValueError(
            "PERSONAL_DATA_OPERATOR: укажите оператора обработки данных (до 300 символов)"
        )
    version = os.getenv("POLICY_VERSION", "2026-01").strip()
    if not version or len(version) > 64:
        raise ValueError("POLICY_VERSION: требуется версия политики (до 64 символов)")
    raw_db = os.getenv("DB_PATH", "leads.db").strip()
    if not raw_db or raw_db == ":memory:":
        raise ValueError("DB_PATH: нужен путь к постоянному файлу SQLite")
    db = Path(raw_db).expanduser()
    if not db.is_absolute():
        db = BASE_DIR / db
    proxy = os.getenv("TELEGRAM_PROXY", "").strip() or None
    if proxy:
        parsed = urlsplit(proxy)
        if parsed.scheme not in {"http", "socks4", "socks5"} or not parsed.hostname:
            raise ValueError("TELEGRAM_PROXY: нужен URL http/socks4/socks5")
    name = os.getenv("AGENT_NAME", DEFAULT_AGENT_NAME).strip() or DEFAULT_AGENT_NAME
    if len(name) > 100:
        raise ValueError("AGENT_NAME: максимум 100 символов")
    return Config(
        token,
        admins,
        managers,
        crm,
        thread,
        int(followup_raw),
        name,
        str(db.resolve()),
        policy,
        operator,
        version,
        integer("RETENTION_DAYS", "180", minimum=1),
        proxy,
    )


class SafeLogFilter(logging.Filter):
    """Keep exception type, never exception payloads/Telegram request data."""

    def filter(self, record: logging.LogRecord) -> bool:
        text = record.getMessage()
        text = re.sub(r"\b\d{8,12}:[A-Za-z0-9_-]{20,}", "[TOKEN]", text)
        text = re.sub(r"(?:https?|socks[45])://\S+", "[URL]", text)
        text = re.sub(
            r"(?<!\w)\+?[78][\s()-]*\d(?:[\s()-]*\d){9}(?!\d)", "[PHONE]", text
        )
        record.msg, record.args = text, ()
        if record.exc_info:
            record.msg += f" ({record.exc_info[0].__name__})"
            record.exc_info = None
            record.exc_text = None
        return True


def configure_logging() -> None:
    handler = logging.StreamHandler()
    handler.addFilter(SafeLogFilter())
    handler.setFormatter(
        logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")
    )
    logging.basicConfig(level=logging.INFO, handlers=[handler], force=True)
