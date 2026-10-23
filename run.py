
import asyncio
import logging
import os
import re
from pathlib import Path

from bot import start_agent_bot


BASE_DIR = Path(__file__).resolve().parent
ENV_FILE = BASE_DIR / "bot.env"

DEFAULT_AGENT_NAME = "МТС Экспресс"

TOKEN_PATTERN = re.compile(
    r"^\d{8,12}:[A-Za-z0-9_-]{35}$"
)


def clean_env_value(value: str) -> str:
    """Убирает внешние пробелы и парные кавычки."""
    value = value.strip()

    if len(value) >= 2 and (
        (value.startswith('"') and value.endswith('"'))
        or (value.startswith("'") and value.endswith("'"))
    ):
        value = value[1:-1]

    return value.strip()


def load_env_file(path: Path) -> None:
    """Загружает настройки из bot.env без перезаписи системных переменных."""
    if not path.is_file():
        logging.warning("Файл конфигурации не найден: %s", path)
        return

    try:
        content = path.read_text(encoding="utf-8-sig")
    except UnicodeDecodeError:
        content = path.read_text(encoding="cp1251")

    for line_number, raw_line in enumerate(content.splitlines(), 1):
        line = raw_line.strip()

        if not line or line.startswith("#"):
            continue

        if "=" not in line:
            logging.warning(
                "Некорректная строка конфигурации: %d",
                line_number,
            )
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        value = clean_env_value(value)

        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", key):
            logging.warning(
                "Недопустимое имя параметра в строке %d",
                line_number,
            )
            continue

        os.environ.setdefault(key, value)

    logging.info("Конфигурация загружена")


def get_config():
    token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()

    if not TOKEN_PATTERN.fullmatch(token):
        raise RuntimeError(
            "TELEGRAM_BOT_TOKEN отсутствует или имеет "
            "неверный формат. Проверьте bot.env"
        )

    admin_id_raw = os.getenv("TELEGRAM_ADMIN_ID", "").strip()

    if not admin_id_raw.isdigit() or int(admin_id_raw) <= 0:
        raise RuntimeError(
            "TELEGRAM_ADMIN_ID должен содержать "
            "положительный числовой Telegram ID"
        )

    admin_id = int(admin_id_raw)

    agent_name = (
        os.getenv("AGENT_NAME", DEFAULT_AGENT_NAME).strip()
        or DEFAULT_AGENT_NAME
    )

    db_path = os.getenv(
        "DB_PATH", str(BASE_DIR / "leads.db")
    ).strip()

    privacy_policy_url = os.getenv(
        "PRIVACY_POLICY_URL", ""
    ).strip()

    personal_data_operator = os.getenv(
        "PERSONAL_DATA_OPERATOR", ""
    ).strip()

    policy_version = os.getenv(
        "POLICY_VERSION", "2026-01"
    ).strip()

    if not privacy_policy_url.startswith(("https://", "http://")):
        logging.warning(
            "PRIVACY_POLICY_URL не задан или некорректен"
        )

    if not personal_data_operator:
        logging.warning(
            "PERSONAL_DATA_OPERATOR не задан"
        )

    logging.info("Токен Telegram прошёл проверку формата")

    return (
        token,
        admin_id,
        agent_name,
        db_path,
        privacy_policy_url,
        personal_data_operator,
        policy_version,
    )


async def main() -> None:
    (
        token,
        admin_id,
        agent_name,
        db_path,
        privacy_policy_url,
        personal_data_operator,
        policy_version,
    ) = get_config()

    logging.info(
        "Запуск бота. Агент: %s; база: %s",
        agent_name,
        db_path,
    )

    await start_agent_bot(
        token=token,
        admin_id=admin_id,
        agent_name=agent_name,
        db_path=db_path,
        privacy_policy_url=privacy_policy_url,
        personal_data_operator=personal_data_operator,
        policy_version=policy_version,
    )


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format=(
            "%(asctime)s | %(levelname)s | "
            "%(name)s | %(message)s"
        ),
    )

    load_env_file(ENV_FILE)

    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logging.info("Бот остановлен пользователем")
    except Exception:
        logging.exception("Критическая ошибка запуска")
        raise
