import argparse
import asyncio
import logging
import os

from bot import start_agent_bot
from config import (
    BASE_DIR,
    DEFAULT_AGENT_NAME as DEFAULT_AGENT_NAME,
    TOKEN_PATTERN as TOKEN_PATTERN,
    clean_env_value as clean_env_value,
    configure_logging,
    get_config,
    load_env_file,
)
from runtime import instance_lock

ENV_FILE = BASE_DIR / "bot.env"


async def main() -> None:
    config = get_config()
    with instance_lock(config.db_path):
        await start_agent_bot(**config.bot_kwargs())


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="МТС Экспресс")
    parser.add_argument(
        "--check-config",
        action="store_true",
        help="Проверить настройки без подключения к Telegram",
    )
    args = parser.parse_args()
    os.umask(0o077)
    configure_logging()
    try:
        load_env_file(ENV_FILE)
        if args.check_config:
            get_config()
            logging.info("Конфигурация корректна; доступ к Telegram не проверялся")
        else:
            asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logging.info("Бот остановлен")
    except (ValueError, RuntimeError) as exc:
        logging.error("%s", exc)
        raise SystemExit(1) from None
    except Exception:
        logging.exception("Ошибка запуска; проверьте сеть, права и настройки")
        raise SystemExit(1) from None
