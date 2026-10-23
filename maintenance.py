"""Offline maintenance, under the same OS lock as the bot; dry run by default."""

import argparse
from config import BASE_DIR, load_env_file, get_config
from database import backup_database, connect, init_db, retention_cleanup_sync
from runtime import instance_lock


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=["backup", "migrate", "privacy", "requests"])
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Применить обезличивание после резервной копии",
    )
    args = parser.parse_args()
    load_env_file(BASE_DIR / "bot.env")
    config = get_config()
    with instance_lock(config.db_path):
        if args.action == "backup":
            print(backup_database(config.db_path))
        elif args.action == "migrate":
            init_db(config.db_path)
            print("Миграция завершена")
        elif args.action == "requests":
            with connect(config.db_path, readonly=True) as connection:
                print(
                    "Запросов на удаление:",
                    connection.execute(
                        "SELECT COUNT(*) FROM erasure_requests"
                    ).fetchone()[0],
                )
        else:
            count = retention_cleanup_sync(
                config.db_path, config.retention_days, apply=args.apply
            )
            print(
                ("Обезличено: " if args.apply else "К обработке (без изменений): ")
                + str(count)
            )


if __name__ == "__main__":
    main()
