"""Local export: read-only SQLite, Excel UTF-8 BOM, private no-clobber CSV."""

from __future__ import annotations
import argparse
from pathlib import Path
import sqlite3
from database import connect
from exports import sanitize_excel_cell, write_private_csv

BASE_DIR = Path(__file__).resolve().parent
DEFAULT_DB = BASE_DIR / "leads.db"


def normalize_phone_for_export(value):
    return sanitize_excel_cell(value)


def validate_db_path(path_value: str) -> Path:
    path = Path(path_value).expanduser().resolve()
    if path.suffix.lower() not in {".db", ".sqlite", ".sqlite3"}:
        raise ValueError("Файл базы должен иметь расширение .db, .sqlite или .sqlite3")
    return path


def get_table_columns(connection):
    return {str(row[1]) for row in connection.execute("PRAGMA table_info(leads)")}


def export_csv(db_path: Path, output_path: Path, *, overwrite=False) -> int:
    if not db_path.is_file():
        raise ValueError("Файл базы не найден; исходная база не создавалась")
    if output_path.resolve() == db_path.resolve():
        raise ValueError("CSV не может заменять базу")
    headers = [
        "ID",
        "Метка дома",
        "Адрес",
        "Услуга",
        "Имя клиента",
        "Телефон",
        "Время звонка",
        "Дата заявки",
        "Бизнес-статус",
        "Статус доставки",
        "Согласие на ПДн",
        "Версия политики",
        "Дата согласия",
        "Источник",
        "Кампания",
        "Категория",
        "Менеджер",
        "Заметка",
    ]
    with connect(str(db_path), readonly=True) as connection:
        columns = get_table_columns(connection)
        if not columns:
            raise ValueError("В базе отсутствует таблица leads")
        required = {
            "id",
            "ref_tag",
            "address",
            "service",
            "name",
            "phone",
            "call_time",
            "created_at",
            "status",
        }
        if not required <= columns:
            raise ValueError(
                "Неподдерживаемая схема базы; сначала выполните миграцию с резервной копией"
            )

        def column(name, fallback="''"):
            return f'"{name}"' if name in columns else fallback

        accepted = column("consent_accepted_at")
        expressions = [
            column(c)
            for c in (
                "id",
                "ref_tag",
                "address",
                "service",
                "name",
                "phone",
                "call_time",
                "created_at",
                "status",
            )
        ]
        expressions += [
            column("notification_status", "'неизвестно'"),
            f"CASE WHEN COALESCE({accepted}, '') != '' THEN 'да' ELSE 'нет' END",
            column("consent_policy_version"),
            accepted,
        ]
        expressions += [
            column(c)
            for c in (
                "source",
                "campaign",
                "category",
                "claimed_by_username",
                "manager_note",
            )
        ]
        rows = connection.execute(
            "SELECT " + ",".join(expressions) + " FROM leads ORDER BY id"
        ).fetchall()
    write_private_csv(output_path, headers, rows, overwrite=overwrite)
    return len(rows)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        description="Локальная выгрузка заявок (доступ ограничивается правами ОС)"
    )
    parser.add_argument("database", nargs="?", default=str(DEFAULT_DB))
    parser.add_argument("--output", type=Path)
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Явно разрешить замену существующей выгрузки",
    )
    args = parser.parse_args(argv)
    try:
        database = validate_db_path(args.database)
        output = args.output or database.with_suffix(".csv")
        count = export_csv(database, output, overwrite=args.overwrite)
        print(f"Выгружено заявок: {count}")
        return 0
    except (OSError, ValueError, sqlite3.Error) as exc:
        print(
            "Ошибка экспорта: "
            + (str(exc) if isinstance(exc, ValueError) else type(exc).__name__)
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
