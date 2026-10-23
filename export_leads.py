from __future__ import annotations

import csv
import os
import re
import sqlite3
import sys
from pathlib import Path
from typing import Any


BASE_DIR = Path(__file__).resolve().parent
DEFAULT_DB = BASE_DIR / "leads.db"


def sanitize_excel_cell(value: Any) -> str:
    """
    Защита CSV от формульной инъекции при открытии в Excel/LibreOffice.

    Табуляция в начале сохраняет значение текстом и не позволяет
    Excel выполнить значение как формулу.
    """
    if value is None:
        return ""

    text = str(value)
    text = text.replace("\r", " ").replace("\n", " ")

    if text.startswith(("=", "+", "-", "@")):
        return "\t" + text

    return text


def normalize_phone_for_export(value: Any) -> str:
    """
    Телефон выгружается как обычный текст.
    Формулы вида ="+7999..." намеренно не используются.
    """
    if value is None:
        return ""

    phone = str(value).strip()
    return sanitize_excel_cell(phone)


def validate_db_path(path_value: str) -> Path:
    """
    Разрешаем только локальный файл SQLite.
    Скрипт предназначен для ручного локального запуска,
    а не для приёма путей из HTTP-запросов.
    """
    path = Path(path_value).expanduser().resolve()

    if path.suffix.lower() not in {".db", ".sqlite", ".sqlite3"}:
        raise ValueError(
            "Файл базы должен иметь расширение .db, .sqlite или .sqlite3"
        )

    return path


def get_table_columns(conn: sqlite3.Connection) -> set[str]:
    rows = conn.execute(
        "PRAGMA table_info(leads)"
    ).fetchall()

    return {str(row[1]) for row in rows}


def main() -> int:
    raw_db_path = sys.argv[1] if len(sys.argv) > 1 else str(DEFAULT_DB)

    try:
        db_path = validate_db_path(raw_db_path)
    except ValueError as exc:
        print(f"❌ Некорректный путь к базе: {exc}")
        return 1

    if not db_path.is_file():
        print(
            f"❌ Файл базы '{db_path}' пока не создан "
            "(нет заявок)."
        )
        return 1

    output_path = db_path.with_suffix(".csv")

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
    ]

    try:
        with sqlite3.connect(
            str(db_path),
            timeout=10.0,
        ) as conn:
            conn.execute("PRAGMA busy_timeout=10000")

            table_names = {
                str(row[0])
                for row in conn.execute(
                    """
                    SELECT name
                    FROM sqlite_master
                    WHERE type = 'table'
                    """
                ).fetchall()
            }

            if "leads" not in table_names:
                print("❌ В базе отсутствует таблица leads.")
                return 1

            columns = get_table_columns(conn)

            def optional_column(
                column_name: str,
                fallback_sql: str,
            ) -> str:
                if column_name in columns:
                    return f'"{column_name}"'
                return fallback_sql

            notification_status = optional_column(
                "notification_status",
                "'неизвестно'",
            )

            consent_accepted_at = optional_column(
                "consent_accepted_at",
                "''",
            )

            consent_policy_version = optional_column(
                "consent_policy_version",
                "''",
            )

            consent_accepted = (
                f"""
                CASE
                    WHEN {consent_accepted_at} IS NOT NULL
                     AND {consent_accepted_at} != ''
                    THEN 'да'
                    ELSE 'нет'
                END
                """
            )

            query = f"""
                SELECT
                    id,
                    ref_tag,
                    address,
                    service,
                    name,
                    phone,
                    call_time,
                    created_at,
                    status,
                    {notification_status},
                    {consent_accepted},
                    {consent_policy_version},
                    {consent_accepted_at}
                FROM leads
                ORDER BY id ASC
            """

            rows = conn.execute(query).fetchall()

        with output_path.open(
            "w",
            newline="",
            encoding="utf-8-sig",
        ) as output_file:
            writer = csv.writer(
                output_file,
                delimiter=";",
                quoting=csv.QUOTE_MINIMAL,
            )

            writer.writerow(headers)

            for row in rows:
                item = list(row)

                # Все поля проходят через защиту от Excel-инъекций.
                item = [
                    sanitize_excel_cell(value)
                    for value in item
                ]

                # Телефон также оставляем текстом.
                item[5] = normalize_phone_for_export(row[5])

                writer.writerow(item)

        print(
            f"✅ Выгружено заявок: {len(rows)}\n"
            f"📄 Файл: {output_path}"
        )
        return 0

    except sqlite3.Error as exc:
        print(f"❌ Ошибка SQLite: {exc}")
        return 1
    except OSError as exc:
        print(f"❌ Ошибка записи CSV: {exc}")
        return 1
    except Exception as exc:
        print(f"❌ Ошибка экспорта: {exc}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
