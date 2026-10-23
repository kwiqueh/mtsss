"""Tariff keyboard and display logic. Offers are edited in tariff_data.json."""

from __future__ import annotations
from datetime import date
import html
import json
import re
from pathlib import Path
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup
from aura_ui import DIVIDER, progress, strip_leading_icon


def load_catalog(path: Path):
    data = json.loads(path.read_text(encoding="utf-8"))
    categories, catalog, tariffs = (
        data[name] for name in ("CATEGORIES_INFO", "CATALOG_STRUCTURE", "TARIFFS")
    )
    for key, tariff in tariffs.items():
        if not key.isascii() or len(("service:" + key).encode()) > 64:
            raise ValueError("Недопустимый ключ тарифа")
        if tariff.get("category") not in categories or not all(
            isinstance(tariff.get(name), str) and tariff[name].strip()
            for name in ("title", "description")
        ):
            raise ValueError("Некорректная карточка тарифа: " + key)
        if type(tariff.get("active", True)) is not bool:
            raise ValueError("active должен быть true/false: " + key)
        if tariff.get("verified_on"):
            date.fromisoformat(tariff["verified_on"])
        if len(tariff["title"]) > 100 or len(tariff["description"]) > 2800:
            raise ValueError("Слишком длинная карточка: " + key)
    for category, items in catalog.items():
        if category not in categories:
            raise ValueError("Неизвестная категория")
        for item in items:
            keys = (
                [v["key"] for v in item["variants"]]
                if item.get("has_variants")
                else [item["id"]]
            )
            if any(
                key not in tariffs or tariffs[key]["category"] != category
                for key in keys
            ):
                raise ValueError("Каталог ссылается на отсутствующий тариф")
    return categories, catalog, tariffs


CATEGORIES_INFO, CATALOG_STRUCTURE, TARIFFS = load_catalog(
    Path(__file__).with_name("tariff_data.json")
)


def tariff_available(key: str) -> bool:
    return key in TARIFFS and TARIFFS[key].get("active", True)


def render_tariff(key: str) -> str:
    tariff = TARIFFS[key]
    region = tariff.get("region") or "не указан, уточняется специалистом"
    verified = tariff.get("verified_on") or "не подтверждена"
    extra = "\n".join(
        html.escape(tariff[name])
        for name in ("price_note", "speed_or_allowance", "conditions")
        if tariff.get(name)
    )
    description = tariff["description"].replace("━━━━━━━━━━━━━━━━━━", "")
    description = re.sub(r"\n{3,}", "\n\n", description).strip()
    return (
        f"{progress(1, 'Выбор')}\n\n"
        f"<b>{html.escape(strip_leading_icon(tariff['title']))}</b>\n"
        f"{DIVIDER}\n\n"
        + description
        + ("\n" + extra if extra else "")
        + f"\n\n<blockquote>Регион · {html.escape(region)}\n"
        f"Проверено · {html.escape(verified)}</blockquote>\n"
        "Стоимость и условия требуют подтверждения. Доступность по адресу "
        "проверяет специалист."
    )


def get_categories_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="Интернет и ТВ", callback_data="cat:combo"
                )
            ],
            [
                InlineKeyboardButton(
                    text="Мобильная связь", callback_data="cat:mobile"
                )
            ],
            [
                InlineKeyboardButton(
                    text="Оборудование и устройства", callback_data="cat:devices"
                )
            ],
            [
                InlineKeyboardButton(
                    text="Телевидение", callback_data="cat:satellite_tv"
                )
            ],
            [
                InlineKeyboardButton(
                    text="Бизнес", callback_data="cat:business"
                )
            ],
            [InlineKeyboardButton(text="Главное меню", callback_data="main_menu")],
            [InlineKeyboardButton(text="Отменить", callback_data="cancel_order")],
        ]
    )


def get_tariffs_by_category_kb(category_key: str) -> InlineKeyboardMarkup:
    buttons = []
    items = CATALOG_STRUCTURE.get(category_key, [])
    for item in items:
        if item.get("has_variants"):
            if not any(tariff_available(v["key"]) for v in item["variants"]):
                continue
        elif not tariff_available(item["id"]):
            continue
        # Buttons use tariff titles, keeping prices in the offer description only.
        cb_data = (
            f"tariff_item:{item['id']}"
            if item.get("has_variants")
            else f"service:{item['id']}"
        )
        buttons.append(
            [
                InlineKeyboardButton(
                    text=strip_leading_icon(item["title"])
                    if item.get("has_variants")
                    else strip_leading_icon(TARIFFS[item["id"]]["title"]),
                    callback_data=cb_data,
                )
            ]
        )

    buttons.append(
        [
            InlineKeyboardButton(
                text="← Все направления", callback_data="action_back_categories"
            )
        ]
    )
    buttons.append(
        [InlineKeyboardButton(text="Главное меню", callback_data="main_menu")]
    )
    buttons.append(
        [InlineKeyboardButton(text="Отменить", callback_data="cancel_order")]
    )
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def get_variants_kb(tariff_item_id: str) -> tuple[str, InlineKeyboardMarkup] | None:
    for cat_items in CATALOG_STRUCTURE.values():
        for item in cat_items:
            if item["id"] == tariff_item_id and item.get("has_variants"):
                prompt = item.get("variant_prompt", "Выберите подходящий вариант:")
                buttons = []
                for v in item["variants"]:
                    if not tariff_available(v["key"]):
                        continue
                    buttons.append(
                        [
                            InlineKeyboardButton(
                                text=strip_leading_icon(TARIFFS[v["key"]]["title"]),
                                callback_data=f"service:{v['key']}",
                            )
                        ]
                    )
                buttons.append(
                    [
                        InlineKeyboardButton(
                            text="← Назад",
                            callback_data="action_back_tariffs",
                        ),
                        InlineKeyboardButton(
                            text="Отменить", callback_data="cancel_order"
                        ),
                    ]
                )
                return prompt, InlineKeyboardMarkup(inline_keyboard=buttons)
    return None


def get_tariff_actions_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="Выбрать предложение", callback_data="action_order"
                )
            ],
            [
                InlineKeyboardButton(
                    text="Обсудить со специалистом", callback_data="action_call"
                )
            ],
            [
                InlineKeyboardButton(
                    text="← Другой тариф", callback_data="action_back_tariffs"
                ),
                InlineKeyboardButton(text="Отменить", callback_data="cancel_order"),
            ],
        ]
    )
