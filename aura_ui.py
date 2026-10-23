"""AURA CORE presentation primitives shared by the Telegram UI and tests."""

from __future__ import annotations

import html
import re
from typing import Any


DIVIDER = "━━━━━━━━━━━━━━"

SECTION_TO_CATEGORY = {
    "internet": "combo",
    "mobile": "mobile",
    "tv": "satellite_tv",
    "business": "business",
    "equipment": "devices",
}


def progress(stage: int, title: str) -> str:
    """Return a compact progress marker which works in Telegram light/dark themes."""
    if stage not in range(1, 5):
        raise ValueError("stage must be between 1 and 4")
    marks = "●" * stage + "○" * (4 - stage)
    return f"<code>{marks}  {stage:02d} / 04</code>  ·  {html.escape(title.upper())}"


def strip_leading_icon(value: str) -> str:
    """Remove legacy decorative emoji from a label without changing catalog data."""
    return re.sub(r"^[^\wА-Яа-яЁё]+\s*", "", value, count=1).strip()


def home_text(
    personal_data_operator: str,
    privacy_policy_url: str,
    consultant_name: str = "",
) -> str:
    consultant = (
        f"\nПерсональный консультант · <b>{html.escape(consultant_name)}</b>"
        if consultant_name
        else ""
    )
    return (
        "<b>МТС ЭКСПРЕСС</b>\n"
        f"{DIVIDER}\n\n"
        "<b>Связь. Без компромиссов.</b>\n"
        "Тарифы и услуги — спокойно, точно, по делу."
        f"{consultant}\n\n"
        "Выберите направление. Стоимость, регион и возможность подключения "
        "специалист подтвердит до оформления.\n\n"
        f"Данные для ответа обрабатывает {html.escape(personal_data_operator)}. "
        f'<a href="{html.escape(privacy_policy_url, quote=True)}">Условия обработки</a> · '
        "/delete_me"
    )


def category_text(category: dict[str, Any]) -> str:
    name = strip_leading_icon(str(category["name"]))
    description = html.escape(str(category.get("desc", "")))
    return (
        f"{progress(1, 'Выбор')}\n\n"
        f"<b>{html.escape(name)}</b>\n"
        f"<i>{description}</i>\n\n"
        "Выберите предложение. Условия и доступность будут подтверждены отдельно."
    )


def data_step(title: str, instruction: str) -> str:
    return f"{progress(2, 'Данные')}\n\n<b>{html.escape(title)}</b>\n{instruction}"
