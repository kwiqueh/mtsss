from __future__ import annotations

import asyncio
from datetime import datetime, timedelta
import html
import logging
import os
import sqlite3
import uuid
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
from aiogram.filters import Command, CommandObject, CommandStart, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
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

from database import (
    parse_ref_tag as parse_ref_tag,
    SQLiteStorage as SQLiteStorage,
    now_moscow as now_moscow,
    init_db as init_db,
    normalize_phone as normalize_phone,
    update_funnel_session_sync as update_funnel_session_sync,
    update_funnel_session as update_funnel_session,
    reset_funnel_session_sync as reset_funnel_session_sync,
    reset_funnel_session as reset_funnel_session,
    cancel_funnel_session_sync as cancel_funnel_session_sync,
    cancel_funnel_session as cancel_funnel_session,
    record_event_sync as record_event_sync,
    record_event as record_event,
    save_lead_sync as save_lead_sync,
    save_lead as save_lead,
    claim_lead_sync as claim_lead_sync,
    claim_lead as claim_lead,
    mark_delivered_sync as mark_delivered_sync,
    mark_delivered as mark_delivered,
    mark_delivery_error_sync as mark_delivery_error_sync,
    mark_delivery_error as mark_delivery_error,
    get_delivery_queue_sync as get_delivery_queue_sync,
    get_delivery_queue as get_delivery_queue,
    recover_stale_deliveries_sync as recover_stale_deliveries_sync,
    recover_stale_deliveries as recover_stale_deliveries,
    record_lead_activity_sync as record_lead_activity_sync,
    record_lead_activity as record_lead_activity,
    assign_lead_to_manager_sync as assign_lead_to_manager_sync,
    assign_lead_to_manager as assign_lead_to_manager,
    update_lead_note_sync as update_lead_note_sync,
    update_lead_note as update_lead_note,
    update_lead_status_sync as update_lead_status_sync,
    update_lead_status as update_lead_status,
    get_lead_by_id_sync as get_lead_by_id_sync,
    generate_export_csv_sync as generate_export_csv_sync,
    fetch_stats_sync as fetch_stats_sync,
    get_abandoned_sessions_sync as get_abandoned_sessions_sync,
    get_abandoned_sessions as get_abandoned_sessions,
    mark_followup_sent_sync as mark_followup_sent_sync,
    mark_followup_sent as mark_followup_sent,
    mark_funnel_blocked_sync as mark_funnel_blocked_sync,
    mark_funnel_blocked as mark_funnel_blocked,
    list_leads_sync as list_leads_sync,
    retry_delivery_sync as retry_delivery_sync,
    connect as connect,
    BUSINESS_STATUSES as BUSINESS_STATUSES,
    claim_followup_sync as claim_followup_sync,
    remember_attribution_sync as remember_attribution_sync,
    fetch_analytics_sync as fetch_analytics_sync,
    retention_cleanup_sync as retention_cleanup_sync,
    request_erasure_sync as request_erasure_sync,
    backup_database as backup_database,
)
from aiogram.fsm.storage.memory import SimpleEventIsolation
from exports import sanitize_excel_cell as sanitize_excel_cell
from tariffs import (
    render_tariff,
    tariff_available,
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


TIME_SLOTS = {
    "time_morning": "Утро (09:00–12:00)",
    "time_day": "День (12:00–17:00)",
    "time_evening": "Вечер (17:00–21:00)",
    "time_any": "В любое время",
}


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

    safe_agent_name = html.escape(str(agent_name or "Прямой переход"))
    safe_house_tag = html.escape(str(house_tag or ""))
    safe_ref_tag = html.escape(str(ref_tag or "direct"))
    safe_address = html.escape(str(address or ""))
    safe_service = html.escape(str(service or ""))
    safe_name = html.escape(str(name or ""))
    safe_phone = html.escape(str(phone or ""))
    safe_call_time = html.escape(str(call_time or ""))
    safe_username = html.escape(str(username or "отсутствует"))
    safe_created_at = html.escape(str(created_at or ""))
    safe_status = html.escape(str(status or ""))
    safe_action_type = html.escape(str(action_type or "Подключение"))

    house_info = f" (дом: {safe_house_tag})" if safe_house_tag else ""

    claimed_text = html.escape(str(claimed_by)) if claimed_by else "<i>не назначен</i>"

    note_text = (
        f"\n📝 <b>Заметка:</b> <i>{html.escape(str(manager_note))}</i>"
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
        f"🏷 <b>Источник / кампания:</b> "
        f"<code>{safe_ref_tag}</code>\n"
        f"🏠 <b>Адрес:</b> {safe_address}\n"
        f"📦 <b>Тариф:</b> {safe_service}\n"
        f"👤 <b>Имя:</b> {safe_name}\n"
        f"📞 <b>Телефон:</b> "
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
                    text="📞 Связались с клиентом",
                    callback_data=(f"lead_status:{lead_id}:связались"),
                ),
                InlineKeyboardButton(
                    text="📅 Назначено",
                    callback_data=(f"lead_status:{lead_id}:назначено"),
                ),
            ],
            [
                InlineKeyboardButton(
                    text="✅ Подключён",
                    callback_data=(f"lead_status:{lead_id}:подключено"),
                ),
                InlineKeyboardButton(
                    text="❌ Отказ",
                    callback_data=(f"lead_status:{lead_id}:отказ"),
                ),
            ],
            [
                InlineKeyboardButton(
                    text="☎️ Не удалось связаться",
                    callback_data=f"lead_status:{lead_id}:не дозвонились",
                )
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
            ],
            [
                InlineKeyboardButton(
                    text="📞 Получить консультацию", callback_data="consultation"
                )
            ],
            [InlineKeyboardButton(text="🔐 О данных", callback_data="privacy_info")],
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
            ],
            [
                KeyboardButton(text="/back"),
                KeyboardButton(text="/menu"),
                KeyboardButton(text="/cancel"),
            ],
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
    submission_id: str = "",
) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="✅ Отправить заявку",
                    callback_data=f"consent_accept:{submission_id}",
                )
            ],
            [
                InlineKeyboardButton(
                    text="✏️ Изменить данные",
                    callback_data="consent_edit",
                )
            ],
            [
                InlineKeyboardButton(text="⬅️ Назад", callback_data="form_back"),
                InlineKeyboardButton(text="🏠 Главное меню", callback_data="main_menu"),
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
                InlineKeyboardButton(text="⬅️ Назад", callback_data="form_back"),
                InlineKeyboardButton(text="🏠 Главное меню", callback_data="main_menu"),
            ],
            [
                InlineKeyboardButton(
                    text="❌ Отменить",
                    callback_data="cancel_order",
                )
            ],
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
        f"Оператор: <i>{html.escape(personal_data_operator)}</i>\n"
        "Отменить оформление: /cancel. Запросить удаление данных: /delete_me."
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


class CustomerContextMiddleware(BaseMiddleware):
    async def __call__(self, handler, event, data):
        message = event.message if isinstance(event, CallbackQuery) else event
        user = event.from_user
        if user is None or not isinstance(message, Message):
            if isinstance(event, CallbackQuery):
                await event.answer("Сообщение недоступно", show_alert=True)
            return
        is_customer = message.chat.type == "private"
        if not is_customer:
            text = message.text or ""
            is_staff_action = (
                (
                    isinstance(event, CallbackQuery)
                    and (event.data or "").startswith(("lead_", "noop"))
                )
                or text.startswith(
                    ("/stats", "/export", "/cancel", "/lead", "/delivery")
                )
                or await data["state"].get_state()
                == ManagerNoteFSM.waiting_for_note_text.state
            )
            if not is_staff_action:
                await message.answer("Оформить заявку можно в личном чате с ботом.")
                if isinstance(event, CallbackQuery):
                    await event.answer()
                return
        result = await handler(event, data)
        if is_customer:
            state = data.get("state")
            current = await state.get_state() if state else None
            if current and current.startswith("OrderFSM:"):
                values = await state.get_data()
                await update_funnel_session(
                    data["db_path"], user.id, values.get("ref_tag", "direct"), current
                )
        return result


router.message.outer_middleware(CustomerContextMiddleware())
router.callback_query.outer_middleware(CustomerContextMiddleware())


@router.message(Command("menu"))
async def cmd_menu(message: Message, state: FSMContext, db_path: str) -> None:
    await state.clear()
    await cancel_funnel_session(db_path, message.from_user.id)
    await message.answer("Главное меню", reply_markup=ReplyKeyboardRemove())
    await message.answer("Выберите действие:", reply_markup=get_start_kb())


@router.callback_query(F.data == "main_menu")
async def callback_menu(
    callback: CallbackQuery, state: FSMContext, db_path: str
) -> None:
    await callback.answer()
    if callback.message:
        await state.clear()
        await cancel_funnel_session(db_path, callback.from_user.id)
        await callback.message.answer(
            "Главное меню", reply_markup=ReplyKeyboardRemove()
        )
        await callback.message.answer("Выберите действие:", reply_markup=get_start_kb())


@router.callback_query(F.data == "privacy_info")
async def privacy_info(
    callback: CallbackQuery,
    privacy_policy_url: str,
    personal_data_operator: str,
    retention_days: int,
) -> None:
    await callback.answer()
    if callback.message:
        await callback.message.answer(
            f"Контакты нужны для ответа на заявку. Оператор данных: {html.escape(personal_data_operator)}.\n"
            f"Срок хранения по настройке: {retention_days} дней; обезличивание выполняет оператор. "
            "Для запроса удаления: /delete_me. Это прекращает напоминания. "
            "Копии в CRM-чате и ранее скачанные файлы оператор удаляет отдельно.\n"
            f'<a href="{html.escape(privacy_policy_url, quote=True)}">Политика обработки данных</a>',
            parse_mode="HTML",
        )


@router.message(Command("delete_me"))
async def delete_me(message: Message, state: FSMContext, db_path: str) -> None:
    await state.clear()
    await asyncio.to_thread(request_erasure_sync, db_path, message.from_user.id)
    await message.answer(
        "Запрос на удаление данных зарегистрирован, напоминания прекращены. "
        "Оператор должен обработать запрос, включая копии в CRM и выгрузках.",
        reply_markup=ReplyKeyboardRemove(),
    )


@router.callback_query(F.data == "consultation")
async def consultation(
    callback: CallbackQuery,
    state: FSMContext,
    db_path: str,
    personal_data_operator: str,
    privacy_policy_url: str,
) -> None:
    await callback.answer()
    if callback.message is None:
        return
    tag, code, name, campaign = await asyncio.to_thread(
        remember_attribution_sync, db_path, callback.from_user.id, None
    )
    await state.clear()
    await state.update_data(
        ref_tag=tag,
        agent_code=code,
        agent_name=name,
        house_tag=campaign,
        service="Консультация",
        service_key="consultation",
        selected_category="consultation",
        address="",
        action_type="Заказ звонка",
        submission_id=uuid.uuid4().hex,
    )
    await state.set_state(OrderFSM.waiting_for_name)
    await reset_funnel_session(
        db_path, callback.from_user.id, tag, OrderFSM.waiting_for_name.state
    )
    await callback.message.answer(
        "Для консультации достаточно имени, телефона и времени звонка. "
        f"Оператор данных: {html.escape(personal_data_operator)}. "
        f'<a href="{html.escape(privacy_policy_url, quote=True)}">Политика обработки данных</a>\n\n'
        "Как к вам обращаться?",
        parse_mode="HTML",
        reply_markup=cancel_kb(),
    )


@router.callback_query(
    StateFilter(
        OrderFSM.waiting_for_name,
        OrderFSM.waiting_for_phone,
        OrderFSM.waiting_for_call_time,
        OrderFSM.waiting_for_consent,
    ),
    F.data == "form_back",
)
async def form_back(callback: CallbackQuery, state: FSMContext) -> None:
    await callback.answer()
    if callback.message is None:
        return
    current = await state.get_state()
    if current == OrderFSM.waiting_for_name.state:
        await state.set_state(OrderFSM.choosing_category)
        await callback.message.answer(
            "Выберите категорию:", reply_markup=get_categories_kb()
        )
    elif current == OrderFSM.waiting_for_phone.state:
        await state.set_state(OrderFSM.waiting_for_name)
        await callback.message.answer("Как к вам обращаться?", reply_markup=cancel_kb())
    elif current == OrderFSM.waiting_for_call_time.state:
        await state.set_state(OrderFSM.waiting_for_phone)
        await callback.message.answer(
            "Введите ваш номер телефона:", reply_markup=get_phone_kb()
        )
    else:
        await state.set_state(OrderFSM.waiting_for_call_time)
        await callback.message.answer(
            "Выберите время звонка:", reply_markup=get_time_kb()
        )


@router.message(Command("leads", "delivery"))
async def cmd_leads(
    message: Message,
    command: CommandObject,
    admin_ids: list[int],
    manager_ids: list[int],
    crm_chat_id: int,
    db_path: str,
) -> None:
    if not message.from_user or message.from_user.id not in set(admin_ids) | set(
        manager_ids
    ):
        return
    if message.chat.id != crm_chat_id and not (
        message.chat.type == "private" and message.from_user.id in admin_ids
    ):
        return
    rows = await asyncio.to_thread(
        list_leads_sync, db_path, delivery_only=command.command == "delivery"
    )
    text = (
        "\n".join(f"№{row[0]} | {row[1]} | CRM: {row[2]} | {row[3]}" for row in rows)
        or "Заявок нет."
    )
    await message.answer(
        text + "\n\nКарточка: /lead ID (в CRM-чате). Проблемы доставки: /delivery."
    )


@router.message(Command("lead"))
async def cmd_lead(
    message: Message,
    command: CommandObject,
    admin_ids: list[int],
    manager_ids: list[int],
    crm_chat_id: int,
    db_path: str,
) -> None:
    if (
        not message.from_user
        or message.from_user.id not in set(admin_ids) | set(manager_ids)
        or message.chat.id != crm_chat_id
    ):
        return
    if not command.args or not command.args.isdigit():
        await message.answer("Формат: /lead ID")
        return
    row = await asyncio.to_thread(get_lead_by_id_sync, db_path, int(command.args))
    if not row:
        await message.answer("Заявка не найдена.")
        return
    await message.answer(
        build_agent_text(row, delivery_prefix="📋 <b>КАРТОЧКА ЗАЯВКИ"),
        parse_mode="HTML",
        reply_markup=lead_actions_kb(row[0], row[14]),
        protect_content=True,
    )


@router.message(Command("retry_delivery"))
async def cmd_retry_delivery(
    message: Message, command: CommandObject, admin_ids: list[int], db_path: str
) -> None:
    if (
        not message.from_user
        or message.from_user.id not in admin_ids
        or message.chat.type != "private"
    ):
        return
    parts = (command.args or "").split()
    if len(parts) != 2 or not parts[0].isdigit() or parts[1] != "CONFIRM":
        await message.answer(
            "Сначала проверьте CRM: уведомление могло быть доставлено. После проверки для повторной отправки: /retry_delivery ID CONFIRM. Возможен дубль карточки."
        )
        return
    updated = await asyncio.to_thread(retry_delivery_sync, db_path, int(parts[0]))
    await message.answer(
        "Доставка поставлена в очередь."
        if updated
        else "Нет заявки с подходящим статусом доставки."
    )


@router.message(Command("export"))
async def cmd_export(
    message: Message,
    admin_ids: list[int],
    db_path: str,
) -> None:
    if not message.from_user or message.from_user.id not in admin_ids:
        return
    if message.chat.type != "private":
        await message.answer(
            "Для выгрузки откройте личный чат с ботом и отправьте /export."
        )
        return

    temporary_path: str | None = None

    try:
        temporary_path, count = await asyncio.to_thread(
            generate_export_csv_sync,
            db_path,
        )

        document = FSInputFile(
            temporary_path,
            filename=(f"leads_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"),
        )

        await message.reply_document(
            document,
            caption=f"📊 Экспорт заявок: {count} шт. Удалите файл после использования.",
            protect_content=True,
        )
    except Exception:
        LOGGER.exception("Ошибка формирования экспорта")

        await message.answer("⚠️ Не удалось сформировать экспорт.")
    finally:
        if temporary_path:
            try:
                os.remove(temporary_path)
            except OSError:
                pass


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
        f"• <b>{html.escape(str(agent_name))}</b>: {count}"
        for agent_name, count in agents[:10]
    )

    statuses_text = "\n".join(
        f"• <b>{html.escape(str(status))}</b>: {count}"
        for status, count in statuses[:15]
    )

    houses_text = "\n".join(
        f"• <code>{html.escape(str(house))}</code>: {count}"
        for house, count in houses[:10]
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

    analytics = await asyncio.to_thread(fetch_analytics_sync, db_path)
    sections = [
        f"Подключено: {analytics['connected']}; конверсия заявка → подключение: {analytics['conversion']:.1f}%"
    ]
    for title, key in [
        ("Категории", "categories"),
        ("Источники", "sources"),
        ("Кампании", "campaigns"),
        ("Даты", "dates"),
        ("Доставка CRM", "delivery"),
    ]:
        sections.append(
            title
            + ":\n"
            + "\n".join(
                f"• {str(label or 'без метки')[:80]}: {count}"
                for label, count in analytics[key]
            )
        )
    await message.answer("\n\n".join(sections))


@router.message(Command("back"))
async def cmd_back(message: Message, state: FSMContext) -> None:
    current = await state.get_state()
    if current == OrderFSM.waiting_for_phone.state:
        await state.set_state(OrderFSM.waiting_for_name)
        await message.answer(
            "Как к вам обращаться?", reply_markup=ReplyKeyboardRemove()
        )
        await message.answer("Введите имя:", reply_markup=cancel_kb())
    elif current in {
        OrderFSM.waiting_for_call_time.state,
        OrderFSM.waiting_for_consent.state,
    }:
        await state.set_state(OrderFSM.waiting_for_phone)
        await message.answer("Введите ваш номер телефона:", reply_markup=get_phone_kb())
    else:
        await message.answer("Для возврата к выбору услуги используйте /menu.")


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
        "❌ Оформление отменено.\n\nЧтобы начать заново, отправьте /start.",
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
            "❌ Оформление отменено.\n\nЧтобы начать заново, отправьте /start.",
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
    personal_data_operator: str,
    privacy_policy_url: str,
) -> None:
    user_id = message.from_user.id

    ref_tag, agent_code, agent_name, house_tag = await asyncio.to_thread(
        remember_attribution_sync, db_path, user_id, command.args
    )

    await state.clear()

    await state.update_data(
        ref_tag=ref_tag,
        agent_code=agent_code,
        agent_name=agent_name,
        house_tag=house_tag,
        started_at=now_moscow(),
        submission_id=uuid.uuid4().hex,
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

    if agent_code in {"alex", "alexander", "sasha", "sergey", "serg"}:
        welcome_agent = (
            f"\nВаш персональный консультант: <b>{html.escape(agent_name)}</b>."
        )

    await message.answer(
        f"👋 <b>Здравствуйте!</b>{welcome_agent}\n\n"
        "Это сервис «МТС Экспресс» для заявок на услуги связи.\n\n"
        "Подберём выгодный тариф и передадим заявку специалисту "
        "после уточнения региона и условий. Доступность подключения проверяет специалист.\n\n"
        f"Контакты для ответа обрабатывает {html.escape(personal_data_operator)}. "
        f'<a href="{html.escape(privacy_policy_url, quote=True)}">Политика обработки данных</a>. '
        "Запросить удаление: /delete_me.",
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
    await callback.answer()

    if not current_state:
        await callback.message.answer(
            "Активной заявки нет. Выберите действие в главном меню.",
            reply_markup=get_start_kb(),
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
            f"🏠 Адрес: <b>{address}</b>\n\n📋 <b>ВЫБЕРИТЕ КАТЕГОРИЮ УСЛУГ:</b>",
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

        if tariff and tariff_available(service_key):
            await callback.message.answer(
                render_tariff(service_key),
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
            reply_markup=get_consent_kb(
                privacy_policy_url, data.get("submission_id", "")
            ),
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

    await state.clear()
    tag, code, name, house = await asyncio.to_thread(
        remember_attribution_sync, db_path, user_id, ref_tag
    )
    await state.update_data(
        ref_tag=tag,
        agent_code=code,
        agent_name=name,
        house_tag=house,
        submission_id=uuid.uuid4().hex,
    )
    await reset_funnel_session(
        db_path, user_id, tag, OrderFSM.waiting_for_address.state
    )
    await state.update_data(address="")
    await state.set_state(OrderFSM.choosing_category)
    await callback.message.answer(
        "Выберите категорию услуг. Цены и доступность зависят от региона и требуют подтверждения специалистом.",
        reply_markup=get_categories_kb(),
    )


@router.message(OrderFSM.waiting_for_address, F.text)
async def process_address(
    message: Message,
    state: FSMContext,
    db_path: str,
) -> None:
    address = " ".join(message.text.split()).strip()

    if address.startswith("/") or len(address) < 4:
        await message.answer(
            "⚠️ Адрес слишком короткий.\nУкажите город, улицу и номер дома."
        )
        return

    if len(address) > 300:
        await message.answer("⚠️ Слишком длинный адрес. Максимум 300 символов.")
        return

    await state.update_data(address=address)
    data = await state.get_data()
    ref_tag = data.get("ref_tag", "direct")
    user_id = message.from_user.id

    if data.get("service_key"):
        await state.set_state(OrderFSM.waiting_for_name)
        await message.answer(
            "Адрес принят для проверки специалистом. Как к вам обращаться?",
            reply_markup=cancel_kb(),
        )
        return
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
    await message.answer("⚠️ Пожалуйста, отправьте адрес обычным текстовым сообщением.")


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


@router.callback_query(
    F.data == "action_back_categories",
    StateFilter(OrderFSM.waiting_for_service, OrderFSM.viewing_tariff),
)
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

    if tariff is None or not tariff_available(service_key):
        await callback.answer(
            "Услуга не найдена.",
            show_alert=True,
        )
        return

    values = await state.get_data()
    if tariff["category"] != values.get("selected_category"):
        await callback.answer(
            "Кнопка из другой категории устарела. Выберите тариф заново.",
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
        render_tariff(service_key),
        reply_markup=get_tariff_actions_kb(),
        parse_mode="HTML",
    )


@router.callback_query(
    F.data == "action_back_tariffs",
    StateFilter(OrderFSM.waiting_for_service, OrderFSM.viewing_tariff),
)
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

    action_type = "Подключение" if callback.data == "action_order" else "Заказ звонка"

    await callback.answer()
    await state.update_data(action_type=action_type)
    data = await state.get_data()
    if (
        data.get("selected_category") in {"combo", "satellite_tv"}
        or data.get("service_key") == "b2b_office_net"
    ):
        await state.set_state(OrderFSM.waiting_for_address)
        await callback.message.answer(
            "Для проверки возможности подключения напишите город, улицу и номер дома. Номер квартиры не нужен. Доступность подтвердит специалист.",
            reply_markup=cancel_kb(),
        )
        return
    await state.update_data(address="")
    await state.set_state(OrderFSM.waiting_for_name)

    await callback.message.answer(
        "👤 <b>Шаг 3 из 6. Как к вам обращаться?</b>\n\nНапишите имя.",
        reply_markup=cancel_kb(),
        parse_mode="HTML",
    )


@router.message(OrderFSM.waiting_for_name, F.text)
async def process_name(
    message: Message,
    state: FSMContext,
) -> None:
    name = " ".join(message.text.split()).strip()

    if name.startswith("/") or len(name) < 2:
        await message.answer("⚠️ Имя слишком короткое.\nНапишите, как к вам обращаться.")
        return

    if len(name) > 100:
        await message.answer("⚠️ Имя слишком длинное. Максимум 100 символов.")
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
    await message.answer("⚠️ Пожалуйста, напишите имя текстом.")


@router.message(OrderFSM.waiting_for_phone)
async def process_phone(
    message: Message,
    state: FSMContext,
) -> None:
    phone: str | None = None

    if message.contact:
        contact_user_id = message.contact.user_id

        if contact_user_id is not None and contact_user_id != message.from_user.id:
            await message.answer(
                "⚠️ Пожалуйста, отправьте свой собственный номер "
                "или введите его вручную."
            )
            return

        phone = normalize_phone(message.contact.phone_number)

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
        "✅ Номер принят.\n\n⏰ <b>Шаг 5 из 6. Когда удобно принять звонок?</b>",
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
        reply_markup=get_consent_kb(privacy_policy_url, data.get("submission_id", "")),
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
    data = await state.get_data()
    if data.get("address"):
        await state.set_state(OrderFSM.waiting_for_address)
        await callback.message.answer(
            "Введите исправленный город, улицу и дом:", reply_markup=cancel_kb()
        )
    else:
        await state.set_state(OrderFSM.waiting_for_name)
        await callback.message.answer(
            "Введите ваше имя заново:", reply_markup=cancel_kb()
        )


@router.callback_query(
    OrderFSM.waiting_for_consent,
    F.data.startswith("consent_accept:"),
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

    await callback.answer()
    data = await state.get_data()

    if (
        callback.data != "consent_accept:" + data.get("submission_id", "")
        or await state.get_state() != OrderFSM.waiting_for_consent.state
    ):
        await callback.message.answer(
            "Эта кнопка устарела. Используйте текущую форму или /menu."
        )
        return
    service_key = data.get("service_key")
    if service_key != "consultation" and not tariff_available(service_key):
        await state.set_state(OrderFSM.choosing_category)
        await callback.message.answer(
            "Предложение больше недоступно. Выберите актуальную услугу.",
            reply_markup=get_categories_kb(),
        )
        return
    required_fields = (
        "service",
        "name",
        "phone",
        "call_time",
    )

    if not all(data.get(field) for field in required_fields):
        await state.clear()

        await callback.message.answer(
            "⚠️ Сессия заявки устарела.\nНачните заново: /start"
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
    except (sqlite3.Error, ValueError):
        LOGGER.exception("Ошибка сохранения заявки")

        await callback.message.answer(
            "⚠️ Не удалось сохранить заявку из-за ошибки базы данных.\n"
            "Попробуйте ещё раз через минуту."
        )
        return

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
            "Если данные не изменились, повторная отправка не создаёт дубль."
        )

    await callback.message.answer(
        f"🎉 <b>Заявка №{lead_id} принята!</b>\n\n"
        f"Специалист <b>{html.escape(str(consultant_name))}</b> "
        "получит заявку и уточнит условия подключения. Удобное время звонка:\n"
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
        lead_id = int(callback.data.split(":", 1)[1])
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

    await callback.answer("✅ Вы взяли заявку в работу.")

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
            type(exc).__name__,
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

    await callback.answer(f"Статус изменён: {new_status}")

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
            type(exc).__name__,
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
        lead_id = int(callback.data.split(":", 1)[1])
    except (ValueError, AttributeError):
        await callback.answer(
            "Некорректный идентификатор заявки.",
            show_alert=True,
        )
        return

    await state.set_state(ManagerNoteFSM.waiting_for_note_text)

    await state.update_data(
        note_lead_id=lead_id,
        crm_chat_id=callback.message.chat.id,
        crm_msg_id=callback.message.message_id,
    )

    await callback.answer()

    await callback.message.reply(
        f"📝 Напишите заметку для заявки №{lead_id}.\nДля отмены отправьте /cancel.",
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
        await message.answer("⛔ Заметки можно добавлять только из CRM-чата.")
        return

    data = await state.get_data()
    raw_lead_id = data.get("note_lead_id")
    target_chat_id = data.get("crm_chat_id")
    target_message_id = data.get("crm_msg_id")

    try:
        lead_id = int(raw_lead_id)
    except (TypeError, ValueError):
        await state.clear()
        await message.answer("⚠️ Сессия заметки устарела.")
        return

    note = " ".join(message.text.split()).strip()

    if not note:
        await message.answer("⚠️ Заметка не может быть пустой.")
        return

    if len(note) > 500:
        await message.answer("⚠️ Заметка слишком длинная. Максимум 500 символов.")
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
        await message.answer("⚠️ Заявка не найдена.")
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

    await message.reply(f"✅ Заметка к заявке №{lead_id} сохранена.")

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
            type(exc).__name__,
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
                        send_kwargs["message_thread_id"] = crm_thread_id

                    sent_message = await bot.send_message(**send_kwargs)

                except TelegramRetryAfter as exc:
                    await mark_delivery_error(
                        db_path=db_path,
                        lead_id=lead_id,
                        error_text=(f"RetryAfter: {exc.retry_after}"),
                    )

                    await asyncio.sleep(float(exc.retry_after))

                except TelegramForbiddenError:
                    await mark_delivery_error(
                        db_path=db_path,
                        lead_id=lead_id,
                        error_text="TelegramForbiddenError",
                    )

                    LOGGER.error(
                        "Нет доступа к CRM-чату для заявки #%s",
                        lead_id,
                    )

                except TelegramNetworkError as exc:
                    await mark_delivery_error(
                        db_path=db_path,
                        lead_id=lead_id,
                        error_text=type(exc).__name__,
                    )

                    LOGGER.warning(
                        "Сетевая ошибка доставки заявки #%s: %s",
                        lead_id,
                        type(exc).__name__,
                    )

                except TelegramBadRequest as exc:
                    await mark_delivery_error(
                        db_path=db_path,
                        lead_id=lead_id,
                        error_text=type(exc).__name__,
                    )

                    LOGGER.error(
                        "Ошибка Telegram API для заявки #%s: %s",
                        lead_id,
                        type(exc).__name__,
                    )

                except Exception:
                    await mark_delivery_error(
                        db_path=db_path,
                        lead_id=lead_id,
                        error_text="UnknownDeliveryError",
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
            LOGGER.exception("Критическая ошибка delivery worker")
            await asyncio.sleep(10)

    LOGGER.info("Delivery worker остановлен")


async def followup_worker(
    bot: Bot,
    db_path: str,
    followup_minutes: int,
    stop_event: asyncio.Event,
) -> None:
    if followup_minutes == 0:
        return
    LOGGER.info("Follow-up worker запущен")

    while not stop_event.is_set():
        try:
            threshold = (
                datetime.now(MOSCOW_TZ) - timedelta(minutes=followup_minutes)
            ).strftime("%Y-%m-%d %H:%M:%S")

            abandoned = await get_abandoned_sessions(
                db_path=db_path,
                threshold=threshold,
                limit=20,
            )

            for user_id, ref_tag, activity in abandoned:
                if stop_event.is_set():
                    break

                if not await asyncio.to_thread(
                    claim_followup_sync, db_path, user_id, threshold, activity
                ):
                    continue
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
                            "TelegramBadRequest follow-up для %s: %s",
                            user_id,
                            type(exc).__name__,
                        )

                except TelegramRetryAfter as exc:
                    LOGGER.warning(
                        "RetryAfter follow-up для %s: %s",
                        user_id,
                        exc.retry_after,
                    )
                    await asyncio.sleep(float(exc.retry_after))

                except TelegramNetworkError as exc:
                    LOGGER.warning(
                        "Сетевая ошибка follow-up для %s: %s",
                        user_id,
                        type(exc).__name__,
                    )

                except Exception as exc:
                    LOGGER.exception(
                        "Ошибка follow-up для пользователя %s: %s",
                        user_id,
                        type(exc).__name__,
                    )

            await asyncio.sleep(60)

        except asyncio.CancelledError:
            break

        except Exception:
            LOGGER.exception("Критическая ошибка follow-up worker")
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
    retention_days: int = 180,
    **kwargs: Any,
) -> None:
    if kwargs:
        raise ValueError(
            "Неизвестные настройки start_agent_bot: " + ", ".join(sorted(kwargs))
        )
    if admin_ids is None:
        admin_ids = [admin_id] if admin_id else []
    if not admin_ids or any(
        type(value) is not int or value <= 0 for value in admin_ids
    ):
        raise ValueError("Нужен явно заданный положительный ID администратора")
    admin_ids = list(dict.fromkeys(admin_ids))
    if manager_ids is None:
        manager_ids = []
    if any(type(value) is not int or value <= 0 for value in manager_ids):
        raise ValueError("Некорректный ID менеджера")
    manager_ids = list(dict.fromkeys(admin_ids + manager_ids))
    if not 0 <= followup_minutes <= 10080:
        raise ValueError("Некорректный интервал напоминания")
    if crm_chat_id is None:
        crm_chat_id = admin_ids[0]

    if not proxy:
        proxy = (
            os.getenv(
                "TELEGRAM_PROXY",
                "",
            ).strip()
            or None
        )

    await asyncio.to_thread(init_db, db_path)

    storage = SQLiteStorage(db_path)
    dispatcher = Dispatcher(storage=storage, events_isolation=SimpleEventIsolation())
    dispatcher.include_router(router)

    stop_event = asyncio.Event()

    session = AiohttpSession(proxy=proxy) if proxy else AiohttpSession()

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
            "Бот запущен; прокси включён: %s",
            bool(proxy),
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
                retention_days=retention_days,
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


@router.callback_query()
async def stale_callback(callback: CallbackQuery) -> None:
    await callback.answer(
        "Кнопка устарела или недоступна на этом шаге. Откройте /menu.", show_alert=True
    )


@router.message()
async def unknown_message(message: Message) -> None:
    await message.answer(
        "Используйте кнопки текущего шага. Главное меню: /menu; отмена: /cancel."
    )


@router.error()
async def handle_error(event) -> bool:
    LOGGER.error("Ошибка обработки обновления: %s", type(event.exception).__name__)
    try:
        if event.update.callback_query:
            await event.update.callback_query.answer(
                "Не удалось выполнить действие. Попробуйте ещё раз.", show_alert=True
            )
        elif event.update.message:
            await event.update.message.answer(
                "Не удалось выполнить действие. Данные сохранённой заявки остаются в базе. Попробуйте ещё раз или /menu."
            )
    except Exception:
        LOGGER.error("Не удалось отправить сообщение об ошибке")
    return True
