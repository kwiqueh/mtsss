"""No real customers or Telegram traffic: temporary SQLite and fake Bot API."""

import asyncio
from concurrent.futures import ThreadPoolExecutor
import csv
from datetime import datetime, timedelta
import logging
import os
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import AsyncMock, patch

from aiogram import Bot, Dispatcher
from aiogram.client.session.base import BaseSession
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.base import StorageKey
from aiogram.fsm.storage.memory import SimpleEventIsolation
from aiogram.methods import SendMessage
from aiogram.exceptions import TelegramNetworkError, TelegramForbiddenError
from aiogram.types import Message, Update

import bot
import config
import database as db
from export_leads import export_csv
from exports import sanitize_excel_cell
from runtime import instance_lock
import tariffs
import aura_ui

# Deliberately synthetic format fixture, never sent to Telegram.
TEST_TOKEN = "123456789:" + "x" * 35


def lead(**extra):
    data = dict(
        submission_id="request-1",
        user_id=100,
        phone="+79990000000",
        name="Тест",
        address="Тестовый город, дом 1",
        service="Тестовая услуга",
        call_time="Утро",
        ref_tag="vk_campaign",
        selected_category="mobile",
        consent_accepted_at=db.now_moscow(),
        consent_policy_version="test",
    )
    data.update(extra)
    return data


class ConfigurationTests(unittest.TestCase):
    def base(self):
        return dict(
            TELEGRAM_BOT_TOKEN=TEST_TOKEN,
            TELEGRAM_ADMIN_ID="100",
            PRIVACY_POLICY_URL="https://example.org/privacy",
            PERSONAL_DATA_OPERATOR="Тестовый оператор",
        )

    def test_config_all_parameters(self):
        values = self.base() | dict(
            CRM_CHAT_ID="-100123",
            CRM_THREAD_ID="2",
            FOLLOWUP_MINUTES="7",
            ADMIN_IDS="101,100",
            MANAGER_IDS="102,101",
            DB_PATH="test.db",
        )
        with patch.dict(os.environ, values, clear=True):
            result = config.get_config()
        self.assertEqual(result.admin_ids, [100, 101])
        self.assertEqual(result.manager_ids, [100, 101, 102])
        self.assertEqual(result.bot_kwargs()["followup_minutes"], 7)
        self.assertEqual(result.bot_kwargs()["crm_chat_id"], -100123)
        self.assertEqual(result.bot_kwargs()["crm_thread_id"], 2)
        self.assertNotIn(TEST_TOKEN, repr(result))

    def test_invalid_config(self):
        for changes in [
            dict(TELEGRAM_BOT_TOKEN=""),
            dict(TELEGRAM_ADMIN_ID="0"),
            dict(CRM_CHAT_ID="text"),
            dict(CRM_THREAD_ID="-1"),
            dict(MANAGER_IDS="0"),
            dict(FOLLOWUP_MINUTES="-1"),
            dict(PRIVACY_POLICY_URL="http://example.org"),
            dict(PERSONAL_DATA_OPERATOR=""),
            dict(DB_PATH=""),
            dict(RETENTION_DAYS="0"),
        ]:
            with (
                self.subTest(changes=changes),
                patch.dict(os.environ, self.base() | changes, clear=True),
            ):
                with self.assertRaises(ValueError):
                    config.get_config()

    def test_file_precedence_quotes_bom(self):
        with (
            tempfile.TemporaryDirectory() as directory,
            patch.dict(os.environ, {"AGENT_NAME": "system"}, clear=True),
        ):
            path = Path(directory) / "bot.env"
            path.write_text(
                "\ufeffAGENT_NAME=\"file\"\nPOLICY_VERSION='v2'\n", encoding="utf-8"
            )
            config.load_env_file(path)
            self.assertEqual(os.environ["AGENT_NAME"], "system")
            self.assertEqual(os.environ["POLICY_VERSION"], "v2")
            path.write_text("bad key=x")
            with self.assertRaises(ValueError):
                config.load_env_file(path)

    def test_log_redaction(self):
        record = logging.LogRecord(
            "test",
            logging.ERROR,
            "",
            1,
            "token %s url %s",
            (TEST_TOKEN, "http://user:password@proxy.test:8080"),
            None,
        )
        config.SafeLogFilter().filter(record)
        self.assertNotIn(TEST_TOKEN, record.getMessage())
        self.assertNotIn("password", record.getMessage())
        try:
            raise ValueError("private person address")
        except ValueError:
            import sys

            record.exc_info = sys.exc_info()
        config.SafeLogFilter().filter(record)
        self.assertIsNone(record.exc_info)
        self.assertNotIn("private person", record.getMessage())

    def test_instance_lock(self):
        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / "leads.db")
            with instance_lock(path):
                with self.assertRaises(RuntimeError):
                    with instance_lock(path):
                        pass
            with instance_lock(path):
                pass


class DatabaseTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.path = str(Path(self.directory.name) / "leads.db")
        db.init_db(self.path)

    def count(self):
        with db.connect(self.path, readonly=True) as connection:
            return connection.execute("SELECT COUNT(*) FROM leads").fetchone()[0]

    def test_create_and_restart(self):
        first = db.save_lead_sync(self.path, lead())[0]
        db.init_db(self.path)
        self.assertEqual(db.get_lead_by_id_sync(self.path, first)[4], "Тест")
        with db.connect(self.path) as connection:
            self.assertEqual(
                connection.execute("PRAGMA integrity_check").fetchone()[0], "ok"
            )

    def test_idempotency_parallel(self):
        with ThreadPoolExecutor(max_workers=8) as pool:
            results = list(
                pool.map(lambda _: db.save_lead_sync(self.path, lead())[0], range(16))
            )
        self.assertEqual(len(set(results)), 1)
        self.assertEqual(self.count(), 1)

    def test_parallel_users(self):
        with ThreadPoolExecutor(max_workers=8) as pool:
            results = list(
                pool.map(
                    lambda i: db.save_lead_sync(
                        self.path, lead(user_id=100 + i, submission_id=f"request-{i}")
                    )[0],
                    range(20),
                )
            )
        self.assertEqual(len(set(results)), 20)

    def test_new_form_same_payload_and_different_service(self):
        first = db.save_lead_sync(self.path, lead())[0]
        self.assertEqual(
            db.save_lead_sync(self.path, lead(submission_id="request-2"))[0], first
        )
        self.assertNotEqual(
            db.save_lead_sync(
                self.path, lead(submission_id="request-3", service="Другая услуга")
            )[0],
            first,
        )
        self.assertEqual(self.count(), 2)

    def test_invalid_leads(self):
        for change in [
            dict(phone="abc79990000000"),
            dict(name="x"),
            dict(submission_id=""),
            dict(service=""),
        ]:
            with self.subTest(change=change), self.assertRaises(ValueError):
                db.save_lead_sync(self.path, lead(**change))
        self.assertEqual(self.count(), 0)

    def test_status_assignment_audit_and_closed(self):
        first = db.save_lead_sync(self.path, lead())[0]
        self.assertTrue(
            db.assign_lead_to_manager_sync(self.path, first, 100, "employee")
        )
        self.assertFalse(db.assign_lead_to_manager_sync(self.path, first, 101, "other"))
        with db.connect(self.path) as connection:
            self.assertEqual(
                connection.execute(
                    "SELECT old_status,new_status FROM lead_status_history"
                ).fetchone(),
                ("новая", "в работе"),
            )
        self.assertFalse(db.update_lead_status_sync(self.path, first, "made-up", 100))
        self.assertTrue(db.update_lead_status_sync(self.path, first, "подключено", 100))
        self.assertTrue(db.update_lead_status_sync(self.path, first, "подключено", 100))
        self.assertEqual(db.fetch_analytics_sync(self.path)["conversion"], 100)
        second = db.save_lead_sync(
            self.path, lead(submission_id="request-2", service="other")
        )[0]
        db.update_lead_status_sync(self.path, second, "отказ", 100)
        self.assertFalse(
            db.assign_lead_to_manager_sync(self.path, second, 101, "other")
        )

    def test_stale_delivery_requires_review(self):
        first = db.save_lead_sync(self.path, lead())[0]
        db.claim_lead_sync(self.path, first)
        with db.connect(self.path) as c:
            c.execute("UPDATE leads SET claimed_at='2000-01-01 00:00:00'")
        self.assertEqual(db.recover_stale_deliveries_sync(self.path), 1)
        self.assertEqual(db.get_delivery_queue_sync(self.path), [])
        self.assertTrue(db.retry_delivery_sync(self.path, first))
        self.assertEqual(len(db.get_delivery_queue_sync(self.path)), 1)

    def test_migration_failure_rolls_back_and_retains_backup(self):
        old = str(Path(self.directory.name) / "unsupported.db")
        with sqlite3.connect(old) as c:
            c.execute("CREATE TABLE leads(id INTEGER PRIMARY KEY, private_data TEXT)")
            c.execute("INSERT INTO leads VALUES(1,'synthetic')")
        with self.assertRaises(RuntimeError):
            db.init_db(old)
        with db.connect(old, readonly=True) as c:
            self.assertEqual(
                c.execute("SELECT private_data FROM leads").fetchone()[0], "synthetic"
            )
            self.assertEqual(c.execute("PRAGMA user_version").fetchone()[0], 0)
            self.assertEqual(len(c.execute("PRAGMA table_info(leads)").fetchall()), 2)

    def test_source_first_touch(self):
        self.assertEqual(
            db.remember_attribution_sync(self.path, 100, "vk_launch")[0], "vk_launch"
        )
        self.assertEqual(
            db.remember_attribution_sync(self.path, 100, "tg_other")[0], "vk_launch"
        )
        self.assertEqual(
            db.remember_attribution_sync(self.path, 100, None)[0], "vk_launch"
        )
        first = db.save_lead_sync(self.path, lead(ref_tag="direct"))[0]
        with db.connect(self.path) as c:
            self.assertEqual(
                c.execute(
                    "SELECT source,campaign,ref_tag FROM leads WHERE id=?", (first,)
                ).fetchone(),
                ("vk", "launch", "vk_launch"),
            )
        for tag in ["x" * 65, "vk_кампания", "vk bad", "unknown_campaign"]:
            self.assertEqual(db.parse_ref_tag(tag)[0], "direct")
        self.assertEqual(db.parse_ref_tag("alex_dom12")[3], "dom12")

    def test_migrate_old_database_preserves_row_and_backup(self):
        old = str(Path(self.directory.name) / "legacy.db")
        with sqlite3.connect(old) as c:
            c.execute(
                "CREATE TABLE leads(id INTEGER PRIMARY KEY,ref_tag TEXT,address TEXT,service TEXT,name TEXT,phone TEXT,call_time TEXT,created_at TEXT)"
            )
            c.execute(
                "INSERT INTO leads VALUES(7,'alex_house','Тест','Тест','Имя','+79990000000','Утро','2025-01-01 00:00:00')"
            )
        db.init_db(old)
        db.init_db(old)
        self.assertEqual(db.get_lead_by_id_sync(old, 7)[4], "Имя")
        backups = list((Path(self.directory.name) / "backups").glob("*.db"))
        self.assertEqual(len(backups), 1)
        with db.connect(str(backups[0]), readonly=True) as c:
            self.assertEqual(c.execute("SELECT COUNT(*) FROM leads").fetchone()[0], 1)

    def test_readonly_and_export_empty_no_clobber(self):
        with db.connect(self.path, readonly=True) as c:
            with self.assertRaises(sqlite3.OperationalError):
                c.execute("DELETE FROM leads")
        target = Path(self.directory.name) / "empty.csv"
        self.assertEqual(export_csv(Path(self.path), target), 0)
        self.assertTrue(target.read_bytes().startswith(b"\xef\xbb\xbf"))
        with self.assertRaises(FileExistsError):
            export_csv(Path(self.path), target)
        self.assertEqual(self.count(), 0)

    def test_export_data_and_formula_guard(self):
        db.save_lead_sync(self.path, lead(name=" \t=1+2"))
        target = Path(self.directory.name) / "leads.csv"
        self.assertEqual(export_csv(Path(self.path), target), 1)
        with target.open(encoding="utf-8-sig", newline="") as f:
            rows = list(csv.reader(f, delimiter=";"))
        self.assertEqual(len(rows[0]), 18)
        self.assertTrue(rows[1][4].startswith("'"))
        self.assertTrue(rows[1][5].startswith("'"))
        for value in ["=1", "+1", "-1", "@sum", " \t=1", "\r@sum"]:
            self.assertTrue(sanitize_excel_cell(value).startswith("'"))
        name, count = db.generate_export_csv_sync(self.path)
        self.addCleanup(lambda: Path(name).unlink(missing_ok=True))
        self.assertEqual(count, 1)

    def test_erasure_dryrun_and_anonymization(self):
        first = db.save_lead_sync(self.path, lead())[0]
        db.request_erasure_sync(self.path, 100)
        self.assertEqual(db.retention_cleanup_sync(self.path, 180), 1)
        self.assertEqual(db.get_lead_by_id_sync(self.path, first)[4], "Тест")
        self.assertEqual(db.retention_cleanup_sync(self.path, 180, apply=True), 1)
        row = db.get_lead_by_id_sync(self.path, first)
        self.assertEqual((row[4], row[5], row[9]), ("Удалено", "", None))
        self.assertEqual(self.count(), 1)
        self.assertEqual(db.get_delivery_queue_sync(self.path), [])

    def test_claim_and_ambiguous_delivery_not_retried(self):
        first = db.save_lead_sync(self.path, lead())[0]
        self.assertTrue(db.claim_lead_sync(self.path, first))
        self.assertFalse(db.claim_lead_sync(self.path, first))
        self.assertTrue(
            db.mark_delivery_error_sync(self.path, first, "TelegramNetworkError")
        )
        self.assertEqual(db.get_delivery_queue_sync(self.path), [])
        with db.connect(self.path) as c:
            self.assertEqual(
                c.execute("SELECT notification_status FROM leads").fetchone()[0],
                "проверить",
            )


class AsyncTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.path = str(Path(self.directory.name) / "leads.db")
        db.init_db(self.path)

    async def test_storage_string_state_atomic_data_restart(self):
        store = db.SQLiteStorage(self.path)
        key = StorageKey(bot_id=1, chat_id=100, user_id=100)
        await store.set_state(key, "OrderFSM:waiting_for_name")
        await asyncio.gather(
            *(store.update_data(key, {f"field{i}": i}) for i in range(20))
        )
        reopened = db.SQLiteStorage(self.path)
        self.assertEqual(len(await reopened.get_data(key)), 20)
        self.assertEqual(await reopened.get_state(key), "OrderFSM:waiting_for_name")
        await reopened.set_state(key, None)
        self.assertEqual(await reopened.get_data(key), {})

    async def test_authorization(self):
        callback = AsyncMock()
        callback.from_user.id = 999
        callback.message.chat.id = -100
        self.assertFalse(await bot.verify_manager_access(callback, [100], [101], -100))
        callback.from_user.id = 101
        callback.message.chat.id = -200
        self.assertFalse(await bot.verify_manager_access(callback, [100], [101], -100))
        callback.message.chat.id = -100
        self.assertTrue(await bot.verify_manager_access(callback, [100], [101], -100))
        await bot.process_lead_status(callback, self.path, [100], [102], -100)
        with db.connect(self.path) as c:
            self.assertEqual(
                c.execute("SELECT COUNT(*) FROM lead_status_history").fetchone()[0], 0
            )

    async def test_aura_direct_section_starts_existing_funnel(self):
        callback = AsyncMock()
        callback.data = "section:internet"
        callback.from_user.id = 777
        callback.message = AsyncMock()
        state = AsyncMock()
        await bot.open_home_section(callback, state, self.path)
        state.set_state.assert_awaited_once_with(bot.OrderFSM.waiting_for_service)
        state.update_data.assert_awaited_once()
        values = state.update_data.await_args.kwargs
        self.assertEqual(values["selected_category"], "combo")
        callback.message.edit_text.assert_awaited_once()
        rendered = callback.message.edit_text.await_args.args[0]
        self.assertIn("01 / 04", rendered)
        with db.connect(self.path) as connection:
            session = connection.execute(
                "SELECT current_step, completed, cancelled "
                "FROM funnel_sessions WHERE user_id=777"
            ).fetchone()
        self.assertEqual(
            tuple(session), (bot.OrderFSM.waiting_for_service.state, 0, 0)
        )

    async def test_export_group_blocked(self):
        message = AsyncMock()
        message.from_user.id = 100
        message.chat.type = "supergroup"
        await bot.cmd_export(message, [100], self.path)
        message.reply_document.assert_not_called()

    async def test_delivery_success_and_network_uncertainty(self):
        for index, failure in enumerate([False, True]):
            with self.subTest(failure=failure):
                first = db.save_lead_sync(
                    self.path,
                    lead(submission_id=f"worker-{index}", service=f"worker-{index}"),
                )[0]
                api = AsyncMock()
                stop = asyncio.Event()

                async def send(**kwargs):
                    stop.set()
                    if failure:
                        raise TelegramNetworkError(
                            method=SendMessage(chat_id=100, text="test"),
                            message="synthetic failure",
                        )
                    message = AsyncMock()
                    message.chat.id = 100
                    message.message_id = 7
                    return message

                api.send_message.side_effect = send
                with patch("bot.asyncio.sleep", new=AsyncMock()):
                    await bot.delivery_worker(api, self.path, 100, None, stop)
                self.assertEqual(api.send_message.await_count, 1)
                with db.connect(self.path) as c:
                    status = c.execute(
                        "SELECT notification_status FROM leads WHERE id=?", (first,)
                    ).fetchone()[0]
                self.assertEqual(status, "проверить" if failure else "доставлено")

    async def test_followup_once_and_restart(self):
        db.reset_funnel_session_sync(
            self.path, 100, "vk_campaign", "OrderFSM:viewing_tariff"
        )
        activity = (datetime.now(db.MOSCOW_TZ) - timedelta(minutes=30)).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        with db.connect(self.path) as c:
            c.execute("UPDATE funnel_sessions SET last_activity_at=?", (activity,))
        api = AsyncMock()
        stop = asyncio.Event()

        async def send(**kwargs):
            stop.set()

        api.send_message.side_effect = send
        with patch("bot.asyncio.sleep", new=AsyncMock()):
            await bot.followup_worker(api, self.path, 25, stop)
        self.assertEqual(api.send_message.await_count, 1)
        db.init_db(self.path)
        self.assertEqual(db.get_abandoned_sessions_sync(self.path, db.now_moscow()), [])
        self.assertFalse(
            db.claim_followup_sync(self.path, 100, db.now_moscow(), activity)
        )
        db.reset_funnel_session_sync(
            self.path, 101, "direct", "OrderFSM:viewing_tariff"
        )
        db.cancel_funnel_session_sync(self.path, 101)
        self.assertEqual(db.get_abandoned_sessions_sync(self.path, db.now_moscow()), [])
        # Newer session must not be claimed using a stale selection snapshot.
        db.reset_funnel_session_sync(
            self.path, 102, "direct", "OrderFSM:viewing_tariff"
        )
        self.assertFalse(
            db.claim_followup_sync(self.path, 102, db.now_moscow(), activity)
        )

    async def test_followup_failure_not_spammed(self):
        db.reset_funnel_session_sync(
            self.path, 100, "direct", "OrderFSM:viewing_tariff"
        )
        with db.connect(self.path) as c:
            c.execute(
                "UPDATE funnel_sessions SET last_activity_at='2000-01-01 00:00:00'"
            )
        api = AsyncMock()
        stop = asyncio.Event()

        async def send(**kwargs):
            stop.set()
            raise TelegramForbiddenError(
                method=SendMessage(chat_id=100, text="test"), message="synthetic"
            )

        api.send_message.side_effect = send
        with patch("bot.asyncio.sleep", new=AsyncMock()):
            await bot.followup_worker(api, self.path, 25, stop)
        self.assertEqual(api.send_message.await_count, 1)
        self.assertEqual(db.get_abandoned_sessions_sync(self.path, db.now_moscow()), [])

    async def test_followup_disabled_and_completed(self):
        api = AsyncMock()
        await bot.followup_worker(api, self.path, 0, asyncio.Event())
        api.send_message.assert_not_called()
        db.reset_funnel_session_sync(
            self.path, 100, "direct", "OrderFSM:viewing_tariff"
        )
        db.save_lead_sync(self.path, lead())
        self.assertEqual(db.get_abandoned_sessions_sync(self.path, db.now_moscow()), [])


class FakeSession(BaseSession):
    def __init__(self):
        super().__init__()
        self.calls = []
        self.next_id = 1000

    async def close(self):
        pass

    async def stream_content(self, *args, **kwargs):
        yield b""

    async def make_request(self, telegram_bot, method, timeout=None):
        self.calls.append(method)
        if method.__api_method__ in {"sendMessage", "editMessageText"}:
            self.next_id += 1
            return Message.model_validate(
                dict(
                    message_id=self.next_id,
                    date=1,
                    chat=dict(id=method.chat_id, type="private"),
                    text=getattr(method, "text", ""),
                ),
                context={"bot": telegram_bot},
            )
        return True


class DispatcherTests(unittest.IsolatedAsyncioTestCase):
    async def test_full_consultation_with_duplicate_clicks_stale_button_and_two_users(
        self,
    ):
        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / "integration.db")
            db.init_db(path)
            session = FakeSession()
            api = Bot(TEST_TOKEN, session=session)
            dispatcher = Dispatcher(
                storage=db.SQLiteStorage(path), events_isolation=SimpleEventIsolation()
            )
            dispatcher.include_router(bot.router)
            kwargs = dict(
                db_path=path,
                admin_ids=[100],
                manager_ids=[100],
                crm_chat_id=100,
                agent_name="Тест",
                policy_version="test",
                privacy_policy_url="https://example.org/privacy",
                personal_data_operator="Тест",
                retention_days=180,
            )
            counter = 0

            async def feed(user, text=None, callback=None):
                nonlocal counter
                counter += 1
                person = dict(id=user, is_bot=False, first_name="Тест")
                message = dict(
                    message_id=counter,
                    date=1,
                    chat=dict(id=user, type="private"),
                    from_user=person,
                )
                if callback:
                    payload = dict(
                        callback_query=dict(
                            id=str(counter),
                            from_user=person,
                            chat_instance="test",
                            data=callback,
                            message=message,
                        )
                    )
                else:
                    message["text"] = text
                    if text.startswith("/"):
                        message["entities"] = [
                            dict(
                                type="bot_command",
                                offset=0,
                                length=len(text.split()[0]),
                            )
                        ]
                    payload = dict(message=message)
                update = Update.model_validate(
                    dict(update_id=counter, **payload), context={"bot": api}
                )
                # No wall-clock delay needed: disable only UI rate limiting, not event isolation.
                with patch.object(
                    bot.ThrottlingMiddleware, "__call__", new=pass_throttle
                ):
                    return await dispatcher.feed_update(api, update, **kwargs)

            await asyncio.gather(
                feed(200, text="/start vk_launch"), feed(201, text="/start tg_launch")
            )
            for user in [200, 201]:
                await feed(user, callback="consultation")
                await feed(user, text="Тест")
                await feed(user, text="invalid")
                await feed(user, text="+79990000000")
                await feed(user, callback="time_any")
                state = FSMContext(
                    dispatcher.storage,
                    StorageKey(bot_id=api.id, chat_id=user, user_id=user),
                )
                self.assertEqual(
                    await state.get_state(), bot.OrderFSM.waiting_for_consent.state
                )
                nonce = (await state.get_data())["submission_id"]
                await feed(user, callback="consent_accept:old")
                self.assertEqual(
                    await state.get_state(), bot.OrderFSM.waiting_for_consent.state
                )
                await asyncio.gather(
                    feed(user, callback="consent_accept:" + nonce),
                    feed(user, callback="consent_accept:" + nonce),
                )
                self.assertIsNone(await state.get_state())
            with db.connect(path) as c:
                self.assertEqual(
                    c.execute("SELECT COUNT(*) FROM leads").fetchone()[0], 2
                )
                self.assertEqual(
                    c.execute("SELECT source FROM leads ORDER BY user_id").fetchall(),
                    [("vk",), ("tg",)],
                )
                self.assertEqual(
                    c.execute("SELECT DISTINCT address FROM leads").fetchall(), [("",)]
                )
            # Full home-internet path: category -> variant -> address -> form -> consent.
            user = 202
            await feed(user, text="/start qr_house")
            await feed(user, callback="start_process")
            await feed(user, callback="cat:combo")
            await feed(user, callback="tariff_item:combo_super")
            await feed(user, callback="service:combo_super_200")
            await feed(user, callback="action_order")
            state = FSMContext(
                dispatcher.storage,
                StorageKey(bot_id=api.id, chat_id=user, user_id=user),
            )
            self.assertEqual(
                await state.get_state(), bot.OrderFSM.waiting_for_address.state
            )
            await feed(user, text="Тестовый город, улица Тестовая, дом 1")
            await feed(user, text="Тест")
            await feed(user, callback="form_back")
            self.assertEqual(
                await state.get_state(), bot.OrderFSM.waiting_for_name.state
            )
            await feed(user, text="Тест")
            await feed(user, text="+79990000000")
            await feed(user, callback="time_any")
            nonce = (await state.get_data())["submission_id"]
            await feed(user, callback="consent_accept:" + nonce)
            self.assertIsNone(await state.get_state())
            with db.connect(path) as c:
                self.assertEqual(
                    c.execute("SELECT COUNT(*) FROM leads").fetchone()[0], 3
                )
                self.assertEqual(
                    c.execute(
                        "SELECT category FROM leads WHERE user_id=202"
                    ).fetchone()[0],
                    "combo",
                )
            await feed(user, callback="action_back_tariffs")
            self.assertIsNone(await state.get_state())
            await feed(user, text="/menu")
            await feed(user, callback="consultation")
            await feed(user, text="/cancel")
            self.assertIsNone(await state.get_state())
            await dispatcher.storage.close()
            await api.session.close()


async def pass_throttle(self, handler, event, data):
    return await handler(event, data)


class TariffTests(unittest.TestCase):
    def test_catalog_all_references_and_callback_limits(self):
        self.assertEqual(len(tariffs.TARIFFS), 30)
        keyboards = [
            tariffs.get_categories_kb(),
            tariffs.get_tariff_actions_kb(),
            bot.lead_actions_kb(123),
        ]
        for category in tariffs.CATEGORIES_INFO:
            keyboards.append(tariffs.get_tariffs_by_category_kb(category))
        for category in tariffs.CATALOG_STRUCTURE.values():
            for item in category:
                if item.get("has_variants"):
                    keyboards.append(tariffs.get_variants_kb(item["id"])[1])
        for keyboard in keyboards:
            for row in keyboard.inline_keyboard:
                for button in row:
                    if button.callback_data:
                        self.assertLessEqual(len(button.callback_data.encode()), 64)
        for key in tariffs.TARIFFS:
            self.assertIn("требуют подтверждения", tariffs.render_tariff(key))
            self.assertLess(len(tariffs.render_tariff(key)), 4096)

    def test_inactive_offer_hidden(self):
        with patch.dict(tariffs.TARIFFS["mobile_red"], active=False):
            keyboard = tariffs.get_tariffs_by_category_kb("mobile")
            self.assertFalse(
                any(
                    button.callback_data == "service:mobile_red"
                    for row in keyboard.inline_keyboard
                    for button in row
                )
            )


class AuraCoreTests(unittest.TestCase):
    def test_progress_and_copy_are_safe(self):
        self.assertIn("03 / 04", aura_ui.progress(3, "Проверка"))
        with self.assertRaises(ValueError):
            aura_ui.progress(0, "Ошибка")
        text = aura_ui.home_text("Оператор <тест>", "https://example.org/?a=1&b=2")
        self.assertIn("Связь. Без компромиссов.", text)
        self.assertNotIn("Оператор <тест>", text)
        self.assertIn("&amp;", text)

    def test_home_menu_has_six_direct_actions(self):
        callbacks = [
            button.callback_data
            for row in bot.get_start_kb().inline_keyboard
            for button in row
            if button.callback_data != "privacy_info"
        ]
        self.assertEqual(
            callbacks,
            [
                "section:internet",
                "section:mobile",
                "section:tv",
                "section:business",
                "section:equipment",
                "consultation",
            ],
        )
        self.assertEqual(set(aura_ui.SECTION_TO_CATEGORY.values()), set(tariffs.CATEGORIES_INFO))

    def test_tariff_cards_keep_confirmation_and_clean_buttons(self):
        text = tariffs.render_tariff("mobile_red")
        self.assertIn("01 / 04", text)
        self.assertIn("требуют подтверждения", text)
        keyboard = tariffs.get_tariffs_by_category_kb("mobile")
        labels = [button.text for row in keyboard.inline_keyboard for button in row]
        self.assertIn("Тариф RED", labels)

    def test_mini_app_prototype_is_local_and_non_submitting(self):
        root = Path(__file__).parents[1] / "miniapp"
        self.assertTrue((root / "index.html").is_file())
        self.assertTrue((root / "styles.css").is_file())
        script = (root / "app.js").read_text(encoding="utf-8")
        self.assertIn('../tariff_data.json', script)
        self.assertNotIn("fetch(\"/api", script)
        self.assertEqual(len(list((root / "assets").glob("*.svg"))), 5)


if __name__ == "__main__":
    unittest.main()
