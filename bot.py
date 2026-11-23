import asyncio
import csv
import html
import io
import logging
import os
import sqlite3
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ChatMemberStatus, ParseMode
from aiogram.filters import Command, CommandStart
from aiogram.types import (
    BufferedInputFile,
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    Message,
    ReplyKeyboardMarkup,
)
from dotenv import load_dotenv

load_dotenv()

BOT_TOKEN = os.environ["BOT_TOKEN"]
GROUP_ID = int(os.environ["GROUP_ID"])  # группа обсуждений канала (-100...)
ADMIN_IDS = {
    int(x) for x in os.environ.get("ADMIN_IDS", "").replace(" ", "").split(",") if x
}
DATABASE_URL = os.environ.get("DATABASE_URL", "").strip()  # Postgres (Neon/Supabase)
DB_PATH = os.environ.get("DB_PATH", "leads.db")  # запасной вариант: файл SQLite
TZ = ZoneInfo(os.environ.get("TZ_NAME", "Europe/Kyiv"))
# Заявкой считается только пост, где есть это слово (без учёта регистра; "заявк" ловит
# и "заявка", и "ЗАЯВКА", и "заявки"). Можно поменять переменной KEYWORD.
KEYWORD = os.environ.get("KEYWORD", "заявк").strip().lower()

MENU_BTN = "📋 Активные заявки"
CLOSED_BTN = "🗂 Закрытые заявки"
STATS_BTN = "📊 Статистика"
EXPORT_BTN = "📥 Выгрузка"

REASONS = {
    "price": "💸 Дорого",
    "noanswer": "📵 Не ответил",
    "changed": "🔄 Передумал",
    "other": "❓ Другое",
}

PERIODS = {
    "day": "Сегодня",
    "week": "7 дней",
    "month": "30 дней",
    "all": "Всё время",
}

router = Router()


# ---------- Хранилище ----------
class SqliteStore:
    """Локальный файл. На бесплатных хостингах стирается при перезапуске!"""

    def __init__(self, path: str):
        self.path = path

    def _conn(self):
        conn = sqlite3.connect(self.path)
        conn.row_factory = sqlite3.Row
        return conn

    async def init(self):
        with self._conn() as c:
            c.execute(
                """
                CREATE TABLE IF NOT EXISTS leads (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    group_msg_id INTEGER,
                    post_msg_id INTEGER,
                    text TEXT,
                    status TEXT NOT NULL DEFAULT 'new',
                    owner_id INTEGER,
                    owner_name TEXT,
                    fail_reason TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    closed_at TEXT
                )
                """
            )
            cols = {r["name"] for r in c.execute("PRAGMA table_info(leads)")}
            if "fail_reason" not in cols:
                c.execute("ALTER TABLE leads ADD COLUMN fail_reason TEXT")
            if "closed_at" not in cols:
                c.execute("ALTER TABLE leads ADD COLUMN closed_at TEXT")

    async def create_lead(self, text, post_msg_id) -> int:
        with self._conn() as c:
            cur = c.execute(
                "INSERT INTO leads (post_msg_id, text) VALUES (?, ?)",
                (post_msg_id, text),
            )
            return cur.lastrowid

    async def set_group_msg(self, lead_id, msg_id):
        with self._conn() as c:
            c.execute("UPDATE leads SET group_msg_id=? WHERE id=?", (msg_id, lead_id))

    async def get(self, lead_id):
        with self._conn() as c:
            row = c.execute("SELECT * FROM leads WHERE id=?", (lead_id,)).fetchone()
            return dict(row) if row else None

    async def take(self, lead_id, owner_id, owner_name) -> bool:
        with self._conn() as c:
            cur = c.execute(
                "UPDATE leads SET status='work', owner_id=?, owner_name=? "
                "WHERE id=? AND status='new'",
                (owner_id, owner_name, lead_id),
            )
            return cur.rowcount > 0

    async def active(self, owner_id) -> list[dict]:
        with self._conn() as c:
            rows = c.execute(
                "SELECT * FROM leads WHERE status='work' AND owner_id=? ORDER BY id",
                (owner_id,),
            ).fetchall()
            return [dict(r) for r in rows]

    async def finish(self, lead_id, owner_id, result, reason=None):
        with self._conn() as c:
            cur = c.execute(
                "UPDATE leads SET status=?, fail_reason=?, closed_at=CURRENT_TIMESTAMP "
                "WHERE id=? AND status='work' AND owner_id=?",
                (result, reason, lead_id, owner_id),
            )
            if cur.rowcount == 0:
                return None
            row = c.execute("SELECT * FROM leads WHERE id=?", (lead_id,)).fetchone()
            return dict(row)

    async def closed(self, owner_id, limit=10) -> list[dict]:
        with self._conn() as c:
            rows = c.execute(
                "SELECT * FROM leads WHERE status IN ('success','fail') "
                "AND owner_id=? ORDER BY id DESC LIMIT ?",
                (owner_id, limit),
            ).fetchall()
            return [dict(r) for r in rows]

    async def reclose(self, lead_id, result, reason=None):
        """Меняет итог уже закрытой заявки (успех / неудача / вернуть в работу)."""
        with self._conn() as c:
            cur = c.execute(
                "UPDATE leads SET status=?, fail_reason=?, "
                "closed_at=CASE WHEN ?='work' THEN NULL ELSE CURRENT_TIMESTAMP END "
                "WHERE id=? AND status IN ('success','fail')",
                (result, reason, result, lead_id),
            )
            if cur.rowcount == 0:
                return None
            row = c.execute("SELECT * FROM leads WHERE id=?", (lead_id,)).fetchone()
            return dict(row)

    async def leads_since(self, since):
        with self._conn() as c:
            if since:
                s = since.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
                rows = c.execute(
                    "SELECT * FROM leads WHERE created_at >= ? ORDER BY id", (s,)
                ).fetchall()
            else:
                rows = c.execute("SELECT * FROM leads ORDER BY id").fetchall()
            return [dict(r) for r in rows]


class PgStore:
    """Облачный Postgres: заявки переживают перезапуски и обновления."""

    def __init__(self, dsn: str):
        # убираем ?sslmode=...&channel_binding=... — asyncpg их не понимает
        self.dsn = dsn.split("?")[0]
        self.pool = None

    async def init(self):
        import asyncpg

        self.pool = await asyncpg.create_pool(
            self.dsn,
            ssl="require",
            min_size=1,
            max_size=3,
            max_inactive_connection_lifetime=60,
            statement_cache_size=0,  # совместимость с pgbouncer (Neon -pooler)
        )
        await self.pool.execute(
            """
            CREATE TABLE IF NOT EXISTS leads (
                id BIGSERIAL PRIMARY KEY,
                group_msg_id BIGINT,
                post_msg_id BIGINT,
                text TEXT,
                status TEXT NOT NULL DEFAULT 'new',
                owner_id BIGINT,
                owner_name TEXT,
                fail_reason TEXT,
                created_at TIMESTAMPTZ DEFAULT now(),
                closed_at TIMESTAMPTZ
            )
            """
        )
        # для баз, созданных прошлой версией бота
        await self.pool.execute(
            "ALTER TABLE leads ADD COLUMN IF NOT EXISTS fail_reason TEXT"
        )
        await self.pool.execute(
            "ALTER TABLE leads ADD COLUMN IF NOT EXISTS closed_at TIMESTAMPTZ"
        )

    async def create_lead(self, text, post_msg_id) -> int:
        return await self.pool.fetchval(
            "INSERT INTO leads (post_msg_id, text) VALUES ($1, $2) RETURNING id",
            post_msg_id,
            text,
        )

    async def set_group_msg(self, lead_id, msg_id):
        await self.pool.execute(
            "UPDATE leads SET group_msg_id=$1 WHERE id=$2", msg_id, lead_id
        )

    async def get(self, lead_id):
        row = await self.pool.fetchrow("SELECT * FROM leads WHERE id=$1", lead_id)
        return dict(row) if row else None

    async def take(self, lead_id, owner_id, owner_name) -> bool:
        got = await self.pool.fetchval(
            "UPDATE leads SET status='work', owner_id=$1, owner_name=$2 "
            "WHERE id=$3 AND status='new' RETURNING id",
            owner_id,
            owner_name,
            lead_id,
        )
        return got is not None

    async def active(self, owner_id) -> list[dict]:
        rows = await self.pool.fetch(
            "SELECT * FROM leads WHERE status='work' AND owner_id=$1 ORDER BY id",
            owner_id,
        )
        return [dict(r) for r in rows]

    async def finish(self, lead_id, owner_id, result, reason=None):
        row = await self.pool.fetchrow(
            "UPDATE leads SET status=$1, fail_reason=$2, closed_at=now() "
            "WHERE id=$3 AND status='work' AND owner_id=$4 RETURNING *",
            result,
            reason,
            lead_id,
            owner_id,
        )
        return dict(row) if row else None

    async def closed(self, owner_id, limit=10) -> list[dict]:
        rows = await self.pool.fetch(
            "SELECT * FROM leads WHERE status IN ('success','fail') "
            "AND owner_id=$1 ORDER BY id DESC LIMIT $2",
            owner_id,
            limit,
        )
        return [dict(r) for r in rows]

    async def reclose(self, lead_id, result, reason=None):
        """Меняет итог уже закрытой заявки (успех / неудача / вернуть в работу)."""
        row = await self.pool.fetchrow(
            "UPDATE leads SET status=$1::text, fail_reason=$2, "
            "closed_at=CASE WHEN $1::text='work' THEN NULL ELSE now() END "
            "WHERE id=$3 AND status IN ('success','fail') RETURNING *",
            result,
            reason,
            lead_id,
        )
        return dict(row) if row else None

    async def leads_since(self, since):
        if since:
            rows = await self.pool.fetch(
                "SELECT * FROM leads WHERE created_at >= $1 ORDER BY id", since
            )
        else:
            rows = await self.pool.fetch("SELECT * FROM leads ORDER BY id")
        return [dict(r) for r in rows]


store = PgStore(DATABASE_URL) if DATABASE_URL else SqliteStore(DB_PATH)


# ---------- Вспомогательное ----------
async def is_admin(bot: Bot, user_id: int) -> bool:
    if user_id in ADMIN_IDS:
        return True
    try:
        m = await bot.get_chat_member(GROUP_ID, user_id)
    except Exception:
        return False
    return m.status in (ChatMemberStatus.CREATOR, ChatMemberStatus.ADMINISTRATOR)


async def get_admin_ids(bot: Bot) -> set[int]:
    ids = set(ADMIN_IDS)
    try:
        for m in await bot.get_chat_administrators(GROUP_ID):
            if not m.user.is_bot:
                ids.add(m.user.id)
    except Exception:
        logging.exception("Не удалось получить список админов группы")
    return ids


def user_label(u) -> str:
    if u.username:
        return f"@{u.username}"
    return html.escape(u.full_name)


def short(text: str, n: int = 600) -> str:
    text = text or ""
    return html.escape(text if len(text) <= n else text[:n] + "…")


def to_local(v) -> str:
    if v is None:
        return ""
    if isinstance(v, str):
        v = datetime.strptime(v, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    return v.astimezone(TZ).strftime("%d.%m.%Y %H:%M")


def menu_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=MENU_BTN), KeyboardButton(text=CLOSED_BTN)],
            [KeyboardButton(text=STATS_BTN), KeyboardButton(text=EXPORT_BTN)],
        ],
        resize_keyboard=True,
    )


def active_card(r: dict):
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="✅ Успех", callback_data=f"done:{r['id']}:success"
                ),
                InlineKeyboardButton(
                    text="❌ Неудача", callback_data=f"done:{r['id']}:fail"
                ),
            ]
        ]
    )
    return f"🟡 <b>Заявка #{r['id']}</b>\n\n{short(r['text'])}", kb


def reasons_kb(lead_id: int, prefix="why", back="back") -> InlineKeyboardMarkup:
    items = list(REASONS.items())
    rows = [
        [
            InlineKeyboardButton(text=label, callback_data=f"{prefix}:{lead_id}:{code}")
            for code, label in items[i : i + 2]
        ]
        for i in range(0, len(items), 2)
    ]
    rows.append(
        [InlineKeyboardButton(text="↩️ Назад", callback_data=f"{back}:{lead_id}")]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def result_line(r: dict) -> tuple[str, str]:
    """Иконка и подпись итога заявки."""
    if r["status"] == "success":
        return "✅", "УСПЕХ"
    if r["status"] == "fail":
        reason = REASONS.get(r.get("fail_reason"), "не указана")
        return "❌", f"НЕУДАЧА\nПричина: {reason}"
    return "🟡", "В РАБОТЕ"


def closed_card(r: dict):
    icon, label = result_line(r)
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="✏️ Изменить итог", callback_data=f"edit:{r['id']}"
                )
            ]
        ]
    )
    return f"{icon} <b>Заявка #{r['id']} — {label}</b>\n\n{short(r['text'])}", kb


def edit_kb(lead_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="✅ Успех", callback_data=f"set:{lead_id}:success"),
                InlineKeyboardButton(text="❌ Неудача", callback_data=f"set:{lead_id}:fail"),
            ],
            [
                InlineKeyboardButton(
                    text="🔄 Вернуть в работу", callback_data=f"set:{lead_id}:work"
                )
            ],
            [InlineKeyboardButton(text="↩️ Отмена", callback_data=f"cancel:{lead_id}")],
        ]
    )


def period_start(code: str):
    now = datetime.now(TZ)
    if code == "day":
        return now.replace(hour=0, minute=0, second=0, microsecond=0)
    if code == "week":
        return now - timedelta(days=7)
    if code == "month":
        return now - timedelta(days=30)
    return None


def build_stats(rows: list[dict], title: str) -> str:
    cnt = Counter(r["status"] for r in rows)
    succ, fail = cnt["success"], cnt["fail"]
    closed = succ + fail
    conv = f"{succ / closed * 100:.0f}%" if closed else "—"

    lines = [
        f"📊 <b>Статистика: {title}</b>",
        "",
        f"Всего заявок: {len(rows)}",
        f"🆕 Не взято: {cnt['new']}",
        f"🟡 В работе: {cnt['work']}",
        f"✅ Успех: {succ}",
        f"❌ Неудача: {fail}",
        f"Конверсия (успех из закрытых): {conv}",
    ]

    reasons = Counter(
        REASONS.get(r["fail_reason"], "Не указана") for r in rows if r["status"] == "fail"
    )
    if reasons:
        lines += ["", "<b>Причины неудач:</b>"]
        lines += [f"{name}: {n}" for name, n in reasons.most_common()]

    by_owner = defaultdict(lambda: [0, 0, 0])  # успех, неудача, в работе
    for r in rows:
        if not r["owner_name"]:
            continue
        idx = {"success": 0, "fail": 1, "work": 2}.get(r["status"])
        if idx is not None:
            by_owner[r["owner_name"]][idx] += 1
    if by_owner:
        lines += ["", "<b>По исполнителям</b> (✅ / ❌ / 🟡):"]
        for name, (s, f, w) in sorted(by_owner.items(), key=lambda kv: -kv[1][0]):
            lines.append(f"{name}: {s} / {f} / {w}")

    return "\n".join(lines)


def stats_kb(current: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text=("• " if code == current else "") + label,
                    callback_data=f"stats:{code}",
                )
                for code, label in PERIODS.items()
            ]
        ]
    )


# ---------- Новая заявка в канале ----------
@router.message(F.chat.id == GROUP_ID, F.is_automatic_forward)
async def on_channel_post(message: Message, bot: Bot):
    text = message.text or message.caption or ""
    if KEYWORD not in text.lower():
        return  # не заявка (объявление, обычный пост) — игнорируем
    lead_id = await store.create_lead(text, message.message_id)

    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="✋ Взять", callback_data=f"take:{lead_id}")]
        ]
    )
    # reply на автофорвард = комментарий под постом
    sent = await message.reply(
        f"🆕 <b>Заявка #{lead_id}</b>\nОжидает исполнителя", reply_markup=kb
    )
    await store.set_group_msg(lead_id, sent.message_id)

    # уведомление каждому админу в личку (придёт push на телефон)
    for admin_id in await get_admin_ids(bot):
        try:
            await bot.send_message(
                admin_id,
                f"🆕 <b>Новая заявка #{lead_id}</b>\n\n{short(text)}",
                reply_markup=kb,
            )
        except Exception:
            logging.info("Не удалось написать админу %s (не нажал /start?)", admin_id)


# ---------- Кнопка «Взять» ----------
@router.callback_query(F.data.startswith("take:"))
async def on_take(cb: CallbackQuery, bot: Bot):
    lead_id = int(cb.data.split(":")[1])

    if not await is_admin(bot, cb.from_user.id):
        await cb.answer("Только администраторы могут брать заявки", show_alert=True)
        return

    name = user_label(cb.from_user)
    if not await store.take(lead_id, cb.from_user.id, name):
        await cb.answer("Заявка уже взята", show_alert=True)
        return

    row = await store.get(lead_id)
    group_text = f"🟡 <b>Заявка #{lead_id} — В РАБОТЕ</b>\nИсполнитель: {name}"
    if row and row.get("group_msg_id"):
        try:
            await bot.edit_message_text(
                chat_id=GROUP_ID,
                message_id=row["group_msg_id"],
                text=group_text,
                reply_markup=None,
            )
        except Exception:
            logging.exception("Не удалось обновить сообщение в группе")

    in_group = cb.message.chat.id == GROUP_ID
    if not in_group:
        try:
            await cb.message.edit_text(
                f"🟡 Вы взяли заявку #{lead_id}. Она в меню «{MENU_BTN}».",
                reply_markup=None,
            )
        except Exception:
            pass
    await cb.answer("Заявка ваша")

    if in_group:  # напомним в личке, где искать заявку
        try:
            await bot.send_message(
                cb.from_user.id,
                f"Вы взяли заявку #{lead_id}. Она в меню «{MENU_BTN}».",
                reply_markup=menu_kb(),
            )
        except Exception:
            pass


# ---------- Меню в личке бота ----------
@router.message(CommandStart(), F.chat.type == "private")
async def on_start(message: Message, bot: Bot):
    if not await is_admin(bot, message.from_user.id):
        await message.answer("Бот доступен только администраторам.")
        return
    await message.answer("Выберите действие:", reply_markup=menu_kb())


@router.message(F.chat.type == "private", F.text == MENU_BTN)
async def on_active(message: Message, bot: Bot):
    if not await is_admin(bot, message.from_user.id):
        return
    rows = await store.active(message.from_user.id)

    if not rows:
        await message.answer("Активных заявок нет.")
        return

    for r in rows:
        text, kb = active_card(r)
        await message.answer(text, reply_markup=kb)


# ---------- Успех / Неудача (с причиной) ----------
async def close_lead(cb: CallbackQuery, bot: Bot, lead_id: int, result: str, reason):
    row = await store.finish(lead_id, cb.from_user.id, result, reason)
    if not row:
        await cb.answer("Заявка уже закрыта или не ваша", show_alert=True)
        return

    if result == "success":
        icon, word, extra = "✅", "УСПЕХ", ""
    else:
        icon, word = "❌", "НЕУДАЧА"
        extra = f"\nПричина: {REASONS.get(reason, 'не указана')}"

    await cb.message.edit_text(
        f"{icon} <b>Заявка #{lead_id} закрыта — {word}</b>{extra}",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(
                        text="✏️ Изменить итог", callback_data=f"edit:{lead_id}"
                    )
                ]
            ]
        ),
    )
    await cb.answer("Готово")

    if row.get("group_msg_id"):
        try:
            await bot.edit_message_text(
                chat_id=GROUP_ID,
                message_id=row["group_msg_id"],
                text=(
                    f"{icon} <b>Заявка #{lead_id} — {word}</b>{extra}\n"
                    f"Исполнитель: {row['owner_name']}"
                ),
                reply_markup=None,
            )
        except Exception:
            logging.exception("Не удалось обновить сообщение в группе")


@router.callback_query(F.data.startswith("done:"))
async def on_done(cb: CallbackQuery, bot: Bot):
    _, lead_id, result = cb.data.split(":")
    lead_id = int(lead_id)

    if result == "success":
        await close_lead(cb, bot, lead_id, "success", None)
        return

    # неудача: сначала спрашиваем причину
    row = await store.get(lead_id)
    if not row or row["status"] != "work" or row["owner_id"] != cb.from_user.id:
        await cb.answer("Заявка уже закрыта или не ваша", show_alert=True)
        return
    await cb.message.edit_text(
        f"❌ <b>Заявка #{lead_id}</b>\nВыберите причину неудачи:",
        reply_markup=reasons_kb(lead_id),
    )
    await cb.answer()


@router.callback_query(F.data.startswith("why:"))
async def on_reason(cb: CallbackQuery, bot: Bot):
    _, lead_id, code = cb.data.split(":")
    if code not in REASONS:
        await cb.answer()
        return
    await close_lead(cb, bot, int(lead_id), "fail", code)


@router.callback_query(F.data.startswith("back:"))
async def on_back(cb: CallbackQuery):
    row = await store.get(int(cb.data.split(":")[1]))
    if not row or row["status"] != "work" or row["owner_id"] != cb.from_user.id:
        await cb.answer("Заявка уже закрыта", show_alert=True)
        return
    text, kb = active_card(row)
    await cb.message.edit_text(text, reply_markup=kb)
    await cb.answer()


# ---------- Закрытые заявки и смена итога ----------
@router.message(F.chat.type == "private", F.text == CLOSED_BTN)
async def on_closed(message: Message, bot: Bot):
    if not await is_admin(bot, message.from_user.id):
        return
    rows = await store.closed(message.from_user.id, 10)
    if not rows:
        await message.answer("Закрытых заявок пока нет.")
        return
    await message.answer("Последние закрытые заявки (до 10):")
    for r in rows:
        text, kb = closed_card(r)
        await message.answer(text, reply_markup=kb)


async def sync_group_message(bot: Bot, row: dict):
    """Обновляет статус заявки в комментариях под постом."""
    if not row.get("group_msg_id"):
        return
    if row["status"] == "work":
        text = (
            f"🟡 <b>Заявка #{row['id']} — В РАБОТЕ</b>\n"
            f"Исполнитель: {row['owner_name']}"
        )
    else:
        icon, label = result_line(row)
        text = (
            f"{icon} <b>Заявка #{row['id']} — {label}</b>\n"
            f"Исполнитель: {row['owner_name']}"
        )
    try:
        await bot.edit_message_text(
            chat_id=GROUP_ID,
            message_id=row["group_msg_id"],
            text=text,
            reply_markup=None,
        )
    except Exception:
        logging.info("Сообщение в группе не изменилось или недоступно")


async def apply_new_result(cb: CallbackQuery, bot: Bot, lead_id: int, result, reason):
    row = await store.reclose(lead_id, result, reason)
    if not row:
        await cb.answer("Итог уже изменён или заявка не закрыта", show_alert=True)
        return
    await sync_group_message(bot, row)

    if row["status"] == "work":
        await cb.message.edit_text(
            f"🟡 <b>Заявка #{lead_id} возвращена в работу</b>\n"
            f"Она снова в меню «{MENU_BTN}».",
            reply_markup=None,
        )
    else:
        text, kb = closed_card(row)
        await cb.message.edit_text(text, reply_markup=kb)
    await cb.answer("Итог изменён")


@router.callback_query(F.data.startswith("edit:"))
async def on_edit(cb: CallbackQuery, bot: Bot):
    if not await is_admin(bot, cb.from_user.id):
        await cb.answer("Только для администраторов", show_alert=True)
        return
    lead_id = int(cb.data.split(":")[1])
    row = await store.get(lead_id)
    if not row or row["status"] not in ("success", "fail"):
        await cb.answer("Заявка не закрыта", show_alert=True)
        return
    await cb.message.edit_text(
        f"✏️ <b>Заявка #{lead_id}</b>\nВыберите новый итог:",
        reply_markup=edit_kb(lead_id),
    )
    await cb.answer()


@router.callback_query(F.data.startswith("cancel:"))
async def on_cancel_edit(cb: CallbackQuery):
    row = await store.get(int(cb.data.split(":")[1]))
    if not row or row["status"] not in ("success", "fail"):
        await cb.answer("Заявка не закрыта", show_alert=True)
        return
    text, kb = closed_card(row)
    await cb.message.edit_text(text, reply_markup=kb)
    await cb.answer()


@router.callback_query(F.data.startswith("set:"))
async def on_set(cb: CallbackQuery, bot: Bot):
    if not await is_admin(bot, cb.from_user.id):
        await cb.answer("Только для администраторов", show_alert=True)
        return
    _, lead_id, result = cb.data.split(":")
    lead_id = int(lead_id)
    if result == "fail":
        await cb.message.edit_text(
            f"❌ <b>Заявка #{lead_id}</b>\nВыберите причину неудачи:",
            reply_markup=reasons_kb(lead_id, prefix="rwhy", back="edit"),
        )
        await cb.answer()
        return
    if result in ("success", "work"):
        await apply_new_result(cb, bot, lead_id, result, None)
    else:
        await cb.answer()


@router.callback_query(F.data.startswith("rwhy:"))
async def on_new_reason(cb: CallbackQuery, bot: Bot):
    if not await is_admin(bot, cb.from_user.id):
        await cb.answer("Только для администраторов", show_alert=True)
        return
    _, lead_id, code = cb.data.split(":")
    if code not in REASONS:
        await cb.answer()
        return
    await apply_new_result(cb, bot, int(lead_id), "fail", code)


# ---------- Статистика ----------
async def render_stats(code: str):
    rows = await store.leads_since(period_start(code))
    return build_stats(rows, PERIODS[code]), stats_kb(code)


@router.message(F.chat.type == "private", F.text == STATS_BTN)
@router.message(F.chat.type == "private", Command("stats"))
async def on_stats(message: Message, bot: Bot):
    if not await is_admin(bot, message.from_user.id):
        return
    text, kb = await render_stats("day")
    await message.answer(text, reply_markup=kb)


@router.callback_query(F.data.startswith("stats:"))
async def on_stats_period(cb: CallbackQuery, bot: Bot):
    if not await is_admin(bot, cb.from_user.id):
        await cb.answer("Только для администраторов", show_alert=True)
        return
    code = cb.data.split(":")[1]
    if code not in PERIODS:
        await cb.answer()
        return
    text, kb = await render_stats(code)
    try:
        await cb.message.edit_text(text, reply_markup=kb)
    except Exception:
        pass  # тот же текст — Telegram не даёт отредактировать
    await cb.answer()


# ---------- Выгрузка ----------
@router.message(F.chat.type == "private", F.text == EXPORT_BTN)
@router.message(F.chat.type == "private", Command("export"))
async def on_export(message: Message, bot: Bot):
    if not await is_admin(bot, message.from_user.id):
        return
    rows = await store.leads_since(None)
    if not rows:
        await message.answer("Заявок пока нет.")
        return

    status_names = {
        "new": "Не взята",
        "work": "В работе",
        "success": "Успех",
        "fail": "Неудача",
    }
    buf = io.StringIO()
    w = csv.writer(buf, delimiter=";")
    w.writerow(
        ["№", "Создана", "Статус", "Исполнитель", "Причина неудачи", "Закрыта", "Текст"]
    )
    for r in rows:
        w.writerow(
            [
                r["id"],
                to_local(r["created_at"]),
                status_names.get(r["status"], r["status"]),
                (r["owner_name"] or "").replace("&amp;", "&"),
                REASONS.get(r["fail_reason"], "") if r["fail_reason"] else "",
                to_local(r["closed_at"]),
                r["text"] or "",
            ]
        )
    data = buf.getvalue().encode("utf-8-sig")  # utf-8-sig — чтобы Excel не ломал кириллицу
    fname = f"leads_{datetime.now(TZ).strftime('%Y-%m-%d')}.csv"
    await message.answer_document(
        BufferedInputFile(data, filename=fname),
        caption=f"Выгрузка заявок: {len(rows)} шт.",
    )


async def start_health_server():
    """Мини-веб-сервер для хостингов, требующих открытый порт (Render и т.п.)."""
    port = os.environ.get("PORT")
    if not port:
        return
    from aiohttp import web

    async def ok(_):
        return web.Response(text="ok")

    app = web.Application()
    app.router.add_get("/", ok)
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", int(port)).start()


async def main():
    logging.basicConfig(level=logging.INFO)
    await store.init()
    logging.info("Хранилище: %s", "Postgres" if DATABASE_URL else "SQLite (временное)")
    await start_health_server()
    bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp = Dispatcher()
    dp.include_router(router)
    await dp.start_polling(bot, allowed_updates=["message", "callback_query"])


if __name__ == "__main__":
    asyncio.run(main())
