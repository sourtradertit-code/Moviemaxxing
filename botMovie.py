"""
Moviemax Bot — полная версия
aiogram 3 + asyncpg + PostgreSQL (Supabase)
"""

import asyncio
import logging
import os
import ssl
import uuid
from datetime import date, timedelta

import asyncpg
import re
from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError
from aiogram.filters import Command, CommandStart, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    Message,
    ReplyKeyboardMarkup,
)
from aiohttp import web
from thefuzz import fuzz, process as fuzz_process

try:
    import requests as req_lib
    REQUESTS_OK = True
except ImportError:
    REQUESTS_OK = False

# ─────────────────────────────────────────────────────────────────────────────
# Конфигурация
# ─────────────────────────────────────────────────────────────────────────────

BOT_TOKEN    = os.environ["BOT_TOKEN"]
DATABASE_URL = os.environ["BOT_DATABASE_URL"]

# Канал подписки: @username или -1001234567890
_CHANNEL_RAW  = os.environ.get("CHANNEL_ID", "")
try:
    CHANNEL_ID = int(_CHANNEL_RAW) if _CHANNEL_RAW else None
except ValueError:
    CHANNEL_ID = _CHANNEL_RAW or None

CHANNEL_LINK  = os.environ.get("CHANNEL_LINK", "")
_ADMIN_RAW    = os.environ.get("ADMIN_IDS", "").replace(",", " ")
ADMIN_IDS     = {int(x) for x in _ADMIN_RAW.split() if x.strip().lstrip("-").isdigit()}
TMDB_API_KEY  = os.environ.get("TMDB_API_KEY", "72546b58867caa004fac6a5a49f01269")
TMDB_LANG     = "ru-RU"
PAGE_SIZE     = 8
SESSION_ID    = str(uuid.uuid4())

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
log = logging.getLogger("moviemax")

bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp  = Dispatcher(storage=MemoryStorage())
router = Router()
dp.include_router(router)

# ─────────────────────────────────────────────────────────────────────────────
# Пул соединений (SSL для Supabase, statement_cache_size=0 обязателен!)
# ─────────────────────────────────────────────────────────────────────────────

_pool: asyncpg.Pool | None = None


def _parse_db_url(url: str) -> dict:
    """Parse PostgreSQL DSN manually to handle special chars in password."""
    clean = url.split("?")[0]
    m = re.match(r'postgres(?:ql)?://([^:]+):(.+)@([^:@]+):(\d+)/(.+)$', clean)
    if m:
        return dict(user=m.group(1), password=m.group(2),
                    host=m.group(3), port=int(m.group(4)),
                    database=m.group(5))
    return {"dsn": clean}


async def _create_pool() -> asyncpg.Pool:
    url = DATABASE_URL
    kw = dict(min_size=2, max_size=10, statement_cache_size=0,
              max_inactive_connection_lifetime=60, command_timeout=30)
    params = _parse_db_url(url)
    use_ssl = "supabase" in url or "neon.tech" in url or "sslmode=require" in url
    if use_ssl:
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        kw["ssl"] = ctx
    if "dsn" in params:
        return await asyncpg.create_pool(params["dsn"], **kw)
    return await asyncpg.create_pool(**params, **kw)


async def get_pool() -> asyncpg.Pool:
    global _pool
    if _pool is None:
        _pool = await _create_pool()
    return _pool


# ─────────────────────────────────────────────────────────────────────────────
# DB retry (надёжность при обрывах)
# ─────────────────────────────────────────────────────────────────────────────

async def _db_retry(fn, retries: int = 4, delay: float = 0.8):
    global _pool
    last = None
    for attempt in range(retries):
        try:
            return await fn()
        except Exception as e:
            last = e
            log.warning(f"DB ошибка попытка {attempt+1}/{retries}: {type(e).__name__}: {e}")
            try:
                if _pool:
                    await _pool.close()
            except Exception:
                pass
            _pool = None
            await asyncio.sleep(delay * (attempt + 1))
    log.error(f"DB: все {retries} попытки исчерпаны: {last}")
    raise last


class DB:
    async def execute(self, q: str, *a):
        async def _r():
            p = await get_pool()
            async with p.acquire() as c:
                await c.execute(q, *a)
        await _db_retry(_r)

    async def one(self, q: str, *a):
        async def _r():
            p = await get_pool()
            async with p.acquire() as c:
                return await c.fetchrow(q, *a)
        return await _db_retry(_r)

    async def all(self, q: str, *a):
        async def _r():
            p = await get_pool()
            async with p.acquire() as c:
                return await c.fetch(q, *a)
        return await _db_retry(_r)

    async def val(self, q: str, *a):
        async def _r():
            p = await get_pool()
            async with p.acquire() as c:
                return await c.fetchval(q, *a)
        return await _db_retry(_r)

    async def log_query(self, query: str):
        try:
            async def _r():
                p = await get_pool()
                async with p.acquire() as c:
                    await c.execute("INSERT INTO logs (query) VALUES ($1)", query)
                    await c.execute(
                        "INSERT INTO query_stats (query, count) VALUES ($1, 1) "
                        "ON CONFLICT (query) DO UPDATE SET count = query_stats.count + 1", query)
            await _db_retry(_r)
        except Exception:
            pass

    async def add_request(self, title: str):
        try:
            await self.execute(
                "INSERT INTO requested (title, count) VALUES ($1, 1) "
                "ON CONFLICT (title) DO UPDATE SET count = requested.count + 1", title)
        except Exception:
            pass


db = DB()

# ─────────────────────────────────────────────────────────────────────────────
# Инициализация БД
# ─────────────────────────────────────────────────────────────────────────────

async def init_db():
    p = await get_pool()
    async with p.acquire() as c:
        await c.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id        BIGINT PRIMARY KEY,
                username       TEXT,
                first_name     TEXT,
                referral_code  TEXT UNIQUE,
                referred_by    TEXT,
                is_banned      BOOLEAN DEFAULT FALSE,
                requests_count INTEGER DEFAULT 0,
                last_active    TIMESTAMPTZ DEFAULT NOW(),
                created_at     TIMESTAMPTZ DEFAULT NOW()
            )""")
        # Add missing columns to existing tables (safe migrations)
        for col_sql in [
            "ALTER TABLE users ADD COLUMN IF NOT EXISTS username TEXT",
            "ALTER TABLE users ADD COLUMN IF NOT EXISTS first_name TEXT",
            "ALTER TABLE users ADD COLUMN IF NOT EXISTS referral_code TEXT",
            "ALTER TABLE users ADD COLUMN IF NOT EXISTS referred_by TEXT",
            "ALTER TABLE users ADD COLUMN IF NOT EXISTS is_banned BOOLEAN DEFAULT FALSE",
            "ALTER TABLE users ADD COLUMN IF NOT EXISTS requests_count INTEGER DEFAULT 0",
            "ALTER TABLE users ADD COLUMN IF NOT EXISTS last_active TIMESTAMPTZ DEFAULT NOW()",
            "ALTER TABLE users ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ DEFAULT NOW()",
        ]:
            await c.execute(col_sql)
        await c.execute("""
            CREATE TABLE IF NOT EXISTS movies (
                id         SERIAL PRIMARY KEY,
                title      TEXT NOT NULL,
                voice      TEXT DEFAULT 'Дубляж',
                quality    TEXT DEFAULT '720p',
                file_id    TEXT NOT NULL,
                views      INTEGER DEFAULT 0,
                created_at TIMESTAMPTZ DEFAULT NOW(),
                UNIQUE(title, voice, quality)
            )""")
        await c.execute("""
            CREATE TABLE IF NOT EXISTS series (
                id         SERIAL PRIMARY KEY,
                title      TEXT NOT NULL,
                voice      TEXT DEFAULT 'Дубляж',
                quality    TEXT DEFAULT '720p',
                season     INTEGER NOT NULL,
                episode    INTEGER NOT NULL,
                file_id    TEXT NOT NULL,
                created_at TIMESTAMPTZ DEFAULT NOW(),
                UNIQUE(title, voice, quality, season, episode)
            )""")
        await c.execute("""
            CREATE TABLE IF NOT EXISTS referrals (
                id          SERIAL PRIMARY KEY,
                referrer_id BIGINT NOT NULL,
                referred_id BIGINT NOT NULL UNIQUE,
                created_at  TIMESTAMPTZ DEFAULT NOW()
            )""")
        await c.execute("""
            CREATE TABLE IF NOT EXISTS bot_session (
                id INTEGER PRIMARY KEY DEFAULT 1,
                session_id TEXT NOT NULL,
                started_at TIMESTAMPTZ DEFAULT NOW()
            )""")
        await c.execute("""
            CREATE TABLE IF NOT EXISTS ad_channels (
                id           SERIAL PRIMARY KEY,
                channel_id   TEXT NOT NULL UNIQUE,
                channel_name TEXT,
                channel_url  TEXT,
                is_active    BOOLEAN DEFAULT TRUE,
                added_at     TIMESTAMPTZ DEFAULT NOW()
            )""")
        await c.execute("""
            CREATE TABLE IF NOT EXISTS ads (
                id           SERIAL PRIMARY KEY,
                link         TEXT NOT NULL,
                link_type    TEXT NOT NULL,
                display_name TEXT NOT NULL,
                is_active    BOOLEAN DEFAULT TRUE,
                created_at   TIMESTAMPTZ DEFAULT NOW()
            )""")
        await c.execute("""
            CREATE TABLE IF NOT EXISTS logs (
                id        SERIAL PRIMARY KEY,
                query     TEXT,
                timestamp TIMESTAMPTZ DEFAULT NOW()
            )""")
        await c.execute("""
            CREATE TABLE IF NOT EXISTS query_stats (
                query TEXT PRIMARY KEY,
                count INTEGER DEFAULT 1
            )""")
        await c.execute("""
            CREATE TABLE IF NOT EXISTS requested (
                id        SERIAL PRIMARY KEY,
                title     TEXT UNIQUE,
                count     INTEGER DEFAULT 1,
                timestamp TIMESTAMPTZ DEFAULT NOW()
            )""")
        await c.execute("""
            CREATE TABLE IF NOT EXISTS bot_activity (
                id         BIGSERIAL PRIMARY KEY,
                user_id    BIGINT NOT NULL,
                action     TEXT NOT NULL,
                query      TEXT,
                created_at TIMESTAMPTZ DEFAULT NOW()
            )""")
        await c.execute("CREATE INDEX IF NOT EXISTS idx_act_date ON bot_activity (created_at)")
        # Safe column migrations for pre-existing tables
        for col_sql in [
            "ALTER TABLE movies ADD COLUMN IF NOT EXISTS id SERIAL",
            "ALTER TABLE movies ADD COLUMN IF NOT EXISTS voice TEXT DEFAULT 'Дубляж'",
            "ALTER TABLE movies ADD COLUMN IF NOT EXISTS quality TEXT DEFAULT '720p'",
            "ALTER TABLE movies ADD COLUMN IF NOT EXISTS views INTEGER DEFAULT 0",
            "ALTER TABLE movies ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ DEFAULT NOW()",
            "ALTER TABLE series ADD COLUMN IF NOT EXISTS id SERIAL",
            "ALTER TABLE series ADD COLUMN IF NOT EXISTS voice TEXT DEFAULT 'Дубляж'",
            "ALTER TABLE series ADD COLUMN IF NOT EXISTS quality TEXT DEFAULT '720p'",
            "ALTER TABLE series ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ DEFAULT NOW()",
            "ALTER TABLE ad_channels ADD COLUMN IF NOT EXISTS channel_name TEXT",
            "ALTER TABLE ad_channels ADD COLUMN IF NOT EXISTS channel_url TEXT",
            "ALTER TABLE ad_channels ADD COLUMN IF NOT EXISTS is_active BOOLEAN DEFAULT TRUE",
            "ALTER TABLE ad_channels ADD COLUMN IF NOT EXISTS added_at TIMESTAMPTZ DEFAULT NOW()",
            "ALTER TABLE ads ADD COLUMN IF NOT EXISTS display_name TEXT DEFAULT ''",
            "ALTER TABLE ads ADD COLUMN IF NOT EXISTS is_active BOOLEAN DEFAULT TRUE",
            "ALTER TABLE ads ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ DEFAULT NOW()",
        ]:
            try:
                await c.execute(col_sql)
            except Exception:
                pass  # column may already exist or constraint may conflict
    log.info("БД инициализирована")


# ─────────────────────────────────────────────────────────────────────────────
# FSM States
# ─────────────────────────────────────────────────────────────────────────────

class AdminSt(StatesGroup):
    MOVIE_NAME  = State()
    MOVIE_VOICE = State()
    MOVIE_QUAL  = State()
    MOVIE_FILE  = State()

    SERIES_NAME    = State()
    SERIES_VOICE   = State()
    SERIES_QUAL    = State()
    SERIES_SEASON  = State()
    SERIES_VIDEOS  = State()

    BROADCAST = State()

    AD_LINK = State()
    AD_TYPE = State()
    AD_NAME = State()

    ADDCH_ID   = State()
    ADDCH_NAME = State()
    ADDCH_URL  = State()
    DELCH_ID   = State()

    BAN_ID   = State()
    UNBAN_ID = State()


class UserSt(StatesGroup):
    AD_WAIT       = State()
    SEARCH_MOVIE  = State()
    SEARCH_SERIES = State()


# ─────────────────────────────────────────────────────────────────────────────
# Вспомогательные функции
# ─────────────────────────────────────────────────────────────────────────────

def is_admin(uid: int) -> bool:
    return uid in ADMIN_IDS


async def get_or_create_user(uid: int, username: str | None, first_name: str | None):
    await db.execute("""
        INSERT INTO users (user_id, username, first_name, referral_code)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (user_id) DO UPDATE
            SET username = EXCLUDED.username, first_name = EXCLUDED.first_name, last_active = NOW()
    """, uid, username, first_name, str(uid))


async def track(uid: int, action: str, query: str | None = None):
    try:
        p = await get_pool()
        async with p.acquire() as c:
            await c.execute(
                "INSERT INTO bot_activity (user_id, action, query) VALUES ($1, $2, $3)",
                uid, action, query)
            await c.execute(
                "UPDATE users SET requests_count = requests_count + 1, last_active = NOW() WHERE user_id = $1",
                uid)
    except Exception:
        pass


async def is_main_subscribed(uid: int) -> bool:
    if not CHANNEL_ID:
        return True
    try:
        m = await bot.get_chat_member(chat_id=CHANNEL_ID, user_id=uid)
        return m.status in ("creator", "administrator", "member")
    except Exception:
        return True


async def get_unsub_channels(uid: int) -> list:
    channels = await db.all("SELECT * FROM ad_channels WHERE is_active = TRUE ORDER BY id")
    result = []
    for ch in channels:
        try:
            m = await bot.get_chat_member(ch["channel_id"], uid)
            if m.status in ("left", "kicked", "banned"):
                result.append(ch)
        except Exception:
            pass
    return result


async def check_all_subs(uid: int) -> bool:
    if not await is_main_subscribed(uid):
        return False
    return len(await get_unsub_channels(uid)) == 0


def sub_kb(not_sub: list, main_unsub: bool = False) -> InlineKeyboardMarkup:
    btns = []
    if main_unsub and CHANNEL_LINK:
        btns.append([InlineKeyboardButton(text="📢 Подписаться на канал", url=CHANNEL_LINK)])
    for ch in not_sub:
        url = ch["channel_url"] or f"https://t.me/{ch['channel_id'].lstrip('@')}"
        btns.append([InlineKeyboardButton(text=f"📢 {ch['channel_name'] or ch['channel_id']}", url=url)])
    btns.append([InlineKeyboardButton(text="✅ Я подписался, проверить", callback_data="check_sub")])
    return InlineKeyboardMarkup(inline_keyboard=btns)


async def send_sub_alert(target):
    uid = target.from_user.id
    not_sub = await get_unsub_channels(uid)
    main_unsub = not await is_main_subscribed(uid)
    kb = sub_kb(not_sub, main_unsub)
    text = "🚫 Для доступа к библиотеке необходимо подписаться на канал!"
    if isinstance(target, Message):
        await target.answer(text, reply_markup=kb)
    elif isinstance(target, CallbackQuery):
        await target.message.answer(text, reply_markup=kb)


async def user_can_use(uid: int) -> bool:
    if is_admin(uid):
        return True
    if await db.val("SELECT is_banned FROM users WHERE user_id = $1", uid):
        return False
    return await check_all_subs(uid)


async def guard_msg(message: Message) -> bool:
    uid = message.from_user.id
    if await db.val("SELECT is_banned FROM users WHERE user_id = $1", uid):
        await message.answer("🚫 Вы заблокированы в этом боте.")
        return True
    if not is_admin(uid) and not await check_all_subs(uid):
        await send_sub_alert(message)
        return True
    return False


def safe_cb(title: str, prefix: str) -> str:
    short = title[:37] + "..." if len(title) > 40 else title
    return (prefix + short)[:60]


# ─────────────────────────────────────────────────────────────────────────────
# Inline реклама перед видео
# ─────────────────────────────────────────────────────────────────────────────

AD_LABELS = {
    "channel": ("Подписаться на канал 📢", "подпишитесь на канал"),
    "bot":     ("Активировать бота 🤖",    "активируйте бота"),
    "group":   ("Вступить в группу 👥",    "вступите в группу"),
}


async def send_video_with_ad(
    target: Message, state: FSMContext, uid: int,
    file_id: str, caption: str, nav: list | None = None
):
    ad = await db.one("SELECT * FROM ads WHERE is_active = TRUE ORDER BY id DESC LIMIT 1")
    if ad:
        btn_text, action = AD_LABELS.get(ad["link_type"], ("Перейти 🔗", "перейдите"))
        await state.clear()
        await state.update_data(
            pending_file=file_id, pending_cap=caption, pending_nav=nav
        )
        await state.set_state(UserSt.AD_WAIT)
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=btn_text, url=ad["link"])],
            [InlineKeyboardButton(text="✅ Уже готово, давай фильм!", callback_data="ad_done")],
        ])
        await target.answer(
            f"📢 Прежде чем получить контент, {action} <b>{ad['display_name']}</b>!\n\n"
            f"После этого нажми кнопку ниже 👇",
            reply_markup=kb,
        )
    else:
        await _deliver_video(target, file_id, caption, nav)


async def _deliver_video(target: Message, file_id: str, caption: str, nav: list | None):
    try:
        await target.answer_video(file_id, caption=caption)
    except TelegramBadRequest:
        try:
            await target.answer_document(file_id, caption=caption)
        except Exception:
            await target.answer("❌ Ошибка при отправке файла.")
            return
    if nav:
        btns = [InlineKeyboardButton(text=b["text"], callback_data=b["cb"]) for b in nav]
        await target.answer(
            "⏭️ Переключение между сериями:",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[btns])
        )


@router.callback_query(UserSt.AD_WAIT, F.data == "ad_done")
async def ad_done(call: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    file_id = data.get("pending_file")
    if not file_id:
        await call.answer("Выберите фильм заново 🎬", show_alert=True)
        return
    caption = data.get("pending_cap", "")
    nav     = data.get("pending_nav")
    await state.clear()
    await call.message.edit_text("⏳ Загружаем для вас...")
    await asyncio.sleep(1)
    await _deliver_video(call.message, file_id, caption, nav)
    await call.answer()


# ─────────────────────────────────────────────────────────────────────────────
# TMDB поиск
# ─────────────────────────────────────────────────────────────────────────────

def search_tmdb(query: str) -> dict | None:
    """Ищет фильм или сериал на TMDB. Сначала фильмы, потом сериалы."""
    if not REQUESTS_OK or not TMDB_API_KEY:
        return None
    base_params = {"api_key": TMDB_API_KEY, "query": query, "language": TMDB_LANG, "page": 1}
    try:
        # Поиск по фильмам
        r = req_lib.get(
            "https://api.themoviedb.org/3/search/movie",
            params=base_params, timeout=8,
        )
        movie_results = r.json().get("results", [])
        if movie_results:
            m = movie_results[0]
            return {
                "title":    m.get("title") or m.get("original_title"),
                "year":     m.get("release_date", "")[:4],
                "rating":   round(float(m.get("vote_average") or 0), 1),
                "overview": (m.get("overview") or "Нет описания")[:300],
                "type":     "🎬 Фильм",
            }
    except Exception as e:
        log.error(f"TMDB movie: {e}")
    try:
        # Поиск по сериалам
        r = req_lib.get(
            "https://api.themoviedb.org/3/search/tv",
            params=base_params, timeout=8,
        )
        tv_results = r.json().get("results", [])
        if tv_results:
            m = tv_results[0]
            return {
                "title":    m.get("name") or m.get("original_name"),
                "year":     m.get("first_air_date", "")[:4],
                "rating":   round(float(m.get("vote_average") or 0), 1),
                "overview": (m.get("overview") or "Нет описания")[:300],
                "type":     "📺 Сериал",
            }
    except Exception as e:
        log.error(f"TMDB tv: {e}")
    return None


# ─────────────────────────────────────────────────────────────────────────────
# Клавиатуры
# ─────────────────────────────────────────────────────────────────────────────

def main_kb() -> ReplyKeyboardMarkup:
    rows = [
        [KeyboardButton(text="📚 Полный список фильмов"), KeyboardButton(text="📚 Список сериалов")],
        [KeyboardButton(text="🔗 Реферальная ссылка"), KeyboardButton(text="ℹ️ Помощь")],
    ]
    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True)


def admin_kb() -> ReplyKeyboardMarkup:
    rows = [
        [KeyboardButton(text="➕ Добавить Фильм"),   KeyboardButton(text="➕ Добавить Сериал")],
        [KeyboardButton(text="➕ Добавить Сезон"),   KeyboardButton(text="🗑 Удалить Фильм")],
        [KeyboardButton(text="🗑 Удалить Сериал"),   KeyboardButton(text="📣 Рассылка")],
        [KeyboardButton(text="📊 Статистика"),        KeyboardButton(text="🔝 Топ запросов")],
        [KeyboardButton(text="📋 Требуемые фильмы"), KeyboardButton(text="📢 Добавить рекламу")],
        [KeyboardButton(text="🗑 Удалить рекламу")],
        [KeyboardButton(text="🚫 Забанить"),          KeyboardButton(text="✅ Разбанить")],
        [KeyboardButton(text="📚 Полный список фильмов"), KeyboardButton(text="📚 Список сериалов")],
        [KeyboardButton(text="🔙 Назад")],
    ]
    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True)


def quality_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="1080p 🔥", callback_data="q_1080p"),
         InlineKeyboardButton(text="720p", callback_data="q_720p")],
        [InlineKeyboardButton(text="480p", callback_data="q_480p")],
    ])


def voice_kb() -> InlineKeyboardMarkup:
    voices = ["TVShows", "LostFilm", "Дубляж", "Кубик в Кубе", "Пифагор", "Другое"]
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=v, callback_data=f"v_{v}")] for v in voices
    ])


def ad_type_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📢 Канал",      callback_data="adtype_channel")],
        [InlineKeyboardButton(text="🤖 Бот",        callback_data="adtype_bot")],
        [InlineKeyboardButton(text="👥 Группа/Чат", callback_data="adtype_group")],
    ])


def back_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Назад 🔙", callback_data="back_admin")]
    ])


def cancel_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="❌ Отмена")]],
        resize_keyboard=True,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Пагинация
# ─────────────────────────────────────────────────────────────────────────────

async def send_movie_page(target, page: int, items: list):
    total = max(1, (len(items) - 1) // PAGE_SIZE + 1)
    chunk = items[page * PAGE_SIZE: (page + 1) * PAGE_SIZE]
    rows = [
        [InlineKeyboardButton(text=f"🎬 {m['title']} [{m['quality']}]",
                              callback_data=safe_cb(m['title'], "play_m_"))]
        for m in chunk
    ]
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton(text="⬅️", callback_data=f"mlist_{page-1}"))
    if page < total - 1:
        nav.append(InlineKeyboardButton(text="➡️", callback_data=f"mlist_{page+1}"))
    if nav:
        rows.append(nav)
    kb = InlineKeyboardMarkup(inline_keyboard=rows)
    text = f"📚 Фильмы ({page+1}/{total}):"
    if isinstance(target, Message):
        await target.answer(text, reply_markup=kb)
    else:
        await target.message.edit_text(text, reply_markup=kb)


async def send_series_page(target, page: int, items: list):
    total = max(1, (len(items) - 1) // PAGE_SIZE + 1)
    chunk = items[page * PAGE_SIZE: (page + 1) * PAGE_SIZE]
    rows = [
        [InlineKeyboardButton(text=f"📺 {r['title']}", callback_data=f"ps_{r['id']}")]
        for r in chunk
    ]
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton(text="⬅️", callback_data=f"slist_{page-1}"))
    if page < total - 1:
        nav.append(InlineKeyboardButton(text="➡️", callback_data=f"slist_{page+1}"))
    if nav:
        rows.append(nav)
    kb = InlineKeyboardMarkup(inline_keyboard=rows)
    text = f"📚 Сериалы ({page+1}/{total}):"
    if isinstance(target, Message):
        await target.answer(text, reply_markup=kb)
    else:
        await target.message.edit_text(text, reply_markup=kb)


# ─────────────────────────────────────────────────────────────────────────────
# Kill-switch
# ─────────────────────────────────────────────────────────────────────────────

async def acquire_session_lock():
    p = await get_pool()
    async with p.acquire() as c:
        await c.execute("""
            INSERT INTO bot_session (id, session_id, started_at)
            VALUES (1, $1, NOW())
            ON CONFLICT (id) DO UPDATE SET session_id = $1, started_at = NOW()
        """, SESSION_ID)
    log.info(f"Сессия захвачена: {SESSION_ID[:8]}...")


async def session_watchdog():
    await asyncio.sleep(30)
    while True:
        try:
            p = await get_pool()
            async with p.acquire() as c:
                row = await c.fetchrow("SELECT session_id FROM bot_session WHERE id = 1")
            if row and row["session_id"] != SESSION_ID:
                log.warning("Новый экземпляр обнаружен — завершаю этот процесс")
                import os as _os
                _os.kill(_os.getpid(), 15)
                return
        except Exception as e:
            log.warning(f"watchdog: {e}")
        await asyncio.sleep(30)


async def shutdown_warning():
    """Отправляет предупреждение админам через 5ч 30м после запуска."""
    await asyncio.sleep(5.5 * 3600)  # 5 часов 30 минут
    for admin_id in ADMIN_IDS:
        try:
            await bot.send_message(
                admin_id,
                "⚠️ <b>Внимание!</b>\n\n"
                "Бот работает уже <b>5 часов 30 минут</b>.\n"
                "Через 30 минут он может остановиться.\n\n"
                "🔄 Не забудьте перезапустить бота!"
            )
        except Exception as e:
            log.warning(f"shutdown_warning: не удалось отправить {admin_id}: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# /start
# ─────────────────────────────────────────────────────────────────────────────

@router.message(CommandStart())
async def cmd_start(msg: Message):
    uid  = msg.from_user.id
    args = msg.text.split()
    await get_or_create_user(uid, msg.from_user.username, msg.from_user.first_name)
    await track(uid, "start")

    ref_code: str | None = None
    if len(args) > 1:
        arg = args[1]
        ref_code = arg[4:] if arg.startswith("ref_") else arg

    if ref_code and ref_code != str(uid):
        try:
            ref_row = await db.one("SELECT user_id FROM users WHERE referral_code = $1", ref_code)
            if not ref_row:
                ref_row = await db.one("SELECT user_id FROM users WHERE user_id = $1::bigint",
                                       int(ref_code))
            if ref_row:
                referrer_id = ref_row["user_id"]
                await db.execute("""
                    INSERT INTO referrals (referrer_id, referred_id)
                    VALUES ($1, $2) ON CONFLICT (referred_id) DO NOTHING
                """, referrer_id, uid)
                try:
                    await bot.send_message(
                        referrer_id,
                        "🎉 По вашей реферальной ссылке зарегистрировался новый пользователь!"
                    )
                except Exception:
                    pass
        except Exception:
            pass

    if await db.val("SELECT is_banned FROM users WHERE user_id = $1", uid):
        await msg.answer("🚫 Вы заблокированы в этом боте.")
        return

    if not is_admin(uid):
        not_sub = await get_unsub_channels(uid)
        main_unsub = not await is_main_subscribed(uid)
        if not_sub or main_unsub:
            await msg.answer(
                "👋 Привет! Сначала подпишитесь на каналы:",
                reply_markup=sub_kb(not_sub, main_unsub)
            )
            return

    name = msg.from_user.first_name or "друг"
    await msg.answer(
        f"👋 Привет, <b>{name}</b>!\n\n"
        "🎬 Напиши название фильма или сериала, и я найду его!\n"
        "Или выбери из списка ниже 🍿",
        reply_markup=main_kb(),
    )


@router.callback_query(F.data == "check_sub")
async def check_sub_cb(call: CallbackQuery):
    await call.answer()
    uid = call.from_user.id
    not_sub   = await get_unsub_channels(uid)
    main_unsub = not await is_main_subscribed(uid)
    if not_sub or main_unsub:
        await call.message.edit_text(
            "❌ Не все каналы. Подпишитесь и нажмите снова:",
            reply_markup=sub_kb(not_sub, main_unsub)
        )
    else:
        await call.message.edit_text("✅ Подписка подтверждена!")
        name = call.from_user.first_name or "друг"
        await call.message.answer(
            f"👋 Добро пожаловать, <b>{name}</b>!\n\nВоспользуйтесь кнопками ниже:",
            reply_markup=main_kb(),
        )


# ─────────────────────────────────────────────────────────────────────────────
# Команды
# ─────────────────────────────────────────────────────────────────────────────

@router.message(Command("myid"))
async def cmd_myid(msg: Message):
    await msg.answer(f"🆔 Ваш Telegram ID: <code>{msg.from_user.id}</code>")


@router.message(Command("admin"))
@router.message(F.text == "адм")
async def cmd_admin(msg: Message, state: FSMContext):
    if not is_admin(msg.from_user.id):
        return
    await state.clear()
    await msg.answer("👑 Режим администратора:", reply_markup=admin_kb())


@router.message(F.text == "ℹ️ Помощь")
async def cmd_help(msg: Message):
    await msg.answer(
        "🎬 <b>Moviemax Bot</b>\n\n"
        "<b>Как искать:</b>\n"
        "• Напишите название фильма или сериала\n"
        "• Или выберите из списка\n\n"
        "<b>Команды:</b>\n"
        "• /start — перезапуск\n"
        "• /myid — ваш Telegram ID\n\n"
        "По вопросам — обратитесь к администратору."
    )


# ─────────────────────────────────────────────────────────────────────────────
# Реферальная ссылка
# ─────────────────────────────────────────────────────────────────────────────

@router.message(F.text == "🔗 Реферальная ссылка")
async def show_referral(msg: Message):
    uid = msg.from_user.id
    await track(uid, "referral")
    bot_info = await bot.get_me()
    link = f"https://t.me/{bot_info.username}?start=ref_{uid}"
    count = await db.val("SELECT COUNT(*) FROM referrals WHERE referrer_id = $1", uid) or 0
    await msg.answer(
        f"🔗 <b>Ваша реферальная ссылка:</b>\n"
        f"<code>{link}</code>\n\n"
        f"👥 Приглашено: <b>{count}</b> чел."
    )


# ─────────────────────────────────────────────────────────────────────────────
# Списки фильмов / сериалов
# ─────────────────────────────────────────────────────────────────────────────

@router.message(F.text == "📚 Полный список фильмов")
async def show_movie_list(msg: Message):
    if not is_admin(msg.from_user.id) and await guard_msg(msg):
        return
    movies = await db.all("SELECT id, title, quality FROM movies ORDER BY title ASC")
    if not movies:
        await msg.answer("Библиотека фильмов пока пуста. ✨")
        return
    await track(msg.from_user.id, "list_movies")
    await send_movie_page(msg, 0, movies)


@router.message(F.text == "📚 Список сериалов")
async def show_series_list(msg: Message):
    if not is_admin(msg.from_user.id) and await guard_msg(msg):
        return
    series = await db.all("SELECT MIN(id) AS id, title FROM series GROUP BY title ORDER BY title ASC")
    if not series:
        await msg.answer("Библиотека сериалов пока пуста. ✨")
        return
    await track(msg.from_user.id, "list_series")
    await send_series_page(msg, 0, series)


@router.callback_query(F.data.startswith("mlist_"))
async def cb_movie_page(call: CallbackQuery):
    if not is_admin(call.from_user.id) and not await check_all_subs(call.from_user.id):
        await call.answer("Подпишитесь на канал!", show_alert=True)
        return
    page  = int(call.data.split("_")[1])
    items = await db.all("SELECT id, title, quality FROM movies ORDER BY title ASC")
    await send_movie_page(call, page, items)
    await call.answer()


@router.callback_query(F.data.startswith("slist_"))
async def cb_series_page(call: CallbackQuery):
    if not is_admin(call.from_user.id) and not await check_all_subs(call.from_user.id):
        await call.answer("Подпишитесь на канал!", show_alert=True)
        return
    page  = int(call.data.split("_")[1])
    items = await db.all("SELECT MIN(id) AS id, title FROM series GROUP BY title ORDER BY title ASC")
    await send_series_page(call, page, items)
    await call.answer()


# ─────────────────────────────────────────────────────────────────────────────
# Просмотр фильма из списка
# ─────────────────────────────────────────────────────────────────────────────

@router.callback_query(F.data.startswith("play_m_"))
async def play_movie_cb(call: CallbackQuery, state: FSMContext):
    if not is_admin(call.from_user.id) and not await check_all_subs(call.from_user.id):
        await send_sub_alert(call)
        await call.answer()
        return
    title_cb = call.data[len("play_m_"):]
    if title_cb.endswith("..."):
        movie = await db.one("SELECT * FROM movies WHERE title LIKE $1", title_cb[:-3] + "%")
    else:
        movie = await db.one("SELECT * FROM movies WHERE title = $1", title_cb)
    if not movie:
        await call.answer("Фильм не найден", show_alert=True)
        return
    await db.execute("UPDATE movies SET views = views + 1 WHERE id = $1", movie["id"])
    await track(call.from_user.id, "get_movie", movie["title"])
    caption = (
        f"🍿 <b>{movie['title']}</b>\n"
        f"🎧 Озвучка: {movie['voice']}\n"
        f"💎 Качество: {movie['quality']}\n\n"
        f"✨ Приятного просмотра! 🎬🛋️🔥"
    )
    await send_video_with_ad(call.message, state, call.from_user.id, movie["file_id"], caption)
    await call.answer()


# ─────────────────────────────────────────────────────────────────────────────
# Сериалы: сезоны → серии → просмотр
# ─────────────────────────────────────────────────────────────────────────────

@router.callback_query(F.data.startswith("ps_"))
async def cb_series_seasons(call: CallbackQuery):
    if not is_admin(call.from_user.id) and not await check_all_subs(call.from_user.id):
        await send_sub_alert(call)
        await call.answer()
        return
    sid = int(call.data.split("_")[1])
    row = await db.one("SELECT title FROM series WHERE id = $1", sid)
    if not row:
        await call.answer("Сериал не найден", show_alert=True)
        return
    title = row["title"]
    seasons = await db.all(
        "SELECT DISTINCT season FROM series WHERE title = $1 ORDER BY season ASC", title)
    kb = InlineKeyboardMarkup(inline_keyboard=[
        *[[InlineKeyboardButton(text=f"📺 Сезон {s['season']}",
                                callback_data=f"seas_{sid}_{s['season']}")] for s in seasons],
        [InlineKeyboardButton(text="🔙 К списку", callback_data="slist_0")],
    ])
    await call.message.edit_text(
        f"🎬 Сериал: <b>{title}</b>\n\nВыберите сезон:", reply_markup=kb)
    await call.answer()


@router.callback_query(F.data.startswith("seas_"))
async def cb_season_episodes(call: CallbackQuery):
    if not is_admin(call.from_user.id) and not await check_all_subs(call.from_user.id):
        await send_sub_alert(call)
        await call.answer()
        return
    _, sid, season = call.data.split("_")
    sid, season = int(sid), int(season)
    row = await db.one("SELECT title FROM series WHERE id = $1", sid)
    if not row:
        await call.answer("Сериал не найден", show_alert=True)
        return
    title = row["title"]
    eps = await db.all(
        "SELECT id, episode FROM series WHERE title = $1 AND season = $2 ORDER BY episode ASC",
        title, season)
    kb = InlineKeyboardMarkup(inline_keyboard=[
        *[[InlineKeyboardButton(text=f"🎞️ Серия {ep['episode']}",
                                callback_data=f"ep_{ep['id']}")] for ep in eps],
        [InlineKeyboardButton(text="🔙 К сезонам", callback_data=f"ps_{sid}")],
    ])
    await call.message.edit_text(
        f"🎬 <b>{title}</b> — Сезон {season}", reply_markup=kb)
    await call.answer()


@router.callback_query(F.data.startswith("ep_"))
async def cb_play_episode(call: CallbackQuery, state: FSMContext):
    if not is_admin(call.from_user.id) and not await check_all_subs(call.from_user.id):
        await send_sub_alert(call)
        await call.answer()
        return
    row_id = int(call.data.split("_")[1])
    info = await db.one(
        "SELECT title, season, episode, voice, quality, file_id FROM series WHERE id = $1", row_id)
    if not info:
        await call.answer("Серия не найдена", show_alert=True)
        return
    caption = (
        f"🎬 <b>{info['title']}</b>\n"
        f"📺 Сезон {info['season']} • Серия {info['episode']}\n"
        f"🎧 {info['voice']} • 💎 {info['quality']}\n\n"
        f"✨ Приятного просмотра! 🔥"
    )
    eps = await db.all(
        "SELECT id, episode FROM series WHERE title = $1 AND season = $2 ORDER BY episode ASC",
        info["title"], info["season"])
    cur = next((i for i, e in enumerate(eps) if e["id"] == row_id), None)
    nav = []
    if cur is not None and len(eps) > 1:
        if cur > 0:
            nav.append({"text": "⬅️ Предыдущая", "cb": f"ep_{eps[cur-1]['id']}"})
        if cur < len(eps) - 1:
            nav.append({"text": "Следующая ➡️", "cb": f"ep_{eps[cur+1]['id']}"})
    await track(call.from_user.id, "get_episode", f"{info['title']} S{info['season']}E{info['episode']}")
    await send_video_with_ad(call.message, state, call.from_user.id, info["file_id"], caption, nav or None)
    await call.answer()


# ─────────────────────────────────────────────────────────────────────────────
# ADMIN PANEL
# ─────────────────────────────────────────────────────────────────────────────

@router.message(F.text == "🔙 Назад")
async def back_main(msg: Message, state: FSMContext):
    await state.clear()
    await msg.answer("Главное меню:", reply_markup=main_kb())


@router.callback_query(F.data == "back_admin")
async def back_admin_cb(call: CallbackQuery, state: FSMContext):
    await state.clear()
    try:
        await call.message.edit_text("✅ Используйте кнопки ниже")
    except Exception:
        pass
    await call.answer()


# ─── Статистика ───────────────────────────────────────────────────────────────

@router.message(F.text == "📊 Статистика")
@router.message(Command("stats"))
async def admin_stats(msg: Message):
    if not is_admin(msg.from_user.id):
        return
    today = date.today()

    total_u   = await db.val("SELECT COUNT(*) FROM users")
    act_u     = await db.val("SELECT COUNT(*) FROM users WHERE is_banned = FALSE")
    ban_u     = await db.val("SELECT COUNT(*) FROM users WHERE is_banned = TRUE")
    new_1d    = await db.val("SELECT COUNT(*) FROM users WHERE created_at >= NOW() - INTERVAL '1 day'")
    new_7d    = await db.val("SELECT COUNT(*) FROM users WHERE created_at >= NOW() - INTERVAL '7 days'")
    new_30d   = await db.val("SELECT COUNT(*) FROM users WHERE created_at >= NOW() - INTERVAL '30 days'")
    act_today = await db.val(
        "SELECT COUNT(DISTINCT user_id) FROM bot_activity WHERE created_at::date = $1", today)
    req_today = await db.val(
        "SELECT COUNT(*) FROM bot_activity WHERE created_at::date = $1", today)
    req_total = await db.val("SELECT COUNT(*) FROM bot_activity")
    n_movies  = await db.val("SELECT COUNT(*) FROM movies")
    n_series  = await db.val("SELECT COUNT(DISTINCT title) FROM series")
    n_refs    = await db.val("SELECT COUNT(*) FROM referrals")

    top_q = await db.all("SELECT query, count FROM query_stats ORDER BY count DESC LIMIT 5")
    top_m = await db.all("SELECT title, views FROM movies ORDER BY views DESC LIMIT 5")

    tq = "\n".join(f"  {i+1}. {r['query']} — {r['count']}×" for i, r in enumerate(top_q)) or "  нет данных"
    tm = "\n".join(f"  {i+1}. {r['title']} — {r['views']}👁" for i, r in enumerate(top_m)) or "  нет данных"

    await msg.answer(
        f"📊 <b>Статистика Moviemax</b>\n\n"
        f"<b>👤 Пользователи:</b>\n"
        f"  Всего: <b>{total_u}</b>  |  Активных: <b>{act_u}</b>  |  Бан: <b>{ban_u}</b>\n"
        f"  Новых за 24ч: <b>{new_1d}</b>  |  7д: <b>{new_7d}</b>  |  30д: <b>{new_30d}</b>\n\n"
        f"<b>📈 Активность:</b>\n"
        f"  Онлайн сегодня: <b>{act_today}</b>\n"
        f"  Запросов сегодня: <b>{req_today}</b>  |  Всего: <b>{req_total}</b>\n\n"
        f"<b>🎬 Контент:</b>\n"
        f"  Фильмов: <b>{n_movies}</b>  |  Сериалов: <b>{n_series}</b>\n\n"
        f"<b>👥 Рефералы:</b> <b>{n_refs}</b>\n\n"
        f"<b>🔍 Топ запросов:</b>\n{tq}\n\n"
        f"<b>🏆 Топ просмотров:</b>\n{tm}"
    )


@router.message(F.text == "🔝 Топ запросов")
async def admin_top_queries(msg: Message):
    if not is_admin(msg.from_user.id):
        return
    top = await db.all("SELECT query, count FROM query_stats ORDER BY count DESC LIMIT 10")
    text = "🔝 <b>Топ-10 запросов:</b>\n\n"
    text += "\n".join(f"{i+1}. {r['query']} — <b>{r['count']}</b> раз"
                      for i, r in enumerate(top)) if top else "Пусто."
    await msg.answer(text)


@router.message(F.text == "📋 Требуемые фильмы")
async def admin_requested(msg: Message):
    if not is_admin(msg.from_user.id):
        return
    rows = await db.all("SELECT title, count FROM requested ORDER BY count DESC")
    if not rows:
        await msg.answer("📋 Запрошенных фильмов нет.")
        return
    text = "📋 <b>Требуемые фильмы:</b>\n\n"
    text += "\n".join(f"{i+1}. {r['title']} — {r['count']} раз" for i, r in enumerate(rows))
    await msg.answer(text)


# ─── Добавить фильм ───────────────────────────────────────────────────────────

@router.message(F.text == "➕ Добавить Фильм")
async def admin_add_movie_btn(msg: Message, state: FSMContext):
    if not is_admin(msg.from_user.id):
        return
    await state.set_state(AdminSt.MOVIE_NAME)
    await msg.answer("🎬 Введите название фильма:", reply_markup=cancel_kb())


@router.message(AdminSt.MOVIE_NAME)
async def admin_movie_name(msg: Message, state: FSMContext):
    if msg.text == "❌ Отмена":
        await state.clear()
        await msg.answer("Отменено.", reply_markup=admin_kb())
        return
    await state.update_data(name=msg.text.strip())
    await msg.answer("🎧 Выберите озвучку:", reply_markup=voice_kb())
    await state.set_state(AdminSt.MOVIE_VOICE)


@router.callback_query(AdminSt.MOVIE_VOICE, F.data.startswith("v_"))
async def admin_movie_voice(call: CallbackQuery, state: FSMContext):
    voice = call.data.split("_", 1)[1]
    await state.update_data(voice=voice)
    await call.message.edit_text(
        f"✅ Озвучка: <b>{voice}</b>\n\n💎 Выберите качество:", reply_markup=quality_kb())
    await state.set_state(AdminSt.MOVIE_QUAL)
    await call.answer()


@router.callback_query(AdminSt.MOVIE_QUAL, F.data.startswith("q_"))
async def admin_movie_quality(call: CallbackQuery, state: FSMContext):
    quality = call.data.split("_", 1)[1]
    await state.update_data(quality=quality)
    await call.message.edit_text(f"✅ Качество: <b>{quality}</b>\n\n📤 Отправьте видеофайл:")
    await state.set_state(AdminSt.MOVIE_FILE)
    await call.answer()


@router.message(AdminSt.MOVIE_FILE)
async def admin_movie_file(msg: Message, state: FSMContext):
    if msg.text == "❌ Отмена":
        await state.clear()
        await msg.answer("Отменено.", reply_markup=admin_kb())
        return
    file_obj = msg.video or msg.document
    if not file_obj:
        await msg.answer("❌ Отправьте видео или документ.")
        return
    file_id = file_obj.file_id
    data = await state.get_data()
    await state.clear()
    try:
        await db.execute(
            "INSERT INTO movies (title, voice, quality, file_id) VALUES ($1, $2, $3, $4)",
            data["name"], data["voice"], data["quality"], file_id)
        await msg.answer(
            f"✅ Фильм добавлен!\n"
            f"🎬 <b>{data['name']}</b> | {data['voice']} | {data['quality']}",
            reply_markup=admin_kb())
    except Exception as e:
        if "unique" in str(e).lower():
            await msg.answer("⚠️ Фильм с такими параметрами уже существует.", reply_markup=admin_kb())
        else:
            await msg.answer(f"❌ Ошибка: {e}", reply_markup=admin_kb())


# ─── Добавить сериал ──────────────────────────────────────────────────────────

@router.message(F.text == "➕ Добавить Сериал")
async def admin_add_series_btn(msg: Message, state: FSMContext):
    if not is_admin(msg.from_user.id):
        return
    await state.set_state(AdminSt.SERIES_NAME)
    await msg.answer("🎬 Введите название сериала:", reply_markup=cancel_kb())


@router.message(F.text == "➕ Добавить Сезон")
async def admin_add_season_btn(msg: Message, state: FSMContext):
    if not is_admin(msg.from_user.id):
        return
    rows = await db.all("SELECT MIN(id) AS id, title FROM series GROUP BY title ORDER BY title ASC")
    if not rows:
        await msg.answer("Нет сериалов. Сначала добавьте сериал через «➕ Добавить Сериал».")
        return
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"📺 {r['title']}", callback_data=f"ases_{r['id']}")] for r in rows
    ])
    await msg.answer("Выберите сериал для добавления сезона:", reply_markup=kb)


@router.callback_query(F.data.startswith("ases_"))
async def admin_add_season_pick(call: CallbackQuery, state: FSMContext):
    sid = int(call.data.split("_")[1])
    row = await db.one("SELECT title FROM series WHERE id = $1", sid)
    if not row:
        await call.answer("Сериал не найден", show_alert=True)
        return
    await state.update_data(name=row["title"])
    await call.message.answer(
        f"✅ Сериал: <b>{row['title']}</b>\n\n🎧 Выберите озвучку:", reply_markup=voice_kb())
    await state.set_state(AdminSt.SERIES_VOICE)
    await call.answer()


@router.message(AdminSt.SERIES_NAME)
async def admin_series_name(msg: Message, state: FSMContext):
    if msg.text == "❌ Отмена":
        await state.clear()
        await msg.answer("Отменено.", reply_markup=admin_kb())
        return
    await state.update_data(name=msg.text.strip())
    await msg.answer("🎧 Выберите озвучку:", reply_markup=voice_kb())
    await state.set_state(AdminSt.SERIES_VOICE)


@router.callback_query(AdminSt.SERIES_VOICE, F.data.startswith("v_"))
async def admin_series_voice(call: CallbackQuery, state: FSMContext):
    voice = call.data.split("_", 1)[1]
    await state.update_data(voice=voice)
    await call.message.edit_text(
        f"✅ Озвучка: <b>{voice}</b>\n\n💎 Выберите качество:", reply_markup=quality_kb())
    await state.set_state(AdminSt.SERIES_QUAL)
    await call.answer()


@router.callback_query(AdminSt.SERIES_QUAL, F.data.startswith("q_"))
async def admin_series_quality(call: CallbackQuery, state: FSMContext):
    quality = call.data.split("_", 1)[1]
    await state.update_data(quality=quality)
    await call.message.edit_text(f"✅ Качество: <b>{quality}</b>\n\n📅 Введите номер сезона:")
    await state.set_state(AdminSt.SERIES_SEASON)
    await call.answer()


@router.message(AdminSt.SERIES_SEASON)
async def admin_series_season(msg: Message, state: FSMContext):
    if not msg.text.isdigit():
        await msg.answer("Введите число:")
        return
    await state.update_data(season=int(msg.text), ep_counter=1)
    await msg.answer(
        "📤 Отправляйте серии по порядку.\nКогда закончите — напишите <b>Готово</b>",
    )
    await state.set_state(AdminSt.SERIES_VIDEOS)


@router.message(AdminSt.SERIES_VIDEOS)
async def admin_series_video(msg: Message, state: FSMContext):
    if msg.text and msg.text.strip().lower() == "готово":
        await msg.answer("🎉 Сериал успешно добавлен!", reply_markup=admin_kb())
        await state.clear()
        return
    file_obj = msg.video or msg.document
    if not file_obj:
        await msg.answer("Отправьте видеофайл или напишите «Готово».")
        return
    file_id = file_obj.file_id
    data = await state.get_data()
    ep = data["ep_counter"]
    try:
        await db.execute(
            "INSERT INTO series (title, voice, quality, season, episode, file_id) "
            "VALUES ($1, $2, $3, $4, $5, $6)",
            data["name"], data["voice"], data["quality"], data["season"], ep, file_id)
        await msg.answer(f"✅ Серия {ep} добавлена!")
    except Exception:
        await msg.answer(f"⚠️ Серия {ep} уже существует — пропускаю.")
    await state.update_data(ep_counter=ep + 1)


# ─── Удаление фильма ──────────────────────────────────────────────────────────

@router.message(F.text == "🗑 Удалить Фильм")
async def admin_del_movie_list(msg: Message):
    if not is_admin(msg.from_user.id):
        return
    movies = await db.all("SELECT id, title FROM movies ORDER BY title ASC")
    if not movies:
        await msg.answer("Фильмов нет.")
        return
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"🗑 {m['title']}", callback_data=f"del_m_{m['title'][:40]}")] for m in movies
    ])
    await msg.answer("Выберите фильм для удаления:", reply_markup=kb)


@router.callback_query(F.data.startswith("del_m_"))
async def admin_del_movie_exec(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        await call.answer("Нет доступа", show_alert=True)
        return
    title_cb = call.data[len("del_m_"):]
    row = await db.one("SELECT title FROM movies WHERE title LIKE $1", title_cb + "%")
    if not row:
        await call.answer("Не найден.", show_alert=True)
        return
    await db.execute("DELETE FROM movies WHERE title = $1", row["title"])
    await call.answer(f"«{row['title']}» удалён!")
    try:
        await call.message.edit_text(f"✅ Фильм «{row['title']}» удалён.")
    except Exception:
        pass


# ─── Удаление сериала (гранулярно) ───────────────────────────────────────────

@router.message(F.text == "🗑 Удалить Сериал")
async def admin_del_series_list(msg: Message):
    if not is_admin(msg.from_user.id):
        return
    rows = await db.all("SELECT MIN(id) AS id, title FROM series GROUP BY title ORDER BY title ASC")
    if not rows:
        await msg.answer("Сериалов нет.")
        return
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"📺 {r['title']}", callback_data=f"dss_{r['id']}")] for r in rows
    ])
    await msg.answer("Выберите сериал:", reply_markup=kb)


@router.callback_query(F.data == "delete_series")
async def admin_del_series_cb(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        await call.answer("Нет доступа", show_alert=True)
        return
    rows = await db.all("SELECT MIN(id) AS id, title FROM series GROUP BY title ORDER BY title ASC")
    if not rows:
        await call.message.edit_text("Сериалов нет.", reply_markup=back_kb())
        return
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"📺 {r['title']}", callback_data=f"dss_{r['id']}")] for r in rows
    ])
    await call.message.edit_text("Выберите сериал:", reply_markup=kb)
    await call.answer()


@router.callback_query(F.data.startswith("dss_"))
async def admin_del_seasons(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        await call.answer("Нет доступа", show_alert=True)
        return
    sid = int(call.data.split("_")[1])
    row = await db.one("SELECT title FROM series WHERE id = $1", sid)
    if not row:
        await call.answer("Не найден", show_alert=True)
        return
    title = row["title"]
    seasons = await db.all("SELECT DISTINCT season FROM series WHERE title = $1 ORDER BY season", title)
    kb = InlineKeyboardMarkup(inline_keyboard=[
        *[[InlineKeyboardButton(text=f"📂 Сезон {s['season']}",
                                callback_data=f"dse_{sid}_{s['season']}")] for s in seasons],
        [InlineKeyboardButton(text="🗑 Удалить весь сериал", callback_data=f"das_{sid}")],
        [InlineKeyboardButton(text="Назад 🔙", callback_data="delete_series")],
    ])
    await call.message.edit_text(
        f"📺 <b>{title}</b>\n\nВыберите сезон:", reply_markup=kb)
    await call.answer()


@router.callback_query(F.data.startswith("das_"))
async def admin_del_all_series(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        await call.answer("Нет доступа", show_alert=True)
        return
    sid = int(call.data.split("_")[1])
    row = await db.one("SELECT title FROM series WHERE id = $1", sid)
    if not row:
        await call.answer("Не найден", show_alert=True)
        return
    await db.execute("DELETE FROM series WHERE title = $1", row["title"])
    await call.answer(f"Сериал «{row['title']}» удалён!", show_alert=True)
    try:
        await call.message.edit_text(f"✅ Сериал «{row['title']}» удалён.")
    except Exception:
        pass


@router.callback_query(F.data.startswith("dse_"))
async def admin_del_episodes(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        await call.answer("Нет доступа", show_alert=True)
        return
    _, sid, season = call.data.split("_")
    sid, season = int(sid), int(season)
    row = await db.one("SELECT title FROM series WHERE id = $1", sid)
    if not row:
        await call.answer("Не найден", show_alert=True)
        return
    title = row["title"]
    eps = await db.all(
        "SELECT id, episode FROM series WHERE title = $1 AND season = $2 ORDER BY episode", title, season)
    kb = InlineKeyboardMarkup(inline_keyboard=[
        *[[InlineKeyboardButton(text=f"🗑 Серия {ep['episode']}",
                                callback_data=f"dep_{ep['id']}")] for ep in eps],
        [InlineKeyboardButton(text=f"🗑 Удалить весь сезон {season}",
                              callback_data=f"dsa_{sid}_{season}")],
        [InlineKeyboardButton(text="Назад 🔙", callback_data=f"dss_{sid}")],
    ])
    await call.message.edit_text(
        f"📺 <b>{title}</b> — Сезон {season}\n\nВыберите серию:", reply_markup=kb)
    await call.answer()


@router.callback_query(F.data.startswith("dsa_"))
async def admin_del_season_all(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        await call.answer("Нет доступа", show_alert=True)
        return
    _, sid, season = call.data.split("_")
    sid, season = int(sid), int(season)
    row = await db.one("SELECT title FROM series WHERE id = $1", sid)
    if not row:
        await call.answer("Не найден", show_alert=True)
        return
    await db.execute("DELETE FROM series WHERE title = $1 AND season = $2", row["title"], season)
    await call.answer(f"Сезон {season} удалён!", show_alert=True)
    seasons = await db.all("SELECT DISTINCT season FROM series WHERE title = $1 ORDER BY season", row["title"])
    if seasons:
        kb = InlineKeyboardMarkup(inline_keyboard=[
            *[[InlineKeyboardButton(text=f"📂 Сезон {s['season']}",
                                    callback_data=f"dse_{sid}_{s['season']}")] for s in seasons],
            [InlineKeyboardButton(text="🗑 Удалить весь сериал", callback_data=f"das_{sid}")],
            [InlineKeyboardButton(text="Назад 🔙", callback_data="delete_series")],
        ])
        await call.message.edit_text(f"📺 <b>{row['title']}</b>", reply_markup=kb)
    else:
        await call.message.edit_text(f"✅ Сериал «{row['title']}» полностью удалён.")


@router.callback_query(F.data.startswith("dep_"))
async def admin_del_one_episode(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        await call.answer("Нет доступа", show_alert=True)
        return
    row_id = int(call.data.split("_")[1])
    info = await db.one("SELECT title, season, episode FROM series WHERE id = $1", row_id)
    if not info:
        await call.answer("Не найдена", show_alert=True)
        return
    await db.execute("DELETE FROM series WHERE id = $1", row_id)
    await call.answer(f"Серия {info['episode']} удалена!", show_alert=True)
    sid_row = await db.one(
        "SELECT MIN(id) AS id FROM series WHERE title = $1 AND season = $2",
        info["title"], info["season"])
    if sid_row and sid_row["id"]:
        eps = await db.all(
            "SELECT id, episode FROM series WHERE title = $1 AND season = $2 ORDER BY episode",
            info["title"], info["season"])
        sid = sid_row["id"]
        kb = InlineKeyboardMarkup(inline_keyboard=[
            *[[InlineKeyboardButton(text=f"🗑 Серия {ep['episode']}",
                                    callback_data=f"dep_{ep['id']}")] for ep in eps],
            [InlineKeyboardButton(text=f"🗑 Весь сезон {info['season']}",
                                  callback_data=f"dsa_{sid}_{info['season']}")],
            [InlineKeyboardButton(text="Назад 🔙", callback_data=f"dss_{sid}")],
        ])
        try:
            await call.message.edit_text(
                f"📺 <b>{info['title']}</b> — Сезон {info['season']}", reply_markup=kb)
        except Exception:
            pass
    else:
        try:
            await call.message.edit_text(f"✅ Все серии сезона удалены.")
        except Exception:
            pass


# ─── Бан / Разбан ─────────────────────────────────────────────────────────────

@router.message(F.text == "🚫 Забанить")
async def admin_ban_start(msg: Message, state: FSMContext):
    if not is_admin(msg.from_user.id):
        return
    await state.set_state(AdminSt.BAN_ID)
    await msg.answer("Введите Telegram ID для бана:", reply_markup=cancel_kb())


@router.message(AdminSt.BAN_ID)
async def admin_ban_exec(msg: Message, state: FSMContext):
    if msg.text == "❌ Отмена":
        await state.clear()
        await msg.answer("Отменено.", reply_markup=admin_kb())
        return
    if not msg.text.lstrip("-").isdigit():
        await msg.answer("Введите числовой ID.")
        return
    uid = int(msg.text.strip())
    await state.clear()
    await db.execute("UPDATE users SET is_banned = TRUE WHERE user_id = $1", uid)
    await msg.answer(f"✅ <code>{uid}</code> заблокирован.", reply_markup=admin_kb())
    try:
        await bot.send_message(uid, "🚫 Вы заблокированы в этом боте.")
    except Exception:
        pass


@router.message(F.text == "✅ Разбанить")
async def admin_unban_start(msg: Message, state: FSMContext):
    if not is_admin(msg.from_user.id):
        return
    await state.set_state(AdminSt.UNBAN_ID)
    await msg.answer("Введите Telegram ID для разбана:", reply_markup=cancel_kb())


@router.message(AdminSt.UNBAN_ID)
async def admin_unban_exec(msg: Message, state: FSMContext):
    if msg.text == "❌ Отмена":
        await state.clear()
        await msg.answer("Отменено.", reply_markup=admin_kb())
        return
    if not msg.text.lstrip("-").isdigit():
        await msg.answer("Введите числовой ID.")
        return
    uid = int(msg.text.strip())
    await state.clear()
    await db.execute("UPDATE users SET is_banned = FALSE WHERE user_id = $1", uid)
    await msg.answer(f"✅ <code>{uid}</code> разблокирован.", reply_markup=admin_kb())
    try:
        await bot.send_message(uid, "✅ Вы разблокированы. Введите /start")
    except Exception:
        pass


@router.message(Command("ban"))
async def cmd_ban(msg: Message):
    if not is_admin(msg.from_user.id):
        return
    parts = msg.text.split()
    if len(parts) < 2 or not parts[1].lstrip("-").isdigit():
        await msg.answer("Использование: <code>/ban USER_ID</code>")
        return
    await db.execute("UPDATE users SET is_banned = TRUE WHERE user_id = $1", int(parts[1]))
    await msg.answer(f"✅ <code>{parts[1]}</code> заблокирован.")


@router.message(Command("unban"))
async def cmd_unban(msg: Message):
    if not is_admin(msg.from_user.id):
        return
    parts = msg.text.split()
    if len(parts) < 2 or not parts[1].lstrip("-").isdigit():
        await msg.answer("Использование: <code>/unban USER_ID</code>")
        return
    await db.execute("UPDATE users SET is_banned = FALSE WHERE user_id = $1", int(parts[1]))
    await msg.answer(f"✅ <code>{parts[1]}</code> разблокирован.")


# ─── Рассылка ─────────────────────────────────────────────────────────────────

@router.message(F.text == "📣 Рассылка")
async def admin_broadcast_start(msg: Message, state: FSMContext):
    if not is_admin(msg.from_user.id):
        return
    await state.set_state(AdminSt.BROADCAST)
    await msg.answer("✍️ Отправьте сообщение (текст/фото/видео):", reply_markup=cancel_kb())


@router.message(AdminSt.BROADCAST)
async def admin_broadcast_exec(msg: Message, state: FSMContext):
    if msg.text == "❌ Отмена":
        await state.clear()
        await msg.answer("Отменено.", reply_markup=admin_kb())
        return
    await state.clear()
    users = await db.all("SELECT user_id FROM users WHERE is_banned = FALSE")
    ok = fail = 0
    status = await msg.answer(f"📤 Рассылаю {len(users)} пользователям...")
    for i, u in enumerate(users):
        try:
            await msg.copy_to(u["user_id"])
            ok += 1
            await asyncio.sleep(0.04)
        except Exception:
            fail += 1
        if (i + 1) % 50 == 0:
            try:
                await status.edit_text(f"📤 {ok}/{len(users)}...")
            except Exception:
                pass
    await status.edit_text(f"✅ Готово! Отправлено: <b>{ok}</b>, ошибок: <b>{fail}</b>")
    await msg.answer("Рассылка завершена.", reply_markup=admin_kb())


# ─── Inline реклама (добавить/удалить) ────────────────────────────────────────

@router.message(F.text == "📢 Добавить рекламу")
async def admin_add_ad_btn(msg: Message, state: FSMContext):
    if not is_admin(msg.from_user.id):
        return
    await state.set_state(AdminSt.AD_LINK)
    await msg.answer(
        "📢 <b>Добавление рекламы перед видео</b>\n\n"
        "Отправьте ссылку (https://...):",
        reply_markup=cancel_kb()
    )


@router.message(AdminSt.AD_LINK)
async def admin_ad_link(msg: Message, state: FSMContext):
    if msg.text == "❌ Отмена":
        await state.clear()
        await msg.answer("Отменено.", reply_markup=admin_kb())
        return
    if not msg.text.startswith("http"):
        await msg.answer("❌ Ссылка должна начинаться с http.")
        return
    await state.update_data(ad_link=msg.text.strip())
    await msg.answer("На что ведёт ссылка?", reply_markup=ad_type_kb())
    await state.set_state(AdminSt.AD_TYPE)


@router.callback_query(AdminSt.AD_TYPE, F.data.startswith("adtype_"))
async def admin_ad_type(call: CallbackQuery, state: FSMContext):
    ad_type = call.data.split("_", 1)[1]
    await state.update_data(ad_type=ad_type)
    labels = {"channel": "канал", "bot": "бот", "group": "группу/чат"}
    await call.message.edit_text(
        f"✅ Тип: <b>{labels.get(ad_type, ad_type)}</b>\n\n"
        f"Введите название (увидят пользователи):"
    )
    await state.set_state(AdminSt.AD_NAME)
    await call.answer()


@router.message(AdminSt.AD_NAME)
async def admin_ad_name(msg: Message, state: FSMContext):
    data = await state.get_data()
    await state.clear()
    await db.execute("UPDATE ads SET is_active = FALSE WHERE is_active = TRUE")
    await db.execute(
        "INSERT INTO ads (link, link_type, display_name, is_active) VALUES ($1, $2, $3, TRUE)",
        data["ad_link"], data["ad_type"], msg.text.strip())
    await msg.answer(
        f"✅ <b>Реклама активирована!</b>\n\n"
        f"📌 <b>{msg.text.strip()}</b>\n"
        f"🔗 {data['ad_link']}",
        reply_markup=admin_kb()
    )


@router.message(F.text == "🗑 Удалить рекламу")
async def admin_remove_ad_btn(msg: Message):
    if not is_admin(msg.from_user.id):
        return
    ad = await db.one("SELECT * FROM ads WHERE is_active = TRUE ORDER BY id DESC LIMIT 1")
    if not ad:
        await msg.answer("Активной рекламы нет.")
        return
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"🗑 Удалить «{ad['display_name']}»",
                              callback_data=f"del_ad_{ad['id']}")]
    ])
    await msg.answer(
        f"📢 Текущая реклама: <b>{ad['display_name']}</b>\n🔗 {ad['link']}",
        reply_markup=kb
    )


@router.callback_query(F.data.startswith("del_ad_"))
async def admin_del_ad_exec(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        await call.answer("Нет доступа", show_alert=True)
        return
    ad_id = int(call.data.split("_")[2])
    await db.execute("UPDATE ads SET is_active = FALSE WHERE id = $1", ad_id)
    await call.message.edit_text("✅ Реклама отключена.")
    await call.answer()


# ─── Каналы обязательной подписки ────────────────────────────────────────────

@router.message(Command("adchannels"))
async def admin_ad_channels(msg: Message):
    if not is_admin(msg.from_user.id):
        return
    channels = await db.all("SELECT * FROM ad_channels WHERE is_active = TRUE ORDER BY id")
    if not channels:
        text = "📋 Каналов обязательной подписки нет.\n\n"
    else:
        text = f"📋 <b>Каналы подписки ({len(channels)}):</b>\n\n"
        for i, ch in enumerate(channels, 1):
            text += f"{i}. <b>{ch['channel_name'] or ch['channel_id']}</b> — <code>{ch['channel_id']}</code>\n"
    text += "\n<b>Команды:</b>\n/addchannel @ch Название https://t.me/ch\n/delchannel @ch"
    await msg.answer(text)


@router.message(Command("addchannel"))
async def cmd_addchannel(msg: Message):
    if not is_admin(msg.from_user.id):
        return
    parts = msg.text.split(maxsplit=3)
    if len(parts) < 4:
        await msg.answer(
            "Использование:\n<code>/addchannel @channel Название https://t.me/channel</code>")
        return
    _, ch_id, name, url = parts
    await db.execute("""
        INSERT INTO ad_channels (channel_id, channel_name, channel_url)
        VALUES ($1, $2, $3)
        ON CONFLICT (channel_id) DO UPDATE SET is_active = TRUE, channel_name = $2, channel_url = $3
    """, ch_id, name, url)
    await msg.answer(f"✅ Канал <b>{name}</b> добавлен.")


@router.message(Command("delchannel"))
async def cmd_delchannel(msg: Message):
    if not is_admin(msg.from_user.id):
        return
    parts = msg.text.split()
    if len(parts) < 2:
        await msg.answer("Использование: <code>/delchannel @channel</code>")
        return
    await db.execute("UPDATE ad_channels SET is_active = FALSE WHERE channel_id = $1", parts[1])
    await msg.answer(f"✅ Канал <code>{parts[1]}</code> деактивирован.")


# ─────────────────────────────────────────────────────────────────────────────
# Поиск по тексту — catch-all (должен быть зарегистрирован ПОСЛЕДНИМ)
# ─────────────────────────────────────────────────────────────────────────────

@router.message(F.text, StateFilter(None))
async def search_any(msg: Message, state: FSMContext):
    uid  = msg.from_user.id
    text = msg.text.strip()

    if not is_admin(uid) and not await check_all_subs(uid):
        await send_sub_alert(msg)
        return

    if await db.val("SELECT is_banned FROM users WHERE user_id = $1", uid):
        await msg.answer("🚫 Вы заблокированы в этом боте.")
        return

    await db.log_query(text)
    await track(uid, "search", text)

    movies = await db.all("SELECT title FROM movies")
    m_titles = [m["title"] for m in movies]
    match = fuzz_process.extractOne(text, m_titles, score_cutoff=65) if m_titles else None
    if match:
        movie = await db.one("SELECT * FROM movies WHERE title = $1", match[0])
        if movie:
            await db.execute("UPDATE movies SET views = views + 1 WHERE id = $1", movie["id"])
            caption = (
                f"🍿 Найдено: <b>{movie['title']}</b>\n"
                f"🎧 Озвучка: {movie['voice']}\n"
                f"💎 Качество: {movie['quality']}\n\n"
                f"✨ Приятного просмотра!"
            )
            await send_video_with_ad(msg, state, uid, movie["file_id"], caption)
            return

    series = await db.all("SELECT MIN(id) AS id, title FROM series GROUP BY title")
    s_titles = [s["title"] for s in series]
    smatch = fuzz_process.extractOne(text, s_titles, score_cutoff=65) if s_titles else None
    if smatch:
        sid_row = await db.one("SELECT MIN(id) AS id FROM series WHERE title = $1", smatch[0])
        await msg.answer(
            f"🎬 Найден сериал: <b>{smatch[0]}</b>",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="📺 Выбрать сезон", callback_data=f"ps_{sid_row['id']}")
            ]])
        )
        return

    tmdb = search_tmdb(text)
    if tmdb:
        await msg.answer(
            f"{tmdb['type']}: <b>{tmdb['title']}</b>"
            + (f" ({tmdb['year']})" if tmdb["year"] else "") + "\n"
            f"⭐ Рейтинг: {tmdb['rating']}\n"
            f"📖 {tmdb['overview']}",
            parse_mode="HTML",
            disable_web_page_preview=True,
        )
        await msg.answer("😔 Фильм ещё не в нашей библиотеке, но запрос отправлен администраторам.")
        await db.add_request(text)
        for admin_id in ADMIN_IDS:
            try:
                await bot.send_message(
                    admin_id,
                    f"📌 Новый запрос!\n"
                    f"👤 @{msg.from_user.username or msg.from_user.id}\n"
                    f"🎬 {text}"
                )
            except Exception:
                pass
        return

    await msg.answer("😔 Ничего не найдено.")
    await db.add_request(text)


# ─────────────────────────────────────────────────────────────────────────────
# main()
# ─────────────────────────────────────────────────────────────────────────────

async def main():
    if not BOT_TOKEN:
        log.error("BOT_TOKEN не задан!")
        return
    if not DATABASE_URL:
        log.error("BOT_DATABASE_URL не задан!")
        return

    log.info("Запуск Moviemax Bot...")
    await init_db()
    await acquire_session_lock()
    await bot.delete_webhook(drop_pending_updates=True)
    asyncio.create_task(session_watchdog())

    port = int(os.getenv("PORT", 5000))

    async def health(_req):
        return web.Response(text="Moviemax Bot is alive! 🎬")

    app = web.Application()
    app.router.add_get("/", health)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    log.info(f"Keep-alive сервер на порту {port}")

    log.info("Бот запущен! 🎬")
    asyncio.create_task(shutdown_warning())
    await dp.start_polling(bot, drop_pending_updates=True)


if __name__ == "__main__":
    asyncio.run(main())
