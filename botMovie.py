import asyncio
import asyncpg
import os
import logging
import re
try:
    import requests
    REQUESTS_AVAILABLE = True
except ImportError:
    REQUESTS_AVAILABLE = False

from aiogram import Bot, Dispatcher, F
from aiogram.types import (
    Message, InlineKeyboardMarkup, InlineKeyboardButton,
    CallbackQuery, ReplyKeyboardMarkup, KeyboardButton
)
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from thefuzz import process

logging.basicConfig(level=logging.INFO)

class Config:
    TOKEN    = os.getenv("BOT_TOKEN", "")
    DB_URL   = os.getenv("DATABASE_URL", "")
    ADMIN_IDS   = {8624275754, 7197661040}
    CHANNEL_ID   = -1004487149553
    CHANNEL_LINK = "https://t.me/+zqyoj3o6RNM0N2I6"
    PAGE_SIZE        = 8
    MAX_CALLBACK_LEN = 60
    TMDB_API_KEY  = "72546b58867caa004fac6a5a49f01269"
    TMDB_LANGUAGE = "ru-RU"

bot = Bot(token=Config.TOKEN)
dp  = Dispatcher(storage=MemoryStorage())

class AdminStates(StatesGroup):
    MOVIE_NAME  = State()
    MOVIE_VOICE = State()
    MOVIE_QUALITY = State()
    MOVIE_FILE  = State()
    SERIES_NAME  = State()
    SERIES_VOICE = State()
    SERIES_QUALITY = State()
    SERIES_SEASON  = State()
    SERIES_WAITING_VIDEOS = State()
    BROADCAST_TEXT = State()
    BULK_UPLOAD    = State()

# ── DATABASE ────────────────────────────────────────────────────────────────
class Database:
    def __init__(self):
        self.pool: asyncpg.Pool | None = None

    async def connect(self):
        self.pool = await asyncpg.create_pool(Config.DB_URL, min_size=1, max_size=5)

    async def execute(self, query: str, params: tuple = ()):
        async with self.pool.acquire() as conn:
            await conn.execute(query, *params)

    async def fetch_one(self, query: str, params: tuple = ()):
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(query, *params)
            return tuple(row) if row else None

    async def fetch_all(self, query: str, params: tuple = ()):
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query, *params)
            return [tuple(r) for r in rows]

    async def log_query(self, query: str):
        async with self.pool.acquire() as conn:
            await conn.execute("INSERT INTO logs (query) VALUES ($1)", query)
            await conn.execute(
                "INSERT INTO query_stats (query, count) VALUES ($1, 1) "
                "ON CONFLICT (query) DO UPDATE SET count = query_stats.count + 1",
                query
            )

    async def add_request(self, title: str):
        async with self.pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO requested (title, count) VALUES ($1, 1) "
                "ON CONFLICT (title) DO UPDATE SET count = requested.count + 1",
                title
            )

db = Database()

async def init_storage():
    async with db.pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id BIGINT PRIMARY KEY,
                is_subscribed INTEGER DEFAULT 0
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS movies (
                title   TEXT,
                voice   TEXT,
                quality TEXT,
                file_id TEXT,
                UNIQUE(title, voice, quality)
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS series (
                id      SERIAL PRIMARY KEY,
                title   TEXT,
                voice   TEXT,
                quality TEXT,
                season  INTEGER,
                episode INTEGER,
                file_id TEXT,
                UNIQUE(title, voice, quality, season, episode)
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS logs (
                id        SERIAL PRIMARY KEY,
                query     TEXT,
                timestamp TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS query_stats (
                query TEXT PRIMARY KEY,
                count INTEGER DEFAULT 1
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS requested (
                id        SERIAL PRIMARY KEY,
                title     TEXT UNIQUE,
                count     INTEGER DEFAULT 1,
                timestamp TIMESTAMPTZ DEFAULT NOW()
            )
        """)

# ── HELPERS ─────────────────────────────────────────────────────────────────
async def is_subscribed(user_id: int) -> bool:
    try:
        member = await bot.get_chat_member(chat_id=Config.CHANNEL_ID, user_id=user_id)
        return member.status in ['creator', 'administrator', 'member']
    except Exception as e:
        logging.error(f"Ошибка проверки подписки: {e}")
        return False

async def send_sub_alert(message_or_call):
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Подписаться 📢", url=Config.CHANNEL_LINK)]
    ])
    text = "🚫 Для доступа к нашей библиотеке необходимо подписаться на канал!"
    if isinstance(message_or_call, Message):
        await message_or_call.answer(text, reply_markup=kb)
    elif isinstance(message_or_call, CallbackQuery):
        await message_or_call.message.answer(text, reply_markup=kb)

def safe_callback(title: str, prefix: str) -> str:
    short = title[:37] + "..." if len(title) > 40 else title
    data  = f"{prefix}{short}"
    return data[:60]

def search_tmdb(query: str):
    if not REQUESTS_AVAILABLE or not Config.TMDB_API_KEY:
        return None
    try:
        resp = requests.get(
            "https://api.themoviedb.org/3/search/movie",
            params={"api_key": Config.TMDB_API_KEY, "query": query,
                    "language": Config.TMDB_LANGUAGE, "page": 1},
            timeout=10
        )
        data = resp.json()
        if data.get("results"):
            m = data["results"][0]
            return {
                "title":    m.get("title") or m.get("original_title"),
                "year":     m.get("release_date", "")[:4],
                "rating":   m.get("vote_average"),
                "overview": m.get("overview", "Нет описания")[:280],
            }
    except Exception as e:
        logging.error(f"TMDB Error: {e}")
    return None

# ── KEYBOARDS ────────────────────────────────────────────────────────────────
def quality_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="1080p", callback_data="q_1080p"),
         InlineKeyboardButton(text="720p",  callback_data="q_720p")],
        [InlineKeyboardButton(text="480p",  callback_data="q_480p")]
    ])

def main_kb():
    return ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="📚 Полный список фильмов"),
         KeyboardButton(text="📚 Список сериалов")]
    ], resize_keyboard=True)

def voice_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="TVShows",     callback_data="v_TVShows")],
        [InlineKeyboardButton(text="Lost Film",   callback_data="v_LostFilm")],
        [InlineKeyboardButton(text="Дубляж",      callback_data="v_Дубляж")],
        [InlineKeyboardButton(text="Кубик в Кубе",callback_data="v_Кубик в Кубе")],
        [InlineKeyboardButton(text="Пифагор",     callback_data="v_Пифагор")]
    ])

def admin_panel_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Добавить Фильм",  callback_data="add_video"),
         InlineKeyboardButton(text="➕ Добавить Сериал", callback_data="add_series")],
        [InlineKeyboardButton(text="➕ Добавить Сезон",  callback_data="add_season")],
        [InlineKeyboardButton(text="🗑 Удалить Фильм",   callback_data="delete_movie"),
         InlineKeyboardButton(text="🗑 Удалить Сериал",  callback_data="delete_series")],
        [InlineKeyboardButton(text="📣 Рассылка",        callback_data="broadcast"),
         InlineKeyboardButton(text="🔎 Логи запросов",   callback_data="logs_0")],
        [InlineKeyboardButton(text="🔝 Топ запросов",    callback_data="top_queries"),
         InlineKeyboardButton(text="📋 Требуемые фильмы",callback_data="requested_movies")],
        [InlineKeyboardButton(text="📦 Массовая загрузка",callback_data="bulk_upload")]
    ])

def back_to_admin_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Назад 🔙", callback_data="back_admin")]
    ])

# ── PAGINATION ───────────────────────────────────────────────────────────────
async def send_list_page(message_or_call, page: int, items: list, content_type: str = "movies"):
    total_pages = max(1, (len(items) - 1) // Config.PAGE_SIZE + 1)
    start       = page * Config.PAGE_SIZE
    page_items  = items[start: start + Config.PAGE_SIZE]
    kb_list = [[InlineKeyboardButton(
        text=f"🎬 {m[0]}",
        callback_data=safe_callback(m[0], f"play_{content_type}_")
    )] for m in page_items]
    nav = []
    if page > 0:              nav.append(InlineKeyboardButton(text="⬅️", callback_data=f"list_{content_type}_{page-1}"))
    if page < total_pages-1:  nav.append(InlineKeyboardButton(text="➡️", callback_data=f"list_{content_type}_{page+1}"))
    if nav: kb_list.append(nav)
    markup = InlineKeyboardMarkup(inline_keyboard=kb_list)
    text   = f"📚 Список {content_type} ({page+1}/{total_pages}):"
    if isinstance(message_or_call, Message):
        await message_or_call.answer(text, reply_markup=markup)
    else:
        await message_or_call.message.edit_text(text, reply_markup=markup)

async def send_series_list_page(message_or_call, page: int, series_data: list):
    total_pages = max(1, (len(series_data) - 1) // Config.PAGE_SIZE + 1)
    start       = page * Config.PAGE_SIZE
    page_items  = series_data[start: start + Config.PAGE_SIZE]
    kb_list = [[InlineKeyboardButton(text=f"🎬 {title}", callback_data=f"ps_{sid}")]
                for sid, title in page_items]
    nav = []
    if page > 0:              nav.append(InlineKeyboardButton(text="⬅️", callback_data=f"slist_{page-1}"))
    if page < total_pages-1:  nav.append(InlineKeyboardButton(text="➡️", callback_data=f"slist_{page+1}"))
    if nav: kb_list.append(nav)
    markup = InlineKeyboardMarkup(inline_keyboard=kb_list)
    text   = f"📚 Список сериалов ({page+1}/{total_pages}):"
    if isinstance(message_or_call, Message):
        await message_or_call.answer(text, reply_markup=markup)
    else:
        await message_or_call.message.edit_text(text, reply_markup=markup)

# ── START ─────────────────────────────────────────────────────────────────────
@dp.message(Command("start"))
async def start_cmd(message: Message):
    await db.execute(
        "INSERT INTO users (user_id, is_subscribed) VALUES ($1, $2) "
        "ON CONFLICT (user_id) DO UPDATE SET is_subscribed = $2",
        (message.from_user.id, 1)
    )
    await message.answer(
        "👋 Привет! Добро пожаловать в нашу видеотеку.\n"
        "Напиши название фильма или сериала, и я найду его для тебя, "
        "или выбери из списка ниже! 🎬🍿",
        reply_markup=main_kb()
    )

# ── LISTS ─────────────────────────────────────────────────────────────────────
@dp.message(F.text == "📚 Полный список фильмов")
async def show_list_cmd(message: Message):
    if not await is_subscribed(message.from_user.id):
        await send_sub_alert(message); return
    movies = await db.fetch_all("SELECT title FROM movies ORDER BY title ASC")
    if not movies:
        await message.answer("Библиотека фильмов пока пуста. ✨"); return
    await send_list_page(message, 0, movies, "movies")

@dp.message(F.text == "📚 Список сериалов")
async def show_series_list(message: Message):
    if not await is_subscribed(message.from_user.id):
        await send_sub_alert(message); return
    series_data = await db.fetch_all(
        "SELECT MIN(id), title FROM series GROUP BY title ORDER BY title ASC"
    )
    if not series_data:
        await message.answer("Библиотека сериалов пока пуста. ✨"); return
    await send_series_list_page(message, 0, series_data)

@dp.callback_query(F.data.startswith("slist_"))
async def callback_series_list_paginated(call: CallbackQuery):
    if not await is_subscribed(call.from_user.id):
        await send_sub_alert(call); return
    page = int(call.data.split("_")[1])
    series_data = await db.fetch_all(
        "SELECT MIN(id), title FROM series GROUP BY title ORDER BY title ASC"
    )
    await send_series_list_page(call, page, series_data)
    await call.answer()

@dp.callback_query(F.data.startswith("list_"))
async def callback_movies_list_paginated(call: CallbackQuery):
    if not await is_subscribed(call.from_user.id):
        await send_sub_alert(call); return
    page  = int(call.data.split("_")[2])
    items = await db.fetch_all("SELECT title FROM movies ORDER BY title ASC")
    await send_list_page(call, page, items, "movies")
    await call.answer()

# ── PLAY MOVIE ────────────────────────────────────────────────────────────────
@dp.callback_query(F.data.startswith("play_movies_"))
async def play_movie_by_btn(call: CallbackQuery):
    if not await is_subscribed(call.from_user.id):
        await send_sub_alert(call); return
    title = call.data[len("play_movies_"):]
    movie = await db.fetch_one(
        "SELECT voice, quality, file_id FROM movies WHERE title = $1", (title,)
    )
    if movie:
        await call.message.answer_video(
            movie[2],
            caption=f"🍿 Название: {title}\n🎧 Озвучка: {movie[0]}\n💎 Качество: {movie[1]}\n\n✨ Приятного просмотра! 🎬🛋️🔥"
        )
    else:
        await call.answer("Фильм не найден", show_alert=True)
    await call.answer()

# ── SERIES ────────────────────────────────────────────────────────────────────
@dp.callback_query(F.data.startswith("ps_"))
async def play_series_handler(call: CallbackQuery):
    if not await is_subscribed(call.from_user.id):
        await send_sub_alert(call); return
    series_id = int(call.data.split("_")[1])
    row = await db.fetch_one("SELECT title FROM series WHERE id = $1", (series_id,))
    if not row:
        await call.answer("Сериал не найден", show_alert=True); return
    title   = row[0]
    seasons = await db.fetch_all(
        "SELECT DISTINCT season FROM series WHERE title = $1 ORDER BY season ASC", (title,)
    )
    if not seasons:
        await call.answer("Сезоны не найдены", show_alert=True); return
    kb_list = [[InlineKeyboardButton(text=f"📺 Сезон {s[0]}", callback_data=f"seas_{series_id}_{s[0]}")] for s in seasons]
    kb_list.append([InlineKeyboardButton(text="🔙 К списку сериалов", callback_data="slist_0")])
    await call.message.edit_text(
        f"🎬 Сериал: <b>{title}</b>\n\nВыберите сезон:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_list),
        parse_mode="HTML"
    )
    await call.answer()

@dp.callback_query(F.data.startswith("seas_"))
async def show_season_episodes(call: CallbackQuery):
    if not await is_subscribed(call.from_user.id):
        await send_sub_alert(call); return
    parts     = call.data.split("_")
    series_id = int(parts[1])
    season    = int(parts[2])
    row = await db.fetch_one("SELECT title FROM series WHERE id = $1", (series_id,))
    if not row:
        await call.answer("Сериал не найден", show_alert=True); return
    title    = row[0]
    episodes = await db.fetch_all(
        "SELECT id, episode FROM series WHERE title = $1 AND season = $2 ORDER BY episode ASC",
        (title, season)
    )
    if not episodes:
        await call.answer("Серии не найдены", show_alert=True); return
    kb_list = [[InlineKeyboardButton(text=f"🎞️ Серия {ep}", callback_data=f"ep_{row_id}")] for row_id, ep in episodes]
    kb_list.append([InlineKeyboardButton(text="🔙 К сезонам", callback_data=f"ps_{series_id}")])
    await call.message.edit_text(
        f"🎬 <b>{title}</b> — Сезон {season}",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_list),
        parse_mode="HTML"
    )
    await call.answer()

@dp.callback_query(F.data.startswith("ep_"))
async def play_episode(call: CallbackQuery):
    if not await is_subscribed(call.from_user.id):
        await send_sub_alert(call); return
    row_id = int(call.data.split("_")[1])
    try:
        info = await db.fetch_one(
            "SELECT title, season, episode, voice, quality, file_id FROM series WHERE id = $1",
            (row_id,)
        )
        if not info:
            await call.answer("Серия не найдена", show_alert=True); return
        title, season, episode, voice, quality, file_id = info
        await call.message.answer_video(
            file_id,
            caption=(
                f"🎬 <b>{title}</b>\n"
                f"📺 Сезон {season} • Серия {episode}\n"
                f"🎧 {voice} • 💎 {quality}\n\n✨ Приятного просмотра! 🔥"
            ),
            parse_mode="HTML"
        )
        episodes = await db.fetch_all(
            "SELECT id, episode FROM series WHERE title = $1 AND season = $2 ORDER BY episode ASC",
            (title, season)
        )
        current_idx = next((i for i, (rid, _) in enumerate(episodes) if rid == row_id), None)
        if current_idx is not None and len(episodes) > 1:
            nav = []
            if current_idx > 0:
                nav.append(InlineKeyboardButton(text="⬅️ Предыдущая", callback_data=f"ep_{episodes[current_idx-1][0]}"))
            if current_idx < len(episodes) - 1:
                nav.append(InlineKeyboardButton(text="Следующая ➡️", callback_data=f"ep_{episodes[current_idx+1][0]}"))
            if nav:
                await call.message.answer(
                    "⏭️ Переключение между сериями:",
                    reply_markup=InlineKeyboardMarkup(inline_keyboard=[nav])
                )
    except Exception as e:
        logging.error(f"Ошибка воспроизведения: {e}")
        await call.answer("Ошибка при воспроизведении.", show_alert=True)
    await call.answer()

# ── ADMIN PANEL ───────────────────────────────────────────────────────────────
@dp.message(F.text == "адм")
async def admin_panel(msg: Message):
    if msg.from_user.id in Config.ADMIN_IDS:
        await msg.answer("🛠 Режим администратора активирован!", reply_markup=admin_panel_kb())

@dp.callback_query(F.data == "back_admin")
async def back_to_admin(call: CallbackQuery):
    await call.message.edit_text("🛠 Режим администратора активирован!", reply_markup=admin_panel_kb())

@dp.callback_query(F.data == "requested_movies")
async def show_requested_movies(call: CallbackQuery):
    if call.from_user.id not in Config.ADMIN_IDS:
        await call.answer("Доступ запрещён!", show_alert=True); return
    requested = await db.fetch_all(
        "SELECT title, count FROM requested ORDER BY count DESC, timestamp DESC"
    )
    if not requested:
        text = "📋 Пока нет запрошенных фильмов."
    else:
        text = "📋 <b>Требуемые фильмы</b>\n\n"
        for i, (title, count) in enumerate(requested, 1):
            text += f"{i}. {title} — {count} запрос(ов)\n"
    await call.message.edit_text(text, reply_markup=back_to_admin_kb(), parse_mode="HTML")
    await call.answer()

# ── ADD MOVIE ─────────────────────────────────────────────────────────────────
@dp.callback_query(F.data == "add_video")
async def add_vid_start(call: CallbackQuery, state: FSMContext):
    await call.message.answer("🎬 Введите название фильма:")
    await state.set_state(AdminStates.MOVIE_NAME)
    await call.answer()

@dp.message(AdminStates.MOVIE_NAME)
async def get_name(msg: Message, state: FSMContext):
    await state.update_data(name=msg.text)
    await msg.answer("🎧 Выберите озвучку:", reply_markup=voice_kb())
    await state.set_state(AdminStates.MOVIE_VOICE)

@dp.callback_query(AdminStates.MOVIE_VOICE, F.data.startswith("v_"))
async def get_voice(call: CallbackQuery, state: FSMContext):
    voice = call.data.split("_", 1)[1]
    await state.update_data(voice=voice)
    await call.message.edit_text(
        f"✅ Озвучка: <b>{voice}</b>\n\n💎 Выберите качество:",
        reply_markup=quality_kb(), parse_mode="HTML"
    )
    await state.set_state(AdminStates.MOVIE_QUALITY)
    await call.answer()

@dp.callback_query(AdminStates.MOVIE_QUALITY, F.data.startswith("q_"))
async def get_quality(call: CallbackQuery, state: FSMContext):
    quality = call.data.split("_", 1)[1]
    await state.update_data(quality=quality)
    await call.message.edit_text(
        f"✅ Качество: <b>{quality}</b>\n\n📤 Отправьте видеофайл:", parse_mode="HTML"
    )
    await state.set_state(AdminStates.MOVIE_FILE)
    await call.answer()

@dp.message(AdminStates.MOVIE_FILE, F.video)
async def get_file(msg: Message, state: FSMContext):
    data = await state.get_data()
    await db.execute(
        "INSERT INTO movies (title, voice, quality, file_id) VALUES ($1, $2, $3, $4) "
        "ON CONFLICT DO NOTHING",
        (data['name'], data['voice'], data['quality'], msg.video.file_id)
    )
    await msg.answer("✅ Фильм успешно сохранён!")
    await state.clear()

# ── ADD SERIES ────────────────────────────────────────────────────────────────
@dp.callback_query(F.data == "add_series")
async def add_series_start(call: CallbackQuery, state: FSMContext):
    await call.message.answer("🎬 Введите название сериала:")
    await state.set_state(AdminStates.SERIES_NAME)
    await call.answer()

@dp.callback_query(F.data == "add_season")
async def add_season_start(call: CallbackQuery, state: FSMContext):
    series_data = await db.fetch_all(
        "SELECT MIN(id), title FROM series GROUP BY title ORDER BY title ASC"
    )
    if not series_data:
        await call.answer("Нет сериалов в базе. Сначала добавьте сериал.", show_alert=True); return
    kb_list = [[InlineKeyboardButton(text=f"📺 {title}", callback_data=f"ases_{sid}")]
                for sid, title in series_data]
    kb_list.append([InlineKeyboardButton(text="Назад 🔙", callback_data="back_admin")])
    await call.message.answer(
        "Выберите сериал для добавления сезона:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_list)
    )
    await call.answer()

@dp.callback_query(F.data.startswith("ases_"))
async def add_season_select_series(call: CallbackQuery, state: FSMContext):
    sid = int(call.data.split("_")[1])
    row = await db.fetch_one("SELECT title FROM series WHERE id = $1", (sid,))
    if not row:
        await call.answer("Сериал не найден", show_alert=True); return
    title = row[0]
    await state.update_data(name=title)
    await call.message.answer(
        f"✅ Сериал: <b>{title}</b>\n\n🎧 Выберите озвучку:",
        reply_markup=voice_kb(), parse_mode="HTML"
    )
    await state.set_state(AdminStates.SERIES_VOICE)
    await call.answer()

@dp.message(AdminStates.SERIES_NAME)
async def series_get_name(msg: Message, state: FSMContext):
    await state.update_data(name=msg.text)
    await msg.answer("🎧 Выберите озвучку:", reply_markup=voice_kb())
    await state.set_state(AdminStates.SERIES_VOICE)

@dp.callback_query(AdminStates.SERIES_VOICE, F.data.startswith("v_"))
async def series_get_voice(call: CallbackQuery, state: FSMContext):
    voice = call.data.split("_", 1)[1]
    await state.update_data(voice=voice)
    await call.message.edit_text(
        f"✅ Озвучка: <b>{voice}</b>\n\n💎 Введите качество:", parse_mode="HTML"
    )
    await state.set_state(AdminStates.SERIES_QUALITY)
    await call.answer()

@dp.message(AdminStates.SERIES_QUALITY)
async def series_get_qual(msg: Message, state: FSMContext):
    await state.update_data(quality=msg.text)
    await msg.answer("📅 Введите номер сезона:")
    await state.set_state(AdminStates.SERIES_SEASON)

@dp.message(AdminStates.SERIES_SEASON)
async def series_get_season(msg: Message, state: FSMContext):
    if not msg.text.isdigit():
        await msg.answer("Введите цифру!"); return
    await state.update_data(season=int(msg.text), ep_counter=1)
    await msg.answer(
        "📤 Отправляйте видео серий по порядку.\nКогда закончите — напишите <b>Готово</b>",
        parse_mode="HTML"
    )
    await state.set_state(AdminStates.SERIES_WAITING_VIDEOS)

@dp.message(AdminStates.SERIES_WAITING_VIDEOS, F.video)
async def save_series_video(msg: Message, state: FSMContext):
    data = await state.get_data()
    ep   = data['ep_counter']
    await db.execute(
        "INSERT INTO series (title, voice, quality, season, episode, file_id) "
        "VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT DO NOTHING",
        (data['name'], data['voice'], data['quality'], data['season'], ep, msg.video.file_id)
    )
    await msg.answer(f"✅ Серия {ep} добавлена!")
    await state.update_data(ep_counter=ep + 1)

@dp.message(AdminStates.SERIES_WAITING_VIDEOS, F.text.lower() == "готово")
async def finish_series(msg: Message, state: FSMContext):
    await msg.answer("🎉 Сериал успешно добавлен!")
    await state.clear()

# ── DELETE MOVIE ──────────────────────────────────────────────────────────────
@dp.callback_query(F.data == "delete_movie")
async def delete_movie_list(call: CallbackQuery):
    movies = await db.fetch_all("SELECT title FROM movies ORDER BY title ASC")
    if not movies:
        await call.answer("Библиотека фильмов пуста.", show_alert=True); return
    kb_list = [[InlineKeyboardButton(text=f"🗑 {m[0]}", callback_data=f"del_m_{m[0][:40]}")] for m in movies]
    kb_list.append([InlineKeyboardButton(text="Назад 🔙", callback_data="back_admin")])
    await call.message.edit_text(
        "Выберите фильм для удаления:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_list)
    )

@dp.callback_query(F.data.startswith("del_m_"))
async def confirm_delete_movie(call: CallbackQuery):
    title = call.data.replace("del_m_", "")
    await db.execute("DELETE FROM movies WHERE title = $1", (title,))
    await call.answer("Фильм удалён!")
    await back_to_admin(call)

# ── DELETE SERIES ─────────────────────────────────────────────────────────────
async def _show_del_series_list(call: CallbackQuery):
    series = await db.fetch_all("SELECT MIN(id), title FROM series GROUP BY title ORDER BY title ASC")
    if not series:
        await call.message.edit_text("Библиотека сериалов пуста.", reply_markup=back_to_admin_kb()); return
    kb_list = [[InlineKeyboardButton(text=f"📺 {title}", callback_data=f"dss_{sid}")]
                for sid, title in series]
    kb_list.append([InlineKeyboardButton(text="Назад 🔙", callback_data="back_admin")])
    await call.message.edit_text("Выберите сериал:", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_list))

async def _show_del_seasons(call: CallbackQuery, sid: int):
    row = await db.fetch_one("SELECT title FROM series WHERE id = $1", (sid,))
    if not row:
        await call.answer("Сериал не найден", show_alert=True); return
    title   = row[0]
    seasons = await db.fetch_all(
        "SELECT DISTINCT season FROM series WHERE title = $1 ORDER BY season ASC", (title,)
    )
    kb_list = [[InlineKeyboardButton(text=f"📂 Сезон {s[0]}", callback_data=f"dse_{sid}_{s[0]}")] for s in seasons]
    kb_list.append([InlineKeyboardButton(text="🗑 Удалить весь сериал", callback_data=f"das_{sid}")])
    kb_list.append([InlineKeyboardButton(text="Назад 🔙", callback_data="delete_series")])
    await call.message.edit_text(
        f"📺 <b>{title}</b>\n\nВыберите сезон или удалите весь сериал:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_list),
        parse_mode="HTML"
    )

async def _show_del_episodes(call: CallbackQuery, sid: int, season: int):
    row = await db.fetch_one("SELECT title FROM series WHERE id = $1", (sid,))
    if not row:
        await call.answer("Сериал не найден", show_alert=True); return
    title    = row[0]
    episodes = await db.fetch_all(
        "SELECT id, episode FROM series WHERE title = $1 AND season = $2 ORDER BY episode ASC",
        (title, season)
    )
    kb_list = [[InlineKeyboardButton(text=f"🗑 Серия {ep}", callback_data=f"dep_{row_id}")] for row_id, ep in episodes]
    kb_list.append([InlineKeyboardButton(text=f"🗑 Удалить весь сезон {season}", callback_data=f"dsa_{sid}_{season}")])
    kb_list.append([InlineKeyboardButton(text="Назад 🔙", callback_data=f"dss_{sid}")])
    await call.message.edit_text(
        f"📺 <b>{title}</b> — Сезон {season}\n\nВыберите серию для удаления:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_list),
        parse_mode="HTML"
    )

@dp.callback_query(F.data == "delete_series")
async def delete_series_list(call: CallbackQuery):
    await _show_del_series_list(call)
    await call.answer()

@dp.callback_query(F.data.startswith("dss_"))
async def delete_series_seasons(call: CallbackQuery):
    sid = int(call.data.split("_")[1])
    await _show_del_seasons(call, sid)
    await call.answer()

@dp.callback_query(F.data.startswith("das_"))
async def delete_all_series(call: CallbackQuery):
    sid = int(call.data.split("_")[1])
    row = await db.fetch_one("SELECT title FROM series WHERE id = $1", (sid,))
    if not row:
        await call.answer("Сериал не найден", show_alert=True); return
    title = row[0]
    await db.execute("DELETE FROM series WHERE title = $1", (title,))
    await call.answer(f"Сериал «{title}» удалён!", show_alert=True)
    await back_to_admin(call)

@dp.callback_query(F.data.startswith("dse_"))
async def delete_series_episodes(call: CallbackQuery):
    parts  = call.data.split("_")
    sid    = int(parts[1])
    season = int(parts[2])
    await _show_del_episodes(call, sid, season)
    await call.answer()

@dp.callback_query(F.data.startswith("dsa_"))
async def delete_season_all(call: CallbackQuery):
    parts  = call.data.split("_")
    sid    = int(parts[1])
    season = int(parts[2])
    row    = await db.fetch_one("SELECT title FROM series WHERE id = $1", (sid,))
    if not row:
        await call.answer("Сериал не найден", show_alert=True); return
    title = row[0]
    await db.execute("DELETE FROM series WHERE title = $1 AND season = $2", (title, season))
    await call.answer(f"Сезон {season} удалён!", show_alert=True)
    await _show_del_seasons(call, sid)

@dp.callback_query(F.data.startswith("dep_"))
async def delete_one_episode(call: CallbackQuery):
    row_id = int(call.data.split("_")[1])
    info   = await db.fetch_one("SELECT title, season, episode FROM series WHERE id = $1", (row_id,))
    if not info:
        await call.answer("Серия не найдена", show_alert=True); return
    title, season, episode = info
    await db.execute("DELETE FROM series WHERE id = $1", (row_id,))
    await call.answer(f"Серия {episode} удалена!", show_alert=True)
    sid_row = await db.fetch_one(
        "SELECT MIN(id) FROM series WHERE title = $1 AND season = $2", (title, season)
    )
    if sid_row and sid_row[0]:
        await _show_del_episodes(call, sid_row[0], season)
    else:
        sid_row2 = await db.fetch_one("SELECT MIN(id) FROM series WHERE title = $1", (title,))
        if sid_row2 and sid_row2[0]:
            await _show_del_seasons(call, sid_row2[0])
        else:
            await back_to_admin(call)

# ── BROADCAST ─────────────────────────────────────────────────────────────────
@dp.callback_query(F.data == "broadcast")
async def bc_start(call: CallbackQuery, state: FSMContext):
    await call.message.answer("Введите текст рассылки:", reply_markup=back_to_admin_kb())
    await state.set_state(AdminStates.BROADCAST_TEXT)

@dp.message(AdminStates.BROADCAST_TEXT)
async def bc_run(msg: Message, state: FSMContext):
    users = await db.fetch_all("SELECT user_id FROM users")
    count = 0
    for u in users:
        try:
            await bot.send_message(u[0], msg.text)
            count += 1
        except:
            pass
    await msg.answer(f"✅ Рассылка завершена. Получило {count} пользователей.")
    await state.clear()

# ── BULK UPLOAD ───────────────────────────────────────────────────────────────
@dp.callback_query(F.data == "bulk_upload")
async def bulk_upload_start(call: CallbackQuery, state: FSMContext):
    await state.update_data(bulk_count=0)
    await call.message.answer(
        "📦 <b>Режим массовой загрузки</b>\n\n"
        "Отправляй видео по одному (до 100 штук).\n"
        "Подпись каждого видео должна быть в формате:\n"
        "<code>Название (Озвучка [Качество])</code>\n\n"
        "Пример: <code>Железный человек (Дублированный [1080p])</code>\n\n"
        "Когда закончишь — напиши <b>Готово</b>",
        parse_mode="HTML"
    )
    await state.set_state(AdminStates.BULK_UPLOAD)
    await call.answer()

@dp.message(AdminStates.BULK_UPLOAD, F.video)
async def bulk_upload_video(msg: Message, state: FSMContext):
    caption = msg.caption or ""
    file_id = msg.video.file_id
    m = re.match(r"^(.+?)\s*\((.+?)\s*\[(.+?)\]\)", caption.strip())
    if m:
        title, voice, quality = m.group(1).strip(), m.group(2).strip(), m.group(3).strip()
    else:
        title   = caption.strip() or "Без названия"
        voice   = "Неизвестно"
        quality = "Неизвестно"
    await db.execute(
        "INSERT INTO movies (title, voice, quality, file_id) VALUES ($1, $2, $3, $4) ON CONFLICT DO NOTHING",
        (title, voice, quality, file_id)
    )
    data  = await state.get_data()
    count = data.get("bulk_count", 0) + 1
    await state.update_data(bulk_count=count)
    try:
        await msg.react([{"type": "emoji", "emoji": "👍"}])
    except:
        pass

@dp.message(AdminStates.BULK_UPLOAD, F.text.lower() == "готово")
async def bulk_upload_done(msg: Message, state: FSMContext):
    data  = await state.get_data()
    count = data.get("bulk_count", 0)
    await msg.answer(f"✅ Загружено {count} фильмов в базу данных!", reply_markup=back_to_admin_kb())
    await state.clear()

# ── SEARCH ────────────────────────────────────────────────────────────────────
@dp.message(F.text)
async def search_content(msg: Message):
    if not await is_subscribed(msg.from_user.id):
        await send_sub_alert(msg); return
    query = msg.text.strip()
    await db.log_query(query)

    all_movies = await db.fetch_all("SELECT title FROM movies")
    titles     = [m[0] for m in all_movies]
    match      = process.extractOne(query, titles, score_cutoff=70) if titles else None

    found_in_library = False
    if match:
        found_in_library = True
        movie = await db.fetch_one(
            "SELECT voice, quality, file_id FROM movies WHERE title = $1", (match[0],)
        )
        if movie:
            await msg.answer_video(
                movie[2],
                caption=f"🍿 Найдено: {match[0]}\n🎧 Озвучка: {movie[0]}\n💎 Качество: {movie[1]}\n\n✨ Приятного просмотра!"
            )

    all_series   = await db.fetch_all("SELECT MIN(id), title FROM series GROUP BY title")
    series_match = process.extractOne(query, [s[1] for s in all_series], score_cutoff=70) if all_series else None
    if series_match:
        title   = series_match[0]
        sid_row = await db.fetch_one("SELECT MIN(id) FROM series WHERE title = $1", (title,))
        sid     = sid_row[0]
        await msg.answer(
            f"🎬 Найден сериал: <b>{title}</b>",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="📺 Выбрать сезон", callback_data=f"ps_{sid}")
            ]]),
            parse_mode="HTML"
        )
        return

    tmdb_result = search_tmdb(query)
    if tmdb_result:
        text = (
            f"🎥 <b>{tmdb_result['title']}</b> ({tmdb_result['year']})\n"
            f"⭐ Рейтинг: {tmdb_result['rating']}\n"
            f"📖 {tmdb_result['overview']}"
        )
        await msg.answer(text, parse_mode="HTML", disable_web_page_preview=True)
        if not found_in_library:
            await db.add_request(query)
            for admin_id in Config.ADMIN_IDS:
                try:
                    await bot.send_message(
                        admin_id,
                        f"📌 Новый запрос!\nПользователь: @{msg.from_user.username or msg.from_user.id}\nФильм: {query}"
                    )
                except:
                    pass
        return

    await msg.answer("😔 Ничего не найдено.")

# ── LOGS ──────────────────────────────────────────────────────────────────────
@dp.callback_query(F.data.startswith("logs_"))
async def show_logs(call: CallbackQuery):
    logs = await db.fetch_all("SELECT query FROM logs ORDER BY id DESC LIMIT 10")
    text = "🔎 Последние 10 запросов:\n" + "\n".join([f"- {l[0]}" for l in logs]) if logs else "Логи пусты."
    await call.message.edit_text(text, reply_markup=back_to_admin_kb())
    await call.answer()

@dp.callback_query(F.data == "top_queries")
async def show_top(call: CallbackQuery):
    top  = await db.fetch_all("SELECT query, count FROM query_stats ORDER BY count DESC LIMIT 10")
    text = ("🔝 Топ-10 частых запросов:\n\n" +
            "\n".join([f"{i+1}. {r[0]} — {r[1]} раз" for i, r in enumerate(top)])
            ) if top else "Статистика пуста."
    await call.message.edit_text(text, reply_markup=back_to_admin_kb())
    await call.answer()

# ── MAIN ──────────────────────────────────────────────────────────────────────
async def main():
    from aiohttp import web

    async def health(request):
        return web.Response(text="OK")

    app    = web.Application()
    app.router.add_get("/", health)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", 5000)
    await site.start()

    await db.connect()
    await init_storage()
    logging.info("Бот запущен на PostgreSQL!")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
