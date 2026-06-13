import asyncio
import aiosqlite
import os
import logging
import hashlib
from aiohttp import web
try:
    import requests
    REQUESTS_AVAILABLE = True
except ImportError:
    REQUESTS_AVAILABLE = False
    logging.warning("Модуль requests не установлен. Поиск по TMDB будет отключён.")

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

# Включение логирования для отладки
logging.basicConfig(level=logging.INFO)

# --- НАСТРОЙКИ ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "bot_data.db")

class Config:
    TOKEN = os.getenv("BOT_TOKEN", "")
    ADMIN_IDS = {8624275754, 7197661040}
    CHANNEL_ID = -1004487149553
    CHANNEL_LINK = "https://t.me/+zqyoj3o6RNM0N2I6"
    DB_NAME = DB_PATH
    PAGE_SIZE = 8
    MAX_CALLBACK_LEN = 60
    
    # TMDB — лучший вариант для русского поиска
    TMDB_API_KEY = "72546b58867caa004fac6a5a49f01269"
    TMDB_LANGUAGE = "ru-RU"

bot = Bot(token=Config.TOKEN)
dp = Dispatcher(storage=MemoryStorage())

# --- FSM (СОСТОЯНИЯ) ---
class AdminStates(StatesGroup):
    # Для фильмов
    MOVIE_NAME = State()
    MOVIE_VOICE = State()
    MOVIE_QUALITY = State()
    MOVIE_FILE = State()
    
    # Для сериалов
    SERIES_NAME = State()
    SERIES_VOICE = State()
    SERIES_QUALITY = State()
    SERIES_SEASON = State()
    SERIES_WAITING_VIDEOS = State()
    
    BROADCAST_TEXT = State()

# --- ИНИЦИАЛИЗАЦИЯ БД ---
async def init_storage():
    async with aiosqlite.connect(Config.DB_NAME) as conn:
        # Таблица пользователей
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY, 
                is_subscribed INTEGER DEFAULT 0
            )
        """)
        # Таблица фильмов
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS movies (
                title TEXT, 
                voice TEXT, 
                quality TEXT, 
                file_id TEXT,
                UNIQUE(title, voice, quality)
            )
        """)
        # Таблица сериалов
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS series (
                id INTEGER PRIMARY KEY AUTOINCREMENT, 
                title TEXT, 
                voice TEXT, 
                quality TEXT, 
                season INTEGER, 
                episode INTEGER, 
                file_id TEXT,
                UNIQUE(title, voice, quality, season, episode)
            )
        """)
        # Таблица логов
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT, 
                query TEXT, 
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
        # Таблица статистики
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS query_stats (
                query TEXT PRIMARY KEY, 
                count INTEGER DEFAULT 1
            )
        """)
        # Новая таблица для требуемых фильмов
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS requested (
                id INTEGER PRIMARY KEY AUTOINCREMENT, 
                title TEXT UNIQUE, 
                count INTEGER DEFAULT 1,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
        await conn.commit()

# --- КЛАСС БАЗЫ ДАННЫХ ---
class Database:
    def __init__(self, db_path: str):
        self.db_path = db_path

    async def execute(self, query: str, params: tuple = ()):
        """Выполнение запросов INSERT, UPDATE, DELETE"""
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(query, params)
            await db.commit()

    async def fetch_one(self, query: str, params: tuple = ()):
        """Получение одной записи"""
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute(query, params) as cursor:
                return await cursor.fetchone()

    async def fetch_all(self, query: str, params: tuple = ()):
        """Получение всех записей"""
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute(query, params) as cursor:
                return await cursor.fetchall()

    async def log_query(self, query: str):
        """Запись в лог"""
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("INSERT INTO logs (query) VALUES (?)", (query,))
            await db.execute(
                "INSERT INTO query_stats (query, count) VALUES (?, 1) ON CONFLICT(query) DO UPDATE SET count = count + 1", 
                (query,)
            )
            await db.commit()

    async def add_request(self, title: str):
        """Добавление запроса в таблицу требуемых фильмов"""
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                "INSERT INTO requested (title, count) VALUES (?, 1) ON CONFLICT(title) DO UPDATE SET count = count + 1",
                (title,)
            )
            await db.commit()

db = Database(Config.DB_NAME)

# --- ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ---

async def is_subscribed(user_id: int) -> bool:
    """Проверка подписки пользователя на канал"""
    try:
        member = await bot.get_chat_member(chat_id=Config.CHANNEL_ID, user_id=user_id)
        return member.status in ['creator', 'administrator', 'member']
    except Exception as e:
        logging.error(f"Ошибка при проверке подписки: {e}")
        return False

async def send_sub_alert(message_or_call):
    """Отправка сообщения с просьбой подписаться"""
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Подписаться 📢", url=Config.CHANNEL_LINK)]
    ])
    text = "🚫 Для доступа к нашей библиотеке необходимо подписаться на канал!"
    
    if isinstance(message_or_call, Message):
        await message_or_call.answer(text, reply_markup=kb)
    elif isinstance(message_or_call, CallbackQuery):
        await message_or_call.message.answer(text, reply_markup=kb)

def safe_callback(title: str, prefix: str) -> str:
    """Безопасный callback_data (не более 64 символов)"""
    if len(title) > 40:
        short = title[:37] + "..."
    else:
        short = title
    data = f"{prefix}{short}"
    if len(data) > 60:
        data = data[:60]
    return data

# --- TMDB ПОИСК (на русском) ---
def search_tmdb(query: str):
    """Поиск по TMDB API на русском языке"""
    if not REQUESTS_AVAILABLE or not Config.TMDB_API_KEY:
        return None
    try:
        url = "https://api.themoviedb.org/3/search/movie"
        params = {
            "api_key": Config.TMDB_API_KEY,
            "query": query,
            "language": Config.TMDB_LANGUAGE,
            "page": 1
        }
        response = requests.get(url, params=params, timeout=10)
        data = response.json()
        
        if data.get("results"):
            movie = data["results"][0]
            return {
                "title": movie.get("title") or movie.get("original_title"),
                "year": movie.get("release_date", "")[:4],
                "rating": movie.get("vote_average"),
                "overview": movie.get("overview", "Нет описания")[:280],
                "tmdb_id": movie.get("id")
            }
    except Exception as e:
        logging.error(f"TMDB Error: {e}")
    return None

# --- КЛАВИАТУРЫ ---

def quality_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="1080p", callback_data="q_1080p"), 
         InlineKeyboardButton(text="720p", callback_data="q_720p")],
        [InlineKeyboardButton(text="480p", callback_data="q_480p")]
    ])

def main_kb() -> ReplyKeyboardMarkup:
    """Главная клавиатура для пользователей"""
    return ReplyKeyboardMarkup(keyboard=[
        [
            KeyboardButton(text="📚 Полный список фильмов"), 
            KeyboardButton(text="📚 Список сериалов")
        ]
    ], resize_keyboard=True)

def voice_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="TVShows", callback_data="v_TVShows")],
        [InlineKeyboardButton(text="Lost Film", callback_data="v_LostFilm")],
        [InlineKeyboardButton(text="Дубляж", callback_data="v_Дубляж")],
        [InlineKeyboardButton(text="Кубик в Кубе", callback_data="v_Кубик в Кубе")],
        [InlineKeyboardButton(text="Пифагор", callback_data="v_Пифагор")]
    ])

def admin_panel_kb() -> InlineKeyboardMarkup:
    """Клавиатура главного меню админки"""
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="➕ Добавить Фильм", callback_data="add_video"),
            InlineKeyboardButton(text="➕ Добавить Сериал", callback_data="add_series")
        ],
        [
            InlineKeyboardButton(text="➕ Добавить Сезон", callback_data="add_season")
        ],
        [
            InlineKeyboardButton(text="🗑 Удалить Фильм", callback_data="delete_movie"),
            InlineKeyboardButton(text="🗑 Удалить Сериал", callback_data="delete_series")
        ],
        [
            InlineKeyboardButton(text="📣 Рассылка", callback_data="broadcast"),
            InlineKeyboardButton(text="🔎 Логи запросов", callback_data="logs_0")
        ],
        [
            InlineKeyboardButton(text="🔝 Топ запросов", callback_data="top_queries"),
            InlineKeyboardButton(text="📋 Требуемые фильмы", callback_data="requested_movies")
        ]
    ])

def back_to_admin_kb() -> InlineKeyboardMarkup:
    """Кнопка возврата в меню админки"""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Назад 🔙", callback_data="back_admin")]
    ])

# --- УНИВЕРСАЛЬНЫЕ ФУНКЦИИ ПАГИНАЦИИ ---

async def send_list_page(message_or_call, page: int, items: list, content_type: str = "movies"):
    total_pages = (len(items) - 1) // Config.PAGE_SIZE + 1
    start = page * Config.PAGE_SIZE
    page_items = items[start : start + Config.PAGE_SIZE]
    
    kb_list = [[InlineKeyboardButton(text=f"🎬 {m[0]}", callback_data=safe_callback(m[0], f"play_{content_type}_"))] for m in page_items]
    
    nav_btns = []
    if page > 0: 
        nav_btns.append(InlineKeyboardButton(text="⬅️", callback_data=f"list_{content_type}_{page-1}"))
    if page < total_pages - 1: 
        nav_btns.append(InlineKeyboardButton(text="➡️", callback_data=f"list_{content_type}_{page+1}"))
    
    if nav_btns:
        kb_list.append(nav_btns)
    
    markup = InlineKeyboardMarkup(inline_keyboard=kb_list)
    text = f"📚 Список {content_type} ({page + 1}/{total_pages}):"
    
    if isinstance(message_or_call, Message):
        await message_or_call.answer(text, reply_markup=markup)
    elif isinstance(message_or_call, CallbackQuery):
        await message_or_call.message.edit_text(text, reply_markup=markup)

# --- СТАРТОВАЯ ЛОГИКА ---

@dp.message(Command("start"))
async def start_cmd(message: Message):
    await db.execute(
        "INSERT OR REPLACE INTO users (user_id, is_subscribed) VALUES (?, ?)", 
        (message.from_user.id, 1)
    )
    await message.answer(
        "👋 Привет! Добро пожаловать в нашу видеотеку.\n"
        "Напиши название фильма или сериала, и я найду его для тебя, "
        "или выбери из списка ниже! 🎬🍿\n\n"
        "Теперь поддерживается расширенная информация с TMDB", 
        reply_markup=main_kb()
    )

# --- ЛОГИКА ПАГИНАЦИИ И СПИСКОВ ---

@dp.message(F.text == "📚 Полный список фильмов")
async def show_list_cmd(message: Message):
    if not await is_subscribed(message.from_user.id):
        await send_sub_alert(message)
        return
    movies = await db.fetch_all("SELECT title FROM movies ORDER BY title ASC")
    if not movies:
        await message.answer("Библиотека фильмов пока пуста. ✨")
        return
    await send_list_page(message, 0, movies, "movies")

@dp.message(F.text == "📚 Список сериалов")
async def show_series_list(message: Message):
    if not await is_subscribed(message.from_user.id):
        await send_sub_alert(message)
        return
    series_data = await db.fetch_all("SELECT MIN(id), title FROM series GROUP BY title ORDER BY title ASC")
    if not series_data:
        await message.answer("Библиотека сериалов пока пуста. ✨")
        return
    await send_series_list_page(message, 0, series_data)

async def send_series_list_page(message_or_call, page: int, series_data: list):
    total_pages = max(1, (len(series_data) - 1) // Config.PAGE_SIZE + 1)
    start = page * Config.PAGE_SIZE
    page_items = series_data[start: start + Config.PAGE_SIZE]

    kb_list = [[InlineKeyboardButton(text=f"🎬 {title}", callback_data=f"ps_{sid}")] for sid, title in page_items]

    nav_btns = []
    if page > 0:
        nav_btns.append(InlineKeyboardButton(text="⬅️", callback_data=f"slist_{page-1}"))
    if page < total_pages - 1:
        nav_btns.append(InlineKeyboardButton(text="➡️", callback_data=f"slist_{page+1}"))
    if nav_btns:
        kb_list.append(nav_btns)

    markup = InlineKeyboardMarkup(inline_keyboard=kb_list)
    text = f"📚 Список сериалов ({page + 1}/{total_pages}):"

    if isinstance(message_or_call, Message):
        await message_or_call.answer(text, reply_markup=markup)
    elif isinstance(message_or_call, CallbackQuery):
        await message_or_call.message.edit_text(text, reply_markup=markup)

@dp.callback_query(F.data.startswith("slist_"))
async def callback_series_list_paginated(call: CallbackQuery):
    if not await is_subscribed(call.from_user.id):
        await send_sub_alert(call)
        return
    page = int(call.data.split("_")[1])
    series_data = await db.fetch_all("SELECT MIN(id), title FROM series GROUP BY title ORDER BY title ASC")
    await send_series_list_page(call, page, series_data)
    await call.answer()

@dp.callback_query(F.data.startswith("list_"))
async def callback_movies_list_paginated(call: CallbackQuery):
    if not await is_subscribed(call.from_user.id):
        await send_sub_alert(call)
        return
    parts = call.data.split("_")
    page = int(parts[2])
    items = await db.fetch_all("SELECT title FROM movies ORDER BY title ASC")
    await send_list_page(call, page, items, "movies")
    await call.answer()

# --- ПРОСМОТР ФИЛЬМОВ ПО КНОПКЕ ---

@dp.callback_query(F.data.startswith("play_movies_"))
async def play_movie_by_btn(call: CallbackQuery):
    if not await is_subscribed(call.from_user.id):
        await send_sub_alert(call)
        return
    title = call.data[len("play_movies_"):]
    movie = await db.fetch_one("SELECT voice, quality, file_id FROM movies WHERE title = ?", (title,))
    if movie:
        await call.message.answer_video(
            movie[2],
            caption=f"🍿 Название: {title}\n🎧 Озвучка: {movie[0]}\n💎 Качество: {movie[1]}\n\n✨ Приятного просмотра! 🎬🛋️🔥"
        )
    else:
        await call.answer("Фильм не найден", show_alert=True)
    await call.answer()

# ====================== СИСТЕМА СЕРИАЛОВ ======================

@dp.callback_query(F.data.startswith("ps_"))
async def play_series_handler(call: CallbackQuery):
    if not await is_subscribed(call.from_user.id):
        await send_sub_alert(call)
        return
    series_id = int(call.data.split("_")[1])
    row = await db.fetch_one("SELECT title FROM series WHERE id = ?", (series_id,))
    if not row:
        await call.answer("Сериал не найден", show_alert=True)
        return
    title = row[0]
    seasons = await db.fetch_all(
        "SELECT DISTINCT season FROM series WHERE title = ? ORDER BY season ASC", (title,)
    )
    if not seasons:
        await call.answer("Сезоны не найдены", show_alert=True)
        return

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
        await send_sub_alert(call)
        return
    parts = call.data.split("_")
    series_id = int(parts[1])
    season = int(parts[2])

    row = await db.fetch_one("SELECT title FROM series WHERE id = ?", (series_id,))
    if not row:
        await call.answer("Сериал не найден", show_alert=True)
        return
    title = row[0]

    episodes = await db.fetch_all(
        "SELECT id, episode FROM series WHERE title = ? AND season = ? ORDER BY episode ASC",
        (title, season)
    )
    if not episodes:
        await call.answer("Серии не найдены", show_alert=True)
        return

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
        await send_sub_alert(call)
        return

    row_id = int(call.data.split("_")[1])

    try:
        info = await db.fetch_one(
            "SELECT title, season, episode, voice, quality, file_id FROM series WHERE id = ?",
            (row_id,)
        )
        if not info:
            await call.answer("Серия не найдена", show_alert=True)
            return

        title, season, episode, voice, quality, file_id = info

        await call.message.answer_video(
            file_id,
            caption=(
                f"🎬 <b>{title}</b>\n"
                f"📺 Сезон {season} • Серия {episode}\n"
                f"🎧 {voice} • 💎 {quality}\n\n"
                f"✨ Приятного просмотра! 🔥"
            ),
            parse_mode="HTML"
        )

        episodes = await db.fetch_all(
            "SELECT id, episode FROM series WHERE title = ? AND season = ? ORDER BY episode ASC",
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
        logging.error(f"Ошибка при воспроизведении серии: {e}")
        await call.answer("Ошибка при воспроизведении видео.", show_alert=True)

    await call.answer()

# --- АДМИН ПАНЕЛЬ ---

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
        await call.answer("Доступ запрещён!", show_alert=True)
        return
    requested = await db.fetch_all("SELECT title, count FROM requested ORDER BY count DESC, timestamp DESC")
    if not requested:
        text = "📋 Пока нет запрошенных фильмов."
    else:
        text = "📋 <b>Требуемые фильмы</b>\n\n"
        for i, (title, count) in enumerate(requested, 1):
            text += f"{i}. {title} — {count} запрос(ов)\n"
    await call.message.edit_text(text, reply_markup=back_to_admin_kb(), parse_mode="HTML")
    await call.answer()

# --- ДОБАВЛЕНИЕ ФИЛЬМА ---

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
    await call.message.edit_text(f"✅ Озвучка выбрана: <b>{voice}</b>\n\n💎 Выберите качество:", reply_markup=quality_kb(), parse_mode="HTML")
    await state.set_state(AdminStates.MOVIE_QUALITY)
    await call.answer()

@dp.callback_query(AdminStates.MOVIE_QUALITY, F.data.startswith("q_"))
async def get_quality(call: CallbackQuery, state: FSMContext):
    quality = call.data.split("_", 1)[1]
    await state.update_data(quality=quality)
    await call.message.edit_text(f"✅ Качество выбрано: <b>{quality}</b>\n\n📤 Отправьте видеофайл:", parse_mode="HTML")
    await state.set_state(AdminStates.MOVIE_FILE)
    await call.answer()

@dp.message(AdminStates.MOVIE_FILE, F.video)
async def get_file(msg: Message, state: FSMContext):
    data = await state.get_data()
    await db.execute(
        "INSERT INTO movies (title, voice, quality, file_id) VALUES (?, ?, ?, ?)", 
        (data['name'], data['voice'], data['quality'], msg.video.file_id)
    )
    await msg.answer("✅ Фильм успешно сохранён!")
    await state.clear()

# --- ДОБАВЛЕНИЕ СЕРИАЛА ---

@dp.callback_query(F.data == "add_series")
async def add_series_start(call: CallbackQuery, state: FSMContext):
    await call.message.answer("🎬 Введите название сериала:")
    await state.set_state(AdminStates.SERIES_NAME)
    await call.answer()

@dp.callback_query(F.data == "add_season")
async def add_season_start(call: CallbackQuery, state: FSMContext):
    series_data = await db.fetch_all("SELECT MIN(id), title FROM series GROUP BY title ORDER BY title ASC")
    if not series_data:
        await call.answer("Нет сериалов в базе. Сначала добавьте сериал.", show_alert=True)
        return
    kb_list = [[InlineKeyboardButton(text=f"📺 {title}", callback_data=f"ases_{sid}")] for sid, title in series_data]
    kb_list.append([InlineKeyboardButton(text="Назад 🔙", callback_data="back_admin")])
    await call.message.answer("Выберите сериал для добавления сезона:", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_list))
    await call.answer()

@dp.callback_query(F.data.startswith("ases_"))
async def add_season_select_series(call: CallbackQuery, state: FSMContext):
    sid = int(call.data.split("_")[1])
    row = await db.fetch_one("SELECT title FROM series WHERE id = ?", (sid,))
    if not row:
        await call.answer("Сериал не найден", show_alert=True)
        return
    title = row[0]
    await state.update_data(name=title)
    await call.message.answer(f"✅ Сериал: <b>{title}</b>\n\n🎧 Выберите озвучку:", reply_markup=voice_kb(), parse_mode="HTML")
    await state.set_state(AdminStates.SERIES_VOICE)
    await call.answer()

@dp.message(AdminStates.SERIES_NAME)
async def series_get_name(msg: Message, state: FSMContext):
    await state.update_data(name=msg.text)
    await msg.answer("🎧 Выберите озвучку:", reply_markup=voice_kb())
    await state.set_state(AdminStates.SERIES_VOICE)

@dp.callback_query(AdminStates.SERIES_VOICE, F.data.startswith("v_"))
async def series_get_voice(call: CallbackQuery, state: FSMContext):
    voice = call.data.split("_")[1]
    await state.update_data(voice=voice)
    await call.message.edit_text(f"✅ Озвучка выбрана: <b>{voice}</b>\n\n💎 Выберите качество:", reply_markup=quality_kb(), parse_mode="HTML")
    await state.set_state(AdminStates.SERIES_QUALITY)
    await call.answer()

@dp.callback_query(AdminStates.SERIES_QUALITY, F.data.startswith("q_"))
async def series_get_qual(call: CallbackQuery, state: FSMContext):
    quality = call.data.split("_", 1)[1]
    await state.update_data(quality=quality)
    await call.message.edit_text(f"✅ Качество выбрано: <b>{quality}</b>\n\n📅 Введите номер сезона:", parse_mode="HTML")
    await state.set_state(AdminStates.SERIES_SEASON)
    await call.answer()

@dp.message(AdminStates.SERIES_SEASON)
async def series_get_season(msg: Message, state: FSMContext):
    if not msg.text.isdigit():
        await msg.answer("Введите цифру!")
        return
    await state.update_data(season=int(msg.text), ep_counter=1)
    await msg.answer(
        "📤 Отправляйте видео серий по порядку.\n"
        "Когда закончите — напишите <b>Готово</b>",
        parse_mode="HTML"
    )
    await state.set_state(AdminStates.SERIES_WAITING_VIDEOS)

@dp.message(AdminStates.SERIES_WAITING_VIDEOS, F.video)
async def save_series_video(msg: Message, state: FSMContext):
    data = await state.get_data()
    ep = data['ep_counter']
    await db.execute(
        "INSERT INTO series (title, voice, quality, season, episode, file_id) VALUES (?, ?, ?, ?, ?, ?)",
        (data['name'], data['voice'], data['quality'], data['season'], ep, msg.video.file_id)
    )
    await msg.answer(f"✅ Серия {ep} добавлена!")
    await state.update_data(ep_counter=ep + 1)

@dp.message(AdminStates.SERIES_WAITING_VIDEOS, F.text.lower() == "готово")
async def finish_series(msg: Message, state: FSMContext):
    await msg.answer("🎉 Сериал успешно добавлен!")
    await state.clear()

# --- УДАЛЕНИЕ ---

@dp.callback_query(F.data == "delete_movie")
async def delete_movie_list(call: CallbackQuery):
    movies = await db.fetch_all("SELECT title FROM movies ORDER BY title ASC")
    if not movies:
        await call.answer("Библиотека фильмов пуста.", show_alert=True)
        return
    kb_list = [[InlineKeyboardButton(text=f"🗑 {m[0]}", callback_data=f"del_m_{m[0][:40]}")] for m in movies]
    kb_list.append([InlineKeyboardButton(text="Назад 🔙", callback_data="back_admin")])
    await call.message.edit_text("Выберите фильм для удаления:", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_list))

@dp.callback_query(F.data.startswith("del_m_"))
async def confirm_delete_movie(call: CallbackQuery):
    title = call.data.replace("del_m_", "")
    await db.execute("DELETE FROM movies WHERE title = ?", (title,))
    await call.answer("Фильм удалён!")
    await back_to_admin(call)

async def _show_del_series_list(call: CallbackQuery):
    series = await db.fetch_all("SELECT MIN(id), title FROM series GROUP BY title ORDER BY title ASC")
    if not series:
        await call.message.edit_text("Библиотека сериалов пуста.", reply_markup=back_to_admin_kb())
        return
    kb_list = [[InlineKeyboardButton(text=f"📺 {title}", callback_data=f"dss_{sid}")] for sid, title in series]
    kb_list.append([InlineKeyboardButton(text="Назад 🔙", callback_data="back_admin")])
    await call.message.edit_text("Выберите сериал:", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_list))

async def _show_del_seasons(call: CallbackQuery, sid: int):
    row = await db.fetch_one("SELECT title FROM series WHERE id = ?", (sid,))
    if not row:
        await call.answer("Сериал не найден", show_alert=True)
        return
    title = row[0]
    seasons = await db.fetch_all("SELECT DISTINCT season FROM series WHERE title = ? ORDER BY season ASC", (title,))
    kb_list = [[InlineKeyboardButton(text=f"📂 Сезон {s[0]}", callback_data=f"dse_{sid}_{s[0]}")] for s in seasons]
    kb_list.append([InlineKeyboardButton(text="🗑 Удалить весь сериал", callback_data=f"das_{sid}")])
    kb_list.append([InlineKeyboardButton(text="Назад 🔙", callback_data="delete_series")])
    await call.message.edit_text(
        f"📺 <b>{title}</b>\n\nВыберите сезон или удалите весь сериал:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_list),
        parse_mode="HTML"
    )

async def _show_del_episodes(call: CallbackQuery, sid: int, season: int):
    row = await db.fetch_one("SELECT title FROM series WHERE id = ?", (sid,))
    if not row:
        await call.answer("Сериал не найден", show_alert=True)
        return
    title = row[0]
    episodes = await db.fetch_all(
        "SELECT id, episode FROM series WHERE title = ? AND season = ? ORDER BY episode ASC",
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
    row = await db.fetch_one("SELECT title FROM series WHERE id = ?", (sid,))
    if not row:
        await call.answer("Сериал не найден", show_alert=True)
        return
    title = row[0]
    await db.execute("DELETE FROM series WHERE title = ?", (title,))
    await call.answer(f"Сериал «{title}» удалён!", show_alert=True)
    await back_to_admin(call)

@dp.callback_query(F.data.startswith("dse_"))
async def delete_series_episodes(call: CallbackQuery):
    parts = call.data.split("_")
    sid = int(parts[1])
    season = int(parts[2])
    await _show_del_episodes(call, sid, season)
    await call.answer()

@dp.callback_query(F.data.startswith("dsa_"))
async def delete_season_all(call: CallbackQuery):
    parts = call.data.split("_")
    sid = int(parts[1])
    season = int(parts[2])
    row = await db.fetch_one("SELECT title FROM series WHERE id = ?", (sid,))
    if not row:
        await call.answer("Сериал не найден", show_alert=True)
        return
    title = row[0]
    await db.execute("DELETE FROM series WHERE title = ? AND season = ?", (title, season))
    await call.answer(f"Сезон {season} удалён!", show_alert=True)
    await _show_del_seasons(call, sid)

@dp.callback_query(F.data.startswith("dep_"))
async def delete_one_episode(call: CallbackQuery):
    row_id = int(call.data.split("_")[1])
    info = await db.fetch_one("SELECT title, season, episode FROM series WHERE id = ?", (row_id,))
    if not info:
        await call.answer("Серия не найдена", show_alert=True)
        return
    title, season, episode = info
    await db.execute("DELETE FROM series WHERE id = ?", (row_id,))
    await call.answer(f"Серия {episode} удалена!", show_alert=True)
    sid_row = await db.fetch_one("SELECT MIN(id) FROM series WHERE title = ? AND season = ?", (title, season))
    if sid_row and sid_row[0]:
        await _show_del_episodes(call, sid_row[0], season)
    else:
        sid_row2 = await db.fetch_one("SELECT MIN(id) FROM series WHERE title = ?", (title,))
        if sid_row2 and sid_row2[0]:
            await _show_del_seasons(call, sid_row2[0])
        else:
            await back_to_admin(call)

# --- РАССЫЛКА ---

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

# --- ПОИСК С TMDB (обновлённый) ---

@dp.message(F.text)
async def search_content(msg: Message):
    if not await is_subscribed(msg.from_user.id):
        await send_sub_alert(msg)
        return
    
    query = msg.text.strip()
    await db.log_query(query)

    # Поиск в фильмах
    all_movies = await db.fetch_all("SELECT title FROM movies")
    titles = [m[0] for m in all_movies]
    match = process.extractOne(query, titles, score_cutoff=70)
    
    found_in_library = False
    if match:
        found_in_library = True
        movie = await db.fetch_one("SELECT voice, quality, file_id FROM movies WHERE title = ?", (match[0],))
        await msg.answer_video(
            movie[2], 
            caption=f"🍿 Найдено в библиотеке: {match[0]}\n🎧 Озвучка: {movie[0]}\n💎 Качество: {movie[1]}\n\n✨ Приятного просмотра!"
        )

    # Поиск сериалов
    all_series = await db.fetch_all("SELECT MIN(id), title FROM series GROUP BY title")
    series_match = process.extractOne(query, [s[1] for s in all_series], score_cutoff=70)
    if series_match:
        title = series_match[0]
        sid_row = await db.fetch_one("SELECT MIN(id) FROM series WHERE title = ?", (title,))
        sid = sid_row[0]
        await msg.answer(
            f"🎬 Найден сериал: <b>{title}</b>",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="📺 Выбрать сезон", callback_data=f"ps_{sid}")
            ]]),
            parse_mode="HTML"
        )
        return

    # Всегда показываем информацию с TMDB
    tmdb_result = search_tmdb(query)
    if tmdb_result:
        text = f"🎥 <b>{tmdb_result['title']}</b> ({tmdb_result['year']})\n" \
               f"⭐ Рейтинг: {tmdb_result['rating']}\n" \
               f"📖 {tmdb_result['overview']}"
        await msg.answer(text, parse_mode="HTML", disable_web_page_preview=True)

        # Если не найдено в библиотеке — отправляем запрос админам
        if not found_in_library:
            await msg.answer("😔 Фильм который вы ищете ещё не был добавлен к нам, но ваш запрос отправлен админам которые скоро добавят фильм.")
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

# --- ЛОГИ И СТАТИСТИКА ---

@dp.callback_query(F.data.startswith("logs_"))
async def show_logs(call: CallbackQuery):
    logs = await db.fetch_all("SELECT query FROM logs ORDER BY id DESC LIMIT 10")
    text = "🔎 Последние 10 запросов:\n" + "\n".join([f"- {l[0]}" for l in logs]) if logs else "Логи пусты."
    await call.message.edit_text(text, reply_markup=back_to_admin_kb())
    await call.answer()

@dp.callback_query(F.data == "top_queries")
async def show_top(call: CallbackQuery):
    top = await db.fetch_all("SELECT query, count FROM query_stats ORDER BY count DESC LIMIT 10")
    text = "🔝 Топ-10 частых запросов:\n\n" + "\n".join([f"{i+1}. {r[0]} — {r[1]} раз" for i, r in enumerate(top)]) if top else "Статистика пуста."
    await call.message.edit_text(text, reply_markup=back_to_admin_kb())
    await call.answer()

# --- KEEP-ALIVE СЕРВЕР ---

async def handle(request):
    return web.Response(text="Bot is alive!")

async def run_web():
    app = web.Application()
    app.router.add_get("/", handle)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", 5000)
    await site.start()

# --- ЗАПУСК БОТА ---

async def main():
    logging.info("Бот запущен!")
    await init_storage()
    await asyncio.gather(run_web(), dp.start_polling(bot))

if __name__ == "__main__":
    asyncio.run(main())
