"""
Парсер @VipkinoDenbot → сохраняет фильмы в bot_data.db
Запуск: python parser.py
Первый раз попросит номер телефона и код (одноразово)
"""

import asyncio
import aiosqlite
import re
import os
import logging
from pyrogram import Client
from pyrogram.errors import FloodWait, QueryIdInvalid

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

API_ID   = int(os.environ.get("PARSER_API_ID", "38260292"))
API_HASH = os.environ.get("PARSER_API_HASH", "75465e743d507b467b61d1be29b32468")

TARGET_BOT = "VipkinoDenbot"
DB_PATH    = "bot_data.db"
DELAY      = 4   # секунд между фильмами (чтобы не забанили)

# ──────────────── Список фильмов для поиска ────────────────
MOVIES = [
    "Железный человек",
    "Мстители",
    "Тор",
    "Капитан Америка",
    "Человек-паук",
    "Бэтмен",
    "Джокер",
    "Интерстеллар",
    "Начало",
    "Тёмный рыцарь",
    "Матрица",
    "Терминатор",
    "Аватар",
    "Титаник",
    "Форрест Гамп",
    "Бойцовский клуб",
    "Зелёная миля",
    "Побег из Шоушенка",
    "Властелин колец",
    "Гарри Поттер",
    "Гладиатор",
    "Пираты Карибского моря",
    "Звёздные войны",
    "Назад в будущее",
    "Индиана Джонс",
    "Джон Уик",
    "Дюна",
    "Оппенгеймер",
    "Барби",
    "Топ Ган",
    "Драйв",
    "Волк с Уолл-стрит",
    "Отступники",
    "Однажды в Голливуде",
    "Паразиты",
    "Игра в кальмара",
    "Ведьмак",
    "Игра престолов",
    "Сломанные цветы",
    "Леон",
    "Французский связной",
    "Такси",
    "Люк",
    "Амели",
    "Достать ножи",
    "Не смотри вверх",
    "Птица",
    "Брат",
    "Брат 2",
    "Бригада",
    "Слово пацана",
    "Мажор",
    "Физрук",
]


def parse_caption(caption: str):
    """
    Разбирает подпись вида:
      Железный человек (Дублированный [1080p])
    Возвращает (title, voice, quality) или None
    """
    if not caption:
        return None
    m = re.match(r"^(.+?)\s*\((.+?)\s*\[(.+?)\]\)", caption.strip())
    if m:
        return m.group(1).strip(), m.group(2).strip(), m.group(3).strip()
    # Запасной вариант — только название
    return caption.strip(), "Неизвестно", "Неизвестно"


async def save_movie(db, title, voice, quality, file_id):
    await db.execute(
        """INSERT OR IGNORE INTO movies (title, voice, quality, file_id)
           VALUES (?, ?, ?, ?)""",
        (title, voice, quality, file_id),
    )
    await db.commit()


async def search_and_save(app: Client, db, movie_name: str):
    log.info(f"🔍 Ищем: {movie_name}")
    try:
        results = await app.get_inline_bot_results(TARGET_BOT, movie_name)
    except FloodWait as e:
        log.warning(f"FloodWait {e.value}s")
        await asyncio.sleep(e.value + 2)
        return
    except Exception as e:
        log.error(f"Ошибка поиска '{movie_name}': {e}")
        return

    if not results.results:
        log.info(f"  Нет результатов для: {movie_name}")
        return

    for result in results.results:
        try:
            # Отправляем inline-результат в чат бота (он пришлёт нам видео)
            sent = await app.send_inline_bot_result(
                TARGET_BOT, results.query_id, result.id
            )
            await asyncio.sleep(3)

            # Читаем последнее сообщение от бота в том чате
            async for msg in app.get_chat_history(TARGET_BOT, limit=5):
                if msg.video and msg.video.file_id:
                    parsed = parse_caption(msg.caption)
                    if parsed:
                        title, voice, quality = parsed
                    else:
                        title, voice, quality = movie_name, "Неизвестно", "Неизвестно"
                    file_id = msg.video.file_id
                    await save_movie(db, title, voice, quality, file_id)
                    log.info(f"  ✅ Сохранён: {title} | {voice} | {quality}")
                    break
            break  # берём только первый результат
        except QueryIdInvalid:
            log.warning("  QueryIdInvalid — пропускаем")
            break
        except FloodWait as e:
            log.warning(f"  FloodWait {e.value}s")
            await asyncio.sleep(e.value + 2)
            break
        except Exception as e:
            log.error(f"  Ошибка при отправке результата: {e}")
            break


async def main():
    async with Client(
        "parser_session",
        api_id=API_ID,
        api_hash=API_HASH,
    ) as app:
        async with aiosqlite.connect(DB_PATH) as db:
            log.info(f"🚀 Начинаем парсинг {len(MOVIES)} фильмов...")
            for i, movie in enumerate(MOVIES, 1):
                log.info(f"[{i}/{len(MOVIES)}]")
                await search_and_save(app, db, movie)
                await asyncio.sleep(DELAY)
            log.info("✅ Парсинг завершён!")


if __name__ == "__main__":
    asyncio.run(main())
