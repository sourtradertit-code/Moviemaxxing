"""
Парсер @VipkinoDenbot → сохраняет фильмы в bot_data.db
Запуск: python parser.py
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
DELAY      = 5

MOVIES = [
    # Марвел
    "Железный человек", "Железный человек 2", "Железный человек 3",
    "Мстители", "Мстители Финал", "Мстители Война бесконечности",
    "Тор", "Тор Мир тьмы", "Тор Рагнарёк", "Тор Любовь и гром",
    "Капитан Америка", "Капитан Америка Зимний солдат",
    "Человек-паук", "Человек-паук Нет пути домой",
    "Доктор Стрэндж", "Чёрная пантера", "Стражи Галактики",
    "Муравей", "Человек-муравей", "Шазам", "Аквамен",
    # DC
    "Бэтмен", "Тёмный рыцарь", "Тёмный рыцарь Возрождение",
    "Бэтмен против Супермена", "Лига справедливости", "Джокер",
    "Супермен", "Флэш", "Хранители",
    # Классика
    "Матрица", "Матрица Перезагрузка", "Матрица Революции",
    "Терминатор", "Терминатор 2", "Терминатор Да придёт спаситель",
    "Аватар", "Аватар Путь воды", "Титаник", "Интерстеллар",
    "Начало", "Довод", "Престиж", "Бэтмен Начало",
    "Форрест Гамп", "Бойцовский клуб", "Зелёная миля",
    "Побег из Шоушенка", "Список Шиндлера", "Пианист",
    # Фантастика
    "Звёздные войны Новая надежда", "Звёздные войны Империя наносит ответный удар",
    "Звёздные войны Возвращение джедая", "Звёздные войны Скрытая угроза",
    "Дюна", "Дюна Часть вторая", "Прибытие", "Гравитация",
    "Марсианин", "Элизиум", "Район 9", "Грань будущего",
    # Боевики
    "Джон Уик", "Джон Уик 2", "Джон Уик 3", "Джон Уик 4",
    "Адреналин", "Транспортировщик", "Форсаж", "Форсаж 7",
    "Миссия невыполнима", "Миссия невыполнима Протокол Фантом",
    "007 Координаты Скайфолл", "007 Спектр", "007 Не время умирать",
    "Никто", "Громкая связь", "Схватка",
    # Триллеры
    "Семь", "Молчание ягнят", "Исчезнувшая", "Девушка с татуировкой дракона",
    "Достать ножи", "Достать ножи Стеклянная луковица",
    "Не смотри вверх", "Клюша", "Побег из Претории",
    # Комедии
    "Один дома", "Один дома 2", "Маска", "Тупой и ещё тупее",
    "Мальчишник в Вегасе", "Мальчишник 2",
    "Иллюзия обмана", "Иллюзия обмана 2",
    # Анимация
    "Лев король", "Король лев", "Алладин", "Красавица и чудовище",
    "Ледниковый период", "Мадагаскар", "Шрек", "Шрек 2",
    "Тачки", "Вверх", "ВАЛЛ-И", "Душа", "Лука",
    "Тайна Коко", "Энканто", "Мулан",
    "Человек-паук Через вселенные", "Человек-паук Паутина вселенных",
    # Ужасы
    "Оно", "Оно Глава вторая", "Пила", "Заклятие",
    "Синистер", "Прочь", "Мы", "Нет",
    "Тихое место", "Тихое место 2", "Астрал",
    # Российское кино
    "Брат", "Брат 2", "Бригада", "Слово пацана",
    "Майор Гром", "Майор Гром Чумной доктор",
    "Холоп", "Холоп 2", "Горько", "Горько 2",
    "Движение вверх", "Легенда №17", "Тренер",
    "Время первых", "Салют 7", "Собибор",
    # Азиатское кино
    "Паразиты", "Олдбой", "Поезд в Пусан",
    "Воин", "Герой", "Дом летающих кинжалов",
    # Криминальные
    "Криминальное чтиво", "Джанго освобождённый",
    "Однажды в Голливуде", "Бесславные ублюдки",
    "Карты деньги два ствола", "Большой куш", "Рок-н-рольщик",
    "Волк с Уолл-стрит", "Отступники", "Хорошие парни",
    "Гангстер", "Лицо со шрамом", "Крёстный отец",
    "Крёстный отец 2", "Однажды в Америке", "Славные парни",
    # Приключения
    "Властелин колец Братство кольца", "Властелин колец Две крепости",
    "Властелин колец Возвращение короля",
    "Хоббит Нежданное путешествие", "Хоббит Пустошь Смауга",
    "Гарри Поттер и философский камень", "Гарри Поттер и тайная комната",
    "Гарри Поттер и узник Азкабана", "Гарри Поттер и Дары Смерти",
    "Пираты Карибского моря", "Пираты Карибского моря Сундук мертвеца",
    "Индиана Джонс", "Индиана Джонс и храм судьбы",
    "Назад в будущее", "Назад в будущее 2",
    "Парк Юрского периода", "Мир Юрского периода",
    "Кинг Конг", "Годзилла", "Годзилла против Конга",
    # Недавние хиты
    "Топ Ган Мэверик", "Оппенгеймер", "Барби",
    "Килlers of the Flower Moon", "Прошлые жизни",
    "Бедные создания", "Зона интересов", "Анатомия падения",
    "Наполеон", "Вавилон", "Все везде и сразу",
    "Ирландец", "1917", "Дюнкерк",
    "Гнев человеческий", "Злой город", "Бенедетта",
]


def parse_caption(caption: str):
    if not caption:
        return None
    m = re.match(r"^(.+?)\s*\((.+?)\s*\[(.+?)\]\)", caption.strip())
    if m:
        return m.group(1).strip(), m.group(2).strip(), m.group(3).strip()
    return caption.strip(), "Неизвестно", "Неизвестно"


async def save_movie(db, title, voice, quality, file_id):
    await db.execute(
        "INSERT OR IGNORE INTO movies (title, voice, quality, file_id) VALUES (?, ?, ?, ?)",
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
        log.info(f"  ❌ Нет результатов: {movie_name}")
        return

    log.info(f"  Найдено {len(results.results)} результатов")

    for result in results.results:
        try:
            await app.send_inline_bot_result(TARGET_BOT, results.query_id, result.id)
            await asyncio.sleep(4)

            # Читаем последние сообщения от бота
            async for msg in app.get_chat_history(TARGET_BOT, limit=3):
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
            break
        except QueryIdInvalid:
            log.warning("  QueryIdInvalid — пропускаем")
            break
        except FloodWait as e:
            log.warning(f"  FloodWait {e.value}s")
            await asyncio.sleep(e.value + 2)
            break
        except Exception as e:
            log.error(f"  Ошибка: {e}")
            break


async def main():
    async with Client("parser_session", api_id=API_ID, api_hash=API_HASH) as app:
        async with aiosqlite.connect(DB_PATH) as db:
            log.info(f"🚀 Начинаем парсинг {len(MOVIES)} фильмов...")
            for i, movie in enumerate(MOVIES, 1):
                log.info(f"[{i}/{len(MOVIES)}]")
                await search_and_save(app, db, movie)
                await asyncio.sleep(DELAY)
            log.info("✅ Парсинг завершён!")


if __name__ == "__main__":
    asyncio.run(main())
