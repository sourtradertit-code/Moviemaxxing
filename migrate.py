"""
Одноразовый скрипт миграции данных из SQLite → PostgreSQL
Запуск: python migrate.py
"""
import asyncio
import asyncpg
import sqlite3
import os

DB_PATH = "bot_data.db"
PG_URL  = os.environ["DATABASE_URL"]

async def main():
    conn = await asyncpg.connect(PG_URL)
    sq   = sqlite3.connect(DB_PATH)
    c    = sq.cursor()

    print("🏗 Создаём таблицы...")
    await conn.execute("""CREATE TABLE IF NOT EXISTS users (
        user_id BIGINT PRIMARY KEY, is_subscribed INTEGER DEFAULT 0)""")
    await conn.execute("""CREATE TABLE IF NOT EXISTS movies (
        title TEXT, voice TEXT, quality TEXT, file_id TEXT, UNIQUE(title, voice, quality))""")
    await conn.execute("""CREATE TABLE IF NOT EXISTS series (
        id SERIAL PRIMARY KEY, title TEXT, voice TEXT, quality TEXT,
        season INTEGER, episode INTEGER, file_id TEXT,
        UNIQUE(title, voice, quality, season, episode))""")
    await conn.execute("""CREATE TABLE IF NOT EXISTS logs (
        id SERIAL PRIMARY KEY, query TEXT, timestamp TIMESTAMPTZ DEFAULT NOW())""")
    await conn.execute("""CREATE TABLE IF NOT EXISTS query_stats (
        query TEXT PRIMARY KEY, count INTEGER DEFAULT 1)""")
    await conn.execute("""CREATE TABLE IF NOT EXISTS requested (
        id SERIAL PRIMARY KEY, title TEXT UNIQUE, count INTEGER DEFAULT 1,
        timestamp TIMESTAMPTZ DEFAULT NOW())""")
    print("  ✅ Таблицы готовы")

    print("📦 Мигрируем movies...")
    c.execute("SELECT title, voice, quality, file_id FROM movies")
    movies = c.fetchall()
    for row in movies:
        await conn.execute(
            "INSERT INTO movies (title, voice, quality, file_id) VALUES ($1,$2,$3,$4) ON CONFLICT DO NOTHING",
            *row
        )
    print(f"  ✅ {len(movies)} фильмов")

    print("📦 Мигрируем series...")
    c.execute("SELECT title, voice, quality, season, episode, file_id FROM series")
    series = c.fetchall()
    for row in series:
        await conn.execute(
            "INSERT INTO series (title, voice, quality, season, episode, file_id) "
            "VALUES ($1,$2,$3,$4,$5,$6) ON CONFLICT DO NOTHING",
            *row
        )
    print(f"  ✅ {len(series)} серий")

    print("📦 Мигрируем users...")
    c.execute("SELECT user_id, is_subscribed FROM users")
    users = c.fetchall()
    for row in users:
        await conn.execute(
            "INSERT INTO users (user_id, is_subscribed) VALUES ($1,$2) ON CONFLICT (user_id) DO UPDATE SET is_subscribed=$2",
            *row
        )
    print(f"  ✅ {len(users)} пользователей")

    print("📦 Мигрируем logs...")
    c.execute("SELECT query FROM logs")
    logs = c.fetchall()
    for row in logs:
        await conn.execute("INSERT INTO logs (query) VALUES ($1)", row[0])
    print(f"  ✅ {len(logs)} логов")

    print("📦 Мигрируем query_stats...")
    c.execute("SELECT query, count FROM query_stats")
    stats = c.fetchall()
    for row in stats:
        await conn.execute(
            "INSERT INTO query_stats (query, count) VALUES ($1,$2) ON CONFLICT (query) DO UPDATE SET count=$2",
            *row
        )
    print(f"  ✅ {len(stats)} статистик")

    print("📦 Мигрируем requested...")
    try:
        c.execute("SELECT title, count FROM requested")
        requested = c.fetchall()
        for row in requested:
            await conn.execute(
                "INSERT INTO requested (title, count) VALUES ($1,$2) ON CONFLICT (title) DO UPDATE SET count=$2",
                *row
            )
        print(f"  ✅ {len(requested)} запросов")
    except Exception as e:
        print(f"  ⚠️ requested: {e}")

    sq.close()
    await conn.close()
    print("\n🎉 Миграция завершена!")

asyncio.run(main())
