import asyncio
import logging
import os

from aiogram import Bot, Dispatcher, F
from aiogram.filters import CommandStart, Command
from aiogram.types import Message

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BOT_TOKEN = os.getenv("BOT_TOKEN", "")

dp = Dispatcher()


@dp.message(CommandStart())
async def cmd_start(message: Message):
    await message.answer(
        f"👋 Привет, {message.from_user.full_name}!\n\n"
        "Я <b>Films Max</b> — твой бот о фильмах.\n\n"
        "Используй /help чтобы увидеть доступные команды.",
        parse_mode="HTML"
    )


@dp.message(Command("help"))
async def cmd_help(message: Message):
    await message.answer(
        "🎬 <b>Films Max — Команды</b>\n\n"
        "/start — Приветственное сообщение\n"
        "/help — Показать это меню\n",
        parse_mode="HTML"
    )


@dp.message(F.text)
async def handle_text(message: Message):
    await message.answer(
        f"Ты написал: <i>{message.text}</i>\n\nИспользуй /help чтобы увидеть доступные команды.",
        parse_mode="HTML"
    )


async def main():
    if not BOT_TOKEN:
        logger.warning("BOT_TOKEN is not set. Please set the BOT_TOKEN environment variable.")
        return

    bot = Bot(token=BOT_TOKEN)
    logger.info("Starting bot...")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
