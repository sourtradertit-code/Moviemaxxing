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
        f"👋 Hello, {message.from_user.full_name}!\n\n"
        "I'm <b>Films Max</b> — your movie bot.\n\n"
        "Use /help to see available commands.",
        parse_mode="HTML"
    )


@dp.message(Command("help"))
async def cmd_help(message: Message):
    await message.answer(
        "🎬 <b>Films Max — Commands</b>\n\n"
        "/start — Welcome message\n"
        "/help — Show this help\n",
        parse_mode="HTML"
    )


@dp.message(F.text)
async def handle_text(message: Message):
    await message.answer(
        f"You said: <i>{message.text}</i>\n\nUse /help to see available commands.",
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
