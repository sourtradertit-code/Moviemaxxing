import asyncio
import logging
import os

from aiogram import Bot, Dispatcher

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BOT_TOKEN = os.getenv("BOT_TOKEN", "")

async def main():
    if not BOT_TOKEN:
        logger.warning("BOT_TOKEN is not set. Please set the BOT_TOKEN environment variable.")
        logger.info("Bot is not started. Set BOT_TOKEN to begin.")
        return

    bot = Bot(token=BOT_TOKEN)
    dp = Dispatcher()

    logger.info("Starting bot...")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
