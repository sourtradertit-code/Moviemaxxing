"""
Запусти этот скрипт ОДИН РАЗ для авторизации:
  python auth_parser.py
Потом запускай parser.py — больше логин не нужен.
"""
import asyncio
import os
from pyrogram import Client

API_ID   = int(os.environ.get("PARSER_API_ID", "38260292"))
API_HASH = os.environ.get("PARSER_API_HASH", "75465e743d507b467b61d1be29b32468")
PHONE    = "+13253866602"

async def main():
    app = Client("parser_session", api_id=API_ID, api_hash=API_HASH)
    await app.connect()

    sent = await app.send_code(PHONE)
    print(f"Код отправлен на {PHONE}")
    code = input("Введи код из Telegram: ").strip()

    try:
        await app.sign_in(PHONE, sent.phone_code_hash, code)
        print("✅ Авторизация успешна! Можно запускать parser.py")
    except Exception as e:
        if "SESSION_PASSWORD_NEEDED" in str(e):
            pwd = input("Введи пароль двухфакторки: ").strip()
            await app.check_password(pwd)
            print("✅ Авторизация успешна!")
        else:
            print(f"Ошибка: {e}")

    await app.disconnect()

asyncio.run(main())
