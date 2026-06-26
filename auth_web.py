"""
Веб-авторизация для парсера.
Запусти: python auth_web.py
Открой ссылку из Webview и введи код там — он не успеет истечь.
"""
import asyncio
from aiohttp import web
from pyrogram import Client

API_ID   = 38260292
API_HASH = "75465e743d507b467b61d1be29b32468"
PHONE    = "+13253866602"

app_state = {"hash": None, "client": None, "done": False}

HTML_FORM = """
<html><body style="font-family:sans-serif;max-width:400px;margin:80px auto;text-align:center">
<h2>Введи код из Telegram</h2>
<p>Код отправлен на <b>{phone}</b></p>
<form method="POST" action="/verify">
  <input name="code" autofocus placeholder="12345" style="font-size:24px;width:150px;text-align:center">
  <br><br>
  <button type="submit" style="font-size:18px;padding:10px 30px">Войти</button>
</form>
</body></html>
"""

HTML_OK = """
<html><body style="font-family:sans-serif;max-width:400px;margin:80px auto;text-align:center">
<h2>✅ Авторизация успешна!</h2>
<p>Можешь закрыть эту страницу.<br>
Сессия сохранена — парсер готов к работе.</p>
</body></html>
"""

HTML_ERR = """
<html><body style="font-family:sans-serif;max-width:400px;margin:80px auto;text-align:center">
<h2>❌ Ошибка</h2><p>{error}</p>
<a href="/">Попробовать снова</a>
</body></html>
"""

async def handle_index(request):
    client = app_state["client"]
    sent = await client.send_code(PHONE)
    app_state["hash"] = sent.phone_code_hash
    return web.Response(
        text=HTML_FORM.format(phone=PHONE),
        content_type="text/html"
    )

async def handle_verify(request):
    data = await request.post()
    code = data.get("code", "").strip()
    client = app_state["client"]
    try:
        await client.sign_in(PHONE, app_state["hash"], code)
        await client.disconnect()
        app_state["done"] = True
        return web.Response(text=HTML_OK, content_type="text/html")
    except Exception as e:
        err = str(e)
        if "SESSION_PASSWORD_NEEDED" in err:
            return web.Response(
                text=HTML_ERR.format(error="Нужен пароль 2FA — напиши разработчику"),
                content_type="text/html"
            )
        return web.Response(text=HTML_ERR.format(error=err), content_type="text/html")

async def main():
    import os, sys
    # Удаляем старую сессию
    if os.path.exists("parser_session.session"):
        os.remove("parser_session.session")

    client = Client("parser_session", api_id=API_ID, api_hash=API_HASH)
    await client.connect()
    app_state["client"] = client

    web_app = web.Application()
    web_app.router.add_get("/auth", handle_index)
    web_app.router.add_post("/verify", handle_verify)

    runner = web.AppRunner(web_app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", 5000)
    await site.start()
    print("Открой /auth в браузере для авторизации")

    while not app_state["done"]:
        await asyncio.sleep(1)

    await runner.cleanup()
    print("Готово! Запускай parser.py")

asyncio.run(main())
