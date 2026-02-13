#!/usr/bin/env python3
"""
Скрипт запуска бота на Render.com.
Создаёт credentials.json из переменной окружения CREDENTIALS_JSON и запускает bot.py.
В Render в Environment задайте CREDENTIALS_JSON = полное содержимое вашего credentials.json.
"""
import asyncio
import os
import sys
import traceback

# Python 3.10+: в MainThread должен быть event loop до любого кода PTB
try:
    asyncio.get_event_loop()
except RuntimeError:
    asyncio.set_event_loop(asyncio.new_event_loop())

def main():
    creds_json = os.environ.get("CREDENTIALS_JSON")
    if creds_json:
        path = os.environ.get("GOOGLE_CREDENTIALS_PATH", "credentials.json")
        try:
            with open(path, "w", encoding="utf-8") as f:
                f.write(creds_json)
            print("credentials.json создан из CREDENTIALS_JSON", file=sys.stderr)
        except Exception as e:
            print(f"Ошибка записи credentials.json: {e}", file=sys.stderr)
            traceback.print_exc(file=sys.stderr)
            sys.exit(1)
    else:
        print("CREDENTIALS_JSON не задан; ожидается файл credentials.json", file=sys.stderr)

    webhook_base = os.environ.get("WEBHOOK_BASE_URL", "").strip()
    if not webhook_base:
        print("WEBHOOK_BASE_URL не задан. Задайте в Environment URL сервиса, например https://dds-telegram-bot-38zf.onrender.com", file=sys.stderr)
        sys.exit(1)

    try:
        import bot
        bot.main()
    except Exception as e:
        print(f"Ошибка запуска бота: {e}", file=sys.stderr)
        traceback.print_exc(file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()
