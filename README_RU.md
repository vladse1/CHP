# CHP Traffic -> Telegram бот (v2, с выбором Communications Center)

Теперь бот автоматически выбирает указанный `COMM_CENTER` на странице (ASP.NET postback), нажимает OK и парсит таблицу.

## Быстрый старт
1) Создай бота в @BotFather и узнай свой chat_id.
2) Установи зависимости:
```bash
python3 -m pip install -r requirements.txt
```
3) Создай `.env` из примера и заполни:
```
TELEGRAM_TOKEN=...
TELEGRAM_CHAT_ID=...
COMM_CENTER=Inland   # или другой центр как в выпадающем списке
```
4) Запусти:
```bash
python3 chp_bot.py
```

Если страница поменяет разметку — скрипт нужно будет адаптировать (он и так ищет элементы максимально «умно», без жёстких id).

Удачи! 🚀
