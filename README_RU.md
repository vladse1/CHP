# CHP Traffic -> Telegram бот (v4)
Что нового:
- Берёт координаты строго из ссылки рядом с `Lat/Lon:` на странице Details.
- Формирует ссылку маршрута Google Maps: `https://www.google.com/maps/dir/?api=1&destination=LAT,LON&travelmode=driving`.
- Если координат нет — карта не прикладывается.

## Быстрый старт
```bash
python3 -m pip install -r requirements.txt
cp .env.example .env  # и заполни TELEGRAM_TOKEN/CHAT_ID/COMM_CENTER
python3 chp_bot.py
```
