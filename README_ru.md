# Поиск похожих тем ставок: Polymarket ↔ Opinion.trade

Этот проект делает три Excel-файла по запросу:
1. `out/polymarket_markets.xlsx` — список рынков Polymarket
2. `out/opinion_markets.xlsx` — список рынков Opinion.trade
3. `out/matches.xlsx` — таблица похожих/одинаковых тем между платформами

## Быстрый старт
```bash
# 1) Python 3.10+
python -V

# 2) Установите зависимости
pip install -r requirements.txt

# 3) Запустите
python main.py
```

Готовые Excel-файлы появятся в папке `out/`.

## Настройки (необязательно)
В `.env` можно задать параметры:
- `POLYMARKET_STATUS=active|closed|all` — фильтр по статусу (по умолчанию `active`)
- `SIMILARITY_THRESHOLD=86` — порог схожести (0–100) для поиска похожих тем
- `MAX_MARKETS_PER_PLATFORM` — ограничение кол-ва рынков (для отладки/скорости)
- `OPINION_USE_SDK=true|false` — использовать SDK Opinion (по умолчанию `true`)

## Как это работает
- **Polymarket**: публичный Gamma API `https://gamma-api.polymarket.com/markets`.
- **Opinion.trade**: Python SDK (`opinion_clob_sdk`) и метод `get_markets()`.
  Если SDK недоступен, скрипт попытается альтернативный путь (см. код) и подскажет, что нужен SDK.

- **Сходство тем**: RapidFuzz (`token_set_ratio`) + нормализация текста.

## Источники/доки
- Polymarket Gamma Markets API — см. официальную документацию.
- Opinion.trade Developer Guide & Methods — см. docs.opinion.trade.

## Примечания
- Для полной информации об отдельных рынках Opinion.trade может потребоваться аутентификация.
- Скрипт обрабатывает пагинацию.
