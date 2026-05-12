# Analytics Dashboard

Аналитическая платформа на базе Apache Superset с автоматической ETL-загрузкой данных из Яндекс.Метрики, Яндекс.Директа и MongoDB.

## Архитектура

```
Яндекс.Метрика Logs API  ──┐
Яндекс.Директ API          ├──► ETL (Python) ──► PostgreSQL ──► Apache Superset
MongoDB                    ─┘
```

**Стек:** Python 3.11 · PostgreSQL 15 · Redis 7 · Apache Superset 6 · Docker Compose

## Возможности

- Загрузка сырых данных посещений и хитов через Яндекс.Метрика Logs API
- Загрузка расходов и контента объявлений через Яндекс.Директ API (Reports API + Ads API v5)
- Загрузка платежей и регистраций из MongoDB
- Пересчёт `visits_calculated` — центрального датасета с **5 моделями маркетинговой атрибуции**: last-click, linear, time-decay, first-touch, last-significant-touch
- Аналогичная атрибуция для хитов (`hits_calculated`, `hit_conversions`)
- Версионирование рекламных креативов (SCD Type 2)
- Прогрев Redis-кэша Superset после каждого ETL-цикла
- Оперативный загрузчик `loader_today` — обновление данных за текущий день каждые 30 минут

## Быстрый старт

```bash
# 1. Скопируйте конфиг и заполните переменные
cp .env.example .env

# 2. Запустите стек
docker compose up -d

# 3. Создайте администратора Superset
docker exec superset_app superset fab create-admin \
  --username admin --firstname Admin --lastname User \
  --email admin@example.com --password yourpassword

# 4. Запустите ETL вручную (или дождитесь планировщика)
docker exec metrika_loader python metrika_loader.py
```

Superset доступен на `http://localhost:8088`.

## Структура ETL

| Модуль | Назначение |
|--------|-----------|
| `metrika_loader.py` | Точка входа, порядок шагов |
| `loader_config.py` | Конфигурация, подключения, константы |
| `loader_metrika.py` | Яндекс.Метрика Logs API + маппинг clientID → userID |
| `loader_mongo.py` | MongoDB: платежи, регистрации, рефереры |
| `loader_direct.py` | Яндекс.Директ: расходы на уровне объявлений и ключевых слов |
| `loader_direct_ads.py` | Яндекс.Директ: контент объявлений, SCD Type 2 |
| `loader_visits.py` | Пересчёт `visits_calculated`, 5 моделей атрибуции |
| `loader_hits.py` | Пересчёт `hits_calculated` и `hit_conversions` |
| `loader_quality.py` | Таблица контроля качества данных |
| `loader_cache_warmer.py` | Прогрев Redis-кэша Superset |
| `scheduler.py` | Планировщик ежедневного ETL |
| `metrika_loader_today.py` | Оперативный загрузчик (каждые 30 мин) |

## Требования к API

- **Яндекс.Метрика**: OAuth-токен с доступом к счётчику
- **Яндекс.Директ**: тот же OAuth-токен; при агентском доступе указать `DIRECT_CLIENT_LOGIN`
- **MongoDB**: подключение к базе с коллекциями пользователей и платежей

## Переменные окружения

Все переменные описаны в [.env.example](.env.example).

`SITE_INTERNAL_DOMAINS` — домены проекта через запятую, используются для разграничения внутренних и внешних переходов в атрибуции.
