# MostAI Analytics

> Сквозная аналитика онлайн-проекта: визиты → атрибуция → выручка. Apache Superset поверх ETL-конвейера на Python с пятью моделями маркетинговой атрибуции.

**Живой стенд:** [mostai-demo.duckdns.org](https://mostai-demo.duckdns.org/superset/dashboard/1/) — открывается без логина, дашборд и весь его внутренний устройство доступны в режиме просмотра. Данные анонимизированы.

---

## О проекте

Полноценная BI-система для онлайн-сервиса: показывает откуда пришёл пользователь, какие касания привели к оплате и сколько на самом деле зарабатывает каждая рекламная кампания. Заменяет связку «Метрика + Директ + ручные таблицы» одним дашбордом, который пересобирается автоматически и хранит исторический контекст.

**Что закрывает дашборд:**

- Сводка трафика, регистраций, оплат с разбивкой по источникам, кампаниям и креативам
- Воронки конверсий с переключением модели атрибуции на лету
- Стоимость пользователя/регистрации/оплаты, ROAS и DRR по каждому срезу
- Галереи рекламных креативов с фактическими расходами и выручкой за период
- Контроль качества данных и расхождений между источниками

## Стек

| Слой | Технология |
|---|---|
| BI | Apache Superset 6.0.1 |
| Хранилище | PostgreSQL 15 (схема `analytics`) |
| Кэш и брокер | Redis 7 |
| ETL | Python 3.11, SQLAlchemy, pandas |
| Источники | Яндекс.Метрика Logs API, Яндекс.Директ Reports + Ads API v5, MongoDB |
| Оркестрация | Docker Compose, собственный планировщик |
| Reverse proxy | nginx + Let's Encrypt (HTTPS, auto-renewal) |

## Архитектура

```
┌─────────────────────┐    ┌──────────────────┐    ┌────────────────────┐
│ Яндекс.Метрика API  │    │                  │    │                    │
│ Яндекс.Директ API   ├───►│  ETL  (Python)   ├───►│  PostgreSQL        │
│ MongoDB             │    │  9 модулей       │    │  схема analytics   │
└─────────────────────┘    └────────┬─────────┘    └──────────┬─────────┘
                                    │                         │
                                    ▼                         ▼
                           ┌──────────────────┐    ┌────────────────────┐
                           │ Redis 7          │◄───┤ Apache Superset    │
                           │ кэш + брокер     │    │ + nginx HTTPS      │
                           └──────────────────┘    └────────────────────┘
```

## Ключевые технические решения

### Пять моделей атрибуции в одном датасете

Центральная таблица `visits_calculated` пересобирается одним проходом и хранит сразу все пять моделей: **last-click**, **linear**, **time-decay**, **first-touch** и **last-significant-touch**. Каждая конверсия (регистрация, оплата) распределяется по визитам цепочки по правилам соответствующей модели. Пересборка — атомарный swap: данные пишутся в таблицу `_new`, после успешного завершения старая таблица заменяется через `ALTER TABLE RENAME` в одной транзакции — нулевое окно неконсистентности для дашборда.

Аналогичная атрибуция реализована для хитов (`hits_calculated`, `hit_conversions`) — пять моделей применяются на уровне отдельных страниц, а не визитов целиком. Общие CTE атрибуции вынесены в `_build_attribution_base_ctes()` и переиспользуются обоими расчётами — бизнес-правило «failed = status≠approved и нет approved в течение часа» живёт в одном месте.

### SCD Type 2 для рекламных креативов

Объявления Яндекс.Директа меняются часто: переписали текст, заменили картинку, поставили на паузу. `loader_direct_ads.py` ведёт SCD Type 2 в таблице `direct_ads` — каждое изменение создаёт новую версию с `valid_from`/`valid_to`. Дашборд показывает креатив **в том виде, в котором он крутился на момент клика** — а не текущую редакцию.

### Виртуальные датасеты с Jinja-фильтрами

Селектор «Модель атрибуции» в дашборде не дублирует чарты на каждую модель. Вместо этого виртуальные датасеты Superset используют Jinja-параметры:

```sql
SELECT ... FROM visits_calculated
WHERE attribution_model = '{{ filter_values("attribution_model")[0] }}'
```

Один чарт — пять режимов отображения, переключение мгновенное.

### Distinct count с группировкой через трёхуровневый SQL

Superset не умеет `COUNT(DISTINCT ...) OVER (PARTITION BY ...)` напрямую. Реализовано через трёхуровневую структуру в виртуальном датасете — внутренний слой делает distinct, средний группирует, внешний агрегирует. Документировано в [memory нашей dev-инфраструктуры](https://github.com/Pianist13r/MostAI_Analytics_Demo) как воспроизводимый паттерн.

### Кастомные Handlebars-чарты с inline HTML

KPI-плашки и hero-карточки рисуются через Handlebars-чарты с собственным HTML/CSS. Для этого осознанно отключён `HTML_SANITIZATION` в Superset — single-user проект, ngrok-аутентификация, риск XSS приемлем. Альтернативой был бы форк Superset с собственным viz plugin'ом — overkill для текущей задачи.

### Прогрев Redis-кэша после ETL

`loader_cache_warmer.py` дёргает Superset REST API сразу после завершения пересборки и форсирует материализацию всех чартов дашборда в Redis. Когда пользователь утром открывает страницу — она рисуется из кэша за полсекунды вместо 30+ секунд на холодный запрос.

### Оперативный today-loader

Помимо ежедневного полного ETL, контейнер `metrika_loader_today` обновляет данные за текущий день каждые 30 минут. Реализовано отдельным loop'ом без скрипт-планировщика — простой `while True: sync_today(); time.sleep(1800)`.

### Единая точка управления cutoff'ами

Все даты-границы определяются в одном месте:

- `REPORT_START_DATE` (`loader_config.py`) — нижняя граница отчётности
- `_TODAY_MSK_TS` (`loader_visits.py`) — верхний cutoff «до вчерашнего дня по МСК» через `NOW() AT TIME ZONE 'Europe/Moscow'`

Меняешь в одном файле — пересчитываются все модули.

### Анонимизация для публичного демо

Скрипт `sync_demo.py` (живёт в private dev-репо) переносит 5 ключевых таблиц с прода на демо-VM с:
- хешированием пользовательских идентификаторов через SHA-256 со стабильной солью
- масштабированием финансовых полей на коэффициент 0.43
- сохранением консистентности JOIN'ов (одинаковые ID хешируются одинаково)

Стабильная соль — критичный нюанс: её изменение разваливает связи между таблицами. Документировано в коде.

## ETL модули

| Модуль | Назначение |
|---|---|
| `metrika_loader.py` | Точка входа, фиксированный порядок шагов ETL |
| `loader_config.py` | env, логирование, SQLAlchemy engine, `REPORT_START_DATE` |
| `loader_metrika.py` | Logs API + Reporting API (clientID → mongoID), миграция типов |
| `loader_mongo.py` | Рефереры, платежи, пользователи из MongoDB (атомарный swap) |
| `loader_direct.py` | Reports API Директа (AD_PERFORMANCE + CRITERIA_PERFORMANCE) |
| `loader_direct_ads.py` | Ads API + AdImages API v5 → SCD Type 2 для креативов |
| `loader_visits.py` | Пересборка `visits_calculated`, 5 моделей атрибуции |
| `loader_hits.py` | Пересборка `hits_calculated` + `hit_conversions`, 5 моделей |
| `loader_quality.py` | Таблица `data_quality` — контроль качества и расхождений |
| `loader_cache_warmer.py` | Прогрев Redis-кэша Superset после ETL |
| `scheduler.py` | Планировщик ежедневного полного ETL |
| `metrika_loader_today.py` | Оперативный loader, цикл каждые 30 минут |

## Структура репозитория

```
.
├── docker-compose.yml          # 4 сервиса: db, redis, superset, nginx
├── nginx.conf                  # reverse proxy + SSL termination
├── superset_config.py          # Superset: Talisman, Jinja, цветовая схема MostAI
├── Dockerfile.superset         # кастомная сборка с pip-зависимостями
├── Dockerfile.loader           # ETL-контейнер
├── .env.example                # шаблон переменных окружения
└── script/                     # ETL модули
    ├── metrika_loader.py
    ├── loader_*.py
    ├── scheduler.py
    └── requirements.txt
```

## Запуск

```bash
# 1. Скопируйте конфиг и заполните переменные
cp .env.example .env

# 2. Запустите стек
docker compose up -d

# 3. Создайте администратора Superset
docker exec superset_app superset fab create-admin \
  --username admin --firstname Admin --lastname User \
  --email admin@example.com --password "$(openssl rand -base64 16)"

# 4. Запустите ETL вручную (или дождитесь планировщика)
docker exec metrika_loader python metrika_loader.py
```

Superset доступен на `http://localhost:8088`. Для HTTPS-варианта нужен реальный домен и получение сертификата через `certbot --webroot`.

## Требования к API

- **Яндекс.Метрика** — OAuth-токен с доступом к счётчику
- **Яндекс.Директ** — тот же OAuth-токен; при агентском доступе указать `DIRECT_CLIENT_LOGIN`
- **MongoDB** — подключение к базе с коллекциями пользователей и платежей

## Переменные окружения

Все переменные описаны в [.env.example](.env.example). Ключевые:

| Переменная | Назначение |
|---|---|
| `YANDEX_TOKEN`, `YANDEX_COUNTER_ID` | Яндекс.Метрика и Яндекс.Директ API |
| `SITE_INTERNAL_DOMAINS` | домены проекта для разграничения внутренних/внешних переходов |
| `SUPERSET_SECRET_KEY` | подпись сессий Superset |
| `MONGO_*` | подключение к MongoDB |
| `POSTGRES_*` | PostgreSQL: метабаза Superset + схема `analytics` |
| `SUPERSET_ADMIN_*` | учётка для cache warmer (loader → Superset API) |

## Контакты

Автор: Pianist13 · [pianist13r@gmail.com](mailto:pianist13r@gmail.com)
