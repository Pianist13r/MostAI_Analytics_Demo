"""
Работа с Яндекс.Метрикой: Logs API (visits/hits) и Reporting API (маппинг clientID → mongoID).
"""
import io
import random
import time
from datetime import datetime, timedelta, timezone
from typing import Optional

import pandas as pd
import requests
from sqlalchemy import BigInteger, DateTime, Integer, SmallInteger, inspect, text

from loader_config import log, engine, COUNTER_ID, API_BASE, STAT_API, HEADERS

# Зеркалит loader_direct.py: явный UTC+3 вместо опоры на TZ контейнера.
_MSK = timezone(timedelta(hours=3))

# Оптимальные типы колонок для visits / hits
_VISIT_SQLA_DTYPE: dict = {
    # visitID — беззнаковый uint64 от Метрики, не умещается в BIGINT → остаётся TEXT
    'ym:s:dateTime':          DateTime(),
    'ym:s:isNewUser':         SmallInteger(),
    'ym:s:pageViews':         Integer(),
    'ym:s:visitDuration':     Integer(),
    'ym:s:bounce':            SmallInteger(),
    'ym:s:deviceCategory':    SmallInteger(),
    'ym:s:cookieEnabled':     SmallInteger(),
    'ym:s:javascriptEnabled': SmallInteger(),
    'ym:s:clientTimeZone':    SmallInteger(),
}
_HIT_SQLA_DTYPE: dict = {
    'ym:pv:dateTime':    DateTime(),
    'ym:pv:isPageView':  SmallInteger(),
    'ym:pv:notBounce':   SmallInteger(),
}


# ---------------------------------------------------------------------------
# Вспомогательные функции
# ---------------------------------------------------------------------------

def _convert_df_types(df: pd.DataFrame, dtype_map: dict) -> pd.DataFrame:
    """Конвертирует типы колонок DataFrame перед записью в PostgreSQL."""
    for col, sqla_type in dtype_map.items():
        if col not in df.columns:
            continue
        if isinstance(sqla_type, DateTime):
            df[col] = pd.to_datetime(df[col], errors='coerce')
        elif isinstance(sqla_type, BigInteger):
            df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
        elif isinstance(sqla_type, Integer):
            df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int32')
        elif isinstance(sqla_type, SmallInteger):
            df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int16')
    return df


def migrate_column_types():
    """Приводит типы колонок к оптимальным для PostgreSQL (однократная операция).

    Проверяет текущий тип ym:s:dateTime в metrika_visits: если он не TEXT,
    значит миграция уже была выполнена — выходим без изменений.
    """
    try:
        with engine.connect() as conn:
            current_type = conn.execute(text("""
                SELECT data_type FROM information_schema.columns
                WHERE table_schema = 'analytics' AND table_name = 'metrika_visits'
                  AND column_name = 'ym:s:dateTime'
            """)).scalar()
    except Exception:
        return
    if current_type in (None, 'timestamp without time zone'):
        return  # таблица не существует или уже мигрирована

    log.info("Миграция типов данных в PostgreSQL (однократно)...")
    migrations = {
        # visitID — беззнаковый uint64 от Метрики, в BIGINT не помещается → оставляем TEXT
        'metrika_visits': """ALTER TABLE metrika_visits
            ALTER COLUMN "ym:s:dateTime"          TYPE TIMESTAMP USING NULLIF("ym:s:dateTime", '')::TIMESTAMP,
            ALTER COLUMN "ym:s:isNewUser"         TYPE SMALLINT  USING "ym:s:isNewUser"::SMALLINT,
            ALTER COLUMN "ym:s:pageViews"         TYPE INTEGER   USING "ym:s:pageViews"::INTEGER,
            ALTER COLUMN "ym:s:visitDuration"     TYPE INTEGER   USING "ym:s:visitDuration"::INTEGER,
            ALTER COLUMN "ym:s:bounce"            TYPE SMALLINT  USING NULLIF("ym:s:bounce", '')::SMALLINT,
            ALTER COLUMN "ym:s:deviceCategory"    TYPE SMALLINT  USING "ym:s:deviceCategory"::SMALLINT,
            ALTER COLUMN "ym:s:cookieEnabled"     TYPE SMALLINT  USING NULLIF("ym:s:cookieEnabled", '')::SMALLINT,
            ALTER COLUMN "ym:s:javascriptEnabled" TYPE SMALLINT  USING NULLIF("ym:s:javascriptEnabled", '')::SMALLINT,
            ALTER COLUMN "ym:s:clientTimeZone"    TYPE SMALLINT  USING NULLIF("ym:s:clientTimeZone", '')::SMALLINT""",
        'metrika_hits': """ALTER TABLE metrika_hits
            ALTER COLUMN "ym:pv:dateTime"   TYPE TIMESTAMP USING NULLIF("ym:pv:dateTime", '')::TIMESTAMP,
            ALTER COLUMN "ym:pv:isPageView" TYPE SMALLINT  USING NULLIF("ym:pv:isPageView", '')::SMALLINT,
            ALTER COLUMN "ym:pv:notBounce"  TYPE SMALLINT  USING NULLIF("ym:pv:notBounce", '')::SMALLINT""",
        'mongo_payments': """ALTER TABLE mongo_payments
            ALTER COLUMN created_at        TYPE TIMESTAMP USING NULLIF(created_at, '')::TIMESTAMP,
            ALTER COLUMN created_at_moscow TYPE TIMESTAMP USING NULLIF(created_at_moscow, '')::TIMESTAMP""",
        'mongo_users': """ALTER TABLE mongo_users
            ALTER COLUMN created_at        TYPE TIMESTAMP USING NULLIF(created_at, '')::TIMESTAMP,
            ALTER COLUMN created_at_moscow TYPE TIMESTAMP USING NULLIF(created_at_moscow, '')::TIMESTAMP""",
        'direct_costs': """ALTER TABLE direct_costs
            ALTER COLUMN "date"      TYPE DATE   USING NULLIF("date", '')::DATE,
            ALTER COLUMN cost_micros TYPE BIGINT USING ROUND(cost_micros)::BIGINT""",
        'direct_criteria_costs': """ALTER TABLE direct_criteria_costs
            ALTER COLUMN "date" TYPE DATE USING NULLIF("date", '')::DATE""",
    }
    for table, sql in migrations.items():
        try:
            with engine.begin() as conn:
                conn.execute(text(sql))
            log.info("  Мигрирована таблица %s.", table)
        except Exception as e:
            log.warning("  Пропуск %s (может не существовать): %s", table, e)
    log.info("Миграция типов завершена.")


def get_last_date(table_name: str, date_column: str) -> str:
    """Возвращает последнюю дату в таблице, или дату 360 дней назад."""
    try:
        with engine.connect() as conn:
            result = conn.execute(
                text(f'SELECT max({date_column}) FROM "{table_name}"')
            ).scalar()
            if result:
                return str(result).split(' ')[0]
    except Exception as e:
        log.warning(
            "Не удалось прочитать последнюю дату из %s: %s — скачиваем данные за 360 дней",
            table_name, e,
        )
    return (datetime.now(_MSK) - timedelta(days=360)).strftime('%Y-%m-%d')


def _create_index(table_name: str):
    """Создаёт базовые индексы для таблицы после первого создания."""
    indexes = {
        'metrika_visits': [
            'CREATE INDEX IF NOT EXISTS idx_visit_id ON metrika_visits ("ym:s:visitID")',
            'CREATE INDEX IF NOT EXISTS idx_visit_dt ON metrika_visits ("ym:s:dateTime")',
        ],
        'metrika_hits': [
            'CREATE INDEX IF NOT EXISTS idx_hit_id ON metrika_hits ("ym:pv:watchID")',
            'CREATE INDEX IF NOT EXISTS idx_hit_dt ON metrika_hits ("ym:pv:dateTime")',
        ],
    }
    with engine.connect() as conn:
        for query in indexes.get(table_name, []):
            conn.execute(text(query))
        conn.commit()


def _deduplicate_by_id(table_name: str, id_col: str):
    """Удаляет строки с повторяющимся id_col, оставляя физически первую (MIN ctid)."""
    with engine.begin() as conn:
        result = conn.execute(text(f'''
            DELETE FROM "{table_name}" t
            USING (
                SELECT {id_col}, MIN(ctid) AS keep_ctid
                FROM "{table_name}"
                GROUP BY {id_col}
                HAVING COUNT(*) > 1
            ) dups
            WHERE t.{id_col} = dups.{id_col}
              AND t.ctid <> dups.keep_ctid
        '''))
        if result.rowcount:
            log.warning("Дедупликация %s по %s: удалено %d дублей.", table_name, id_col, result.rowcount)
        else:
            log.info("Дедупликация %s: дублей не найдено.", table_name)


# ---------------------------------------------------------------------------
# Logs API
# ---------------------------------------------------------------------------

def _find_or_create_log_request(
    source_type: str, fields: str, start_date: str, yesterday: str,
) -> Optional[str]:
    """Создаёт запрос на выгрузку логов или находит существующий. Возвращает request_id."""
    url_plural   = f"{API_BASE}/logrequests"
    url_singular = f"{API_BASE}/logrequest"

    params = {
        "ids":    COUNTER_ID,
        "date1":  start_date,
        "date2":  yesterday,
        "fields": fields,
        "source": source_type,
    }

    res = requests.post(url_plural, params=params, headers=HEADERS, timeout=30)
    res.raise_for_status()
    data = res.json()

    if 'log_request' in data:
        return data['log_request']['request_id']

    # API ответил ошибкой «уже существует» — ищем совпадение среди активных запросов
    log.info("Запрос %s уже существует — ищем совпадение...", source_type)
    all_reqs      = requests.get(url_plural, headers=HEADERS, timeout=30).json()
    target_fields = set(fields.split(','))

    for req in all_reqs.get('requests', []):
        if (req['source'] == source_type
                and req['date1'] == start_date
                and req['date2'] == yesterday
                and set(req['fields']) == target_fields):
            log.info("Найден подходящий запрос ID=%s", req['request_id'])
            return req['request_id']

    # Точного совпадения нет — удаляем старые запросы того же типа и создаём заново
    log.info("Точного совпадения нет — чистим старые запросы и создаём новый...")
    for req in all_reqs.get('requests', []):
        if req['source'] == source_type:
            del_resp = requests.delete(
                f"{url_singular}/{req['request_id']}/clean",
                headers=HEADERS,
                timeout=30,
            )
            if del_resp.status_code not in (200, 204):
                log.warning(
                    "Не удалось удалить запрос %s: HTTP %d — %s",
                    req['request_id'], del_resp.status_code, del_resp.text[:200],
                )
    time.sleep(2)

    retry = requests.post(url_plural, params=params, headers=HEADERS, timeout=30)
    retry.raise_for_status()
    retry_data = retry.json()

    if 'log_request' in retry_data:
        return retry_data['log_request']['request_id']

    log.error("Не удалось создать запрос: %s", retry_data)
    return None


def _wait_until_processed(
    req_id: str, source_type: str, max_wait_minutes: int = 60,
) -> Optional[dict]:
    """Ждёт, пока запрос будет обработан. Возвращает данные запроса или None при ошибке."""
    url      = f"{API_BASE}/logrequest/{req_id}"
    deadline = time.time() + max_wait_minutes * 60

    while time.time() < deadline:
        resp = requests.get(url, headers=HEADERS, timeout=30)
        if resp.status_code != 200:
            time.sleep(20)
            continue

        req_data = resp.json()['log_request']
        status   = req_data['status']

        if status == 'processed':
            return req_data
        if status == 'error':
            log.error("API вернул ошибку для запроса %s", req_id)
            return None

        log.info("Статус %s: %s...", source_type, status)
        time.sleep(20)

    log.error("Превышено время ожидания (%d мин) для запроса %s", max_wait_minutes, req_id)
    return None


def download_logs(source_type: str, fields: str, table_name: str, date_col: str):
    """Инкрементально докачивает данные из Logs API в PostgreSQL.

    Логика: берёт MAX(date_col) как start_date, удаляет строки за этот день
    (они могли быть неполными), затем скачивает всё заново с start_date по вчера.
    """
    start_date = get_last_date(table_name, date_col)
    yesterday  = (datetime.now(_MSK) - timedelta(days=1)).strftime('%Y-%m-%d')

    if start_date == yesterday:
        log.info("Данные для %s уже актуальны (последняя дата: %s)", table_name, start_date)
        return

    log.info("Докачка %s с %s по %s", source_type, start_date, yesterday)

    req_id = _find_or_create_log_request(source_type, fields, start_date, yesterday)
    if not req_id:
        return

    req_data = _wait_until_processed(req_id, source_type)
    if not req_data:
        return

    inspector    = inspect(engine)
    table_exists = table_name in inspector.get_table_names(schema='analytics')

    # Перезаписываем последний день, чтобы не оставлять «обрезанных» данных
    if table_exists:
        with engine.connect() as conn:
            conn.execute(
                text(f'DELETE FROM "{table_name}" WHERE {date_col}::date = CAST(:d AS date)'),
                {"d": start_date},
            )
            conn.commit()

    for part in req_data['parts']:
        part_num = part['part_number']
        resp = requests.get(
            f"{API_BASE}/logrequest/{req_id}/part/{part_num}/download",
            headers=HEADERS,
            timeout=60,
        )
        if resp.status_code != 200:
            log.warning("Не удалось скачать часть %d для %s", part_num, table_name)
            continue

        _dtype_map = _VISIT_SQLA_DTYPE if table_name == 'metrika_visits' else (
            _HIT_SQLA_DTYPE if table_name == 'metrika_hits' else {}
        )
        df = pd.read_csv(io.StringIO(resp.text), sep='\t', dtype=str)
        df = _convert_df_types(df, _dtype_map)

        if table_exists:
            df.to_sql(table_name, con=engine, if_exists='append', index=False)
            log.info("Добавлено %d строк в %s (часть %d)", len(df), table_name, part_num)
        else:
            df.to_sql(table_name, con=engine, if_exists='replace', index=False,
                      dtype=_dtype_map if _dtype_map else None)
            _create_index(table_name)
            table_exists = True
            log.info("Таблица %s создана, добавлено %d строк", table_name, len(df))


# ---------------------------------------------------------------------------
# Reporting API — маппинг clientID → MongoDB ID
# ---------------------------------------------------------------------------

# Таблица loader_state — хранилище ключ-значение для меток последнего запуска
# инкрементальных шагов ETL. Создаётся при первом обращении.
def _ensure_state_table():
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS loader_state (
                key        TEXT PRIMARY KEY,
                value      TEXT,
                updated_at TIMESTAMP DEFAULT NOW()
            )
        """))


def _get_state(key: str) -> Optional[str]:
    _ensure_state_table()
    with engine.begin() as conn:
        row = conn.execute(
            text("SELECT value FROM loader_state WHERE key = :k"),
            {'k': key},
        ).fetchone()
    return row[0] if row else None


def _set_state(key: str, value: str):
    _ensure_state_table()
    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO loader_state (key, value, updated_at)
                VALUES (:k, :v, NOW())
                ON CONFLICT (key) DO UPDATE
                  SET value = EXCLUDED.value, updated_at = NOW()
            """),
            {'k': key, 'v': value},
        )


# Сколько дней назад от сегодня брать данные в инкрементальном режиме.
# 5 дней — буфер на задержку индексации Метрики (~1–2 суток) + смену устройства.
_CLIENTID_INCREMENTAL_BUFFER_DAYS = 5
# Раз в сколько дней делать полный пересмотр (full rescan) от 2025-01-01.
_CLIENTID_FULL_RESCAN_INTERVAL_DAYS = 7
# Ключ в loader_state с датой последнего full rescan.
_CLIENTID_LAST_FULL_KEY = 'clientid_mapping_last_full_run'


def sync_clientid_mapping():
    """Строит таблицу clientid_to_mongoid: Метрика clientID → MongoDB ObjectId.

    Разработчик сайта передаёт MongoDB ID через:
        ym(COUNTER_ID, 'userParams', { UserID: '<mongoId>' })
    Эти данные хранятся в параметрах посетителя и доступны через Reporting API.

    Режимы:
      • Full rescan — date_from = 2025-01-01. Запускается, если предыдущий
        full завершился ≥ 7 дней назад (или его не было вообще).
      • Incremental — date_from = today − 5 дней. Опрашиваются только
        mongoID, активные за этот период. Так покрывается смена устройства
        (новый clientID для старого юзера) и любая задержка индексации Метрики
        в пределах буфера.

    Шаги (общие для обоих режимов):
      1. Через ym:up:paramsLevel1/2 получаем все mongoID за выбранный период.
      2. Для каждого mongoID получаем clientID(ы) через ym:s:clientID за тот же
         период (~0.6с + jitter между запросами, retry на 429/400).
    Таблица накопительная: INSERT ... ON CONFLICT DO NOTHING — старые записи
    не трогает, только добавляет недостающие.
    """
    today_dt  = datetime.now(_MSK).replace(tzinfo=None)  # naive MSK для арифметики с last_full
    today     = today_dt.strftime('%Y-%m-%d')

    last_full_str = _get_state(_CLIENTID_LAST_FULL_KEY)
    is_full = True
    if last_full_str:
        try:
            last_full = datetime.strptime(last_full_str, '%Y-%m-%d')
            days_since_full = (today_dt - last_full).days
            is_full = days_since_full >= _CLIENTID_FULL_RESCAN_INTERVAL_DAYS
        except ValueError:
            is_full = True  # битое значение — на всякий случай делаем full

    if is_full:
        date_from = '2025-01-01'
        log.info("Маппинг clientID → mongoID: режим FULL RESCAN (date_from=%s)", date_from)
    else:
        date_from = (today_dt - timedelta(days=_CLIENTID_INCREMENTAL_BUFFER_DAYS)).strftime('%Y-%m-%d')
        log.info("Маппинг clientID → mongoID: режим INCREMENTAL (date_from=%s, буфер %d дн.; "
                 "последний full %d дн. назад)",
                 date_from, _CLIENTID_INCREMENTAL_BUFFER_DAYS,
                 (today_dt - last_full).days)

    # Шаг 1: все MongoDB ID из параметров посетителя (пагинация по 10 000)
    mongo_ids = []
    offset    = 1
    while True:
        resp = requests.get(STAT_API, headers=HEADERS, timeout=30, params={
            'id':         COUNTER_ID,
            'metrics':    'ym:up:users',
            'dimensions': 'ym:up:paramsLevel1,ym:up:paramsLevel2',
            'date1':      date_from,
            'date2':      today,
            'limit':      10000,
            'offset':     offset,
        })
        resp.raise_for_status()
        data = resp.json()
        rows = data.get('data', [])
        if not rows:
            break
        for row in rows:
            key = row['dimensions'][0]['name']
            val = row['dimensions'][1]['name']
            if key == 'UserID' and val:
                mongo_ids.append(val)
        total = data.get('total_rows', 0)
        if offset + 10000 > total:
            break
        offset += 10000

    log.info("MongoDB ID в параметрах посетителя за период: %d", len(mongo_ids))
    if not mongo_ids:
        # И в full, и в incremental пустой Step 1 — аномалия:
        # на активном проекте каждый день появляются новые регистрации,
        # за 5-дневный буфер mongoID должны быть. Если их нет — что-то сломано
        # (API, фильтр, токен), нужно расследовать.
        log.warning("Step 1 вернул 0 mongoID (режим=%s, период=%s..%s) — "
                    "это аномалия, проверь Reporting API.",
                    'FULL' if is_full else 'INCREMENTAL', date_from, today)
        return

    def _fetch_client_ids(mongo_id, date_from, today):
        """Возвращает список {'metrika_client_id': ..., 'mongo_user_id': ...} или []."""
        resp = requests.get(STAT_API, headers=HEADERS, timeout=30, params={
            'id':         COUNTER_ID,
            'metrics':    'ym:s:visits',
            'dimensions': 'ym:s:clientID',
            'filters':    f"ym:up:paramsLevel2=='{mongo_id}'",
            'date1':      date_from,
            'date2':      today,
            'limit':      100,
        })
        resp.raise_for_status()
        return [
            {'metrika_client_id': row['dimensions'][0]['name'], 'mongo_user_id': mongo_id}
            for row in resp.json().get('data', [])
            if row['dimensions'][0]['name']
        ]

    # Шаг 2: для каждого MongoDB ID получаем clientID(ы)
    pairs        = []
    cooldown     = False  # True = нужен дополнительный cool-down после 429
    quota_failed = []     # IDs, пропущенные из-за исчерпания квоты — повторим после основного цикла

    for i, mongo_id in enumerate(mongo_ids):
        if cooldown:
            log.info("Cool-down 15s после 429/400...")
            time.sleep(15)
            cooldown = False

        for attempt in range(4):  # до 3 повторных попыток при 429
            try:
                pairs.extend(_fetch_client_ids(mongo_id, date_from, today))
                break  # успех — выходим из retry-цикла
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 429:
                    try:
                        err_type = e.response.json().get('errors', [{}])[0].get('error_type', '')
                    except Exception:
                        err_type = ''
                    if err_type == 'quota_requests_by_uid':
                        # Жёсткое исчерпание квоты — ретраи бесполезны, откладываем на конец
                        log.warning("Квота API исчерпана для %s — отложено на retry-pass.", mongo_id)
                        quota_failed.append(mongo_id)
                        cooldown = True
                        break
                    elif attempt < 3:
                        wait = [60, 90, 120][attempt]
                        log.warning("Rate limit 429 для %s — ожидание %ds (попытка %d/3)...",
                                    mongo_id, wait, attempt + 1)
                        time.sleep(wait)
                        cooldown = True
                    else:
                        log.warning("Ошибка при получении clientID для %s: %s", mongo_id, e)
                        log.warning("Тело ответа API: %s", e.response.text)
                        quota_failed.append(mongo_id)
                        break
                elif e.response.status_code == 400 and attempt < 3:
                    wait = [60, 90, 120][attempt]
                    log.warning("Query is too complicated 400 для %s — ожидание %ds (попытка %d/3)...",
                                mongo_id, wait, attempt + 1)
                    time.sleep(wait)
                    cooldown = True
                else:
                    log.warning("Ошибка при получении clientID для %s: %s", mongo_id, e)
                    log.warning("Тело ответа API: %s", e.response.text)
                    break
            except Exception as e:
                log.warning("Ошибка при получении clientID для %s: %s", mongo_id, e)
                break

        time.sleep(0.6 + random.uniform(-0.15, 0.15))  # ~0.6с → держимся под 2 RPS
        if (i + 1) % 20 == 0:
            log.info("  Обработано %d / %d MongoDB ID...", i + 1, len(mongo_ids))

    # Retry-pass: повторяем IDs, пропущенные из-за исчерпания квоты
    if quota_failed:
        log.info("Quota retry-pass: %d ID пропущено из-за квоты. Ждём 10 мин...", len(quota_failed))
        time.sleep(600)
        recovered = 0
        for mongo_id in quota_failed:
            try:
                pairs.extend(_fetch_client_ids(mongo_id, date_from, today))
                recovered += 1
            except Exception as e:
                log.warning("Quota retry-pass не удался для %s: %s", mongo_id, e)
            time.sleep(1.0)
        log.info("Quota retry-pass завершён: восстановлено %d / %d ID.", recovered, len(quota_failed))

    if not pairs:
        # Step 1 нашёл активных mongoID, но Step 2 не вернул ни одной пары.
        # На рабочей системе это аномалия: для каждого активного mongoID должен
        # вернуться хотя бы текущий clientID. Если пар 0 — Step 2 либо
        # систематически падает (429/400), либо API ответил пусто на все запросы.
        log.warning("Step 2 вернул 0 пар clientID↔mongoID при %d активных mongoID "
                    "(режим=%s) — проверь логи на ошибки запросов.",
                    len(mongo_ids), 'FULL' if is_full else 'INCREMENTAL')
        return

    df = pd.DataFrame(pairs).drop_duplicates()

    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS clientid_to_mongoid_new"))
        df.to_sql('clientid_to_mongoid_new', con=conn, if_exists='replace', index=False)
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS clientid_to_mongoid (
                metrika_client_id TEXT,
                mongo_user_id     TEXT
            )
        """))
        # Уникальный индекс гарантирует накопительность: повторная пара не дублируется
        conn.execute(text("""
            CREATE UNIQUE INDEX IF NOT EXISTS uq_cid_mongo
            ON clientid_to_mongoid (metrika_client_id, mongo_user_id)
        """))
        inserted = conn.execute(text("""
            INSERT INTO clientid_to_mongoid (metrika_client_id, mongo_user_id)
            SELECT metrika_client_id, mongo_user_id FROM clientid_to_mongoid_new
            ON CONFLICT DO NOTHING
        """)).rowcount
        conn.execute(text("DROP TABLE IF EXISTS clientid_to_mongoid_new"))
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_cid_metrika
            ON clientid_to_mongoid (metrika_client_id)
        """))

    log.info("Маппинг: получено %d пар, добавлено %d новых (clientID → mongoID).",
             len(df), inserted)

    if is_full:
        _set_state(_CLIENTID_LAST_FULL_KEY, today)
        log.info("Full rescan завершён, метка %s обновлена: %s",
                 _CLIENTID_LAST_FULL_KEY, today)
