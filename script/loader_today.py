"""
Параллельный loader для операционной вкладки «За сегодня».

Грузит свежие данные за today **независимо от основного ETL** в отдельные
таблицы `today_*`. Никакие существующие таблицы не модифицирует.

Источники:
  1. Метрика Reporting API   → today_direct_visits_raw  (визиты Директа за today)
  2. Метрика Reporting API   → today_client_utm         (clientID + UTM + dateTime)
  3. Метрика Reporting API   → today_clientid_to_mongoid (свежий маппинг)
  4. Директ Reports API      → today_direct_costs       (расходы за today)
  5. Директ Ads/Campaigns API→ today_direct_ads / today_direct_campaigns
  6. Директ AdImages API     → today_direct_ad_images
  7. MongoDB payment         → today_mongo_payments     (created_at в today МСК)

Финальная сборка:
  build_today_dataset() — SQL: UTM-мост + pro-rata + JOIN со справочниками
                          → today_dataset (одна строка на ad_id для галереи)

Атрибуция:
  Last-click within today: для каждой оплаты сегодня → последний Direct визит
  того же mongo_user_id (через clientID-мост) → ad_id (через UTM-мост или
  pro-rata по сегодняшним кликам Директа для неоднозначных UTM-пар).
"""
import io
import random
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

import pandas as pd
import requests
from pymongo import MongoClient
from sqlalchemy import (BigInteger, Date, DateTime, Float, Integer,
                        SmallInteger, Text, text)

from loader_config import (COUNTER_ID, DIRECT_API, DIRECT_HEADERS, HEADERS,
                           MONGO_COLLECTION, MONGO_DB_NAME, MONGO_URI,
                           STAT_API, TOKEN, engine, log)


# ---------------------------------------------------------------------------
# Общие константы
# ---------------------------------------------------------------------------
_TODAY_MSK_DATE_SQL = "(NOW() AT TIME ZONE 'Europe/Moscow')::date"

_DIRECT_ADS_API_URL       = "https://api.direct.yandex.com/json/v5/ads"
_DIRECT_CAMPAIGNS_API_URL = "https://api.direct.yandex.com/json/v5/campaigns"
_DIRECT_ADIMAGES_API_URL  = "https://api.direct.yandex.com/json/v5/adimages"

_DIRECT_ADS_HEADERS = {
    "Authorization":   f"Bearer {TOKEN}",
    "Accept-Language": "ru",
    "Content-Type":    "application/json",
    "Client-Login":    os.getenv('DIRECT_CLIENT_LOGIN', ''),
}


def _today_msk() -> str:
    """Сегодняшняя дата в МСК в формате YYYY-MM-DD (Reporting API ждёт МСК)."""
    return (datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(hours=3)).strftime('%Y-%m-%d')


def _start_of_today_utc() -> datetime:
    """Начало сегодняшних суток МСК, переведённое в UTC (для фильтра MongoDB)."""
    today_msk = datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(hours=3)
    today_msk_midnight = today_msk.replace(hour=0, minute=0, second=0, microsecond=0)
    return today_msk_midnight - timedelta(hours=3)


def _execute_sql_block(conn, sql: str) -> None:
    """Выполняет блок SQL, разбитый на отдельные выражения по ';'.

    SQLAlchemy text() не поддерживает multi-statement в одном вызове execute()
    при psycopg2 в режиме autocommit=False — каждое выражение нужно отправлять
    отдельно. Пустые строки между ';' пропускаются.
    """
    for stmt in sql.split(';'):
        stmt = stmt.strip()
        if stmt:
            conn.execute(text(stmt))


# ===========================================================================
# 1) Reporting API Метрики: Direct визиты за today
# ===========================================================================

# Одна группа совместимости измерений Reporting API (все Direct-атрибуты).
# clientID/dateTime сюда добавлять НЕЛЬЗЯ — API тихо вернёт 0 строк.
_DIRECT_VISITS_DIMS = [
    'ym:s:lastDirectClickBanner',     # ad_id (id формата '<version>.<ad_id>')
    'ym:s:lastDirectClickOrder',      # campaign_id (числовой)
    'ym:s:lastDirectPlatformType',    # 'search' / 'context'
    'ym:s:lastDirectPhraseOrCond',
    'ym:s:lastUTMCampaign',
    'ym:s:lastUTMContent',
]

_TODAY_VISITS_RAW_DTYPE = {
    'ad_id_raw':           Text(),
    'ad_id':               BigInteger(),
    'campaign_id':         BigInteger(),
    'campaign_name_metrika': Text(),
    'dpt':                 Text(),
    'phrase':              Text(),
    'utm_campaign':        Text(),
    'utm_content':         Text(),
    'visits':              Integer(),
}


def _request_reporting_api(params: dict, retries: int = 3) -> dict:
    """Один вызов Reporting API с retry на 429/5xx."""
    for attempt in range(retries + 1):
        resp = requests.get(STAT_API, headers=HEADERS, timeout=30, params=params)
        if resp.status_code == 200:
            return resp.json()
        if resp.status_code in (429, 500, 502, 503, 504) and attempt < retries:
            wait = [60, 90, 120][attempt]
            log.warning("Reporting API HTTP %d — пауза %ds (попытка %d/%d).",
                        resp.status_code, wait, attempt + 1, retries)
            time.sleep(wait)
            continue
        log.warning("Reporting API HTTP %d: %s", resp.status_code, resp.text[:500])
        resp.raise_for_status()
    raise RuntimeError("Reporting API: исчерпаны попытки")


def _paginate_reporting_api(base_params: dict, page_size: int = 5000) -> list:
    """Полная пагинация Reporting API. Возвращает список 'data' со всех страниц."""
    rows = []
    offset = 1
    while True:
        params = {**base_params, 'limit': page_size, 'offset': offset}
        data = _request_reporting_api(params)
        chunk = data.get('data', [])
        rows.extend(chunk)
        total = data.get('total_rows', 0)
        if offset + len(chunk) - 1 >= total or not chunk:
            break
        offset += len(chunk)
        time.sleep(0.5 + random.uniform(-0.1, 0.1))  # держимся под 2 RPS
    return rows


def fetch_direct_visits_today() -> int:
    """Тянет Direct визиты за today и сохраняет в today_direct_visits_raw."""
    today = _today_msk()
    log.info("today: запрос Direct визитов за %s через Reporting API...", today)

    raw = _paginate_reporting_api({
        'id':         COUNTER_ID,
        'metrics':    'ym:s:visits',
        'dimensions': ','.join(_DIRECT_VISITS_DIMS),
        'date1':      today,
        'date2':      today,
    })

    if not raw:
        log.info("today: за %s Direct визитов ещё нет (возможно ранний утренний час).", today)
        df = pd.DataFrame(columns=list(_TODAY_VISITS_RAW_DTYPE.keys()))
    else:
        rows = []
        for r in raw:
            d = r['dimensions']
            ad_id_raw = d[0].get('id')  # '1.17712820202'
            # ad_id Метрики идёт с префиксом '<version>.' — нужен числовой хвост
            ad_id_num = None
            if ad_id_raw and '.' in ad_id_raw:
                tail = ad_id_raw.split('.', 1)[1]
                if tail.isdigit():
                    ad_id_num = int(tail)
            elif ad_id_raw and ad_id_raw.isdigit():
                ad_id_num = int(ad_id_raw)

            campaign_id_raw = d[1].get('id')
            campaign_id_num = int(campaign_id_raw) if campaign_id_raw and campaign_id_raw.isdigit() else None

            rows.append({
                'ad_id_raw':             ad_id_raw,
                'ad_id':                 ad_id_num,
                'campaign_id':           campaign_id_num,
                'campaign_name_metrika': d[1].get('name'),
                'dpt':                   d[2].get('id'),         # 'search'/'context'
                'phrase':                d[3].get('name'),
                'utm_campaign':          d[4].get('name'),
                'utm_content':           d[5].get('name'),
                'visits':                int(r['metrics'][0] or 0),
            })
        df = pd.DataFrame(rows)
        # Только Direct: ad_id должен быть, иначе строка не Direct (фильтр API не помогает,
        # т.к. EXISTS не принят — фильтруем здесь).
        df = df[df['ad_id'].notna()]

    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS today_direct_visits_raw"))
    df.to_sql('today_direct_visits_raw', con=engine, if_exists='replace',
              index=False, dtype=_TODAY_VISITS_RAW_DTYPE)
    log.info("today_direct_visits_raw: %d строк (визитов: %d).",
             len(df), int(df['visits'].sum()) if not df.empty else 0)
    return len(df)


# ===========================================================================
# 2) Reporting API Метрики: clientID + dateTime + UTM (для атрибуции оплат)
# ===========================================================================

# Эти измерения совместимы с clientID. lastDirectClick* — НЕТ.
_CLIENT_UTM_DIMS = [
    'ym:s:clientID',
    'ym:s:dateTime',
    'ym:s:lastUTMSource',
    'ym:s:lastUTMMedium',
    'ym:s:lastUTMCampaign',
    'ym:s:lastUTMContent',
]

_TODAY_CLIENT_UTM_DTYPE = {
    'metrika_client_id': Text(),
    'visit_dt':          DateTime(),
    'utm_source':        Text(),
    'utm_medium':        Text(),
    'utm_campaign':      Text(),
    'utm_content':       Text(),
    'visits':            Integer(),
}


def fetch_client_utm_today() -> int:
    """Тянет (clientID, dateTime, UTM) за today, фильтруя в Python только Direct
    UTM (yandex/ya × cpc/paid). Сохраняет в today_client_utm.

    Этот срез нужен для last-click атрибуции оплат: clientID несовместим с
    lastDirectClickBanner, поэтому ad_id восстанавливаем через UTM-мост в SQL.
    """
    today = _today_msk()
    log.info("today: запрос clientID+UTM за %s через Reporting API...", today)

    raw = _paginate_reporting_api({
        'id':         COUNTER_ID,
        'metrics':    'ym:s:visits',
        'dimensions': ','.join(_CLIENT_UTM_DIMS),
        'date1':      today,
        'date2':      today,
    })

    rows = []
    for r in raw:
        d = r['dimensions']
        utm_src    = d[2].get('name')
        utm_medium = d[3].get('name')
        # Direct trace: yandex/ya + cpc/paid. Если UTM пустой — может быть Direct
        # без разметки; такие тоже сохраняем (атрибуция к ad_id будет через
        # pro-rata).
        is_direct_utm = (
            utm_src in ('yandex', 'ya') and
            utm_medium in ('cpc', 'paid')
        )
        is_empty_utm = not utm_src and not utm_medium
        if not (is_direct_utm or is_empty_utm):
            continue

        rows.append({
            'metrika_client_id': d[0].get('name'),
            'visit_dt':          d[1].get('name'),
            'utm_source':        utm_src,
            'utm_medium':        utm_medium,
            'utm_campaign':      d[4].get('name'),
            'utm_content':       d[5].get('name'),
            'visits':            int(r['metrics'][0] or 0),
        })

    df = pd.DataFrame(rows, columns=list(_TODAY_CLIENT_UTM_DTYPE.keys()))
    if not df.empty:
        df['visit_dt'] = pd.to_datetime(df['visit_dt'], errors='coerce')

    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS today_client_utm"))
    df.to_sql('today_client_utm', con=engine, if_exists='replace',
              index=False, dtype=_TODAY_CLIENT_UTM_DTYPE)
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS ix_today_client_utm_cid
                ON today_client_utm (metrika_client_id, visit_dt)
        """))
    log.info("today_client_utm: %d строк (потенциальных Direct визитов).", len(df))
    return len(df)


# ===========================================================================
# 3) Reporting API Метрики: clientID → mongoID (свежий маппинг для today)
# ===========================================================================

_TODAY_CID_MONGO_DTYPE = {
    'metrika_client_id': Text(),
    'mongo_user_id':     Text(),
}


def fetch_clientid_mapping_today() -> int:
    """Тянет clientID → mongoID для пользователей, оплативших или зарегистрировавшихся сегодня.

    Двухэтапная стратегия:
      1. Возвращающиеся пользователи — SQL-выборка из analytics.clientid_to_mongoid
         (накопительная таблица основного ETL), 0 API-запросов.
      2. Новые пользователи — по одному запросу на ID:
             dimensions=ym:s:clientID, filters=ym:up:paramsLevel2=='<id>'
         Комбинация ym:s:clientID+ym:up:paramsLevel2 как совместные dimensions
         даёт 400 (API не принимает их вместе), поэтому paramsLevel2 используется
         только как фильтр — как в основном ETL (loader_metrika.py).
         Типично 40–60 новых ID за сутки → ~30–40 с при паузе 0.6 с/запрос.

    Сохраняет результат в today_clientid_to_mongoid.
    """
    today = _today_msk()

    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT DISTINCT mongo_user_id FROM today_mongo_payments
            WHERE mongo_user_id IS NOT NULL AND mongo_user_id <> ''
            UNION
            SELECT DISTINCT mongo_user_id FROM today_mongo_users
            WHERE mongo_user_id IS NOT NULL AND mongo_user_id <> ''
        """)).fetchall()
    mongo_ids = [r[0] for r in rows]

    if not mongo_ids:
        log.info("today: маппинг clientID → mongoID пропущен — нет оплат и регистраций.")
        df = pd.DataFrame(columns=['metrika_client_id', 'mongo_user_id'])
    else:
        log.info("today: маппинг clientID → mongoID для %d пользователей сегодня...",
                 len(mongo_ids))
        pairs: list[dict] = []

        # Шаг 1: возвращающиеся — из накопительного кэша основного ETL.
        try:
            with engine.connect() as conn:
                existing = conn.execute(text("""
                    SELECT metrika_client_id, mongo_user_id
                    FROM analytics.clientid_to_mongoid
                    WHERE mongo_user_id = ANY(:ids)
                """), {"ids": mongo_ids}).fetchall()
            for row in existing:
                pairs.append({'metrika_client_id': row[0], 'mongo_user_id': row[1]})
            known_ids = {row[1] for row in existing}
        except Exception as e:
            log.warning("today: clientid_to_mongoid недоступна, пропускаем кэш: %s", e)
            known_ids = set()

        new_ids = [mid for mid in mongo_ids if mid not in known_ids]
        log.info("today: кэш: %d пар (%d ID); API: %d новых ID.",
                 len(pairs), len(known_ids), len(new_ids))

        # Шаг 2: новые — по одному запросу на ID (как в loader_metrika.py).
        for idx, mid in enumerate(new_ids):
            for attempt in range(4):
                try:
                    resp = requests.get(STAT_API, headers=HEADERS, timeout=30, params={
                        'id':         COUNTER_ID,
                        'metrics':    'ym:s:visits',
                        'dimensions': 'ym:s:clientID',
                        'filters':    f"ym:up:paramsLevel2=='{mid}'",
                        'date1':      today,
                        'date2':      today,
                        'limit':      100,
                    })
                    resp.raise_for_status()
                    for row in resp.json().get('data', []):
                        cid = row['dimensions'][0].get('name')
                        if cid:
                            pairs.append({'metrika_client_id': cid, 'mongo_user_id': mid})
                    break
                except requests.exceptions.HTTPError as e:
                    if e.response.status_code in (429, 400) and attempt < 3:
                        wait = [60, 90, 120][attempt]
                        log.warning(
                            "clientID mapping ID %d/%d HTTP %d — ждём %ds (попытка %d/3)...",
                            idx + 1, len(new_ids), e.response.status_code, wait, attempt + 1,
                        )
                        time.sleep(wait)
                    else:
                        log.warning("clientID mapping ID %d/%d ошибка: %s",
                                    idx + 1, len(new_ids), e)
                        break
                except Exception as e:
                    log.warning("clientID mapping ID %d/%d исключение: %s",
                                idx + 1, len(new_ids), e)
                    break

            time.sleep(0.6 + random.uniform(-0.15, 0.15))

        df = pd.DataFrame(pairs).drop_duplicates() if pairs else \
             pd.DataFrame(columns=['metrika_client_id', 'mongo_user_id'])

    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS today_clientid_to_mongoid"))
    df.to_sql('today_clientid_to_mongoid', con=engine, if_exists='replace',
              index=False, dtype=_TODAY_CID_MONGO_DTYPE)
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS ix_today_cid_mongo_cid
                ON today_clientid_to_mongoid (metrika_client_id)
        """))
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS ix_today_cid_mongo_uid
                ON today_clientid_to_mongoid (mongo_user_id)
        """))
    log.info("today_clientid_to_mongoid: %d пар.", len(df))
    return len(df)


# ===========================================================================
# 4) Direct Reports API: расходы за today
# ===========================================================================

_DIRECT_AD_FIELDS = ["Date", "CampaignId", "CampaignName", "AdGroupName", "AdId",
                     "Impressions", "Clicks", "Cost"]
# Hour-агрегат через Direct API не получается — ни AD_PERFORMANCE_REPORT, ни
# CUSTOM_REPORT не принимают Hour с этим набором полей (error_code=8000).
# Поэтому почасовые KPI-спарклайны строим только по тем источникам, где время
# доступно: today_client_utm (visit_dt из Reporting API) и MongoDB.

_TODAY_DIRECT_COSTS_DTYPE = {
    'date':          Date(),
    'campaign_id':   BigInteger(),
    'campaign_name': Text(),
    'ad_group_name': Text(),
    'ad_id':         BigInteger(),
    'impressions':   Integer(),
    'clicks':        Integer(),
    'cost_micros':   BigInteger(),
    'cost':          Float(precision=53),
}

_DIRECT_REPORTS_MAX_WAIT_S = 30 * 60
_DIRECT_REPORTS_RETRY_S    = 10


def _request_direct_report(date1: str, date2: str,
                            fields: list = None,
                            report_type: str = "AD_PERFORMANCE_REPORT",
                            name_prefix: str = "today_costs") -> str:
    """Асинхронный запрос Reports API Директа: 202/201 → ждём → 200 → TSV."""
    if fields is None:
        fields = _DIRECT_AD_FIELDS
    body = {
        "params": {
            "SelectionCriteria": {"DateFrom": date1, "DateTo": date2},
            "FieldNames":      fields,
            "ReportName":      f"{name_prefix}_{date1}_{int(time.time())}",
            "ReportType":      report_type,
            "DateRangeType":   "CUSTOM_DATE",
            "Format":          "TSV",
            "IncludeVAT":      "YES",
        }
    }
    deadline = time.time() + _DIRECT_REPORTS_MAX_WAIT_S
    while time.time() < deadline:
        resp = requests.post(DIRECT_API, json=body, headers=DIRECT_HEADERS, timeout=60)
        if resp.status_code == 200:
            return resp.text
        if resp.status_code in (201, 202):
            wait = int(resp.headers.get('Retry-After', _DIRECT_REPORTS_RETRY_S))
            log.info("today: Direct Reports HTTP %d — ждём %d с...", resp.status_code, wait)
            time.sleep(wait)
            continue
        log.warning("today: Direct Reports HTTP %d: %s", resp.status_code, resp.text[:500])
        resp.raise_for_status()
    raise TimeoutError("today: Direct Reports — превышено ожидание")


def fetch_direct_costs_today() -> int:
    """Тянет AD_PERFORMANCE_REPORT за today (uniquely: today=today, не до вчера)
    и сохраняет в today_direct_costs."""
    today = _today_msk()
    log.info("today: запрос AD_PERFORMANCE_REPORT за %s..%s...", today, today)

    try:
        raw = _request_direct_report(today, today,
                                      fields=_DIRECT_AD_FIELDS,
                                      report_type="AD_PERFORMANCE_REPORT",
                                      name_prefix="today_costs")
    except Exception as e:
        log.warning("today: не удалось получить AD_PERFORMANCE_REPORT: %s", e)
        df = pd.DataFrame(columns=list(_TODAY_DIRECT_COSTS_DTYPE.keys()))
    else:
        df = pd.read_csv(io.StringIO(raw), sep='\t', dtype=str)
        df = df.rename(columns={
            "Date":         "date",
            "CampaignId":   "campaign_id",
            "CampaignName": "campaign_name",
            "AdGroupName":  "ad_group_name",
            "AdId":         "ad_id",
            "Impressions":  "impressions",
            "Clicks":       "clicks",
            "Cost":         "cost_micros",
        })
        if not df.empty:
            df['campaign_id'] = pd.to_numeric(df['campaign_id'], errors='coerce').astype('Int64')
            df['ad_id']       = pd.to_numeric(df['ad_id'],       errors='coerce').astype('Int64')
            df['impressions'] = pd.to_numeric(df['impressions'], errors='coerce').fillna(0).astype('Int32')
            df['clicks']      = pd.to_numeric(df['clicks'],      errors='coerce').fillna(0).astype('Int32')
            df['cost_micros'] = pd.to_numeric(df['cost_micros'], errors='coerce').fillna(0).astype('Int64')
            df['cost']        = (df['cost_micros'] / 1_000_000).round(6)
            df['date']        = pd.to_datetime(df['date'], errors='coerce').dt.date

    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS today_direct_costs"))
    df.to_sql('today_direct_costs', con=engine, if_exists='replace',
              index=False, dtype=_TODAY_DIRECT_COSTS_DTYPE)
    log.info("today_direct_costs: %d строк | расход %.2f, кликов %d.",
             len(df),
             float(df['cost'].sum()) if not df.empty else 0.0,
             int(df['clicks'].sum()) if not df.empty else 0)
    return len(df)


# ===========================================================================
# 5) Direct Ads / Campaigns / AdImages API: справочники snapshot за today
# ===========================================================================

_TODAY_ADS_DTYPE = {
    "ad_id":            BigInteger(),
    "campaign_id":      BigInteger(),
    "ad_group_id":      BigInteger(),
    "state":            Text(),
    "status":           Text(),
    "ad_type":          Text(),
    "title":            Text(),
    "title2":           Text(),
    "body":             Text(),
    "href":             Text(),
    "display_url_path": Text(),
    "image_hash":       Text(),
}
_TODAY_CAMPAIGNS_DTYPE = {
    "campaign_id":   BigInteger(),
    "campaign_name": Text(),
}
_TODAY_AD_IMAGES_DTYPE = {
    "image_hash":   Text(),
    "name":         Text(),
    "type":         Text(),
    "subtype":      Text(),
    "original_url": Text(),
    "preview_url":  Text(),
}


def _fetch_direct_campaigns_full() -> list[dict]:
    """Все кампании аккаунта (включая ARCHIVED/ENDED — нужно для исторических ad_id)."""
    body = {
        "method": "get",
        "params": {
            "SelectionCriteria": {
                "States": ["ARCHIVED", "CONVERTED", "ENDED", "OFF", "ON", "SUSPENDED"],
            },
            "FieldNames": ["Id", "Name"],
            "Page": {"Limit": 10000},
        },
    }
    resp = requests.post(_DIRECT_CAMPAIGNS_API_URL, json=body,
                         headers=_DIRECT_ADS_HEADERS, timeout=60)
    resp.raise_for_status()
    data = resp.json()
    if "error" in data:
        raise RuntimeError(f"Campaigns API error: {data['error']}")
    return data.get("result", {}).get("Campaigns", [])


def _fetch_direct_ads_paginated(campaign_ids: list[int]) -> list[dict]:
    all_ads = []
    offset = 0
    while True:
        body = {
            "method": "get",
            "params": {
                "SelectionCriteria": {"CampaignIds": campaign_ids},
                "FieldNames": ["Id", "CampaignId", "AdGroupId", "State", "Status", "Type"],
                "TextAdFieldNames": ["Title", "Title2", "Text", "Href", "DisplayUrlPath", "AdImageHash"],
                "DynamicTextAdFieldNames": ["Text"],
                "Page": {"Limit": 10000, "Offset": offset},
            },
        }
        resp = requests.post(_DIRECT_ADS_API_URL, json=body,
                             headers=_DIRECT_ADS_HEADERS, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        if "error" in data:
            raise RuntimeError(f"Ads API error: {data['error']}")
        ads = data.get("result", {}).get("Ads", [])
        all_ads.extend(ads)
        limited_by = data.get("result", {}).get("LimitedBy")
        if limited_by is None:
            break
        offset = limited_by
        time.sleep(0.3)
    return all_ads


def _fetch_direct_adimages_paginated() -> list[dict]:
    all_imgs = []
    offset = 0
    while True:
        body = {
            "method": "get",
            "params": {
                "SelectionCriteria": {},
                "FieldNames": ["AdImageHash", "OriginalUrl", "PreviewUrl",
                               "Name", "Type", "Subtype"],
                "Page": {"Limit": 10000, "Offset": offset},
            },
        }
        resp = requests.post(_DIRECT_ADIMAGES_API_URL, json=body,
                             headers=_DIRECT_ADS_HEADERS, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        if "error" in data:
            raise RuntimeError(f"AdImages API error: {data['error']}")
        imgs = data.get("result", {}).get("AdImages", [])
        all_imgs.extend(imgs)
        limited_by = data.get("result", {}).get("LimitedBy")
        if limited_by is None:
            break
        offset = limited_by
        time.sleep(0.3)
    return all_imgs


def fetch_direct_dictionaries_today() -> tuple[int, int, int]:
    """Грузит свежий снимок Директа: campaigns, ads, ad_images."""
    log.info("today: загрузка справочников Директа (campaigns/ads/adimages)...")

    # Campaigns
    try:
        campaigns_raw = _fetch_direct_campaigns_full()
    except Exception as e:
        log.warning("today: не удалось получить Campaigns: %s", e)
        campaigns_raw = []

    camp_df = pd.DataFrame([
        {"campaign_id": int(c["Id"]), "campaign_name": c.get("Name")}
        for c in campaigns_raw if c.get("Id") is not None
    ], columns=["campaign_id", "campaign_name"])

    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS today_direct_campaigns"))
    camp_df.to_sql('today_direct_campaigns', con=engine, if_exists='replace',
                   index=False, dtype=_TODAY_CAMPAIGNS_DTYPE)
    log.info("today_direct_campaigns: %d записей.", len(camp_df))

    # Ads
    if not camp_df.empty:
        try:
            ads_raw = _fetch_direct_ads_paginated(camp_df['campaign_id'].astype(int).tolist())
        except Exception as e:
            log.warning("today: не удалось получить Ads: %s", e)
            ads_raw = []
    else:
        ads_raw = []

    ads_rows = []
    for ad in ads_raw:
        ad_type = ad.get("Type", "")
        text_ad = ad.get("TextAd") or {}
        dyn_ad  = ad.get("DynamicTextAd") or {}
        if ad_type == "TEXT_AD":
            title  = text_ad.get("Title") or None
            title2 = text_ad.get("Title2") or None
            body   = text_ad.get("Text") or None
            href   = text_ad.get("Href") or None
            disp   = text_ad.get("DisplayUrlPath") or None
            img_h  = text_ad.get("AdImageHash") or None
        elif ad_type == "DYNAMIC_TEXT_AD":
            title = title2 = href = disp = img_h = None
            body = dyn_ad.get("Text") or None
        else:
            title = title2 = body = href = disp = img_h = None
        ads_rows.append({
            "ad_id":            ad.get("Id"),
            "campaign_id":      ad.get("CampaignId"),
            "ad_group_id":      ad.get("AdGroupId"),
            "state":            ad.get("State"),
            "status":           ad.get("Status"),
            "ad_type":          ad_type or None,
            "title":            title,
            "title2":           title2,
            "body":             body,
            "href":             href,
            "display_url_path": disp,
            "image_hash":       img_h,
        })
    ads_df = pd.DataFrame(ads_rows, columns=list(_TODAY_ADS_DTYPE.keys()))
    if not ads_df.empty:
        ads_df["ad_id"]       = pd.to_numeric(ads_df["ad_id"],       errors="coerce").astype("Int64")
        ads_df["campaign_id"] = pd.to_numeric(ads_df["campaign_id"], errors="coerce").astype("Int64")
        ads_df["ad_group_id"] = pd.to_numeric(ads_df["ad_group_id"], errors="coerce").astype("Int64")

    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS today_direct_ads"))
    ads_df.to_sql('today_direct_ads', con=engine, if_exists='replace',
                  index=False, dtype=_TODAY_ADS_DTYPE)
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS ix_today_direct_ads_ad_id
                ON today_direct_ads (ad_id)
        """))
    log.info("today_direct_ads: %d объявлений.", len(ads_df))

    # AdImages
    try:
        imgs_raw = _fetch_direct_adimages_paginated()
    except Exception as e:
        log.warning("today: не удалось получить AdImages: %s", e)
        imgs_raw = []

    imgs_df = pd.DataFrame([{
        "image_hash":   img.get("AdImageHash"),
        "name":         img.get("Name"),
        "type":         img.get("Type"),
        "subtype":      img.get("Subtype"),
        "original_url": img.get("OriginalUrl"),
        "preview_url":  img.get("PreviewUrl"),
    } for img in imgs_raw], columns=list(_TODAY_AD_IMAGES_DTYPE.keys()))
    imgs_df = imgs_df[imgs_df["image_hash"].notna()] if not imgs_df.empty else imgs_df

    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS today_direct_ad_images"))
    imgs_df.to_sql('today_direct_ad_images', con=engine, if_exists='replace',
                   index=False, dtype=_TODAY_AD_IMAGES_DTYPE)
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS ix_today_direct_ad_images_hash
                ON today_direct_ad_images (image_hash)
        """))
    log.info("today_direct_ad_images: %d картинок.", len(imgs_df))

    return len(camp_df), len(ads_df), len(imgs_df)


# ===========================================================================
# 6) MongoDB: платежи за today
# ===========================================================================

_TODAY_PAYMENTS_DTYPE = {
    'payment_id':        Text(),
    'mongo_user_id':     Text(),
    'amount':            Float(precision=53),
    'status':            Text(),
    'provider':          Text(),
    'created_at':        DateTime(),
    'created_at_moscow': DateTime(),
}


def fetch_mongo_payments_today() -> int:
    """Тянет платежи MongoDB c created_at >= начало сегодняшних суток МСК."""
    cutoff_utc = _start_of_today_utc()
    cutoff_msk = cutoff_utc + timedelta(hours=3)
    log.info("today: загрузка mongo_payments с created_at >= %s МСК (%s UTC)...",
             cutoff_msk.strftime('%Y-%m-%d %H:%M:%S'),
             cutoff_utc.strftime('%Y-%m-%d %H:%M:%S'))

    mongo_client = None
    rows = []
    try:
        mongo_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        mongo_client.admin.command('ping')
        collection = mongo_client[MONGO_DB_NAME]['payment']
        cursor = collection.find({'created_at': {'$gte': cutoff_utc}})
        for doc in cursor:
            ca = doc.get('created_at')
            if isinstance(ca, datetime):
                ca_utc = ca.replace(tzinfo=None) if ca.tzinfo else ca
                ca_moscow = ca_utc + timedelta(hours=3)
            else:
                ca_utc = ca_moscow = None
            rows.append({
                'payment_id':        str(doc['_id']),
                'mongo_user_id':     str(doc.get('user', '')),
                'amount':            doc.get('amount', 0.0),
                'status':            doc.get('status', ''),
                'provider':          doc.get('provider', ''),
                'created_at':        ca_utc,
                'created_at_moscow': ca_moscow,
            })
    except Exception as e:
        log.warning("today: ошибка MongoDB: %s", e)
    finally:
        if mongo_client:
            mongo_client.close()

    df = pd.DataFrame(rows, columns=list(_TODAY_PAYMENTS_DTYPE.keys()))

    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS today_mongo_payments"))
    df.to_sql('today_mongo_payments', con=engine, if_exists='replace',
              index=False, dtype=_TODAY_PAYMENTS_DTYPE)
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS ix_today_mongo_payments_user
                ON today_mongo_payments (mongo_user_id)
        """))
    approved = int((df['status'] == 'approved').sum()) if not df.empty else 0
    log.info("today_mongo_payments: %d платежей (approved: %d, сумма approved: %.2f).",
             len(df), approved,
             float(df.loc[df['status']=='approved', 'amount'].sum()) if not df.empty else 0.0)
    return len(df)


# ===========================================================================
# 6b) MongoDB: регистрации за today (для атрибуции регистраций к ad_id)
# ===========================================================================

_TODAY_USERS_DTYPE = {
    'mongo_user_id':     Text(),
    'platform':          Text(),
    'created_at':        DateTime(),
    'created_at_moscow': DateTime(),
}


def fetch_mongo_users_today() -> int:
    """Тянет регистрации MongoDB c created_at >= начало сегодняшних суток МСК.
    Источник — та же коллекция, что и в основном loader_mongo.sync_users_to_sqlite
    (env MONGO_COLLECTION, по умолчанию 'user')."""
    cutoff_utc = _start_of_today_utc()
    cutoff_msk = cutoff_utc + timedelta(hours=3)
    log.info("today: загрузка mongo_users (регистраций) с created_at >= %s МСК (%s UTC)...",
             cutoff_msk.strftime('%Y-%m-%d %H:%M:%S'),
             cutoff_utc.strftime('%Y-%m-%d %H:%M:%S'))

    mongo_client = None
    rows = []
    try:
        mongo_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        mongo_client.admin.command('ping')
        collection = mongo_client[MONGO_DB_NAME][MONGO_COLLECTION]
        cursor = collection.find(
            {'created_at': {'$gte': cutoff_utc}},
            {'_id': 1, 'created_at': 1, 'platform': 1},
        )
        for doc in cursor:
            ca = doc.get('created_at')
            if isinstance(ca, datetime):
                ca_utc    = ca.replace(tzinfo=None) if ca.tzinfo else ca
                ca_moscow = ca_utc + timedelta(hours=3)
            else:
                ca_utc = ca_moscow = None
            rows.append({
                'mongo_user_id':     str(doc['_id']),
                'platform':          doc.get('platform', ''),
                'created_at':        ca_utc,
                'created_at_moscow': ca_moscow,
            })
    except Exception as e:
        log.warning("today: ошибка MongoDB (users): %s", e)
    finally:
        if mongo_client:
            mongo_client.close()

    df = pd.DataFrame(rows, columns=list(_TODAY_USERS_DTYPE.keys()))

    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS today_mongo_users"))
    df.to_sql('today_mongo_users', con=engine, if_exists='replace',
              index=False, dtype=_TODAY_USERS_DTYPE)
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS ix_today_mongo_users_user
                ON today_mongo_users (mongo_user_id)
        """))
    log.info("today_mongo_users: %d регистраций.", len(df))
    return len(df)


# ===========================================================================
# 7) SQL-сборка: today_dataset — финальная таблица для дашборда
# ===========================================================================

# Алгоритм:
#   1. visits_by_ad     — визиты по ad_id напрямую из today_direct_visits_raw
#                         (там ad_id известен в каждой строке, JOIN не нужен)
#   2. utm_bridge       — для каждой пары (utm_campaign, utm_content): список
#                         ad_id-кандидатов (1 = однозначный, N = неоднозначный)
#   3. payment_attr_raw — last-click атрибуция: для каждой оплаты сегодня
#                         выбираем визит с MAX(visit_dt <= created_at_moscow)
#                         через мост clientID→mongoID
#   4. exact_attr       — оплаты, чей UTM-мост однозначный (n=1) → точный ad_id
#   5. prorata_attr     — оплаты с n>1: распределяем сумму по весу кликов
#                         Директа за today (fallback: equal split, если
#                         clicks=0 у всех кандидатов)
#   6. payments_per_ad  — агрегат всех атрибуций по ad_id
#   7. all_ads          — UNION ad_id из visits / costs / ads (на случай если
#                         объявление в одной таблице есть, в другой нет)
#   8. SELECT с JOIN-ами на справочники: имя кампании в формате 'name (id)',
#                         тексты/картинки/состояние объявления
_BUILD_SQL = """
-- ─────────────────────────────────────────────────────────────────────────
-- Шаг 1: today_attributed_payments_new — оплаты, реально атрибутированные
-- к Direct-объявлениям (через мост clientID→mongoID + UTM).
-- Сохраняем со временной меткой created_at_moscow + ad_id + weight + quality.
-- ─────────────────────────────────────────────────────────────────────────
DROP TABLE IF EXISTS today_attributed_payments_new;
CREATE TABLE today_attributed_payments_new AS
WITH utm_bridge AS (
    SELECT utm_campaign, utm_content,
           ARRAY_AGG(DISTINCT ad_id) AS ad_ids,
           COUNT(DISTINCT ad_id)     AS n
    FROM today_direct_visits_raw
    WHERE ad_id IS NOT NULL AND utm_campaign IS NOT NULL AND utm_content IS NOT NULL
    GROUP BY utm_campaign, utm_content
),
payment_attr_raw AS (
    -- Last-click within today: каждая оплата → самый поздний Direct визит
    -- того же mongo_user_id. DISTINCT ON гарантирует учёт ровно один раз.
    SELECT DISTINCT ON (p.payment_id)
        p.payment_id, p.mongo_user_id, p.amount, p.status, p.created_at_moscow,
        cu.utm_campaign, cu.utm_content
    FROM today_mongo_payments p
    JOIN today_clientid_to_mongoid cm ON cm.mongo_user_id = p.mongo_user_id
    JOIN today_client_utm cu ON cu.metrika_client_id = cm.metrika_client_id
                            AND cu.visit_dt <= p.created_at_moscow
    ORDER BY p.payment_id, cu.visit_dt DESC
),
exact_pay AS (
    SELECT pa.payment_id, pa.mongo_user_id, pa.amount, pa.status, pa.created_at_moscow,
           b.ad_ids[1] AS ad_id, 1.0::float AS weight, 'exact'::text AS quality
    FROM payment_attr_raw pa
    JOIN utm_bridge b ON b.utm_campaign = pa.utm_campaign AND b.utm_content = pa.utm_content
    WHERE b.n = 1
),
ambig_pay AS (
    SELECT pa.payment_id, pa.mongo_user_id, pa.amount, pa.status, pa.created_at_moscow,
           candidate AS ad_id, COALESCE(c.clicks, 0)::float AS clicks
    FROM payment_attr_raw pa
    JOIN utm_bridge b ON b.utm_campaign = pa.utm_campaign AND b.utm_content = pa.utm_content
    CROSS JOIN UNNEST(b.ad_ids) AS candidate
    LEFT JOIN today_direct_costs c ON c.ad_id = candidate
    WHERE b.n > 1
),
prorata_pay AS (
    SELECT payment_id, mongo_user_id, amount, status, created_at_moscow, ad_id,
        CASE WHEN SUM(clicks) OVER (PARTITION BY payment_id) > 0
             THEN clicks / SUM(clicks) OVER (PARTITION BY payment_id)
             ELSE 1.0 / COUNT(*) OVER (PARTITION BY payment_id)
        END AS weight,
        'prorata'::text AS quality
    FROM ambig_pay
)
SELECT * FROM exact_pay
UNION ALL SELECT * FROM prorata_pay;

CREATE INDEX IF NOT EXISTS ix_today_attr_pay_new_ad_id ON today_attributed_payments_new (ad_id);
CREATE INDEX IF NOT EXISTS ix_today_attr_pay_new_dt    ON today_attributed_payments_new (created_at_moscow);

-- ─────────────────────────────────────────────────────────────────────────
-- Шаг 2: today_attributed_regs_new — регистрации, реально атрибутированные.
-- ─────────────────────────────────────────────────────────────────────────
DROP TABLE IF EXISTS today_attributed_regs_new;
CREATE TABLE today_attributed_regs_new AS
WITH utm_bridge AS (
    SELECT utm_campaign, utm_content,
           ARRAY_AGG(DISTINCT ad_id) AS ad_ids,
           COUNT(DISTINCT ad_id)     AS n
    FROM today_direct_visits_raw
    WHERE ad_id IS NOT NULL AND utm_campaign IS NOT NULL AND utm_content IS NOT NULL
    GROUP BY utm_campaign, utm_content
),
reg_attr_raw AS (
    SELECT DISTINCT ON (u.mongo_user_id)
        u.mongo_user_id, u.created_at_moscow,
        cu.utm_campaign, cu.utm_content
    FROM today_mongo_users u
    JOIN today_clientid_to_mongoid cm ON cm.mongo_user_id = u.mongo_user_id
    JOIN today_client_utm cu ON cu.metrika_client_id = cm.metrika_client_id
                            AND cu.visit_dt <= u.created_at_moscow
    ORDER BY u.mongo_user_id, cu.visit_dt DESC
),
exact_reg AS (
    SELECT ra.mongo_user_id, ra.created_at_moscow,
           b.ad_ids[1] AS ad_id, 1.0::float AS weight, 'exact'::text AS quality
    FROM reg_attr_raw ra
    JOIN utm_bridge b ON b.utm_campaign = ra.utm_campaign AND b.utm_content = ra.utm_content
    WHERE b.n = 1
),
ambig_reg AS (
    SELECT ra.mongo_user_id, ra.created_at_moscow,
           candidate AS ad_id, COALESCE(c.clicks, 0)::float AS clicks
    FROM reg_attr_raw ra
    JOIN utm_bridge b ON b.utm_campaign = ra.utm_campaign AND b.utm_content = ra.utm_content
    CROSS JOIN UNNEST(b.ad_ids) AS candidate
    LEFT JOIN today_direct_costs c ON c.ad_id = candidate
    WHERE b.n > 1
),
prorata_reg AS (
    SELECT mongo_user_id, created_at_moscow, ad_id,
        CASE WHEN SUM(clicks) OVER (PARTITION BY mongo_user_id) > 0
             THEN clicks / SUM(clicks) OVER (PARTITION BY mongo_user_id)
             ELSE 1.0 / COUNT(*) OVER (PARTITION BY mongo_user_id)
        END AS weight,
        'prorata'::text AS quality
    FROM ambig_reg
)
SELECT * FROM exact_reg
UNION ALL SELECT * FROM prorata_reg;

CREATE INDEX IF NOT EXISTS ix_today_attr_reg_new_ad_id ON today_attributed_regs_new (ad_id);
CREATE INDEX IF NOT EXISTS ix_today_attr_reg_new_dt    ON today_attributed_regs_new (created_at_moscow);

-- ─────────────────────────────────────────────────────────────────────────
-- Шаг 3: today_dataset_new — финальная таблица для галереи и таблицы метрик.
-- Использует уже атрибутированные события из шагов 1-2 — простая агрегация.
-- ─────────────────────────────────────────────────────────────────────────
DROP TABLE IF EXISTS today_dataset_new;
CREATE TABLE today_dataset_new AS
WITH visits_by_ad AS (
    SELECT ad_id, SUM(visits)::float AS visits_total
    FROM today_direct_visits_raw
    WHERE ad_id IS NOT NULL
    GROUP BY ad_id
),
payments_per_ad AS (
    SELECT
        ad_id,
        SUM(CASE WHEN status = 'approved' THEN weight          ELSE 0 END) AS payments_approved_count,
        SUM(CASE WHEN status = 'approved' THEN amount * weight ELSE 0 END) AS payments_approved_sum,
        SUM(CASE WHEN status != 'approved' THEN weight         ELSE 0 END) AS payments_other_count,
        SUM(CASE WHEN status != 'approved' THEN amount * weight ELSE 0 END) AS payments_other_sum,
        SUM(CASE WHEN quality = 'exact'   THEN weight          ELSE 0 END) AS exact_weight_pay,
        SUM(weight)                                                        AS total_weight_pay
    FROM today_attributed_payments_new
    GROUP BY ad_id
),
regs_per_ad AS (
    SELECT
        ad_id,
        SUM(weight)                                              AS registrations_count,
        SUM(CASE WHEN quality = 'exact' THEN weight ELSE 0 END)  AS exact_weight_reg,
        SUM(weight)                                              AS total_weight_reg
    FROM today_attributed_regs_new
    GROUP BY ad_id
),
all_ads AS (
    SELECT ad_id FROM today_direct_visits_raw WHERE ad_id IS NOT NULL
    UNION
    SELECT ad_id FROM today_direct_costs       WHERE ad_id IS NOT NULL
    UNION
    SELECT ad_id FROM today_direct_ads         WHERE ad_id IS NOT NULL
    UNION
    SELECT ad_id FROM payments_per_ad
    UNION
    SELECT ad_id FROM regs_per_ad
)
SELECT
    aa.ad_id,
    COALESCE(da.campaign_id, dc.campaign_id) AS campaign_id,
    -- Имя кампании в формате 'name (id)' для совместимости с основным отчётом.
    -- Источник имени — Direct Campaigns API (today_direct_campaigns), fallback —
    -- название из direct_costs (CampaignName поля). 'нет имени (id)' — крайний
    -- случай, когда campaign_id есть, но в обоих справочниках его не нашли.
    CASE
        WHEN COALESCE(da.campaign_id, dc.campaign_id) IS NULL THEN NULL
        ELSE COALESCE(tc.campaign_name, dc.campaign_name, 'нет имени')
             || ' (' || COALESCE(da.campaign_id, dc.campaign_id)::text || ')'
    END                                       AS campaign_name,
    da.ad_group_id,
    da.ad_type,
    da.title                                  AS ad_title,
    da.title2                                 AS ad_title2,
    da.body                                   AS ad_body,
    da.href                                   AS ad_href,
    da.display_url_path                       AS ad_display_url_path,
    img.preview_url                           AS ad_image_preview,
    img.original_url                          AS ad_image_original,
    -- Активность объявления: 1 = state=ON и status=ACCEPTED, 0 = иное состояние,
    -- NULL = не найдено в справочнике (создано сегодня после snapshot, или не Direct).
    CASE
        WHEN da.ad_id IS NULL                                    THEN NULL
        WHEN da.state = 'ON' AND da.status = 'ACCEPTED'           THEN 1
        ELSE 0
    END                                       AS ad_state_active,
    -- Метрики
    COALESCE(v.visits_total, 0)               AS visits_total,
    COALESCE(dc.impressions, 0)               AS direct_impressions,
    COALESCE(dc.clicks, 0)                    AS direct_clicks,
    COALESCE(dc.cost, 0)                      AS direct_cost,
    COALESCE(pp.payments_approved_count, 0)   AS payments_approved_count,
    COALESCE(pp.payments_approved_sum, 0)     AS payments_approved_sum,
    COALESCE(pp.payments_other_count, 0)      AS payments_other_count,
    COALESCE(pp.payments_other_sum, 0)        AS payments_other_sum,
    COALESCE(rp.registrations_count, 0)       AS registrations_count,
    -- Доля точной атрибуции (объединённая по оплатам и регистрациям).
    -- 1.0 = всё атрибутировано точно, <1 = была доля pro-rata, NULL = событий нет.
    CASE
        WHEN COALESCE(pp.total_weight_pay, 0) + COALESCE(rp.total_weight_reg, 0) > 0
            THEN
                (COALESCE(pp.exact_weight_pay, 0) + COALESCE(rp.exact_weight_reg, 0))
                / (COALESCE(pp.total_weight_pay, 0) + COALESCE(rp.total_weight_reg, 0))
        ELSE NULL
    END                                       AS attribution_quality
FROM all_ads aa
LEFT JOIN today_direct_ads        da  ON da.ad_id        = aa.ad_id
LEFT JOIN today_direct_costs      dc  ON dc.ad_id        = aa.ad_id
LEFT JOIN today_direct_campaigns  tc  ON tc.campaign_id  = COALESCE(da.campaign_id, dc.campaign_id)
LEFT JOIN today_direct_ad_images  img ON img.image_hash  = da.image_hash
LEFT JOIN visits_by_ad            v   ON v.ad_id         = aa.ad_id
LEFT JOIN payments_per_ad         pp  ON pp.ad_id        = aa.ad_id
LEFT JOIN regs_per_ad             rp  ON rp.ad_id        = aa.ad_id
-- В дашборде показываем только активные за сегодня объявления (хоть какая-то
-- метрика > 0). Иначе галерея забьётся всеми историческими ad_id из справочника.
WHERE COALESCE(v.visits_total, 0)              > 0
   OR COALESCE(dc.impressions, 0)              > 0
   OR COALESCE(dc.clicks, 0)                   > 0
   OR COALESCE(dc.cost, 0)                     > 0
   OR COALESCE(pp.total_weight_pay, 0)         > 0
   OR COALESCE(rp.total_weight_reg, 0)         > 0
;

CREATE INDEX IF NOT EXISTS ix_today_dataset_new_ad_id    ON today_dataset_new (ad_id);
CREATE INDEX IF NOT EXISTS ix_today_dataset_new_camp_id  ON today_dataset_new (campaign_id);
"""

# Атомарный swap: всё внутри одной транзакции PostgreSQL → пользователи дашборда
# либо видят старую таблицу, либо новую, никогда не «таблица не существует».
# DROP + ALTER RENAME в одной транзакции = single AccessExclusiveLock,
# который держится миллисекунды. Свопим все 3 таблицы (атрибуции оплат,
# атрибуции регистраций, основной dataset) одним подходом.
_SWAP_SQL = """
DROP TABLE IF EXISTS today_attributed_payments;
ALTER TABLE today_attributed_payments_new RENAME TO today_attributed_payments;
ALTER INDEX IF EXISTS ix_today_attr_pay_new_ad_id RENAME TO ix_today_attr_pay_ad_id;
ALTER INDEX IF EXISTS ix_today_attr_pay_new_dt    RENAME TO ix_today_attr_pay_dt;

DROP TABLE IF EXISTS today_attributed_regs;
ALTER TABLE today_attributed_regs_new RENAME TO today_attributed_regs;
ALTER INDEX IF EXISTS ix_today_attr_reg_new_ad_id RENAME TO ix_today_attr_reg_ad_id;
ALTER INDEX IF EXISTS ix_today_attr_reg_new_dt    RENAME TO ix_today_attr_reg_dt;

DROP TABLE IF EXISTS today_dataset;
ALTER TABLE today_dataset_new RENAME TO today_dataset;
ALTER INDEX IF EXISTS ix_today_dataset_new_ad_id   RENAME TO ix_today_dataset_ad_id;
ALTER INDEX IF EXISTS ix_today_dataset_new_camp_id RENAME TO ix_today_dataset_camp_id;
"""


def build_today_dataset() -> int:
    """Собирает today_dataset из всех staging-таблиц через SQL.

    Использует swap-pattern: новая таблица собирается в `today_dataset_new`,
    затем в одной транзакции старая дропается и новая переименовывается.
    Это гарантирует, что Superset всегда видит существующую таблицу
    (либо старую с прошлого прогона, либо новую) — между прогонами никогда
    нет окна «relation does not exist».

    Не должно вызываться до того, как fetch_* функции отработали.
    """
    log.info("today: сборка today_dataset через SQL (swap-pattern)...")
    with engine.begin() as conn:
        # Шаг 1: собираем новую таблицу + индексы (на _new, не блокирует читателей)
        _execute_sql_block(conn, _BUILD_SQL)
        # Шаг 2: атомарно дропаем старую и переименовываем новую (внутри той же
        # транзакции — DROP+RENAME под единым AccessExclusiveLock)
        _execute_sql_block(conn, _SWAP_SQL)

        n_rows = conn.execute(text("SELECT COUNT(*) FROM today_dataset")).scalar() or 0
        # Сводная статистика по качеству атрибуции
        stats = conn.execute(text("""
            SELECT
                COUNT(*)                                            AS rows,
                COALESCE(SUM(visits_total), 0)::float               AS visits,
                COALESCE(SUM(direct_clicks), 0)::int                AS clicks,
                COALESCE(SUM(direct_cost), 0)::float                AS cost,
                COALESCE(SUM(payments_approved_count), 0)::float    AS pay_cnt,
                COALESCE(SUM(payments_approved_sum), 0)::float      AS pay_sum,
                COALESCE(SUM(registrations_count), 0)::float        AS reg_cnt,
                AVG(attribution_quality)                            AS avg_quality
            FROM today_dataset
        """)).mappings().one()

    log.info(
        "today_dataset: %d строк | визиты: %.0f | клики: %d | расход: %.2f | "
        "оплаты approved: %.2f шт. на %.2f | регистрации: %.2f | "
        "средняя точность атрибуции: %s",
        n_rows, stats['visits'], stats['clicks'], stats['cost'],
        stats['pay_cnt'], stats['pay_sum'], stats['reg_cnt'],
        f"{stats['avg_quality']:.2%}" if stats['avg_quality'] is not None else "—",
    )
    return n_rows


# ===========================================================================
# 8) today_hourly: почасовая агрегация для KPI-карточек со спарклайнами
# ===========================================================================

# Источники по часам:
#   • визиты/уникальные юзеры — `today_client_utm` (visit_dt → date_trunc('hour'))
#   • оплаты approved         — `today_mongo_payments` (created_at_moscow)
#   • регистрации             — `today_mongo_users` (created_at_moscow)
#
# Direct API не даёт почасовых данных (Hour не работает ни в AD_PERFORMANCE,
# ни в CUSTOM_REPORT с нашим набором полей). Поэтому клики/показы/расход
# на KPI-карточках показываются только агрегатом за день, без trendline.
#
# generate_series создаёт все 24 часа сегодняшних суток МСК — пустые часы
# попадают в спарклайн как 0 (Superset покажет ровную линию там, где ничего
# не было; визуально это лучше, чем «провал» из-за отсутствующего ряда).
_BUILD_HOURLY_SQL = """
DROP TABLE IF EXISTS today_hourly_new;
CREATE TABLE today_hourly_new AS
WITH today_msk AS (
    SELECT (NOW() AT TIME ZONE 'Europe/Moscow')::date AS d
),
hours AS (
    SELECT generate_series(
        (SELECT d FROM today_msk)::timestamp,
        (SELECT d FROM today_msk)::timestamp + INTERVAL '23 hours',
        INTERVAL '1 hour'
    ) AS hr
),
-- Визиты — СТРОГИЙ Direct UTM. Без empty-UTM (чтобы не считать прямые
-- переходы / органику без меток). Это даёт цифру, согласованную с галереей.
visits_h AS (
    SELECT date_trunc('hour', visit_dt) AS hr,
           COALESCE(SUM(visits), 0)::int AS visits,
           COUNT(DISTINCT metrika_client_id) AS users
    FROM today_client_utm
    WHERE utm_source IN ('yandex', 'ya')
      AND utm_medium IN ('cpc', 'paid')
    GROUP BY 1
),
-- Оплаты и регистрации — ТОЛЬКО Direct-атрибутированные (через UTM-мост).
-- Берём из today_attributed_* (собирается build_today_dataset),
-- учитываем weight на случай pro-rata распределения. Получаем тот же
-- итог, что в today_dataset.payments_approved_*.
pay_h AS (
    SELECT date_trunc('hour', created_at_moscow) AS hr,
           SUM(CASE WHEN status='approved' THEN amount * weight ELSE 0 END)::float AS pay_sum,
           SUM(CASE WHEN status='approved' THEN weight          ELSE 0 END)::float AS pay_cnt
    FROM today_attributed_payments
    WHERE created_at_moscow IS NOT NULL
    GROUP BY 1
),
reg_h AS (
    SELECT date_trunc('hour', created_at_moscow) AS hr,
           SUM(weight)::float AS regs
    FROM today_attributed_regs
    WHERE created_at_moscow IS NOT NULL
    GROUP BY 1
)
SELECT h.hr                            AS hr,
       COALESCE(v.visits,  0)          AS visits,
       COALESCE(v.users,   0)          AS users,
       COALESCE(p.pay_cnt, 0)::float   AS pay_cnt,
       COALESCE(p.pay_sum, 0)::float   AS pay_sum,
       COALESCE(r.regs,    0)::float   AS regs
FROM hours h
LEFT JOIN visits_h v ON v.hr = h.hr
LEFT JOIN pay_h    p ON p.hr = h.hr
LEFT JOIN reg_h    r ON r.hr = h.hr
ORDER BY h.hr;

CREATE INDEX IF NOT EXISTS ix_today_hourly_new_hr ON today_hourly_new (hr);
"""

_SWAP_HOURLY_SQL = """
DROP TABLE IF EXISTS today_hourly;
ALTER TABLE today_hourly_new RENAME TO today_hourly;
ALTER INDEX IF EXISTS ix_today_hourly_new_hr RENAME TO ix_today_hourly_hr;
"""


def build_today_hourly() -> int:
    """Собирает today_hourly (24 строки, по одной на час сегодняшних суток МСК).
    Используется для big_number чартов с trendline на дашборде «За сегодня».
    Swap-pattern — Superset видит непрерывную таблицу.
    """
    log.info("today: сборка today_hourly через SQL (swap-pattern)...")
    with engine.begin() as conn:
        _execute_sql_block(conn, _BUILD_HOURLY_SQL)
        _execute_sql_block(conn, _SWAP_HOURLY_SQL)
        stats = conn.execute(text("""
            SELECT COUNT(*) AS rows,
                   SUM(visits) AS v,
                   SUM(regs)   AS r,
                   SUM(pay_sum) AS pay
            FROM today_hourly
        """)).mappings().one()
    log.info(
        "today_hourly: %d строк (часов) | сумма по дню: визиты=%s, "
        "оплаты=%.2f, регистрации=%s",
        stats['rows'], stats['v'], float(stats['pay'] or 0), stats['r'],
    )
    return stats['rows']


# ===========================================================================
# Точка входа
# ===========================================================================

def sync_today():
    """Полный прогон today-loader: 6 источников + сборка today_dataset.

    Порядок: справочники → расходы → визиты Метрики → платежи MongoDB →
    маппинг clientID (по mongoID плательщиков) → SQL-сборка.

    Маппинг идёт ПОСЛЕ платежей: список mongoIDs для запросов читается из
    today_mongo_payments — обращаемся к Reporting API только за нужными
    юзерами (5–50 запросов вместо тысяч).
    """
    log.info("=== today-loader: старт ===")

    # 1. Справочники Директа (нужны для меток, текстов, картинок и pro-rata)
    fetch_direct_dictionaries_today()

    # 2. Расходы Директа за today (для метрик и весов pro-rata)
    fetch_direct_costs_today()

    # 3. Direct визиты за today (Метрика Reporting API)
    fetch_direct_visits_today()

    # 4. clientID + UTM (для last-click атрибуции оплат)
    fetch_client_utm_today()

    # 5. Платежи и регистрации за today (этот шаг должен быть ДО clientid-маппинга,
    #    т.к. список mongoIDs для маппинга берётся из этих таблиц).
    fetch_mongo_payments_today()
    fetch_mongo_users_today()

    # 6. Маппинг clientID → mongoID — только для плательщиков+регистраций сегодня
    fetch_clientid_mapping_today()

    # 7. Финальная сборка today_dataset (галерея + симметричный блок)
    build_today_dataset()

    # 8. Часовая агрегация today_hourly (KPI-карточки со спарклайнами)
    build_today_hourly()

    log.info("=== today-loader: завершён ===")

