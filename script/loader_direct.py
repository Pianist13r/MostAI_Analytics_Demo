"""
Загрузка данных о расходах из Яндекс.Директ Reports API v5.

Таблица direct_costs — инкрементальная: каждый запуск докачивает данные
начиная с последней даты (или с 2025-01-01 при первом запуске), перезаписывая
последний день (он мог быть неполным).

Дедупликация: уникальный индекс + DELETE по ctid — аналог _deduplicate_by_id
из loader_metrika.py.
"""
import io
import time
from datetime import datetime, timedelta, timezone

# Россия не переходит на летнее время с 2014 → фиксированный UTC+3 безопасен
# и не требует tzdata в контейнере. Используется для расчёта `yesterday` так,
# чтобы он совпадал с cutoff'ом visits_calculated (NOW() AT TIME ZONE 'Europe/Moscow').
_MSK = timezone(timedelta(hours=3))

import pandas as pd
import requests
from sqlalchemy import BigInteger, Date, Float, Integer, Text, inspect, text

from loader_config import log, engine, DIRECT_API, DIRECT_HEADERS

_DIRECT_SQLA_DTYPE = {
    'date':          Date(),
    'campaign_id':   BigInteger(),
    'campaign_name': Text(),
    'ad_group_name': Text(),
    'ad_id':         Text(),
    'impressions':   Integer(),
    'clicks':        Integer(),
    'cost_micros':   BigInteger(),
    'cost':          Float(precision=53),
}

_DIRECT_CRITERIA_SQLA_DTYPE = {
    'date':          Date(),
    'campaign_id':   BigInteger(),
    'campaign_name': Text(),
    'ad_group_name': Text(),
    'criterion':     Text(),
    'criterion_type': Text(),
    'impressions':   Integer(),
    'clicks':        Integer(),
    'cost_micros':   BigInteger(),
    'cost':          Float(precision=53),
}

# ---------------------------------------------------------------------------
# Константы
# ---------------------------------------------------------------------------
DIRECT_FIELDS          = ["Date", "CampaignId", "CampaignName", "AdGroupName", "AdId",
                           "Impressions", "Clicks", "Cost"]
DIRECT_CRITERIA_FIELDS = ["Date", "CampaignId", "CampaignName", "AdGroupName",
                           "Criterion", "CriterionType", "Impressions", "Clicks", "Cost"]
DIRECT_START_DATE      = "2025-01-01"
DIRECT_MAX_WAIT_S      = 30 * 60
DIRECT_RETRY_DEFAULT_S = 10


# ---------------------------------------------------------------------------
# Вспомогательные функции
# ---------------------------------------------------------------------------

def _get_last_direct_date() -> str:
    """Возвращает последнюю дату из direct_costs, или DIRECT_START_DATE при пустой/отсутствующей таблице."""
    try:
        inspector = inspect(engine)
        if 'direct_costs' not in inspector.get_table_names(schema='analytics'):
            return DIRECT_START_DATE
        with engine.connect() as conn:
            result = conn.execute(text('SELECT MAX("date") FROM direct_costs')).scalar()
            if result:
                return str(result)
    except Exception as e:
        log.warning("Не удалось прочитать последнюю дату из direct_costs: %s — скачиваем с %s",
                    e, DIRECT_START_DATE)
    return DIRECT_START_DATE


def _build_report_body(start_date: str, end_date: str,
                       fields: list, report_type: str, name_prefix: str) -> dict:
    """Формирует тело запроса к Reports API."""
    return {
        "params": {
            "SelectionCriteria": {
                "DateFrom": start_date,
                "DateTo":   end_date,
            },
            "FieldNames":      fields,
            "ReportName":      f"{name_prefix}_vat_{start_date}_{end_date}",
            "ReportType":      report_type,
            "DateRangeType":   "CUSTOM_DATE",
            "Format":          "TSV",
            "IncludeVAT":      "YES",
        }
    }


def _request_report(start_date: str, end_date: str,
                    fields: list, report_type: str, name_prefix: str) -> str:
    """Запрашивает отчёт и ждёт готовности. Возвращает тело TSV-ответа.

    Яндекс.Директ Reports API работает асинхронно:
      202 — отчёт готовится, нужно повторить запрос через Retry-After секунд.
      201 — отчёт поставлен в очередь (тоже повторяем).
      200 — отчёт готов, тело ответа = TSV.
    """
    body     = _build_report_body(start_date, end_date, fields, report_type, name_prefix)
    deadline = time.time() + DIRECT_MAX_WAIT_S

    while time.time() < deadline:
        resp = requests.post(DIRECT_API, json=body, headers=DIRECT_HEADERS, timeout=60)

        if resp.status_code == 200:
            return resp.text

        if resp.status_code in (201, 202):
            retry_after = int(resp.headers.get('Retry-After', DIRECT_RETRY_DEFAULT_S))
            log.info("Директ: отчёт готовится (HTTP %d), ждём %d с...", resp.status_code, retry_after)
            time.sleep(retry_after)
            continue

        # Любой другой статус — ошибка
        log.warning("Директ: неожиданный ответ HTTP %d: %s", resp.status_code, resp.text[:500])
        resp.raise_for_status()

    raise TimeoutError(
        f"Директ: превышено время ожидания ({DIRECT_MAX_WAIT_S // 60} мин) для отчёта "
        f"{start_date}..{end_date}"
    )


_AD_RENAME = {
    "Date":           "date",
    "CampaignId":     "campaign_id",
    "CampaignName":   "campaign_name",
    "AdGroupName":    "ad_group_name",
    "AdId":           "ad_id",
    "Impressions":    "impressions",
    "Clicks":         "clicks",
    "Cost":           "cost_micros",
}

_CRITERIA_RENAME = {
    "Date":           "date",
    "CampaignId":     "campaign_id",
    "CampaignName":   "campaign_name",
    "AdGroupName":    "ad_group_name",
    "Criterion":      "criterion",
    "CriterionType":  "criterion_type",
    "Impressions":    "impressions",
    "Clicks":         "clicks",
    "Cost":           "cost_micros",
}


def _parse_tsv(raw: str, rename_map: dict) -> pd.DataFrame:
    """Парсит TSV-ответ API, переименовывает колонки и вычисляет cost в рублях."""
    df = pd.read_csv(io.StringIO(raw), sep='\t', dtype=str)
    df = df.rename(columns=rename_map)

    df['campaign_id'] = pd.to_numeric(df['campaign_id'], errors='coerce').astype('Int64')
    df['impressions'] = pd.to_numeric(df['impressions'], errors='coerce').fillna(0).astype('Int32')
    df['clicks']      = pd.to_numeric(df['clicks'],      errors='coerce').fillna(0).astype('Int32')
    df['cost_micros'] = pd.to_numeric(df['cost_micros'], errors='coerce').fillna(0).astype('Int64')
    df['cost']        = (df['cost_micros'] / 1_000_000).round(6)
    df['date']        = pd.to_datetime(df['date'], errors='coerce').dt.date

    return df


def _ensure_campaign_id_column(table_name: str):
    """Миграция: добавляет campaign_id BIGINT в существующие таблицы при первом запуске
    после релиза. Идемпотентна.

    `ALTER TABLE IF EXISTS` — таблицы нет → no-op без ошибки. `ADD COLUMN IF NOT EXISTS` —
    колонка уже есть → no-op. Не используем inspect(engine).get_table_names(), потому что
    он по умолчанию ищет в схеме `public`, а таблицы лежат в `analytics`.
    """
    with engine.begin() as conn:
        conn.execute(text(
            f'ALTER TABLE IF EXISTS {table_name} ADD COLUMN IF NOT EXISTS campaign_id BIGINT'
        ))


def _deduplicate_direct_costs():
    """Удаляет дубли по (date, ad_id), оставляя физически первую строку (MIN ctid)."""
    with engine.begin() as conn:
        result = conn.execute(text('''
            DELETE FROM direct_costs t
            USING (
                SELECT "date", ad_id, MIN(ctid) AS keep_ctid
                FROM direct_costs
                GROUP BY "date", ad_id
                HAVING COUNT(*) > 1
            ) dups
            WHERE t."date" = dups."date"
              AND t.ad_id = dups.ad_id
              AND t.ctid <> dups.keep_ctid
        '''))
        if result.rowcount:
            log.warning("Дедупликация direct_costs: удалено %d дублей.", result.rowcount)
        else:
            log.info("Дедупликация direct_costs: дублей не найдено.")


# ---------------------------------------------------------------------------
# Основная функция
# ---------------------------------------------------------------------------

def sync_direct_costs():
    """Инкрементально докачивает расходы из Яндекс.Директ в таблицу direct_costs.

    Логика аналогична download_logs из loader_metrika.py:
      - берёт последнюю дату из таблицы (или 2025-01-01 при отсутствии);
      - удаляет строки за start_date (последний день мог быть неполным);
      - скачивает данные с start_date по вчера;
      - добавляет их в таблицу (append или replace при первом создании);
      - создаёт уникальный индекс и выполняет дедупликацию.
    """
    # Миграция campaign_id: должна выполняться ДО early-return, иначе при
    # «данные актуальны» колонки не будет, а update_direct_campaigns её ждёт.
    _ensure_campaign_id_column('direct_costs')

    start_date = _get_last_direct_date()
    yesterday  = (datetime.now(_MSK) - timedelta(days=1)).strftime('%Y-%m-%d')

    if start_date >= yesterday:
        log.info("Данные Директа уже актуальны (последняя дата: %s)", start_date)
        return

    log.info("Директ: загрузка расходов с %s по %s", start_date, yesterday)

    try:
        raw = _request_report(start_date, yesterday,
                              DIRECT_FIELDS, "AD_PERFORMANCE_REPORT", "costs_ad")
    except Exception as e:
        log.warning("Директ: не удалось получить отчёт: %s", e)
        return

    try:
        df = _parse_tsv(raw, _AD_RENAME)
    except Exception as e:
        log.warning("Директ: не удалось разобрать TSV-ответ: %s", e)
        return

    if df.empty:
        log.info("Директ: отчёт пустой (нет данных за период %s – %s)", start_date, yesterday)
        return

    inspector    = inspect(engine)
    table_exists = 'direct_costs' in inspector.get_table_names(schema='analytics')

    # Удаляем строки за start_date — они могли быть неполными
    if table_exists:
        with engine.begin() as conn:
            conn.execute(
                text('DELETE FROM direct_costs WHERE "date" = :d'),
                {"d": start_date},
            )
        log.info("Директ: удалены строки за %s (перезапись последнего дня)", start_date)

    # Загружаем в таблицу
    if_exists = 'append' if table_exists else 'replace'
    df.to_sql('direct_costs', con=engine, if_exists=if_exists, index=False,
              dtype=_DIRECT_SQLA_DTYPE if not table_exists else None)
    log.info("Директ: загружено %d строк (если_существует=%s)", len(df), if_exists)

    # Гарантируем уникальный индекс
    with engine.begin() as conn:
        conn.execute(text('''
            CREATE UNIQUE INDEX IF NOT EXISTS uq_direct_costs
            ON direct_costs ("date", ad_id)
        '''))

    # Дедупликация на случай повторных загрузок
    _deduplicate_direct_costs()

    log.info("Директ: синхронизация завершена (%d строк загружено).", len(df))


# ---------------------------------------------------------------------------
# Keyword-level (CRITERIA_PERFORMANCE_REPORT) → direct_criteria_costs
# ---------------------------------------------------------------------------

def _get_last_direct_criteria_date() -> str:
    try:
        inspector = inspect(engine)
        if 'direct_criteria_costs' not in inspector.get_table_names(schema='analytics'):
            return DIRECT_START_DATE
        with engine.connect() as conn:
            result = conn.execute(text('SELECT MAX("date") FROM direct_criteria_costs')).scalar()
            if result:
                return str(result)
    except Exception as e:
        log.warning("Не удалось прочитать последнюю дату из direct_criteria_costs: %s — скачиваем с %s",
                    e, DIRECT_START_DATE)
    return DIRECT_START_DATE


def _deduplicate_direct_criteria_costs():
    """Удаляет дубли по (date, campaign_name, ad_group_name, criterion), оставляя физически первую строку (MIN ctid)."""
    with engine.begin() as conn:
        result = conn.execute(text('''
            DELETE FROM direct_criteria_costs t
            USING (
                SELECT "date", campaign_name, ad_group_name, criterion, MIN(ctid) AS keep_ctid
                FROM direct_criteria_costs
                GROUP BY "date", campaign_name, ad_group_name, criterion
                HAVING COUNT(*) > 1
            ) dups
            WHERE t."date"          = dups."date"
              AND t.campaign_name   = dups.campaign_name
              AND t.ad_group_name   = dups.ad_group_name
              AND t.criterion       = dups.criterion
              AND t.ctid           <> dups.keep_ctid
        '''))
        if result.rowcount:
            log.warning("Дедупликация direct_criteria_costs: удалено %d дублей.", result.rowcount)
        else:
            log.info("Дедупликация direct_criteria_costs: дублей не найдено.")


def sync_direct_criteria_costs():
    """Инкрементально докачивает расходы по ключевым словам в direct_criteria_costs."""
    # Миграция campaign_id до early-return (см. комментарий в sync_direct_costs).
    _ensure_campaign_id_column('direct_criteria_costs')

    start_date = _get_last_direct_criteria_date()
    yesterday  = (datetime.now(_MSK) - timedelta(days=1)).strftime('%Y-%m-%d')

    if start_date >= yesterday:
        log.info("Данные Директа (criteria) уже актуальны (последняя дата: %s)", start_date)
        return

    log.info("Директ criteria: загрузка с %s по %s", start_date, yesterday)

    try:
        raw = _request_report(start_date, yesterday,
                              DIRECT_CRITERIA_FIELDS, "CRITERIA_PERFORMANCE_REPORT", "costs_crit")
    except Exception as e:
        log.warning("Директ criteria: не удалось получить отчёт: %s", e)
        return

    try:
        df = _parse_tsv(raw, _CRITERIA_RENAME)
    except Exception as e:
        log.warning("Директ criteria: не удалось разобрать TSV-ответ: %s", e)
        return

    if df.empty:
        log.info("Директ criteria: отчёт пустой за период %s – %s", start_date, yesterday)
        return

    inspector    = inspect(engine)
    table_exists = 'direct_criteria_costs' in inspector.get_table_names(schema='analytics')

    if table_exists:
        with engine.begin() as conn:
            conn.execute(
                text('DELETE FROM direct_criteria_costs WHERE "date" = :d'),
                {"d": start_date},
            )
        log.info("Директ criteria: удалены строки за %s", start_date)

    if_exists = 'append' if table_exists else 'replace'
    df.to_sql('direct_criteria_costs', con=engine, if_exists=if_exists, index=False,
              dtype=_DIRECT_CRITERIA_SQLA_DTYPE if not table_exists else None)
    log.info("Директ criteria: загружено %d строк", len(df))

    with engine.begin() as conn:
        conn.execute(text('''
            CREATE UNIQUE INDEX IF NOT EXISTS uq_direct_criteria_costs
            ON direct_criteria_costs ("date", campaign_name, ad_group_name, criterion)
        '''))

    _deduplicate_direct_criteria_costs()

    log.info("Директ criteria: синхронизация завершена (%d строк).", len(df))


# ---------------------------------------------------------------------------
# Справочник direct_campaigns: campaign_id → последнее актуальное название
# ---------------------------------------------------------------------------
# Зачем: при переименовании РК старые даты в direct_costs/direct_criteria_costs
# и в Метрике (DirectClickOrderName) хранят старое название, новые — новое.
# В дашборде это выглядит как две отдельные кампании. Справочник джойнится в
# visits_calculated по числовому campaign_id и подставляет последнее название.
# Источник истины — direct_costs ∪ direct_criteria_costs: API Директа всегда
# возвращает текущее название кампании, поэтому MAX(date) даёт актуальное имя.
def update_direct_campaigns():
    """Создаёт/обновляет справочник direct_campaigns (campaign_id → последнее название).

    Идемпотентна: каждый ETL-прогон обновляет имя у campaign_id, если оно изменилось.
    """
    with engine.begin() as conn:
        conn.execute(text('''
            CREATE TABLE IF NOT EXISTS direct_campaigns (
                campaign_id   BIGINT PRIMARY KEY,
                campaign_name TEXT,
                updated_at    TIMESTAMP DEFAULT NOW()
            )
        '''))
        # UNION ALL по двум таблицам, выбираем последнее имя по MAX(date) на campaign_id.
        # WHERE кампании NOT NULL — отсекаем строки без ID (старые данные до миграции
        # либо строки с пустым CampaignId в API-ответе).
        result = conn.execute(text('''
            INSERT INTO direct_campaigns (campaign_id, campaign_name, updated_at)
            SELECT DISTINCT ON (campaign_id)
                   campaign_id, campaign_name, NOW()
            FROM (
                SELECT campaign_id, campaign_name, "date"
                FROM direct_costs WHERE campaign_id IS NOT NULL
                UNION ALL
                SELECT campaign_id, campaign_name, "date"
                FROM direct_criteria_costs WHERE campaign_id IS NOT NULL
            ) src
            ORDER BY campaign_id, "date" DESC
            ON CONFLICT (campaign_id) DO UPDATE
               SET campaign_name = EXCLUDED.campaign_name,
                   updated_at    = EXCLUDED.updated_at
               WHERE direct_campaigns.campaign_name IS DISTINCT FROM EXCLUDED.campaign_name
        '''))
        log.info("direct_campaigns: справочник обновлён (затронуто %d записей).", result.rowcount or 0)
