"""
Выгрузка контента объявлений из Яндекс.Директ Ads API v5.

Таблица direct_ads — версионная (SCD Type 2):
- (ad_id, valid_from) — составной первичный ключ
- valid_to = NULL — текущая активная версия
- При изменении контента (title, title2, body, href, display_url_path, ad_type)
  закрываем старую версию (valid_to = сегодня) и вставляем новую.
- Изменения state/status НЕ создают новую версию — обновляются в текущей строке.

Поддерживаемые типы: TEXT_AD, DYNAMIC_TEXT_AD.
Прочие типы (IMAGE_AD, SMART_AD…) хранятся без текстовых полей.
"""
import time
from datetime import datetime
from typing import Any

import pandas as pd
import requests
from sqlalchemy import BigInteger, Date, Text, text
from sqlalchemy import inspect as sa_inspect

from loader_config  import log, engine, TOKEN
from loader_direct  import DIRECT_START_DATE as _BACKFILL_DATE  # noqa: E402

# ---------------------------------------------------------------------------
# Константы
# ---------------------------------------------------------------------------
_ADS_API_URL       = "https://api.direct.yandex.com/json/v5/ads"
_CAMPAIGNS_API_URL = "https://api.direct.yandex.com/json/v5/campaigns"
_ADIMAGES_API_URL  = "https://api.direct.yandex.com/json/v5/adimages"
_PAGE_LIMIT        = 10_000  # максимум по документации Директа

_ADS_HEADERS = {
    "Authorization":   f"Bearer {TOKEN}",
    "Accept-Language": "ru",
    "Content-Type":    "application/json",
    "Client-Login":    os.getenv('DIRECT_CLIENT_LOGIN', ''),
}

_STAGE_TABLE = "direct_ads_stage"

# Типы для staging-таблицы (без valid_from/valid_to — они проставляются в SQL)
_STAGE_DTYPE = {
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

# Поля, изменение которых порождает новую версию
_VERSIONED_FIELDS = ("title", "title2", "body", "href", "display_url_path", "ad_type", "image_hash")

# Типы для таблицы direct_ad_images
_IMG_DTYPE = {
    "image_hash":   Text(),
    "name":         Text(),
    "type":         Text(),
    "subtype":      Text(),
    "original_url": Text(),
    "preview_url":  Text(),
}

# _BACKFILL_DATE импортирован выше как DIRECT_START_DATE из loader_direct.py —
# единственный источник истины для даты начала эры данных Директа в БД.


# ---------------------------------------------------------------------------
# Получение списка campaign_id
# ---------------------------------------------------------------------------

def _get_campaign_ids() -> list[int]:
    """Возвращает список campaign_id: из БД (direct_campaigns → direct_costs) или через API."""
    inspector = sa_inspect(engine)
    existing  = inspector.get_table_names(schema='analytics')

    for table_name in ("direct_campaigns", "direct_costs"):
        if table_name not in existing:
            continue
        col_names = {c["name"] for c in inspector.get_columns(table_name, schema='analytics')}
        if "campaign_id" not in col_names:
            log.info("Директ Ads API: %s не имеет колонки campaign_id, пропускаю.", table_name)
            continue
        with engine.connect() as conn:
            rows = conn.execute(
                text(f"SELECT DISTINCT campaign_id FROM {table_name} WHERE campaign_id IS NOT NULL")
            ).fetchall()
        ids = [int(r[0]) for r in rows if r[0] is not None]
        if ids:
            log.info("Директ Ads API: campaign_ids взяты из %s (%d кампаний).", table_name, len(ids))
            return ids

    log.info("Директ Ads API: local tables не найдены, запрашиваю кампании через API...")
    return _fetch_campaign_ids_from_api()


def _fetch_campaign_ids_from_api() -> list[int]:
    """Запрашивает список ID кампаний из Campaigns API Директа.

    States перечислены явно (включая ARCHIVED/ENDED) — иначе API по умолчанию
    отдаёт только активные кампании, и архивные креативы теряются на JOIN.
    """
    body = {
        "method": "get",
        "params": {
            "SelectionCriteria": {
                "States": ["ARCHIVED", "CONVERTED", "ENDED", "OFF", "ON", "SUSPENDED"],
            },
            "FieldNames": ["Id"],
            "Page": {"Limit": 10000},
        },
    }
    resp = requests.post(_CAMPAIGNS_API_URL, json=body, headers=_ADS_HEADERS, timeout=60)
    resp.raise_for_status()
    data = resp.json()

    error = data.get("error")
    if error:
        raise RuntimeError(
            f"Campaigns API вернул ошибку: code={error.get('error_code')} "
            f"detail={error.get('error_detail')}"
        )

    campaigns = data.get("result", {}).get("Campaigns", [])
    ids = [int(c["Id"]) for c in campaigns if c.get("Id")]
    log.info("Директ Ads API: получено %d кампаний из Campaigns API.", len(ids))
    return ids


# ---------------------------------------------------------------------------
# Запрос к Ads API
# ---------------------------------------------------------------------------

def _fetch_ads_page(campaign_ids: list[int], offset: int) -> dict[str, Any]:
    """Запрашивает одну страницу объявлений для указанных кампаний."""
    body = {
        "method": "get",
        "params": {
            "SelectionCriteria": {
                "CampaignIds": campaign_ids,
            },
            "FieldNames": ["Id", "CampaignId", "AdGroupId", "State", "Status", "Type"],
            "TextAdFieldNames": ["Title", "Title2", "Text", "Href", "DisplayUrlPath", "AdImageHash"],
            "DynamicTextAdFieldNames": ["Text"],
            "Page": {"Limit": _PAGE_LIMIT, "Offset": offset},
        },
    }
    resp = requests.post(_ADS_API_URL, json=body, headers=_ADS_HEADERS, timeout=60)
    resp.raise_for_status()
    return resp.json()


def _fetch_all_ads(campaign_ids: list[int]) -> list[dict]:
    """Выгружает все объявления по списку кампаний с пагинацией."""
    all_ads: list[dict] = []
    offset = 0

    while True:
        log.info("Директ Ads API: запрос offset=%d (%d кампаний)...", offset, len(campaign_ids))
        data = _fetch_ads_page(campaign_ids, offset)

        error = data.get("error")
        if error:
            raise RuntimeError(
                f"Ads API вернул ошибку: code={error.get('error_code')} "
                f"detail={error.get('error_detail')}"
            )

        ads = data.get("result", {}).get("Ads", [])
        all_ads.extend(ads)
        log.info("Директ Ads API: получено %d объявлений (всего: %d)", len(ads), len(all_ads))

        limited_by = data.get("result", {}).get("LimitedBy")
        if limited_by is None:
            break
        offset = limited_by
        time.sleep(0.3)

    return all_ads


# ---------------------------------------------------------------------------
# Нормализация в DataFrame
# ---------------------------------------------------------------------------

def _normalize(ads: list[dict]) -> pd.DataFrame:
    """Преобразует список сырых объявлений API в плоский DataFrame."""
    rows = []
    for ad in ads:
        ad_type = ad.get("Type", "")
        text_ad = ad.get("TextAd") or {}
        dyn_ad  = ad.get("DynamicTextAd") or {}

        image_hash = None
        if ad_type == "TEXT_AD":
            title            = text_ad.get("Title") or None
            title2           = text_ad.get("Title2") or None
            body             = text_ad.get("Text") or None
            href             = text_ad.get("Href") or None
            display_url_path = text_ad.get("DisplayUrlPath") or None
            image_hash       = text_ad.get("AdImageHash") or None
        elif ad_type == "DYNAMIC_TEXT_AD":
            title            = None
            title2           = None
            body             = dyn_ad.get("Text") or None
            href             = None  # URL генерируется автоматически
            display_url_path = None
        else:
            title = title2 = body = href = display_url_path = None

        rows.append({
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
            "display_url_path": display_url_path,
            "image_hash":       image_hash,
        })

    df = pd.DataFrame(rows)
    if df.empty:
        return df

    df["ad_id"]       = pd.to_numeric(df["ad_id"],       errors="coerce").astype("Int64")
    df["campaign_id"] = pd.to_numeric(df["campaign_id"], errors="coerce").astype("Int64")
    df["ad_group_id"] = pd.to_numeric(df["ad_group_id"], errors="coerce").astype("Int64")
    return df


# ---------------------------------------------------------------------------
# Создание целевой таблицы и SCD-применение
# ---------------------------------------------------------------------------

def _ensure_direct_ads_table():
    """Создаёт версионную таблицу direct_ads, если её ещё нет.
    + миграция: добавляет колонку image_hash для уже существующих установок."""
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS direct_ads (
                ad_id            BIGINT NOT NULL,
                campaign_id      BIGINT,
                ad_group_id      BIGINT,
                state            TEXT,
                status           TEXT,
                ad_type          TEXT,
                title            TEXT,
                title2           TEXT,
                body             TEXT,
                href             TEXT,
                display_url_path TEXT,
                image_hash       TEXT,
                valid_from       DATE NOT NULL,
                valid_to         DATE,
                PRIMARY KEY (ad_id, valid_from)
            )
        """))
        # Миграция для existing installations
        conn.execute(text("ALTER TABLE direct_ads ADD COLUMN IF NOT EXISTS image_hash TEXT"))
        # Покрывающий индекс для быстрого JOIN из visits_calculated
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS ix_direct_ads_lookup
            ON direct_ads (ad_id, valid_from, valid_to)
        """))


def _verify_ads_exist_in_api(ad_ids: list[int]) -> set[int]:
    """Запрашивает Ads API по конкретным ad_id (SelectionCriteria.Ids) и возвращает
    те из них, которые API вернул (то есть ещё существуют в системе Директа).

    Ad_id, отсутствующие в ответе, — действительно удалены из кабинета, а не
    пропущены из-за ошибки основного фетча по кампаниям. Это критически важно:
    staging строится по списку campaign_id, и если кампания появилась позже
    момента фетча или её id не попал в список, её объявления будут выглядеть
    «удалёнными», хотя на самом деле живы.

    Батчи по 1 000 (документированный лимит SelectionCriteria.Ids — 10 000,
    оставляем запас). Пауза 0.2 с между батчами.
    """
    if not ad_ids:
        return set()
    existing: set[int] = set()
    batch_size = 1_000
    for i in range(0, len(ad_ids), batch_size):
        batch = ad_ids[i: i + batch_size]
        body = {
            "method": "get",
            "params": {
                "SelectionCriteria": {"Ids": batch},
                "FieldNames": ["Id"],
                "Page": {"Limit": _PAGE_LIMIT},
            },
        }
        resp = requests.post(_ADS_API_URL, json=body, headers=_ADS_HEADERS, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        error = data.get("error")
        if error:
            raise RuntimeError(
                f"Ads API (verify): code={error.get('error_code')} "
                f"detail={error.get('error_detail')}"
            )
        for ad in data.get("result", {}).get("Ads", []):
            if ad.get("Id"):
                existing.add(int(ad["Id"]))
        time.sleep(0.2)
    return existing


def _apply_scd2(today: str):
    """Применяет SCD Type 2 поверх staging-таблицы.

    Вызывается строго после загрузки актуального снимка в `_STAGE_TABLE`.

    Реализована в двух транзакциях, чтобы не держать длинную блокировку
    во время HTTP-запроса к API:
      • Транзакция 1: версионирование контента (шаги 1–3) + поиск кандидатов.
      • HTTP: верификация кандидатов через Ads API (SelectionCriteria.Ids).
      • Транзакция 2: закрытие только подтверждённых API удалённых ad_id.
    """
    content_diff = " OR ".join(
        f"d.{f} IS DISTINCT FROM s.{f}" for f in _VERSIONED_FIELDS
    )

    # ── Транзакция 1: изменения контента/state + поиск кандидатов на closure ──
    with engine.begin() as conn:
        # 1. Закрываем версии, у которых изменился КОНТЕНТ
        result_close = conn.execute(text(f"""
            UPDATE direct_ads d
            SET valid_to = :today
            FROM {_STAGE_TABLE} s
            WHERE d.ad_id    = s.ad_id
              AND d.valid_to IS NULL
              AND ({content_diff})
        """), {"today": today})
        n_closed = result_close.rowcount or 0

        # 2. Вставляем новые версии (для новых ad_id и для тех, чей контент изменился).
        # NOT EXISTS отсекает строки, где открытая версия осталась актуальной.
        #
        # valid_from:
        #   - если в direct_ads уже есть какие-то записи по этому ad_id — это
        #     смена версии после редактирования, valid_from = сегодня;
        #   - если ad_id встречается впервые — backfill, valid_from = _BACKFILL_DATE,
        #     чтобы все исторические визиты тоже получили этот контент через JOIN.
        result_insert = conn.execute(text(f"""
            INSERT INTO direct_ads (
                ad_id, campaign_id, ad_group_id, state, status, ad_type,
                title, title2, body, href, display_url_path, image_hash,
                valid_from, valid_to
            )
            SELECT s.ad_id, s.campaign_id, s.ad_group_id, s.state, s.status, s.ad_type,
                   s.title, s.title2, s.body, s.href, s.display_url_path, s.image_hash,
                   CASE WHEN EXISTS (
                            SELECT 1 FROM direct_ads d2 WHERE d2.ad_id = s.ad_id
                        )
                        THEN CAST(:today    AS date)
                        ELSE CAST(:backfill AS date)
                   END,
                   NULL
            FROM {_STAGE_TABLE} s
            WHERE NOT EXISTS (
                SELECT 1 FROM direct_ads d
                WHERE d.ad_id    = s.ad_id
                  AND d.valid_to IS NULL
            )
            ON CONFLICT (ad_id, valid_from) DO UPDATE
              SET campaign_id      = EXCLUDED.campaign_id,
                  ad_group_id      = EXCLUDED.ad_group_id,
                  state            = EXCLUDED.state,
                  status           = EXCLUDED.status,
                  ad_type          = EXCLUDED.ad_type,
                  title            = EXCLUDED.title,
                  title2           = EXCLUDED.title2,
                  body             = EXCLUDED.body,
                  href             = EXCLUDED.href,
                  display_url_path = EXCLUDED.display_url_path,
                  image_hash       = EXCLUDED.image_hash
        """), {"today": today, "backfill": _BACKFILL_DATE})
        n_inserted = result_insert.rowcount or 0

        # 3. Для строк, где контент НЕ изменился — обновляем state/status в текущей версии.
        # Версионировать паузы/возобновления — это шум; достаточно обновить на месте.
        result_state = conn.execute(text(f"""
            UPDATE direct_ads d
            SET state       = s.state,
                status      = s.status,
                campaign_id = COALESCE(s.campaign_id, d.campaign_id),
                ad_group_id = COALESCE(s.ad_group_id, d.ad_group_id)
            FROM {_STAGE_TABLE} s
            WHERE d.ad_id    = s.ad_id
              AND d.valid_to IS NULL
              AND (
                  d.state  IS DISTINCT FROM s.state  OR
                  d.status IS DISTINCT FROM s.status
              )
        """))
        n_state = result_state.rowcount or 0

        # 4a. Санити-чек + сбор кандидатов на closure.
        # Если staging покрывает <80% активных версий — фетч явно оборвался,
        # и даже запрашивать API не имеет смысла: возможно сломан целый пласт.
        n_active = conn.execute(text(
            "SELECT COUNT(*) FROM direct_ads WHERE valid_to IS NULL"
        )).scalar() or 0
        n_stage = conn.execute(text(
            f"SELECT COUNT(DISTINCT ad_id) FROM {_STAGE_TABLE}"
        )).scalar() or 0

        if n_active == 0 or n_stage >= 0.8 * n_active:
            candidates_rows = conn.execute(text(f"""
                SELECT d.ad_id FROM direct_ads d
                WHERE d.valid_to IS NULL
                  AND NOT EXISTS (SELECT 1 FROM {_STAGE_TABLE} s WHERE s.ad_id = d.ad_id)
            """)).fetchall()
        else:
            candidates_rows = None  # сигнал: пропустить closure целиком

    # ── API-верификация (вне транзакции, чтобы не держать блокировку) ──────────
    n_deleted = 0

    if candidates_rows is None:
        log.warning(
            "SCD2 closure ПРОПУЩЕН: в staging %d уникальных ad_id, в БД %d активных — "
            "похоже на оборванный фетч. Closure отложен до следующего запуска.",
            n_stage, n_active,
        )
    elif not candidates_rows:
        log.info("SCD2 closure: кандидатов на удаление нет.")
    else:
        candidate_ids = [r[0] for r in candidates_rows]
        log.info(
            "SCD2 closure: %d кандидат(ов) отсутствуют в staging — "
            "верифицируем через Ads API (SelectionCriteria.Ids)...",
            len(candidate_ids),
        )
        try:
            still_existing = _verify_ads_exist_in_api(candidate_ids)
        except Exception as e:
            # При ошибке API не рискуем: считаем всех кандидатов живыми.
            log.warning(
                "SCD2 verify: API-верификация не удалась (%s) — "
                "closure пропущен для безопасности.",
                e,
            )
            still_existing = set(candidate_ids)

        truly_deleted = [aid for aid in candidate_ids if aid not in still_existing]

        if still_existing:
            log.warning(
                "SCD2 verify: %d ad_id присутствуют в API, но отсутствуют в staging — "
                "closure пропущен (возможно, кампании появились после момента фетча): %s",
                len(still_existing),
                ', '.join(str(x) for x in sorted(still_existing)),
            )

        if truly_deleted:
            log.info(
                "SCD2 closure: %d ad_id подтверждены API как удалённые из кабинета: %s",
                len(truly_deleted),
                ', '.join(str(x) for x in sorted(truly_deleted)),
            )
            # ── Транзакция 2: закрываем только подтверждённые ──────────────────
            with engine.begin() as conn:
                result_deleted = conn.execute(text("""
                    UPDATE direct_ads d
                    SET valid_to = :today
                    WHERE d.valid_to IS NULL
                      AND d.ad_id = ANY(:ids)
                """), {"today": today, "ids": truly_deleted})
                n_deleted = result_deleted.rowcount or 0
        else:
            log.info(
                "SCD2 verify: все %d кандидат(ов) ещё существуют в API — "
                "closure не выполняется.",
                len(candidate_ids),
            )

    log.info(
        "direct_ads SCD2: закрыто %d версий по контенту, вставлено %d новых, "
        "обновлено state/status у %d строк, закрыто %d удалённых из API.",
        n_closed, n_inserted, n_state, n_deleted,
    )


# ---------------------------------------------------------------------------
# Картинки объявлений: /json/v5/adimages
# ---------------------------------------------------------------------------

def _fetch_all_images() -> list[dict]:
    """Выгружает все картинки рекламного аккаунта через AdImages API. Пагинация."""
    all_imgs: list[dict] = []
    offset = 0
    while True:
        body = {
            "method": "get",
            "params": {
                "SelectionCriteria": {},
                "FieldNames": ["AdImageHash", "OriginalUrl", "PreviewUrl", "Name", "Type", "Subtype"],
                "Page": {"Limit": _PAGE_LIMIT, "Offset": offset},
            },
        }
        resp = requests.post(_ADIMAGES_API_URL, json=body, headers=_ADS_HEADERS, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        if "error" in data:
            raise RuntimeError(
                f"AdImages API: {data['error'].get('error_code')} {data['error'].get('error_detail')}"
            )
        imgs = data.get("result", {}).get("AdImages", [])
        all_imgs.extend(imgs)
        log.info("Директ AdImages: получено %d картинок (всего: %d)", len(imgs), len(all_imgs))
        limited_by = data.get("result", {}).get("LimitedBy")
        if limited_by is None:
            break
        offset = limited_by
        time.sleep(0.3)
    return all_imgs


def sync_direct_ad_images():
    """UPSERT в direct_ad_images без удаления старых строк.

    Стратегия — append-only: однажды виденная картинка остаётся в таблице
    навсегда, даже если Яндекс удалит её из медиатеки аккаунта. Это нужно,
    чтобы preview_url для старых объявлений в visits_calculated не «протухал»
    при удалении картинок из кабинета. Хэш генерируется от содержимого, поэтому
    коллизий между разными картинками нет; при изменении URL у того же хэша
    (редко, при миграции CDN) обновляем строку.
    """
    try:
        raw = _fetch_all_images()
    except Exception as e:
        log.warning("Директ AdImages: ошибка при выгрузке: %s", e)
        return

    if not raw:
        log.info("Директ AdImages: список пуст.")
        return

    rows = [{
        "image_hash":   img.get("AdImageHash"),
        "name":         img.get("Name"),
        "type":         img.get("Type"),
        "subtype":      img.get("Subtype"),
        "original_url": img.get("OriginalUrl"),
        "preview_url":  img.get("PreviewUrl"),
    } for img in raw]
    df = pd.DataFrame(rows)
    df = df[df["image_hash"].notna()]
    if df.empty:
        log.info("Директ AdImages: все строки без image_hash, пропускаю.")
        return

    # Гарантируем существование целевой таблицы и уникального индекса по image_hash
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS direct_ad_images (
                image_hash   TEXT PRIMARY KEY,
                name         TEXT,
                type         TEXT,
                subtype      TEXT,
                original_url TEXT,
                preview_url  TEXT
            )
        """))

    # Грузим свежий снимок в staging, оттуда — UPSERT в целевую таблицу.
    stage = "direct_ad_images_stage"
    with engine.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {stage}"))
    df.to_sql(stage, con=engine, if_exists="replace", index=False, dtype=_IMG_DTYPE)
    with engine.begin() as conn:
        result = conn.execute(text(f"""
            INSERT INTO direct_ad_images (image_hash, name, type, subtype, original_url, preview_url)
            SELECT image_hash, name, type, subtype, original_url, preview_url
            FROM {stage}
            ON CONFLICT (image_hash) DO UPDATE
              SET name         = EXCLUDED.name,
                  type         = EXCLUDED.type,
                  subtype      = EXCLUDED.subtype,
                  original_url = EXCLUDED.original_url,
                  preview_url  = EXCLUDED.preview_url
        """))
        n_upserted = result.rowcount or 0
        conn.execute(text(f"DROP TABLE IF EXISTS {stage}"))
        total = conn.execute(text("SELECT COUNT(*) FROM direct_ad_images")).scalar()

    log.info(
        "Директ AdImages: UPSERT %d картинок (всего в таблице: %d, удалённые из кабинета сохранены).",
        n_upserted, total,
    )


# ---------------------------------------------------------------------------
# Основная функция
# ---------------------------------------------------------------------------

def sync_direct_ads():
    """Синхронизирует direct_ads с актуальным снимком объявлений из Директа (SCD Type 2)."""
    campaign_ids = _get_campaign_ids()
    if not campaign_ids:
        log.warning("Директ Ads API: нет кампаний для запроса — выгрузка пропущена.")
        return

    try:
        raw_ads = _fetch_all_ads(campaign_ids)
    except Exception as e:
        log.warning("Директ Ads API: ошибка при выгрузке: %s", e)
        return

    if not raw_ads:
        log.info("Директ Ads API: список объявлений пуст.")
        return

    df = _normalize(raw_ads)
    if df.empty:
        log.info("Директ Ads API: после нормализации DataFrame пустой.")
        return

    today = datetime.now().strftime("%Y-%m-%d")

    # Гарантируем существование целевой таблицы
    _ensure_direct_ads_table()

    # Сбрасываем staging и загружаем туда полный снимок
    with engine.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {_STAGE_TABLE}"))

    df.to_sql(
        _STAGE_TABLE,
        con=engine,
        if_exists="replace",
        index=False,
        dtype=_STAGE_DTYPE,
    )

    try:
        _apply_scd2(today)
    finally:
        with engine.begin() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS {_STAGE_TABLE}"))

    # Сводная статистика
    with engine.connect() as conn:
        total      = conn.execute(text("SELECT COUNT(*) FROM direct_ads")).scalar()
        active     = conn.execute(text("SELECT COUNT(*) FROM direct_ads WHERE valid_to IS NULL")).scalar()
        historical = total - active

    log.info(
        "Директ Ads API: снапшот применён (всего строк: %d, активных версий: %d, исторических: %d).",
        total, active, historical,
    )
