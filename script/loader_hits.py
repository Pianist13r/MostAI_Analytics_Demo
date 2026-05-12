"""
SQL-генерация и обновление hits_calculated и hit_conversions —
датасетов уровня просмотра страницы для дашборда «URL/Страницы».

Архитектура (ровно как в loader_visits.py):
  • hits_calculated   — каждый просмотр страницы (хит), обогащённый атрибуцией
                        родительского визита для сквозных фильтров Superset.
  • hit_conversions   — last-page-click атрибуция: один хит на одно событие
                        (платёж / регистрация). Аналог last-click атрибуции
                        в visits_calculated, но единица — хит, а не визит.

Naming convention для совместимости фильтров Superset с visits_calculated:
  • Имена атрибуции — те же: last_source, last_campaign, last_content, last_phrase,
    last_significant_*, first_*. Один Native Filter «Источник/Кампания/...»
    через chartsInScope таргетит и старые, и новые чарты.
  • Время:
      hit_dt   — TIMESTAMP (точное время хита, для оконных функций и time-grain час/минута)
      hit_date — DATE      (для группировок и JOIN с direct_costs.date)
      dt       — DATE      (alias на hit_date) — для совместимости с native filter «Даты»,
                            у которого target = temporal column. В visits_calculated `dt`
                            тоже DATE — оставляем единое имя на всех датасетах.

URL-нормализация (без справочника, чистая регулярка):
  url      — как пришёл из Метрики
  url_path — lowercase + срез host + срез ?query / #fragment + удаление trailing slash
             (пустой → '/'). Главное поле для группировки в чартах «топ-страниц».
  url_host — хост для отделения поддоменов (app.example.com vs example.com).
  url_template — задел на будущее (склейка /blog/123-... → /blog/:slug), пока NULL.

Атомарная подмена через `_new`-таблицы — копия паттерна из loader_visits.py,
чтобы hits_calculated/hit_conversions оставались доступны во время пересчёта.
"""
from sqlalchemy import text

from loader_config import log, engine, REPORT_START_DATE
from loader_visits import (
    _TODAY_MSK_TS, _TODAY_MSK_DATE,
    _build_attribution_base_ctes, _build_failed_filtered_cte,
)


# Список доменов, считающихся «внутренними» — для referer_internal.
# Синхронизирован с _source_case() в loader_visits.py.
_INTERNAL_DOMAINS = tuple(
    d.strip()
    for d in os.getenv('SITE_INTERNAL_DOMAINS', 'example.com,app.example.com').split(',')
    if d.strip()
)


# ---------------------------------------------------------------------------
# Нормализация url_path (автоматическая, без справочника)
# ---------------------------------------------------------------------------
# 1. Срезаем https://host
# 2. Срезаем ?query и #hash
# 3. Удаляем trailing slash (RTRIM('/'))
# 4. lowercase
# 5. Если результат пуст → '/' (главная)
# Регулярки POSIX, проверены на PostgreSQL 15.
_URL_PATH_EXPR = r"""
    NULLIF(
        LOWER(
            RTRIM(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(mh."ym:pv:URL", '^https?://[^/]+', ''),
                    '[?#].*$', ''
                ),
                '/'
            )
        ),
        ''
    )
"""

_URL_HOST_EXPR = r"""
    LOWER(NULLIF(
        REGEXP_REPLACE(mh."ym:pv:URL", '^https?://([^/]+).*$', '\1'),
        ''
    ))
"""

# referer_internal: 1 если реферер на нашем домене, иначе 0.
# NULL-referer считается 0 (внешний/прямой).
_REFERER_INTERNAL_EXPR = " OR ".join(
    f"mh.\"ym:pv:referer\" LIKE '%{d}%'" for d in _INTERNAL_DOMAINS
)


def _get_hits_calculated_sql(suffix: str = "") -> str:
    """SQL для пересоздания hits_calculated{suffix}.

    INNER JOIN visits_calculated по visitID — хиты-сироты (без визита-родителя
    в visits_calculated, например с дат < 2025-09-01) отбрасываются. Это
    согласуется с фильтром визитов и не теряет полезные данные.
    """
    header = f"CREATE TABLE hits_calculated{suffix} AS"

    # Источник видим в visits_calculated через triplet last/first/last_significant.
    # is_significant хита = значимость родительского визита (visit.last_source
    # не является «Прямой переход» / «Внутренний переход»).
    is_sig_expr = """
        CASE WHEN vc.last_source NOT IN ('Прямой переход', 'Внутренний переход')
             THEN 1 ELSE 0 END
    """

    return f"""
        {header}
        SELECT
            -- Идентификаторы ─────────────────────────────────────────────────
            mh."ym:pv:watchID"           AS watch_id,
            mh."ym:pv:visitID"           AS visit_id,
            mh."ym:pv:clientID"          AS client_id,
            mh."ym:pv:counterUserIDHash" AS counter_user_id_hash,
            vc.unified_user_id           AS unified_user_id,
            vc.unified_user_id_source    AS unified_user_id_source,

            -- Время ──────────────────────────────────────────────────────────
            mh."ym:pv:dateTime"          AS hit_dt,            -- TIMESTAMP
            mh."ym:pv:dateTime"::date    AS hit_date,          -- DATE
            mh."ym:pv:dateTime"::date    AS dt,                -- DATE alias (как в visits_calculated)

            -- URL: оригинал, нормализованный path, хост, заглушка на template ─
            mh."ym:pv:URL"               AS url,
            COALESCE({_URL_PATH_EXPR}, '/')  AS url_path,
            {_URL_HOST_EXPR}             AS url_host,
            NULL::text                   AS url_template,

            -- Контент / referer / поведение ─────────────────────────────────
            mh."ym:pv:title"             AS page_title,
            mh."ym:pv:referer"           AS referer,
            CASE WHEN mh."ym:pv:referer" IS NOT NULL
                  AND ({_REFERER_INTERNAL_EXPR})
                 THEN 1 ELSE 0 END        AS referer_internal,
            COALESCE(mh."ym:pv:isPageView", 0) AS is_pageview,
            COALESCE(mh."ym:pv:notBounce", 0)  AS not_bounce,
            mh."ym:pv:goalsID"           AS goals_raw,
            -- Массив целей: parsed из CSV-строки. Пустая строка → NULL → пустой массив.
            CASE WHEN COALESCE(mh."ym:pv:goalsID", '') = '' THEN ARRAY[]::text[]
                 ELSE string_to_array(mh."ym:pv:goalsID", ',') END  AS goals,
            CASE WHEN COALESCE(mh."ym:pv:goalsID", '') = '' THEN 0 ELSE 1 END
                                         AS has_goal,

            -- Порядок внутри визита (оконные функции по visitID) ────────────
            ROW_NUMBER() OVER (
                PARTITION BY mh."ym:pv:visitID"
                ORDER BY mh."ym:pv:dateTime", mh."ym:pv:watchID"
            )                            AS hit_seq,
            CASE WHEN ROW_NUMBER() OVER (
                PARTITION BY mh."ym:pv:visitID"
                ORDER BY mh."ym:pv:dateTime", mh."ym:pv:watchID"
            ) = 1 THEN 1 ELSE 0 END      AS is_landing,
            CASE WHEN ROW_NUMBER() OVER (
                PARTITION BY mh."ym:pv:visitID"
                ORDER BY mh."ym:pv:dateTime" DESC, mh."ym:pv:watchID" DESC
            ) = 1 THEN 1 ELSE 0 END      AS is_exit,
            -- Время на странице = разница до следующего хита визита.
            -- Для последнего хита (LEAD = NULL) → NULL: длительность не определима.
            -- GREATEST(0, ...) защищает от отрицательных значений при аномальном
            -- порядке хитов (clock skew), не трогая NULL последнего хита.
            GREATEST(0, EXTRACT(EPOCH FROM (
                LEAD(mh."ym:pv:dateTime") OVER (
                    PARTITION BY mh."ym:pv:visitID"
                    ORDER BY mh."ym:pv:dateTime", mh."ym:pv:watchID"
                ) - mh."ym:pv:dateTime"
            ))::int)                     AS seconds_on_page,
            LAG(mh."ym:pv:URL") OVER (
                PARTITION BY mh."ym:pv:visitID"
                ORDER BY mh."ym:pv:dateTime", mh."ym:pv:watchID"
            )                            AS prev_url,
            LEAD(mh."ym:pv:URL") OVER (
                PARTITION BY mh."ym:pv:visitID"
                ORDER BY mh."ym:pv:dateTime", mh."ym:pv:watchID"
            )                            AS next_url,

            -- Атрибуция родительского визита (копия для сквозных фильтров) ──
            -- ИМЕНА И СЕМАНТИКА — точная копия visits_calculated, чтобы один
            -- Native Filter работал на оба датасета через chartsInScope.
            vc.last_source,
            vc.first_source,
            vc.last_significant_source,
            vc.last_campaign,
            vc.first_campaign,
            vc.last_significant_campaign,
            vc.last_content,
            vc.first_content,
            vc.last_significant_content,
            vc.last_phrase,
            vc.first_phrase,
            vc.last_significant_phrase,
            vc.last_ad_id,
            vc.first_ad_id,
            vc.last_significant_ad_id,
            vc.last_ad_title,
            vc.first_ad_title,
            vc.last_significant_ad_title,
            vc.ad_cost_per_visit,

            -- Значимость хита (наследуется от родительского визита) ─────────
            {is_sig_expr}                AS is_significant,

            -- Тех-атрибуты родительского визита (для фильтров): прямые имена
            -- visits_calculated, чтобы Native Filter «Девайс/Браузер/...»
            -- работал и на хитах через одинаковое имя колонки.
            vc."ym:s:deviceCategory"     AS "ym:s:deviceCategory",
            vc."ym:s:browser"            AS "ym:s:browser",
            vc."ym:s:browserLanguage"    AS "ym:s:browserLanguage",
            vc."ym:s:regionCity"         AS "ym:s:regionCity",
            vc."ym:s:regionCountry"      AS "ym:s:regionCountry"

        FROM metrika_hits mh
        INNER JOIN visits_calculated vc
                ON vc."ym:s:visitID" = mh."ym:pv:visitID"
        WHERE mh."ym:pv:dateTime" >= '{REPORT_START_DATE}'::timestamp
          AND mh."ym:pv:dateTime" < {_TODAY_MSK_TS}
    """


def _get_hit_conversions_sql(suffix: str = "") -> str:
    """SQL для пересоздания hit_conversions{suffix}.

    Last-page-click: для каждого события (платёж/регистрация) выбирается
    самый поздний хит того же пользователя до момента события. Через
    DISTINCT ON (event_type, event_id) ORDER BY hit_dt DESC.

    Семантически: для платежа сумма ровно та же, что в visits_calculated
    last-click — `payments_approved_sum`. Просто разрез теперь URL, а не визит.

    Структура колонок намеренно повторяет hits_calculated там, где
    возможно — те же имена атрибуции и URL — чтобы один Native Filter
    управлял обоими датасетами.
    """
    header = f"CREATE TABLE hit_conversions{suffix} AS"

    return f"""
        {header}
        WITH
        {_build_failed_filtered_cte()},
        events AS (
            -- Approved платежи
            SELECT
                'payment_approved'::text AS event_type,
                p.payment_id              AS event_id,
                p.amount                  AS event_amount,
                p.created_at_moscow       AS event_dt,
                p.mongo_user_id           AS mongo_user_id
            FROM mongo_payments p
            WHERE p.status = 'approved'
              AND p.created_at_moscow < {_TODAY_MSK_TS}
            UNION ALL
            -- Истинно неуспешные платежи (после фильтра 1ч)
            SELECT
                'payment_failed'::text,
                p.payment_id,
                p.amount,
                p.created_at_moscow,
                p.mongo_user_id
            FROM mongo_payments p
            JOIN failed_filtered ff ON ff.payment_id = p.payment_id
            UNION ALL
            -- Регистрации
            SELECT
                'registration'::text,
                u.mongo_user_id,
                0::numeric,
                u.created_at_moscow,
                u.mongo_user_id
            FROM mongo_users u
            WHERE u.created_at_moscow < {_TODAY_MSK_TS}
        )
        SELECT DISTINCT ON (e.event_type, e.event_id)
            e.event_type,
            e.event_id,
            e.event_amount,
            e.event_dt                AS event_dt,    -- TIMESTAMP
            e.event_dt::date          AS event_date,  -- DATE
            e.event_dt::date          AS dt,          -- DATE alias (как в visits_calculated/hits_calculated)
            h.unified_user_id,
            h.watch_id,
            h.visit_id,
            h.hit_dt,
            h.hit_date,
            h.url,
            h.url_path,
            h.url_host,
            h.page_title,
            h.referer,
            h.is_landing,
            h.is_exit,
            h.is_significant,
            h.has_goal,
            -- Атрибуция (имена синхронизированы с hits_calculated и visits_calculated)
            h.last_source,
            h.first_source,
            h.last_significant_source,
            h.last_campaign,
            h.first_campaign,
            h.last_significant_campaign,
            h.last_content,
            h.first_content,
            h.last_significant_content,
            h.last_phrase,
            h.first_phrase,
            h.last_significant_phrase,
            h.last_ad_id,
            h.first_ad_id,
            h.last_significant_ad_id,
            h.last_ad_title,
            h.first_ad_title,
            h.last_significant_ad_title,
            h."ym:s:deviceCategory",
            h."ym:s:browser",
            h."ym:s:browserLanguage",
            h."ym:s:regionCity",
            h."ym:s:regionCountry"
        FROM events e
        JOIN unified_user_id_members{suffix} m
              ON m.mongo_user_id = e.mongo_user_id
        JOIN hits_calculated{suffix} h
              ON h.unified_user_id = m.unified_user_id
             AND h.hit_dt <= e.event_dt
        ORDER BY e.event_type, e.event_id, h.hit_dt DESC
    """


# ---------------------------------------------------------------------------
# 4 модели атрибуции на хитах (linear, timedecay, first_touch, lsig_touch)
# Архитектурно — копия `_ATTRIBUTION_*` из loader_visits.py, адаптированная
# для единицы «хит» (watch_id) вместо «визит» (visitID). Имена колонок
# (`payments_*_X`, `registrations_count_X`) совпадают, чтобы Jinja2-метрики
# Superset (одинаковые на ID9 и ID20) переключались корректно.
# Halflife time-decay для хитов = 6 часов (а не 7 дней как у визитов): хиты
# внутри визита идут с разницей в минуты, поэтому шкала суток дала бы
# фактически плоский (без затухания) вес внутри одной сессии.
# ---------------------------------------------------------------------------

# Базовые CTE атрибуции хитов — failed_filtered/all_payments/all_regs строит
# общая функция _build_attribution_base_ctes из loader_visits.py: бизнес-правило
# и cutoff живут в одном месте. Различия только два:
#   • members_table — `unified_user_id_members` (без `_new`): к моменту запуска
#     update_hits_calculated() подмена unified_user_id_members уже выполнена.
#   • sig_cte = hit_sig: каждой строке хита приписан флаг is_sig (1 если
#     родительский визит значимый, наследуется через is_significant).
_HIT_SIG_CTE = """hit_sig AS (
    SELECT
        watch_id,
        unified_user_id,
        hit_dt,
        is_significant AS is_sig
    FROM hits_calculated_new
)"""

_HIT_ATTRIBUTION_BASE_CTES = _build_attribution_base_ctes(
    members_table='unified_user_id_members',
    sig_cte=_HIT_SIG_CTE,
)


def _hit_attribution_update_template(suffix: str) -> str:
    """Финальный CTE combined + UPDATE hits_calculated_new для модели `suffix`.

    Шаблон полностью симметричен `_attribution_update_template` из loader_visits.py,
    но обновляет `hits_calculated_new` через `watch_id` вместо `"ym:s:visitID"`.

    Пустой suffix (last-click) даёт колонки без хвостового подчёркивания:
    `payments_approved_count` вместо `payments_approved_count_`.
    """
    sfx = f'_{suffix}' if suffix else ''
    return f"""
        ),
        combined AS (
            SELECT
                watch_id AS hit_key,
                COALESCE(pa.approved_count, 0) AS approved_count,
                COALESCE(pa.approved_sum,   0) AS approved_sum,
                COALESCE(pa.failed_count,   0) AS failed_count,
                COALESCE(pa.failed_sum,     0) AS failed_sum,
                COALESCE(ra.reg_count,      0) AS reg_count
            FROM payment_agg pa
            FULL OUTER JOIN reg_agg ra USING (watch_id)
        )
        UPDATE hits_calculated_new hc
        SET
            payments_approved_count{sfx} = c.approved_count,
            payments_approved_sum{sfx}   = c.approved_sum,
            payments_failed_count{sfx}   = c.failed_count,
            payments_failed_sum{sfx}     = c.failed_sum,
            registrations_count{sfx}     = c.reg_count
        FROM combined c
        WHERE hc.watch_id = c.hit_key
    """


def _build_weighted_hit_attribution_sql(suffix: str, weight_expr: str) -> str:
    """Linear / time-decay для хитов. Каждый хит до события получает вес
    `weight_expr` (зависит от времени между хитом и событием), нормализованный
    по сумме всех весов по событию. Eligible — значимые хиты, иначе fallback
    на все хиты пользователя до события (через `total_sig_before > 0` логику).

    `weight_expr` использует плейсхолдер `{evt}`, который подставляется как `ap`
    (для платежей) или `ar` (для регистраций) — даёт доступ к event_dt."""
    weight_pay = weight_expr.format(evt='ap')
    weight_reg = weight_expr.format(evt='ar')

    body = f"""
        WITH {_HIT_ATTRIBUTION_BASE_CTES},
        payment_pairs AS (
            SELECT
                hs.watch_id, ap.payment_id, ap.amount,
                ap.is_approved, ap.is_failed, hs.is_sig,
                SUM(hs.is_sig) OVER (PARTITION BY ap.unified_user_id, ap.payment_id) AS total_sig_before,
                {weight_pay} AS raw_weight
            FROM all_payments ap
            JOIN hit_sig hs
                ON hs.unified_user_id = ap.unified_user_id
               AND hs.hit_dt <= ap.event_dt
        ),
        eligible_payment AS (
            SELECT
                watch_id, payment_id, amount, is_approved, is_failed, raw_weight,
                SUM(raw_weight) OVER (PARTITION BY payment_id) AS weight_sum
            FROM payment_pairs
            WHERE (total_sig_before > 0 AND is_sig = 1) OR total_sig_before = 0
        ),
        payment_agg AS (
            SELECT
                watch_id,
                SUM(is_approved * raw_weight / weight_sum)          AS approved_count,
                SUM(is_approved * amount * raw_weight / weight_sum) AS approved_sum,
                SUM(is_failed  * raw_weight / weight_sum)            AS failed_count,
                SUM(is_failed  * amount * raw_weight / weight_sum)   AS failed_sum
            FROM eligible_payment
            GROUP BY watch_id
        ),
        reg_pairs AS (
            SELECT
                hs.watch_id, ar.mongo_user_id, hs.is_sig,
                SUM(hs.is_sig) OVER (PARTITION BY ar.unified_user_id, ar.mongo_user_id) AS total_sig_before,
                {weight_reg} AS raw_weight
            FROM all_regs ar
            JOIN hit_sig hs
                ON hs.unified_user_id = ar.unified_user_id
               AND hs.hit_dt <= ar.event_dt
        ),
        eligible_reg AS (
            SELECT
                watch_id, mongo_user_id, raw_weight,
                SUM(raw_weight) OVER (PARTITION BY mongo_user_id) AS weight_sum
            FROM reg_pairs
            WHERE (total_sig_before > 0 AND is_sig = 1) OR total_sig_before = 0
        ),
        reg_agg AS (
            SELECT watch_id, SUM(raw_weight / weight_sum) AS reg_count
            FROM eligible_reg
            GROUP BY watch_id
    """
    return body + _hit_attribution_update_template(suffix)


def _build_single_hit_attribution_sql(suffix: str, picker_order_by: str) -> str:
    """First-touch / lsig-touch на хитах: один-выигрывающий хит на событие
    через `DISTINCT ON (event_id) ORDER BY picker_order_by`. Этот хит получает
    всю конверсию, остальные — 0.
    First-touch: `hc.hit_dt ASC` (самый ранний хит до события).
    Lsig-touch: `is_significant DESC, hit_dt DESC` (последний значимый, fallback
    на любой последний при отсутствии значимых)."""
    body = f"""
        WITH {_HIT_ATTRIBUTION_BASE_CTES},
        payment_picked AS (
            SELECT DISTINCT ON (ap.payment_id)
                hc.watch_id, ap.payment_id, ap.amount,
                ap.is_approved, ap.is_failed
            FROM all_payments ap
            JOIN hits_calculated_new hc
                ON hc.unified_user_id = ap.unified_user_id
               AND hc.hit_dt <= ap.event_dt
            ORDER BY ap.payment_id, {picker_order_by}
        ),
        payment_agg AS (
            SELECT
                watch_id,
                SUM(is_approved * 1.0)    AS approved_count,
                SUM(is_approved * amount) AS approved_sum,
                SUM(is_failed * 1.0)      AS failed_count,
                SUM(is_failed * amount)   AS failed_sum
            FROM payment_picked
            GROUP BY watch_id
        ),
        reg_picked AS (
            SELECT DISTINCT ON (ar.mongo_user_id)
                hc.watch_id, ar.mongo_user_id
            FROM all_regs ar
            JOIN hits_calculated_new hc
                ON hc.unified_user_id = ar.unified_user_id
               AND hc.hit_dt <= ar.event_dt
            ORDER BY ar.mongo_user_id, {picker_order_by}
        ),
        reg_agg AS (
            SELECT watch_id, COUNT(*) AS reg_count
            FROM reg_picked
            GROUP BY watch_id
    """
    return body + _hit_attribution_update_template(suffix)


# Конфигурация моделей атрибуции для хитов. Time-decay halflife = 6 часов
# (не 7 дней как для визитов), чтобы внутри одного визита (хиты разнесены
# на минуты) поздние хиты получали ощутимо больший вес.
_HIT_ATTRIBUTION_MODELS = [
    ('linear',      'weighted', '1.0'),
    ('timedecay',   'weighted',
         'EXP(-EXTRACT(EPOCH FROM ({evt}.event_dt - hs.hit_dt)) / 3600.0 * LN(2) / 6.0)'),
    ('first_touch', 'single',   'hc.hit_dt ASC'),
    ('lsig_touch',  'single',
         'hc.is_significant DESC, hc.hit_dt DESC'),
]

_HIT_ATTRIBUTION_METRIC_TEMPLATES = [
    'payments_approved_count{sfx}',
    'payments_approved_sum{sfx}',
    'payments_failed_count{sfx}',
    'payments_failed_sum{sfx}',
    'registrations_count{sfx}',
]


def _build_hit_attribution_sql(suffix: str, kind: str, expr: str) -> str:
    if kind == 'weighted':
        return _build_weighted_hit_attribution_sql(suffix, expr)
    if kind == 'single':
        return _build_single_hit_attribution_sql(suffix, expr)
    raise ValueError(f"Unknown hit attribution kind: {kind}")


# ---------------------------------------------------------------------------
# Last-click на хитах через шаблон single-hit
# ---------------------------------------------------------------------------
# Last-click для хитов реализован через тот же `_build_single_hit_attribution_sql`,
# что и first_touch/lsig_touch, но picker_order_by = `hc.hit_dt DESC` (последний
# хит до события — это и есть last-page-click).
# Колонки `payments_approved_count`, `payments_approved_sum`,
# `payments_failed_count`, `payments_failed_sum`, `registrations_count`
# (без суффикса) хранят результат last-click. Они аналогичны столбцам в
# visits_calculated (где они вычисляются через `payment_agg` / `reg_agg` CTE
# одновременно с CREATE TABLE). На хитах мы добавляем их UPDATE-шагом —
# шаблон single-hit отлично подходит.

# ---------------------------------------------------------------------------
# Обновление hits_calculated и hit_conversions
# ---------------------------------------------------------------------------

def update_hits_calculated():
    """Полный пересчёт hits_calculated и hit_conversions.

    Зависит от visits_calculated (JOIN по visitID для атрибуции) и
    unified_user_id_members (для атрибуции конверсий хитам).
    Должна вызываться ПОСЛЕ update_visits_calculated().

    Атомарная подмена через _new-таблицы — старые таблицы остаются доступны
    во время пересчёта, переключение занимает миллисекунды.
    """
    # Гарантируем, что metrika_hits существует (даже пустая) — иначе
    # JOIN упадёт на свежей БД. На реальной системе это никогда не nominal-путь.
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS metrika_hits (
                "ym:pv:watchID"            TEXT,
                "ym:pv:visitID"            TEXT,
                "ym:pv:counterUserIDHash"  TEXT,
                "ym:pv:clientID"           TEXT,
                "ym:pv:dateTime"           TIMESTAMP,
                "ym:pv:URL"                TEXT,
                "ym:pv:title"              TEXT,
                "ym:pv:referer"            TEXT,
                "ym:pv:goalsID"            TEXT,
                "ym:pv:isPageView"         SMALLINT,
                "ym:pv:notBounce"          SMALLINT
            )
        """))

    index_queries_hits = [
        'CREATE INDEX IF NOT EXISTS idx_hc_watch_id_new      ON hits_calculated_new (watch_id)',
        'CREATE INDEX IF NOT EXISTS idx_hc_visit_id_new      ON hits_calculated_new (visit_id)',
        'CREATE INDEX IF NOT EXISTS idx_hc_uid_dt_new        ON hits_calculated_new (unified_user_id, hit_dt)',
        'CREATE INDEX IF NOT EXISTS idx_hc_dt_new            ON hits_calculated_new (dt)',
        'CREATE INDEX IF NOT EXISTS idx_hc_url_path_new      ON hits_calculated_new (url_path)',
        'CREATE INDEX IF NOT EXISTS idx_hc_dt_url_path_new   ON hits_calculated_new (dt, url_path)',
        'CREATE INDEX IF NOT EXISTS idx_hc_dt_lsig_src_new   ON hits_calculated_new (dt, last_significant_source)',
        'CREATE INDEX IF NOT EXISTS idx_hc_dt_last_src_new   ON hits_calculated_new (dt, last_source)',
        'CREATE INDEX IF NOT EXISTS idx_hc_landing_new       ON hits_calculated_new (is_landing) WHERE is_landing = 1',
    ]

    index_queries_conv = [
        'CREATE INDEX IF NOT EXISTS idx_hcv_event_new        ON hit_conversions_new (event_type, event_id)',
        'CREATE INDEX IF NOT EXISTS idx_hcv_dt_new           ON hit_conversions_new (dt)',
        'CREATE INDEX IF NOT EXISTS idx_hcv_event_date_new   ON hit_conversions_new (event_date)',
        'CREATE INDEX IF NOT EXISTS idx_hcv_url_path_type_new ON hit_conversions_new (url_path, event_type)',
        'CREATE INDEX IF NOT EXISTS idx_hcv_dt_lsig_src_new  ON hit_conversions_new (dt, last_significant_source)',
        'CREATE INDEX IF NOT EXISTS idx_hcv_dt_last_src_new  ON hit_conversions_new (dt, last_source)',
        'CREATE INDEX IF NOT EXISTS idx_hcv_user_dt_new      ON hit_conversions_new (unified_user_id, event_dt)',
    ]

    # Шаг 1: чистим хвосты предыдущего упавшего запуска
    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS hit_conversions_new"))
        conn.execute(text("DROP TABLE IF EXISTS hits_calculated_new"))

    # Шаг 2: строим _new-таблицы
    log.info("Строим hits_calculated_new...")
    with engine.begin() as conn:
        conn.execute(text(_get_hits_calculated_sql("_new")))

        # Шаг 2а: добавляем колонки атрибуции (last-click без суффикса + 4 модели × 5 метрик).
        log.info("Добавляем колонки атрибуции в hits_calculated_new (last-click + 4 модели × 5 метрик)...")
        # Last-click — те же имена, что в visits_calculated (без суффикса).
        for col_template in _HIT_ATTRIBUTION_METRIC_TEMPLATES:
            col = col_template.format(sfx='')
            conn.execute(text(
                f'ALTER TABLE hits_calculated_new ADD COLUMN "{col}" DOUBLE PRECISION DEFAULT 0'
            ))
        # 4 модели с суффиксами (linear/timedecay/first_touch/lsig_touch).
        for suffix, _, _ in _HIT_ATTRIBUTION_MODELS:
            for col_template in _HIT_ATTRIBUTION_METRIC_TEMPLATES:
                col = col_template.format(sfx=f'_{suffix}')
                conn.execute(text(
                    f'ALTER TABLE hits_calculated_new ADD COLUMN "{col}" DOUBLE PRECISION DEFAULT 0'
                ))

        # Шаг 2б: считаем 4 модели через декларативный шаблон.
        for suffix, kind, expr in _HIT_ATTRIBUTION_MODELS:
            log.info("Рассчитываем %s-атрибуцию хитов (%s, halflife=%s)...",
                     suffix, kind, '6h' if suffix == 'timedecay' else 'n/a')
            conn.execute(text(_build_hit_attribution_sql(suffix, kind, expr)))

        # Шаг 2в: last-click — это single-hit picker по `hit_dt DESC`.
        # Реализован через тот же _build_single_hit_attribution_sql, но без суффикса —
        # шаблон сам опускает разделитель `_` при пустом suffix, поэтому ни post-fix
        # str.replace, ни rstrip больше не нужны.
        log.info("Рассчитываем last-click атрибуцию хитов...")
        last_click_sql = _build_single_hit_attribution_sql('', 'hc.hit_dt DESC')
        conn.execute(text(last_click_sql))

        for q in index_queries_hits:
            conn.execute(text(q))

    log.info("Строим hit_conversions_new (last-page-click атрибуция)...")
    with engine.begin() as conn:
        # hit_conversions JOIN-ит hits_calculated_new (свежепостроенную)
        # и unified_user_id_members (боевая таблица, уже подменённая в update_visits_calculated).
        # _get_hit_conversions_sql применяет suffix к обеим — откатываем для members.
        sql = _get_hit_conversions_sql("_new").replace(
            "unified_user_id_members_new",
            "unified_user_id_members",
        )
        conn.execute(text(sql))
        for q in index_queries_conv:
            conn.execute(text(q))

    # Шаг 3: атомарная подмена
    log.info("Подменяем hits_calculated и hit_conversions...")
    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS hit_conversions"))
        conn.execute(text("DROP TABLE IF EXISTS hits_calculated"))
        conn.execute(text("ALTER TABLE hits_calculated_new RENAME TO hits_calculated"))
        conn.execute(text("ALTER TABLE hit_conversions_new RENAME TO hit_conversions"))

    # Регрессия:
    #   1. SUM(hits_calculated.payments_approved_sum) [last-click через single-hit picker]
    #      должен совпадать с SUM(hit_conversions.event_amount WHERE approved) — обе
    #      реализации last-click, разные структурные источники.
    #   2. Обе должны совпасть с SUM(visits_calculated.payments_approved_sum) до 0.88%
    #      (известная дельта 5 660 ₽ — orphan-визиты без хитов в сентябре-октябре 2025).
    #   3. Sum по 4 моделям (linear/timedecay/first_touch/lsig_touch) тоже близок
    #      к visits-сумме (с той же orphan-дельтой), потому что вес нормализуется по
    #      «всем подходящим хитам пользователя» и сумма shares = 1.0 на событие.
    with engine.connect() as conn:
        cnt_hits = conn.execute(text("SELECT COUNT(*) FROM hits_calculated")).scalar()
        cnt_conv = conn.execute(text("SELECT COUNT(*) FROM hit_conversions")).scalar()
        sum_hcv  = conn.execute(text(
            "SELECT COALESCE(SUM(event_amount), 0) FROM hit_conversions "
            "WHERE event_type = 'payment_approved'"
        )).scalar()
        sum_vc   = conn.execute(text(
            "SELECT COALESCE(SUM(payments_approved_sum), 0) FROM visits_calculated"
        )).scalar()
        regs_hcv = conn.execute(text(
            "SELECT COUNT(*) FROM hit_conversions WHERE event_type = 'registration'"
        )).scalar()
        regs_vc  = conn.execute(text(
            "SELECT COALESCE(SUM(registrations_count), 0) FROM visits_calculated"
        )).scalar()
        # Дополнительные суммы по 4 моделям + last-click через колонки hits_calculated.
        sums_hits = {}
        for suffix in ('', 'linear', 'timedecay', 'first_touch', 'lsig_touch'):
            col = 'payments_approved_sum' + (f'_{suffix}' if suffix else '')
            sums_hits[suffix or 'last_click'] = conn.execute(text(
                f'SELECT COALESCE(SUM("{col}"), 0) FROM hits_calculated'
            )).scalar()
        regs_hits = {}
        for suffix in ('', 'linear', 'timedecay', 'first_touch', 'lsig_touch'):
            col = 'registrations_count' + (f'_{suffix}' if suffix else '')
            regs_hits[suffix or 'last_click'] = conn.execute(text(
                f'SELECT COALESCE(SUM("{col}"), 0) FROM hits_calculated'
            )).scalar()

        log.info("hits_calculated: %s строк; hit_conversions: %s строк.", cnt_hits, cnt_conv)
        log.info("Регрессия approved-сумм:")
        log.info("  visits_calculated last-click  = %s ₽", sum_vc)
        log.info("  hit_conversions  last-click   = %s ₽ (дельта от visits = %.2f ₽)",
                 sum_hcv, abs(float(sum_hcv) - float(sum_vc)))
        for k, v in sums_hits.items():
            log.info("  hits_calculated  %-12s = %s ₽ (дельта от visits = %.2f ₽)",
                     k, v, abs(float(v) - float(sum_vc)))
        log.info("Регрессия регистраций:")
        log.info("  visits_calculated last-click  = %s", regs_vc)
        log.info("  hit_conversions  last-click   = %s", regs_hcv)
        for k, v in regs_hits.items():
            log.info("  hits_calculated  %-12s = %s", k, v)

    with engine.connect() as conn:
        conn.execute(text("ANALYZE hits_calculated"))
        conn.execute(text("ANALYZE hit_conversions"))
        conn.commit()
    log.info("ANALYZE hits_calculated/hit_conversions выполнен.")


