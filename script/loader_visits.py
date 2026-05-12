"""
SQL-генерация и обновление visits_calculated — центрального датасета аналитики.

Таблица пересоздаётся полностью при каждом запуске, потому что:
  - unified_user_id зависит от clientid_to_mongoid (который обновляется накопительно)
  - is_new_unified_user вычисляется оконной функцией по всей истории пользователя
  - частичное обновление потребовало бы пересчёта для всех визитов изменившихся пользователей —
    проще и надёжнее пересоздать всё целиком
"""
from sqlalchemy import text

from loader_config import log, engine, REPORT_START_DATE


# ---------------------------------------------------------------------------
# Cutoff «до вчера»: события с этой даты и позже не учитываются.
# Postgres работает в UTC, loader — в Europe/Moscow; mongo_*.created_at_moscow и
# direct_*.date уже в МСК. Берём ровно начало московских суток через AT TIME ZONE,
# не полагаясь на серверное TZ контейнера БД.
#
# Применяется к: ПЛАТЕЖАМ, РЕГИСТРАЦИЯМ, РАСХОДАМ ДИРЕКТА, ВИЗИТАМ.
# Не применяется к: mongo_referrers (справочник), clientid_to_mongoid (накопительный
# маппинг) — они должны жить «как есть».
# ---------------------------------------------------------------------------
_TODAY_MSK_TS   = "date_trunc('day', NOW() AT TIME ZONE 'Europe/Moscow')"
_TODAY_MSK_DATE = "(NOW() AT TIME ZONE 'Europe/Moscow')::date"


# ---------------------------------------------------------------------------
# Строители SQL-выражений
# ---------------------------------------------------------------------------

def _source_case(utm_src: str, utm_med: str, ref: str, dpt: str, ts: str, from_field: str) -> str:
    """Возвращает SQL CASE WHEN для определения источника визита (Level 1).

    Параметры — SQL-выражения соответствующих полей (last/first вариант).
    Порядок WHEN = приоритет: чем выше — тем важнее.
    Специфичные источники (vk_ads, Zen_Posts, ya, ig) предшествуют общим категориям,
    чтобы не попасть в «Социальные сети» или «Прочая реклама».
    """
    return f"""
        CASE
            -- Реферальная программа (проверяем по таблице mongo_referrers)
            WHEN EXISTS (
                SELECT 1 FROM mongo_referrers m
                WHERE {from_field} = m.identifier
            ) THEN 'Реферальная программа'

            -- Платная реклама ─────────────────────────────────────────────
            -- vk_ads: реальный utm_source от рекламного кабинета ВК (medium=cpa/cpc)
            WHEN {utm_src} = 'vk_ads'                           THEN 'ВК реклама'
            -- vk+paid: по спецификации, на случай перехода на стандартную разметку
            WHEN {utm_src} = 'vk'       AND {utm_med} = 'paid'  THEN 'ВК реклама'
            WHEN {utm_src} = 'ok'       AND {utm_med} = 'paid'  THEN 'OK реклама'
            WHEN {utm_src} = 'telegram' AND {utm_med} = 'paid'  THEN 'Telegram Ads'
            WHEN {utm_src} = 'tbank'    AND {utm_med} = 'paid'  THEN 'Т-Банк реклама'
            -- Zen_Posts: реальный utm_source продвижения в Дзене (medium=social)
            WHEN {utm_src} = 'Zen_Posts'                        THEN 'Дзен продвижение'
            -- dzen+paid: по спецификации, на случай перехода
            WHEN {utm_src} = 'dzen'     AND {utm_med} = 'paid'  THEN 'Дзен продвижение'
            
            -- Яндекс.Директ без UTM (только DirectPlatformType от Метрики) ─
            WHEN {dpt} = 'Search'                               THEN 'Яндекс: Директ, Поиск'
            WHEN {dpt} = 'Context'                              THEN 'Яндекс: Директ, Сети'
            
            -- Яндекс.Директ с UTM ─────────────────────────────────────────
            -- medium=cpc: реальный medium от Директа; paid: по спецификации
            WHEN {utm_src} = 'yandex' AND {utm_med} IN ('paid', 'cpc')    THEN 'Яндекс: Директ'
            -- ya: альтернативный utm_source от Директа (встречается в данных)
            WHEN {utm_src} = 'ya'      AND {utm_med} IN ('paid', 'cpc')   THEN 'Яндекс: Директ'

            -- Telegram-каналы ─────────────────────────────────────────────
            WHEN {utm_src} = 'telegram_community'               THEN 'Telegram community'
            WHEN {utm_src} = 'telegram_main'                    THEN 'Telegram main'
            WHEN {utm_src} = 'telegram_cyber'                   THEN 'cyberBrukwa'
            -- tg_official: неразмеченный TG-источник, встречается в данных
            WHEN {utm_src} = 'tg_official'                      THEN 'Telegram'
            WHEN {utm_src} = 'telegram'                         THEN 'Telegram'

            -- Социальные платформы ─────────────────────────────────────────
            WHEN {utm_src} = 'vk'                               THEN 'ВКонтакте'
            WHEN {utm_src} = 'ok'                               THEN 'Одноклассники'
            -- odnoklassniki: альтернативное написание utm_source для ОК
            WHEN {utm_src} = 'odnoklassniki'                    THEN 'Одноклассники'
            WHEN {utm_src} = 'instagram'                        THEN 'Instagram'
            -- ig: реальный utm_source из Instagram (встречается в данных)
            WHEN {utm_src} = 'ig'                               THEN 'Instagram'
            WHEN {utm_src} = 'pinterest'                        THEN 'Pinterest'
            WHEN {utm_src} = 'max'                              THEN 'MAX'
            WHEN {utm_src} = 'youtube'                          THEN 'YouTube'
            WHEN {utm_src} = 'tiktok'                           THEN 'TikTok'

            -- Дзен органика + SEO-площадки ────────────────────────────────
            WHEN {utm_src} = 'dzen'                             THEN 'Дзен'
            WHEN {utm_src} = 'vk_article'                       THEN 'VK Articles'
            WHEN {utm_src} = 'habr'                             THEN 'Habr'
            WHEN {utm_src} = 'vc'                               THEN 'VC.ru'
            WHEN {utm_src} = 'spark'                            THEN 'Spark'
            WHEN {utm_src} = 'cossa'                            THEN 'Cossa'
            WHEN {utm_src} = 'sostav'                           THEN 'Sostav'
            WHEN {utm_src} = 'teletype'                         THEN 'Teletype'
            WHEN {utm_src} = 'timeweb'                          THEN 'Timeweb'
            WHEN {utm_src} = 'partnerkin'                       THEN 'Партнёркин'

            -- Конкретные рефереры ──────────────────────────────────────────
            WHEN {ref} = 'prompt1.ru'                           THEN 'prompt1.ru'
            WHEN {ref} = 'visariomedia.com'                     THEN 'visariomedia.com'
            WHEN {ref} = 'rzgfduezfyp.com'                      THEN 'rzgfduezfyp.com'
            WHEN {ref} = 'xadsmart.com'                         THEN 'xadsmart.com'
            WHEN {ref} = 'link.avito.ru'                        THEN 'link.avito.ru'
            WHEN {ref} IN (
                'accounts.google.com', 'accounts.google.ru',
                'oauth.yandex.ru',
                'app.example.com', 'example.com',
                'merch.tochka.com', 'iamx.tochka.com'
            )                                                    THEN 'Внутренний переход'

            -- Прочие рекламные сети (встречаются в данных, в спецификации не описаны)
            WHEN {utm_src} = 'popads'                           THEN 'Прочие рекламные сети'

            -- Общие категории трафика (TrafficSource от Метрики) ──────────
            WHEN {ts} = 'organic'                               THEN 'Органический поиск'
            WHEN {ts} = 'direct'                                THEN 'Прямой переход'
            WHEN {ts} = 'internal'                              THEN 'Внутренний переход'
            WHEN {ts} = 'social'                                THEN 'Социальные сети'
            WHEN {ts} = 'messenger'                             THEN 'Мессенджеры'
            WHEN {ts} = 'recommend'                             THEN 'Рекомендательные системы'
            WHEN {ts} = 'referral'                              THEN 'Прочие сайты'
            WHEN {ts} = 'ad'                                    THEN 'Прочая реклама'

            ELSE NULL
        END"""


def _sig_window(field: str) -> str:
    """Carry-forward оконная функция для last_significant_* полей.

    Идея: не показывать «Прямой переход» как источник, если до этого был платный трафик.
    Вместо этого «несём вперёд» последний значимый источник.

    Реализация через cumsum-группировку:
      grp = накопленное число значимых визитов до текущего включительно.
      grp=0: значимых визитов ещё не было → берём текущее значение (LAST_VALUE).
      grp>0: для всех визитов с тем же grp берём источник первого в группе (FIRST_VALUE),
             то есть источник, с которого началась эта «волна» значимого трафика.
    """
    return f"""CASE
                    WHEN grp = 0 THEN
                        LAST_VALUE({field}) OVER (
                            PARTITION BY uid, grp
                            ORDER BY dt ASC
                            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                        )
                    ELSE
                        FIRST_VALUE({field}) OVER (PARTITION BY uid, grp ORDER BY dt ASC)
                END"""


def _get_visits_calculated_sql(suffix: str = "") -> str:
    """Возвращает SQL для полного пересоздания visits_calculated{suffix}."""
    header = f"CREATE TABLE visits_calculated{suffix} AS"

    # is_new_unified_user = 1 только для самого первого визита пользователя
    new_user_expr = """CASE WHEN ROW_NUMBER() OVER (
                    PARTITION BY vi.unified_user_id
                    ORDER BY mv."ym:s:dateTime" ASC
                ) = 1 THEN 1 ELSE 0 END"""

    # ── Level 1: источник ────────────────────────────────────────────────────
    last_src = _source_case(
        utm_src   ='t."ym:s:cross_device_lastUTMSource"',
        utm_med   ='t."ym:s:cross_device_lastUTMMedium"',
        ref       ='t."ym:s:cross_device_lastReferalSource"',
        dpt       ='t."ym:s:cross_device_lastDirectPlatformType"',
        ts        ='t."ym:s:cross_device_lastTrafficSource"',
        from_field='t."ym:s:from"',
    )
    first_src = _source_case(
        utm_src   ='t."ym:s:cross_device_firstUTMSource"',
        utm_med   ='t."ym:s:cross_device_firstUTMMedium"',
        ref       ='t."ym:s:cross_device_firstReferalSource"',
        dpt       ='t."ym:s:cross_device_firstDirectPlatformType"',
        ts        ='t."ym:s:cross_device_firstTrafficSource"',
        from_field='first_visit_from',  # первое ym:s:from по пользователю (из CTE)
    )

    # ── Level 2: кампания ────────────────────────────────────────────────────
    # Если визит классифицирован как Direct (last_src_calc начинается на
    # 'Яндекс: Директ') → справочник direct_campaigns по числовому ID.
    # Это даёт последнее актуальное имя кампании независимо от dpt-аномалий
    # Метрики и склеивает все переименования. UTM/DirectClickOrderName — fallback,
    # если в справочнике нет ID (старые визиты до миграции, удалённые РК и т.п.).
    # Для не-Direct визитов — utm_campaign как было: трогать чужие источники
    # (ВК, Telegram, органика) не нужно.
    last_cmp = """COALESCE(
                t.last_ref_label,
                CASE WHEN t.last_src_calc LIKE 'Яндекс: Директ%'
                     THEN COALESCE(
                         -- 1. ID напрямую из Метрики (cross_device_lastDirectClickOrder)
                         dc_last.campaign_name || ' (' || dc_last.campaign_id::text || ')',
                         -- 2. ID извлечён регуляркой из utm_campaign (Direct автогенерит
                         --    utm в формате 'имя_<id>'). Покрывает случаи, когда Метрика
                         --    отдала DirectClickOrder='nan'/'' для явно Direct-визитов.
                         dc_last_utm.campaign_name || ' (' || dc_last_utm.campaign_id::text || ')',
                         -- 3. сырое utm_campaign / DirectClickOrderName — последний fallback
                         NULLIF(t."ym:s:cross_device_lastUTMCampaign", ''),
                         NULLIF(t."ym:s:cross_device_lastDirectClickOrderName", '')
                     ) END,
                NULLIF(t."ym:s:cross_device_lastUTMCampaign", '')
            )"""
    first_cmp = """COALESCE(
                t.first_ref_label,
                CASE WHEN t.first_src_calc LIKE 'Яндекс: Директ%'
                     THEN COALESCE(
                         dc_first.campaign_name || ' (' || dc_first.campaign_id::text || ')',
                         dc_first_utm.campaign_name || ' (' || dc_first_utm.campaign_id::text || ')',
                         NULLIF(t."ym:s:cross_device_firstUTMCampaign", ''),
                         NULLIF(t."ym:s:cross_device_firstDirectClickOrderName", '')
                     ) END,
                NULLIF(t."ym:s:cross_device_firstUTMCampaign", '')
            )"""

    # ── Level 3: объявление ──────────────────────────────────────────────────
    # utm_content; fallback → DirectClickBannerName для Директа без UTM
    last_cnt = """COALESCE(
                NULLIF(t."ym:s:cross_device_lastUTMContent", ''),
                CASE WHEN t."ym:s:cross_device_lastDirectPlatformType" IN ('Search', 'Context')
                     THEN NULLIF(t."ym:s:cross_device_lastDirectClickBannerName", '') END
            )"""
    first_cnt = """COALESCE(
                NULLIF(t."ym:s:cross_device_firstUTMContent", ''),
                CASE WHEN t."ym:s:cross_device_firstDirectPlatformType" IN ('Search', 'Context')
                     THEN NULLIF(t."ym:s:cross_device_firstDirectClickBannerName", '') END
            )"""

    # ── Level 4: фраза ──────────────────────────────────────────────────────
    # utm_term; fallback → DirectPhraseOrCond для Директа без UTM
    last_phr = """COALESCE(
                NULLIF(t."ym:s:cross_device_lastUTMTerm", ''),
                CASE WHEN t."ym:s:cross_device_lastDirectPlatformType" IN ('Search', 'Context')
                     THEN NULLIF(t."ym:s:cross_device_lastDirectPhraseOrCond", '') END
            )"""
    first_phr = """COALESCE(
                NULLIF(t."ym:s:cross_device_firstUTMTerm", ''),
                CASE WHEN t."ym:s:cross_device_firstDirectPlatformType" IN ('Search', 'Context')
                     THEN NULLIF(t."ym:s:cross_device_firstDirectPhraseOrCond", '') END
            )"""

    return f"""
        {header}
        WITH visitor_ids AS (
            -- Определяем unified_user_id для каждого визита.
            -- Приоритет: 1) MongoDB ObjectId из маппинга, 2) h_+hash, 3) c_+clientID
            SELECT
                mv."ym:s:visitID",
                COALESCE(
                    cu.unified_user_id,
                    'h_' || mv."ym:s:counterUserIDHash",
                    'c_' || mv."ym:s:clientID"
                ) AS unified_user_id,
                CASE
                    WHEN cu.unified_user_id IS NOT NULL AND cu.mongo_count > 1 THEN 'mongo_household'
                    WHEN cu.unified_user_id IS NOT NULL                         THEN 'mongo'
                    WHEN mv."ym:s:counterUserIDHash" IS NOT NULL                THEN 'hash'
                    ELSE                                                              'client_id'
                END AS unified_user_id_source
            FROM metrika_visits mv
            LEFT JOIN clientid_to_unified{suffix} cu ON cu.metrika_client_id = mv."ym:s:clientID"
        ),
        {_build_failed_filtered_cte()},
        payment_attribution AS (
            -- Last-click: для каждого платежа выбираем самый поздний визит пользователя
            -- до момента платежа, попавший в visits_calculated (≥ REPORT_START_DATE).
            -- DISTINCT ON (payment_id) гарантирует, что один реальный платёж учтётся
            -- ровно один раз, даже если у пользователя несколько clientID Метрики
            -- (типичный случай — смена устройства, ~6% юзеров).
            -- Cutoff «до вчера»: сегодняшние платежи отбрасываются. Завтра тот же
            -- платёж попадёт в отчёт (полный пересчёт visits_calculated при каждом ETL).
            SELECT DISTINCT ON (p.payment_id)
                mv."ym:s:visitID",
                p.payment_id,
                p.amount,
                p.status,
                fpf.payment_id IS NOT NULL AS is_failed_filtered
            FROM mongo_payments p
            JOIN unified_user_id_members{suffix} uim ON uim.mongo_user_id = p.mongo_user_id
            JOIN visitor_ids vi ON vi.unified_user_id = uim.unified_user_id
            JOIN metrika_visits mv ON mv."ym:s:visitID" = vi."ym:s:visitID"
                                  AND mv."ym:s:dateTime" <= p.created_at_moscow
                                  AND mv."ym:s:dateTime" >= '{REPORT_START_DATE}'::timestamp
            LEFT JOIN failed_filtered fpf ON fpf.payment_id = p.payment_id
            WHERE p.created_at_moscow < {_TODAY_MSK_TS}
            ORDER BY p.payment_id, mv."ym:s:dateTime" DESC
        ),
        payment_agg AS (
            SELECT
                "ym:s:visitID",
                SUM(CASE WHEN status = 'approved' THEN 1     ELSE 0   END) AS payments_approved_count,
                SUM(CASE WHEN status != 'approved' AND is_failed_filtered THEN 1     ELSE 0   END) AS payments_failed_count,
                SUM(CASE WHEN status = 'approved' THEN amount ELSE 0.0 END) AS payments_approved_sum,
                SUM(CASE WHEN status != 'approved' AND is_failed_filtered THEN amount ELSE 0.0 END) AS payments_failed_sum
            FROM payment_attribution
            GROUP BY "ym:s:visitID"
        ),
        reg_attribution AS (
            -- Last-click для регистраций: для каждой регистрации (mongo_user_id)
            -- выбираем самый поздний визит пользователя до момента регистрации.
            -- Cutoff «до вчера»: сегодняшние регистрации не учитываются.
            SELECT DISTINCT ON (u.mongo_user_id)
                mv."ym:s:visitID",
                u.mongo_user_id
            FROM mongo_users u
            JOIN unified_user_id_members{suffix} uim ON uim.mongo_user_id = u.mongo_user_id
            JOIN visitor_ids vi ON vi.unified_user_id = uim.unified_user_id
            JOIN metrika_visits mv ON mv."ym:s:visitID" = vi."ym:s:visitID"
                                  AND mv."ym:s:dateTime" <= u.created_at_moscow
                                  AND mv."ym:s:dateTime" >= '{REPORT_START_DATE}'::timestamp
            WHERE u.created_at_moscow < {_TODAY_MSK_TS}
            ORDER BY u.mongo_user_id, mv."ym:s:dateTime" DESC
        ),
        reg_agg AS (
            SELECT "ym:s:visitID", COUNT(*) AS registrations_count
            FROM reg_attribution
            GROUP BY "ym:s:visitID"
        ),
        raw_with_first_from AS (
            -- Добавляем first_visit_from: ym:s:from самого первого визита пользователя.
            -- Нужен для first_source реферальной программы (ссылка приходит в ym:s:from).
            SELECT
                t.*,
                vi.unified_user_id,
                FIRST_VALUE(t."ym:s:from") OVER (
                    PARTITION BY vi.unified_user_id
                    ORDER BY t."ym:s:dateTime" ASC
                ) AS first_visit_from
            FROM metrika_visits t
            JOIN visitor_ids vi ON vi."ym:s:visitID" = t."ym:s:visitID"
        ),
        with_ref_labels AS (
            -- Находим ref_label для текущего (last) и первого (first) визита пользователя.
            -- Двумя LEFT JOIN'ами вместо двух коррелированных подзапросов: при ~150k+
            -- визитах подзапрос отрабатывал per row, JOIN строит hash один раз.
            -- Безопасно благодаря UNIQUE-индексу uq_mongo_referrers_id на (identifier).
            SELECT
                rf.*,
                m_last.ref_label  AS last_ref_label,
                m_first.ref_label AS first_ref_label
            FROM raw_with_first_from rf
            LEFT JOIN mongo_referrers m_last
                ON m_last.identifier  = rf."ym:s:from"
            LEFT JOIN mongo_referrers m_first
                ON m_first.identifier = rf.first_visit_from
        ),
        pre_sources_src AS (
            -- Сначала вычисляем источник визита (last_src_calc/first_src_calc).
            -- Это нужно, чтобы на следующем шаге last_campaign могло проверить
            -- "является ли визит Direct" и применить справочник direct_campaigns
            -- только для Direct-визитов (не для случайных кросс-девайс-склеек,
            -- где есть DirectClickOrder, но визит классифицирован как другой канал).
            SELECT
                t.*,
                {last_src}  AS last_src_calc,
                {first_src} AS first_src_calc
            FROM with_ref_labels t
        ),
        direct_ads_text_versions AS (
            -- Per-(ad_id, value) таймлайн title объявления. SCD2 пишет новую
            -- строку при любом изменении контента, но title мог не меняться —
            -- здесь дедупим по value.
            SELECT
                ad_id, val,
                MIN(valid_from)           AS first_seen,
                MAX(valid_to)             AS last_to,
                BOOL_OR(valid_to IS NULL) AS is_current
            FROM (
                SELECT ad_id, valid_from, valid_to, title AS val
                FROM direct_ads
                WHERE title IS NOT NULL
            ) u
            GROUP BY ad_id, val
        ),
        direct_ads_text_agg AS (
            -- Одна строка на ad_id со склейкой версий title через '\\n'.
            -- Одна версия — просто значение. Несколько версий —
            -- '<val> [01.03.26—28.04.26]\\n<val> [...]', причём текущая
            -- (is_current) идёт без диапазона дат.
            SELECT
                ad_id,
                STRING_AGG(label, E'\\n' ORDER BY first_seen) AS title
            FROM (
                SELECT
                    ad_id, first_seen,
                    CASE
                        WHEN COUNT(*) OVER (PARTITION BY ad_id) = 1 OR is_current
                            THEN val
                        ELSE val || ' [' || TO_CHAR(first_seen, 'DD.MM.YY')
                                 || '—' || TO_CHAR(last_to,    'DD.MM.YY') || ']'
                    END AS label
                FROM direct_ads_text_versions
            ) labeled
            GROUP BY ad_id
        ),
        pre_sources AS (
            -- Источник, кампания, объявление и фраза для каждого визита.
            -- JOIN direct_campaigns по числовому ID (без фильтра по dpt — Метрика
            -- иногда отдаёт dpt='nan'/'' для явно Direct-визитов). Само использование
            -- результата JOIN ограничено в last_cmp условием last_src_calc LIKE 'Яндекс: Директ%'.
            -- direct_ads_text_agg агрегирует все SCD2-версии title в одну строку
            -- на ad_id со склейкой версий и датами — нужен для атрибуции и для
            -- виртуальных датасетов галерей (#25/#26/#29), которые делают JOIN
            -- сами; в visits_calculated пишем только last/first/lsig title.
            SELECT
                "ym:s:visitID"  AS rid,
                unified_user_id AS uid,
                "ym:s:dateTime" AS dt,
                t.last_src_calc  AS last_source,
                t.first_src_calc AS first_source,
                {last_cmp}      AS last_campaign,
                {first_cmp}     AS first_campaign,
                {last_cnt}      AS last_content,
                {first_cnt}     AS first_content,
                {last_phr}      AS last_phrase,
                {first_phr}     AS first_phrase,
                -- Креатив (Level 5): ID объявления + агрегированный title.
                -- Guard по источнику — аналогично last_campaign: ad_id и title
                -- выставляются только когда визит атрибутирован как Direct.
                -- Без guard cross_device_last*/first* заполнены из истории
                -- пользователя (предыдущий Direct-клик), из-за чего реферальные
                -- и органические визиты получали фантомный ad_id, title и стоимость.
                -- last_significant_ad_id/title наследуют корректные значения
                -- автоматически через _sig_window поверх исправленных полей.
                CASE WHEN t.last_src_calc  LIKE 'Яндекс: Директ%'
                     THEN NULLIF(NULLIF(t."ym:s:cross_device_lastDirectClickBanner",  ''), '0')
                END AS last_ad_id,
                CASE WHEN t.first_src_calc LIKE 'Яндекс: Директ%'
                     THEN NULLIF(NULLIF(t."ym:s:cross_device_firstDirectClickBanner", ''), '0')
                END AS first_ad_id,
                CASE WHEN t.last_src_calc  LIKE 'Яндекс: Директ%' THEN dat_l.title END AS last_ad_title,
                CASE WHEN t.first_src_calc LIKE 'Яндекс: Директ%' THEN dat_f.title END AS first_ad_title
            FROM pre_sources_src t
            LEFT JOIN direct_campaigns dc_last
                   ON dc_last.campaign_id::text = NULLIF(t."ym:s:cross_device_lastDirectClickOrder", '')
            LEFT JOIN direct_campaigns dc_first
                   ON dc_first.campaign_id::text = NULLIF(t."ym:s:cross_device_firstDirectClickOrder", '')
            -- Регулярка ловит хвост '_<digits>' в utm_campaign (формат автогенерации
            -- Direct: 'имя_<campaign_id>'). substring c POSIX-regex возвращает захват
            -- группы, либо NULL если не совпало → JOIN не сработает.
            LEFT JOIN direct_campaigns dc_last_utm
                   ON dc_last_utm.campaign_id::text = SUBSTRING(t."ym:s:cross_device_lastUTMCampaign" FROM '_(\d+)$')
            LEFT JOIN direct_campaigns dc_first_utm
                   ON dc_first_utm.campaign_id::text = SUBSTRING(t."ym:s:cross_device_firstUTMCampaign" FROM '_(\d+)$')
            -- Тексты last/first объявления — склейка истории через direct_ads_text_agg.
            LEFT JOIN direct_ads_text_agg dat_l
                   ON dat_l.ad_id::text = NULLIF(NULLIF(t."ym:s:cross_device_lastDirectClickBanner",  ''), '0')
            LEFT JOIN direct_ads_text_agg dat_f
                   ON dat_f.ad_id::text = NULLIF(NULLIF(t."ym:s:cross_device_firstDirectClickBanner", ''), '0')
        ),
        base_mapping AS (
            -- Помечаем «значимые» визиты (не Прямой/Внутренний)
            SELECT
                rid, uid, dt,
                last_source,   first_source,
                last_campaign, first_campaign,
                last_content,  first_content,
                last_phrase,   first_phrase,
                last_ad_title, first_ad_title,
                last_ad_id,    first_ad_id,
                CASE WHEN last_source NOT IN ('Прямой переход', 'Внутренний переход')
                     THEN 1 ELSE 0 END AS is_sig
            FROM pre_sources
        ),
        grouped_mapping AS (
            -- grp = накопленная сумма значимых визитов.
            -- Все визиты с одинаковым grp находятся «под крылом» одного значимого источника.
            SELECT *,
                SUM(is_sig) OVER (PARTITION BY uid ORDER BY dt) AS grp
            FROM base_mapping
        ),
        sources AS (
            -- Вычисляем last_significant_* через carry-forward оконную функцию
            SELECT
                rid,
                last_source,   first_source,
                last_campaign, first_campaign,
                last_content,  first_content,
                last_phrase,   first_phrase,
                last_ad_title, first_ad_title,
                last_ad_id,    first_ad_id,
                {_sig_window('last_source')}    AS last_significant_source,
                {_sig_window('last_campaign')}  AS last_significant_campaign,
                {_sig_window('last_content')}   AS last_significant_content,
                {_sig_window('last_phrase')}    AS last_significant_phrase,
                {_sig_window('last_ad_title')}  AS last_significant_ad_title,
                {_sig_window('last_ad_id')}     AS last_significant_ad_id
            FROM grouped_mapping
        ),
        direct_kw AS (
            -- CPC на уровне (date, criterion) из CRITERIA_PERFORMANCE_REPORT.
            -- Агрегируем по criterion за дату: сумма по всем кампаниям/группам.
            -- Cutoff «до вчера»: расходы за сегодня не учитываются. Загрузчик
            -- скачивает данные только до вчера, но повторная защита здесь —
            -- страховка от ручных загрузок и будущих изменений.
            SELECT
                "date",
                criterion,
                CASE WHEN SUM(clicks) > 0
                     THEN ROUND(CAST(SUM(cost) / SUM(clicks) AS NUMERIC), 4)
                     ELSE NULL END  AS cpc
            FROM direct_criteria_costs
            WHERE "date" < {_TODAY_MSK_DATE}
            GROUP BY "date", criterion
        ),
        direct_ad AS (
            -- Fallback CPC на уровне (date, ad_id) — для автотаргетинга и несовпадений.
            -- Метрика пишет "Autotargeting", Директ пишет "---": по ключу не совпадут,
            -- поэтому для таких визитов берём средний CPC по объявлению за день.
            SELECT
                "date",
                CAST(ad_id AS TEXT) AS ad_id,
                CASE WHEN SUM(clicks) > 0
                     THEN ROUND(CAST(SUM(cost) / SUM(clicks) AS NUMERIC), 4)
                     ELSE NULL END  AS cpc
            FROM direct_costs
            WHERE "date" < {_TODAY_MSK_DATE}
            GROUP BY "date", ad_id
        )
        SELECT
            mv.*,
            mv."ym:s:dateTime"::date AS dt,
            s.last_source,
            s.first_source,
            s.last_significant_source,
            s.last_campaign,
            s.first_campaign,
            s.last_significant_campaign,
            s.last_content,
            s.first_content,
            s.last_significant_content,
            s.last_phrase,
            s.first_phrase,
            s.last_significant_phrase,
            -- Креатив (Level 5): заголовок объявления и ID объявления —
            -- триплет first / last / last_significant по аналогии с L1-L4.
            s.last_ad_title,
            s.first_ad_title,
            s.last_significant_ad_title,
            s.last_ad_id,
            s.first_ad_id,
            s.last_significant_ad_id,
            vi.unified_user_id,
            vi.unified_user_id_source,
            {new_user_expr} AS is_new_unified_user,
            COALESCE(pa.payments_approved_count, 0)   AS payments_approved_count,
            COALESCE(pa.payments_failed_count,   0)   AS payments_failed_count,
            COALESCE(pa.payments_approved_sum,   0.0) AS payments_approved_sum,
            COALESCE(pa.payments_failed_sum,     0.0) AS payments_failed_sum,
            COALESCE(ra.registrations_count,     0)   AS registrations_count,
            CASE WHEN s.last_source LIKE 'Яндекс: Директ%'
                 THEN COALESCE(kw.cpc, ad.cpc) END    AS ad_cost_per_visit
        FROM metrika_visits mv
        LEFT JOIN sources     s  ON s.rid              = mv."ym:s:visitID"
        LEFT JOIN visitor_ids vi ON vi."ym:s:visitID"  = mv."ym:s:visitID"
        LEFT JOIN payment_agg pa ON pa."ym:s:visitID"  = mv."ym:s:visitID"
        LEFT JOIN reg_agg     ra ON ra."ym:s:visitID"  = mv."ym:s:visitID"
        LEFT JOIN direct_kw   kw ON (
            mv."ym:s:dateTime"::date = kw."date"::date
            AND CASE
                    WHEN mv."ym:s:cross_device_lastDirectPhraseOrCond" = 'Autotargeting'
                        THEN '---autotargeting'
                    ELSE REPLACE(mv."ym:s:cross_device_lastDirectPhraseOrCond",
                                 ' (ad display criteria)', '')
                END = kw.criterion
        )
        LEFT JOIN direct_ad   ad ON (
            mv."ym:s:dateTime"::date                         = ad."date"::date
            AND mv."ym:s:cross_device_lastDirectClickBanner" = ad.ad_id
        )
        WHERE mv."ym:s:dateTime" >= '{REPORT_START_DATE}'::timestamp
          -- Cutoff «до вчера» на сами визиты — страховка. Logs API в loader_metrika
          -- сам качает только до yesterday, но повторный фильтр здесь защищает от:
          --   а) ручных загрузок за сегодня;
          --   б) согласованности с конверсиями: иначе для сегодняшних визитов
          --      ad_cost/payments/regs = 0, и точка «сегодня» в дашборде искажает CR.
          AND mv."ym:s:dateTime" < {_TODAY_MSK_TS}
    """


# ---------------------------------------------------------------------------
# Атрибуция: общий фундамент для 4 моделей (linear, timedecay, first_touch, lsig_touch)
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Базовые CTE атрибуции — общие для визитов и хитов.
#   failed_filtered  — истинно неуспешные платежи (status≠approved + нет approved
#                      в течение 1ч). Бизнес-правило живёт ровно в одном месте.
#   all_payments     — платежи через unified_user_id_members. Имя таблицы members
#                      параметризовано: для визитов — `_new` (таблица ещё не
#                      подменена); для хитов — без `_new` (подмена уже произошла).
#   all_regs         — регистрации, аналогично через members.
#   <sig_cte>        — visit_sig для визитов / hit_sig для хитов; задаётся снаружи.
# Cutoff «до вчера» — через {_TODAY_MSK_TS} наверху файла.
# ---------------------------------------------------------------------------

def _build_failed_filtered_cte() -> str:
    """Единственная точка определения «истинно неуспешного» платежа.

    Неуспешным считается платёж со status≠approved, после которого
    у того же пользователя НЕТ approved в течение 1 часа.
    Если approved прошёл — это была попытка оплаты, не учитываем.

    Используется в двух местах:
      • _build_attribution_base_ctes  — attribution-модели visits
      • _get_visits_calculated_sql    — last-click в visits_calculated
    """
    return f"""failed_filtered AS (
    SELECT p.payment_id
    FROM mongo_payments p
    WHERE p.status != 'approved'
      AND p.created_at_moscow < {_TODAY_MSK_TS}
      AND NOT EXISTS (
          SELECT 1 FROM mongo_payments p2
          WHERE p2.mongo_user_id      = p.mongo_user_id
            AND p2.status             = 'approved'
            AND p2.created_at_moscow  > p.created_at_moscow
            AND p2.created_at_moscow <= p.created_at_moscow + INTERVAL '1 hour'
      )
)"""


def _build_attribution_base_ctes(members_table: str, sig_cte: str) -> str:
    """Возвращает SQL-текст базовых CTE атрибуции (без префикса 'WITH').

    Используется в loader_visits (4 модели атрибуции по визитам).
    Параметризовано имя таблицы `unified_user_id_members{_new}` и
    итоговый CTE с флагом is_sig.
    """
    return f"""
{_build_failed_filtered_cte()},
all_payments AS (
    SELECT
        uim.unified_user_id,
        p.payment_id,
        p.amount,
        p.created_at_moscow                                        AS event_dt,
        CASE WHEN p.status = 'approved' THEN 1 ELSE 0 END         AS is_approved,
        CASE WHEN p.status != 'approved'
                  AND ff.payment_id IS NOT NULL THEN 1 ELSE 0 END AS is_failed
    FROM mongo_payments p
    JOIN {members_table} uim ON uim.mongo_user_id = p.mongo_user_id
    LEFT JOIN failed_filtered ff ON ff.payment_id = p.payment_id
    WHERE p.created_at_moscow < {_TODAY_MSK_TS}
),
all_regs AS (
    SELECT uim.unified_user_id, u.mongo_user_id, u.created_at_moscow AS event_dt
    FROM mongo_users u
    JOIN {members_table} uim ON uim.mongo_user_id = u.mongo_user_id
    WHERE u.created_at_moscow < {_TODAY_MSK_TS}
),
{sig_cte}
"""


_VISIT_SIG_CTE = """visit_sig AS (
    SELECT
        "ym:s:visitID",
        unified_user_id,
        "ym:s:dateTime"                                              AS visit_dt,
        CASE WHEN last_source NOT IN ('Прямой переход', 'Внутренний переход')
             THEN 1 ELSE 0 END                                       AS is_sig
    FROM visits_calculated_new
)"""

_ATTRIBUTION_BASE_CTES = _build_attribution_base_ctes(
    members_table='unified_user_id_members_new',
    sig_cte=_VISIT_SIG_CTE,
)


def _attribution_update_template(suffix: str, payment_agg_cte: str,
                                 reg_agg_cte: str, payment_alias: str = 'pa',
                                 reg_alias: str = 'ra') -> str:
    """Унифицированный финальный блок: combined + UPDATE для модели `suffix`.

    payment_agg_cte и reg_agg_cte должны определять CTE с именами `payment_agg`
    и `reg_agg`, каждое с колонками: "ym:s:visitID", approved_count, approved_sum,
    failed_count, failed_sum (для регистраций — только reg_count).
    """
    return f"""
        ),
        combined AS (
            SELECT
                "ym:s:visitID" AS visit_id,
                COALESCE({payment_alias}.approved_count, 0) AS approved_count,
                COALESCE({payment_alias}.approved_sum,   0) AS approved_sum,
                COALESCE({payment_alias}.failed_count,   0) AS failed_count,
                COALESCE({payment_alias}.failed_sum,     0) AS failed_sum,
                COALESCE({reg_alias}.reg_count,          0) AS reg_count
            FROM payment_agg {payment_alias}
            FULL OUTER JOIN reg_agg {reg_alias} USING ("ym:s:visitID")
        )
        UPDATE visits_calculated_new vc
        SET
            payments_approved_count_{suffix} = c.approved_count,
            payments_approved_sum_{suffix}   = c.approved_sum,
            payments_failed_count_{suffix}   = c.failed_count,
            payments_failed_sum_{suffix}     = c.failed_sum,
            registrations_count_{suffix}     = c.reg_count
        FROM combined c
        WHERE vc."ym:s:visitID" = c.visit_id
    """


def _build_weighted_attribution_sql(suffix: str, weight_expr: str) -> str:
    """SQL UPDATE для weighted-моделей (linear, timedecay).

    Каждый визит до события получает вес `weight_expr`; конверсия делится между
    эligible-визитами пропорционально весам (нормализация по SUM(raw_weight)
    того же события). Eligible — значимые визиты, иначе fallback на все.

    `weight_expr` содержит плейсхолдер `{evt}`, который при использовании
    подставится либо как `ap` (для платежей), либо как `ar` (для регистраций).
    Linear: weight_expr = '1.0' (получаем долю 1/N как было в исходной модели).
    Timedecay: weight_expr = exp(-days*ln(2)/7), нормализованный по сумме весов.
    """
    weight_pay = weight_expr.format(evt='ap')
    weight_reg = weight_expr.format(evt='ar')

    body = f"""
        WITH {_ATTRIBUTION_BASE_CTES},
        payment_pairs AS (
            SELECT
                vs."ym:s:visitID", ap.payment_id, ap.amount,
                ap.is_approved, ap.is_failed, vs.is_sig,
                SUM(vs.is_sig) OVER (PARTITION BY ap.unified_user_id, ap.payment_id) AS total_sig_before,
                {weight_pay} AS raw_weight
            FROM all_payments ap
            JOIN visit_sig vs
                ON vs.unified_user_id = ap.unified_user_id
               AND vs.visit_dt <= ap.event_dt
        ),
        eligible_payment AS (
            SELECT
                "ym:s:visitID", payment_id, amount, is_approved, is_failed, raw_weight,
                SUM(raw_weight) OVER (PARTITION BY payment_id) AS weight_sum
            FROM payment_pairs
            WHERE (total_sig_before > 0 AND is_sig = 1) OR total_sig_before = 0
        ),
        payment_agg AS (
            SELECT
                "ym:s:visitID",
                SUM(is_approved * raw_weight / weight_sum)          AS approved_count,
                SUM(is_approved * amount * raw_weight / weight_sum)  AS approved_sum,
                SUM(is_failed  * raw_weight / weight_sum)            AS failed_count,
                SUM(is_failed  * amount * raw_weight / weight_sum)   AS failed_sum
            FROM eligible_payment
            GROUP BY "ym:s:visitID"
        ),
        reg_pairs AS (
            SELECT
                vs."ym:s:visitID", ar.mongo_user_id, vs.is_sig,
                SUM(vs.is_sig) OVER (PARTITION BY ar.unified_user_id, ar.mongo_user_id) AS total_sig_before,
                {weight_reg} AS raw_weight
            FROM all_regs ar
            JOIN visit_sig vs
                ON vs.unified_user_id = ar.unified_user_id
               AND vs.visit_dt <= ar.event_dt
        ),
        eligible_reg AS (
            SELECT
                "ym:s:visitID", mongo_user_id, raw_weight,
                SUM(raw_weight) OVER (PARTITION BY mongo_user_id) AS weight_sum
            FROM reg_pairs
            WHERE (total_sig_before > 0 AND is_sig = 1) OR total_sig_before = 0
        ),
        reg_agg AS (
            SELECT "ym:s:visitID", SUM(raw_weight / weight_sum) AS reg_count
            FROM eligible_reg
            GROUP BY "ym:s:visitID"
    """
    return body + _attribution_update_template(suffix, '', '')


def _build_single_visit_attribution_sql(suffix: str, picker_order_by: str) -> str:
    """SQL UPDATE для моделей «один-выигравший-визит» (first_touch, lsig_touch).

    Для каждого события (платёж/регистрация) выбирается ровно один визит того же
    пользователя, удовлетворяющий visit_dt ≤ event_dt и сортировке `picker_order_by`.
    Этот визит получает всю конверсию, остальные — 0.

    First-touch: ORDER BY vc."ym:s:dateTime" ASC (самый ранний визит до события).
    Lsig-touch: ORDER BY (is_sig DESC, dateTime DESC) — последний значимый,
    с автоматическим fallback на любой последний при отсутствии значимых.

    Замечание: исходная first_touch использовала отдельную CTE first_visits
    (DISTINCT ON unified_user_id) с последующим JOIN. Семантически эквивалентно
    DISTINCT ON (event_id) ORDER BY visit_dt ASC, потому что:
      - оба варианта берут единственный самый ранний визит пользователя до события;
      - оба исключают платежи без визита до них (visit_dt ≤ event_dt).
    """
    body = f"""
        WITH {_ATTRIBUTION_BASE_CTES},
        payment_picked AS (
            SELECT DISTINCT ON (ap.payment_id)
                vc."ym:s:visitID", ap.payment_id, ap.amount,
                ap.is_approved, ap.is_failed
            FROM all_payments ap
            JOIN visits_calculated_new vc
                ON vc.unified_user_id = ap.unified_user_id
               AND vc."ym:s:dateTime" <= ap.event_dt
            ORDER BY ap.payment_id, {picker_order_by}
        ),
        payment_agg AS (
            SELECT
                "ym:s:visitID",
                SUM(is_approved * 1.0)    AS approved_count,
                SUM(is_approved * amount) AS approved_sum,
                SUM(is_failed * 1.0)      AS failed_count,
                SUM(is_failed * amount)   AS failed_sum
            FROM payment_picked
            GROUP BY "ym:s:visitID"
        ),
        reg_picked AS (
            SELECT DISTINCT ON (ar.mongo_user_id)
                vc."ym:s:visitID", ar.mongo_user_id
            FROM all_regs ar
            JOIN visits_calculated_new vc
                ON vc.unified_user_id = ar.unified_user_id
               AND vc."ym:s:dateTime" <= ar.event_dt
            ORDER BY ar.mongo_user_id, {picker_order_by}
        ),
        reg_agg AS (
            SELECT "ym:s:visitID", COUNT(*) AS reg_count
            FROM reg_picked
            GROUP BY "ym:s:visitID"
    """
    return body + _attribution_update_template(suffix, '', '')


# Конфигурация моделей атрибуции. Suffix = имя группы колонок visits_calculated.
_ATTRIBUTION_MODELS = [
    ('linear',      'weighted', '1.0'),
    ('timedecay',   'weighted',
         'EXP(-EXTRACT(EPOCH FROM ({evt}.event_dt - vs.visit_dt)) / 86400.0 * LN(2) / 7.0)'),
    ('first_touch', 'single',   'vc."ym:s:dateTime" ASC'),
    ('lsig_touch',  'single',
         "CASE WHEN vc.last_source NOT IN ('Прямой переход', 'Внутренний переход') "
         "THEN 1 ELSE 0 END DESC, vc.\"ym:s:dateTime\" DESC"),
]

_ATTRIBUTION_METRIC_TEMPLATES = [
    'payments_approved_count_{suffix}',
    'payments_approved_sum_{suffix}',
    'payments_failed_count_{suffix}',
    'payments_failed_sum_{suffix}',
    'registrations_count_{suffix}',
]


def _build_attribution_sql(suffix: str, kind: str, expr: str) -> str:
    if kind == 'weighted':
        return _build_weighted_attribution_sql(suffix, expr)
    if kind == 'single':
        return _build_single_visit_attribution_sql(suffix, expr)
    raise ValueError(f"Unknown attribution kind: {kind}")


# ---------------------------------------------------------------------------
# Обновление visits_calculated
# ---------------------------------------------------------------------------

def update_visits_calculated():
    """Пересоздаёт visits_calculated и вспомогательные таблицы маппинга.

    Схема атомарной подмены (visits_calculated доступна во время пересчёта):
      1. Строим clientid_to_unified_new, unified_user_id_members_new, visits_calculated_new
      2. Создаём индексы на _new-таблицах
      3. Быстрая подмена: DROP старых → RENAME новых (одна транзакция, миллисекунды)
      4. ANALYZE
    """
    # Гарантируем, что зависимые таблицы существуют (даже пустыми),
    # чтобы LEFT JOIN в _get_visits_calculated_sql() не падал при первом запуске
    with engine.begin() as conn:
        conn.execute(text('''
            CREATE TABLE IF NOT EXISTS clientid_to_mongoid (
                metrika_client_id TEXT,
                mongo_user_id     TEXT
            )
        '''))
        conn.execute(text('''
            CREATE TABLE IF NOT EXISTS mongo_payments (
                payment_id        TEXT,
                mongo_user_id     TEXT,
                amount            DOUBLE PRECISION,
                status            TEXT,
                provider          TEXT,
                created_at        TIMESTAMP,
                created_at_moscow TIMESTAMP
            )
        '''))
        conn.execute(text('''
            CREATE TABLE IF NOT EXISTS mongo_users (
                mongo_user_id     TEXT,
                created_at        TIMESTAMP,
                created_at_moscow TIMESTAMP,
                platform          TEXT
            )
        '''))
        conn.execute(text('''
            CREATE TABLE IF NOT EXISTS direct_costs (
                date           DATE,
                campaign_id    BIGINT,
                campaign_name  TEXT,
                ad_group_name  TEXT,
                ad_id          TEXT,
                impressions    INTEGER,
                clicks         INTEGER,
                cost_micros    BIGINT,
                cost           DOUBLE PRECISION
            )
        '''))
        conn.execute(text('''
            CREATE TABLE IF NOT EXISTS direct_criteria_costs (
                date           DATE,
                campaign_id    BIGINT,
                campaign_name  TEXT,
                ad_group_name  TEXT,
                criterion      TEXT,
                criterion_type TEXT,
                impressions    INTEGER,
                clicks         INTEGER,
                cost_micros    BIGINT,
                cost           DOUBLE PRECISION
            )
        '''))
        # Справочник РК Директа: campaign_id → последнее актуальное название.
        # Если sync_direct_costs/criteria по какой-то причине не отработал, таблица
        # всё равно существует пустой, и LEFT JOIN в visits_calculated не упадёт.
        conn.execute(text('''
            CREATE TABLE IF NOT EXISTS direct_campaigns (
                campaign_id   BIGINT PRIMARY KEY,
                campaign_name TEXT,
                updated_at    TIMESTAMP DEFAULT NOW()
            )
        '''))
        # Версионный справочник креативов Директа (SCD Type 2): каждая
        # активная версия имеет valid_to=NULL. Если sync_direct_ads ещё не
        # отработал — таблица создаётся пустой, JOIN в visits_calculated даёт NULL.
        conn.execute(text('''
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
        '''))
        conn.execute(text("ALTER TABLE direct_ads ADD COLUMN IF NOT EXISTS image_hash TEXT"))
        # Справочник картинок объявлений (URL'ы Yandex.Direct CDN). JOIN
        # по image_hash в pre_sources. Если sync_direct_ad_images не отработал —
        # таблица создаётся пустой, картинки в галерее будут NULL.
        conn.execute(text('''
            CREATE TABLE IF NOT EXISTS direct_ad_images (
                image_hash   TEXT,
                name         TEXT,
                type         TEXT,
                subtype      TEXT,
                original_url TEXT,
                preview_url  TEXT
            )
        '''))

    # clientid_to_unified: один clientID → один unified_user_id (склейка через '|' при коллизиях).
    unified_sql = """
        CREATE TABLE clientid_to_unified_new AS
        SELECT
            metrika_client_id,
            STRING_AGG(mongo_user_id, '|' ORDER BY mongo_user_id) AS unified_user_id,
            COUNT(mongo_user_id)                                   AS mongo_count
        FROM clientid_to_mongoid
        GROUP BY metrika_client_id
    """

    # unified_user_id_members: расщепляет составные ID обратно для JOIN с платежами/Mongo.
    # DISTINCT обязателен: если у одного пользователя несколько clientID, замапленных
    # на тот же mongoID (типичный случай — смена устройства/браузера, ~6% юзеров),
    # JOIN clientid_to_unified × clientid_to_mongoid даёт одинаковую пару (uid, mongoID)
    # столько раз, сколько у пользователя clientID. Без DISTINCT в JOIN с mongo_payments
    # один и тот же платёж учитывается N раз. Уникальный индекс ниже — страховка
    # от будущего регресса.
    members_sql = """
        CREATE TABLE unified_user_id_members_new AS
        SELECT DISTINCT cu.unified_user_id, ctm.mongo_user_id
        FROM clientid_to_unified_new cu
        JOIN clientid_to_mongoid ctm ON ctm.metrika_client_id = cu.metrika_client_id
    """

    index_queries_new = [
        'CREATE INDEX IF NOT EXISTS idx_calculated_visitID_new  ON visits_calculated_new ("ym:s:visitID")',
        'CREATE INDEX IF NOT EXISTS idx_visits_datetime_new     ON visits_calculated_new ("ym:s:dateTime")',
        'CREATE INDEX IF NOT EXISTS idx_visits_dt_new           ON visits_calculated_new ("dt")',
        'CREATE INDEX IF NOT EXISTS idx_visits_source_new2      ON visits_calculated_new (last_significant_source)',
        'CREATE INDEX IF NOT EXISTS idx_perf_main_new           ON visits_calculated_new ("dt", last_significant_source, "ym:s:isNewUser", "ym:s:counterUserIDHash")',
        'CREATE INDEX IF NOT EXISTS idx_visits_source_isnew_new ON visits_calculated_new (last_significant_source, "ym:s:isNewUser")',
        'CREATE INDEX IF NOT EXISTS idx_visits_country_new      ON visits_calculated_new ("ym:s:regionCountry")',
        'CREATE INDEX IF NOT EXISTS idx_visits_city_new         ON visits_calculated_new ("ym:s:regionCity")',
        'CREATE INDEX IF NOT EXISTS idx_visits_browser_new      ON visits_calculated_new ("ym:s:browser")',
        'CREATE INDEX IF NOT EXISTS idx_visits_lang_new         ON visits_calculated_new ("ym:s:browserLanguage")',
        'CREATE INDEX IF NOT EXISTS idx_visits_device_new       ON visits_calculated_new ("ym:s:deviceCategory")',
        'CREATE INDEX IF NOT EXISTS idx_visits_unified_user_new ON visits_calculated_new (unified_user_id)',
        'CREATE INDEX IF NOT EXISTS idx_visits_campaign_new     ON visits_calculated_new (last_significant_campaign)',
        'CREATE INDEX IF NOT EXISTS idx_visits_content_new      ON visits_calculated_new (last_significant_content)',
        'CREATE INDEX IF NOT EXISTS idx_perf_campaign_new       ON visits_calculated_new (dt, last_significant_source, last_significant_campaign)',
        'CREATE INDEX IF NOT EXISTS idx_perf_campaign_dt_new    ON visits_calculated_new (dt, last_significant_campaign)',
        'CREATE INDEX IF NOT EXISTS idx_dt_city_new             ON visits_calculated_new (dt, "ym:s:regionCity")',
        'CREATE INDEX IF NOT EXISTS idx_dt_country_new          ON visits_calculated_new (dt, "ym:s:regionCountry")',
        'CREATE INDEX IF NOT EXISTS idx_dt_browser_new          ON visits_calculated_new (dt, "ym:s:browser")',
        'CREATE INDEX IF NOT EXISTS idx_dt_lang_new             ON visits_calculated_new (dt, "ym:s:browserLanguage")',
        'CREATE INDEX IF NOT EXISTS idx_dt_device_new           ON visits_calculated_new (dt, "ym:s:deviceCategory")',
        'CREATE INDEX IF NOT EXISTS idx_dt_uid_new              ON visits_calculated_new (dt, unified_user_id)',
        'CREATE INDEX IF NOT EXISTS idx_dt_first_src_new        ON visits_calculated_new (dt, first_source)',
        'CREATE INDEX IF NOT EXISTS idx_dt_last_src_new         ON visits_calculated_new (dt, last_source)',
    ]

    # Шаг 1: очищаем хвосты предыдущего упавшего запуска
    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS visits_calculated_new"))
        conn.execute(text("DROP TABLE IF EXISTS clientid_to_unified_new"))
        conn.execute(text("DROP TABLE IF EXISTS unified_user_id_members_new"))
        # устаревшие/дублирующие индексы на старой таблице (если ещё существуют)
        conn.execute(text("DROP INDEX IF EXISTS idx_perf_optimization"))
        conn.execute(text("DROP INDEX IF EXISTS idx_visits_dt_max"))
        conn.execute(text("DROP INDEX IF EXISTS idx_campaign_new"))

    # Шаг 2: строим новые таблицы — visits_calculated остаётся доступной всё это время
    log.info("Строим visits_calculated_new (полный пересчёт)...")
    with engine.begin() as conn:
        conn.execute(text(unified_sql))
        # UNIQUE-индекс одновременно служит способом lookup и защитой от дублей:
        # если ETL вдруг построит таблицу с дубликатами по metrika_client_id —
        # CREATE UNIQUE INDEX упадёт, и проблема обнаружится сразу, а не через
        # завышенные цифры в дашборде.
        conn.execute(text(
            "CREATE UNIQUE INDEX IF NOT EXISTS uq_ctu_client_new "
            "ON clientid_to_unified_new (metrika_client_id)"
        ))
        conn.execute(text(members_sql))
        # Аналогичный UNIQUE-индекс на (uid, mongoID): дублей быть не должно
        # (см. комментарий в members_sql про DISTINCT).
        conn.execute(text(
            "CREATE UNIQUE INDEX IF NOT EXISTS uq_uid_members_pair_new "
            "ON unified_user_id_members_new (unified_user_id, mongo_user_id)"
        ))
        # Дополнительный обычный индекс на mongo_user_id — для быстрого JOIN
        # с mongo_payments / mongo_users (часто встречается в WHERE).
        conn.execute(text(
            "CREATE INDEX IF NOT EXISTS idx_uid_members_mongo_new "
            "ON unified_user_id_members_new (mongo_user_id)"
        ))
        conn.execute(text(_get_visits_calculated_sql("_new")))

        # Добавляем 20 колонок атрибуции (4 модели × 5 метрик), DEFAULT 0.
        log.info("Добавляем колонки атрибуции в visits_calculated_new (4 модели × 5 метрик)...")
        for suffix, _, _ in _ATTRIBUTION_MODELS:
            for col_template in _ATTRIBUTION_METRIC_TEMPLATES:
                col = col_template.format(suffix=suffix)
                conn.execute(text(
                    f'ALTER TABLE visits_calculated_new ADD COLUMN "{col}" DOUBLE PRECISION DEFAULT 0'
                ))

        # Заполняем все 4 модели через единый шаблон (см. _build_attribution_sql).
        for suffix, kind, expr in _ATTRIBUTION_MODELS:
            log.info("Рассчитываем %s-атрибуцию конверсий (%s)...", suffix, kind)
            conn.execute(text(_build_attribution_sql(suffix, kind, expr)))

        for q in index_queries_new:
            conn.execute(text(q))

    # Шаг 3: атомарная подмена — быстро, visits_calculated недоступна миллисекунды
    log.info("Подменяем visits_calculated...")
    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS visits_calculated"))
        conn.execute(text("DROP TABLE IF EXISTS clientid_to_unified"))
        conn.execute(text("DROP TABLE IF EXISTS unified_user_id_members"))
        conn.execute(text("ALTER TABLE visits_calculated_new RENAME TO visits_calculated"))
        conn.execute(text("ALTER TABLE clientid_to_unified_new RENAME TO clientid_to_unified"))
        conn.execute(text("ALTER TABLE unified_user_id_members_new RENAME TO unified_user_id_members"))
    log.info("visits_calculated и вспомогательные таблицы подменены.")

    with engine.connect() as conn:
        conn.execute(text("ANALYZE"))
        conn.commit()
    log.info("ANALYZE выполнен — статистика индексов обновлена.")
