"""
Конфигурация, логирование и общие константы.

Этот модуль импортируется всеми остальными: `from loader_config import log, engine, ...`
Никакой логики здесь нет — только инициализация из переменных окружения.
"""
import logging
import os
import urllib.parse
from logging.handlers import RotatingFileHandler
from pathlib import Path

from sqlalchemy import create_engine, text

# ---------------------------------------------------------------------------
# Логирование — вывод в консоль и в файл одновременно.
# Файл ротируется: 10 MB на сегмент × 5 архивов = до 60 MB суммарно
# (loader.log + loader.log.1 … loader.log.5).
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler(),
        RotatingFileHandler(
            '/app/data/loader.log',
            maxBytes=10 * 1024 * 1024,
            backupCount=5,
            encoding='utf-8',
        ),
    ],
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Cutoff «снизу»: визиты, платежи, регистрации раньше этой даты не попадают
# в visits_calculated/hits_calculated. Исторические данные у Метрики/MongoDB
# есть и до неё, но в отчётности и атрибуции не используются.
# Меняется единообразно во всех модулях ETL — ровно одна точка правды.
# ---------------------------------------------------------------------------
REPORT_START_DATE = '2025-09-01'

# ---------------------------------------------------------------------------
# Яндекс.Метрика
# ---------------------------------------------------------------------------
TOKEN      = os.environ['YANDEX_TOKEN']
COUNTER_ID = os.environ['YANDEX_COUNTER_ID']
API_BASE   = f"https://api-metrika.yandex.net/management/v1/counter/{COUNTER_ID}"
STAT_API   = "https://api-metrika.yandex.net/stat/v1/data"
HEADERS    = {'Authorization': f'OAuth {TOKEN}'}

# ---------------------------------------------------------------------------
# Яндекс.Директ
# ---------------------------------------------------------------------------
DIRECT_API     = "https://api.direct.yandex.com/json/v5/reports"
DIRECT_HEADERS = {
    'Authorization':       f'Bearer {TOKEN}',
    'Accept-Language':     'ru',
    'skipReportHeader':    'true',
    'skipColumnHeader':    'false',
    'skipReportSummary':   'true',
    'returnMoneyInMicros': 'true',
    'Content-Type':        'application/json',
    'Client-Login':        os.getenv('DIRECT_CLIENT_LOGIN', ''),
}

# ---------------------------------------------------------------------------
# MongoDB
# ---------------------------------------------------------------------------
MONGO_USER       = os.environ['MONGO_USER']
MONGO_PASSWORD   = os.environ['MONGO_PASSWORD']
MONGO_HOST       = os.environ['MONGO_HOST']
MONGO_DB_NAME    = os.environ['MONGO_DB_NAME']
MONGO_COLLECTION = os.getenv('MONGO_COLLECTION', 'user')

MONGO_URI = (
    f"mongodb://{urllib.parse.quote_plus(MONGO_USER)}"
    f":{urllib.parse.quote_plus(MONGO_PASSWORD)}"
    f"@{MONGO_HOST}/{MONGO_DB_NAME}?authSource=admin"
)

# ---------------------------------------------------------------------------
# PostgreSQL (аналитическая БД — те же реквизиты, что у Superset metastore)
# ---------------------------------------------------------------------------
_pg_user     = os.environ['POSTGRES_USER']
_pg_password = os.environ['POSTGRES_PASSWORD']
_pg_host     = os.getenv('POSTGRES_HOST', 'db')
_pg_db       = os.environ['POSTGRES_DB']

engine = create_engine(
    f"postgresql+psycopg2://{_pg_user}:{_pg_password}@{_pg_host}/{_pg_db}",
    pool_size=5,
    max_overflow=10,
    pool_pre_ping=True,
    connect_args={"options": "-c search_path=analytics"},
)

# ---------------------------------------------------------------------------
# Инициализация аналитических SQL-функций.
# Выполняется при каждом старте ETL — CREATE OR REPLACE идемпотентен.
# Функции хранятся в схеме analytics и пережили бы drop analytics schema,
# но не пережили бы полный сброс кластера PostgreSQL — поэтому пересоздаём.
# ---------------------------------------------------------------------------
_SQL_FUNCTIONS_FILE = Path(__file__).parent / "analytics_functions.sql"


def _ensure_analytics_functions() -> None:
    sql = _SQL_FUNCTIONS_FILE.read_text(encoding="utf-8")
    with engine.begin() as conn:
        conn.execute(text(sql))


try:
    _ensure_analytics_functions()
    log.info("analytics SQL functions: OK")
except Exception as _e:
    log.warning("analytics SQL functions: %s", _e)

# ---------------------------------------------------------------------------
# Поля для выгрузки через Logs API
# ---------------------------------------------------------------------------
VISIT_FIELDS = ",".join([
    "ym:s:visitID",
    "ym:s:watchIDs",
    "ym:s:dateTime",
    "ym:s:isNewUser",
    "ym:s:startURL",
    "ym:s:endURL",
    "ym:s:pageViews",
    "ym:s:visitDuration",
    "ym:s:bounce",
    "ym:s:regionCountry",
    "ym:s:regionCity",
    "ym:s:clientID",
    "ym:s:counterUserIDHash",
    "ym:s:goalsID",
    "ym:s:goalsDateTime",
    "ym:s:goalsPrice",
    "ym:s:cross_device_firstTrafficSource",
    "ym:s:cross_device_lastTrafficSource",
    "ym:s:cross_device_last_significantTrafficSource",
    "ym:s:cross_device_firstAdvEngine",
    "ym:s:cross_device_lastAdvEngine",
    "ym:s:cross_device_firstReferalSource",
    "ym:s:cross_device_lastReferalSource",
    "ym:s:cross_device_last_significantReferalSource",
    "ym:s:cross_device_firstSearchEngine",
    "ym:s:cross_device_lastSearchEngine",
    "ym:s:cross_device_firstSocialNetwork",
    "ym:s:cross_device_lastSocialNetwork",
    "ym:s:cross_device_firstSocialNetworkProfile",
    "ym:s:cross_device_lastSocialNetworkProfile",
    "ym:s:referer",
    "ym:s:cross_device_firstDirectClickOrder",
    "ym:s:cross_device_lastDirectClickOrder",
    "ym:s:cross_device_firstDirectBannerGroup",
    "ym:s:cross_device_lastDirectBannerGroup",
    "ym:s:cross_device_firstDirectClickBanner",
    "ym:s:cross_device_lastDirectClickBanner",
    "ym:s:cross_device_firstDirectClickOrderName",
    "ym:s:cross_device_lastDirectClickOrderName",
    "ym:s:cross_device_firstClickBannerGroupName",
    "ym:s:cross_device_lastClickBannerGroupName",
    "ym:s:cross_device_firstDirectClickBannerName",
    "ym:s:cross_device_lastDirectClickBannerName",
    "ym:s:cross_device_firstDirectPhraseOrCond",
    "ym:s:cross_device_lastDirectPhraseOrCond",
    "ym:s:cross_device_firstDirectPlatformType",
    "ym:s:cross_device_lastDirectPlatformType",
    "ym:s:cross_device_last_significantDirectPlatformType",
    "ym:s:cross_device_firstDirectPlatform",
    "ym:s:cross_device_lastDirectPlatform",
    "ym:s:cross_device_firstDirectConditionType",
    "ym:s:cross_device_lastDirectConditionType",
    "ym:s:from",
    "ym:s:cross_device_firstUTMCampaign",
    "ym:s:cross_device_lastUTMCampaign",
    "ym:s:cross_device_firstUTMContent",
    "ym:s:cross_device_lastUTMContent",
    "ym:s:cross_device_firstUTMMedium",
    "ym:s:cross_device_lastUTMMedium",
    "ym:s:cross_device_firstUTMSource",
    "ym:s:cross_device_lastUTMSource",
    "ym:s:cross_device_last_significantUTMSource",
    "ym:s:cross_device_firstUTMTerm",
    "ym:s:cross_device_lastUTMTerm",
    "ym:s:cross_device_firstopenstatAd",
    "ym:s:cross_device_firstopenstatCampaign",
    "ym:s:cross_device_firstopenstatService",
    "ym:s:cross_device_firstopenstatSource",
    "ym:s:cross_device_firsthasGCLID",
    "ym:s:cross_device_firstGCLID",
    "ym:s:cross_device_firstRecommendationSystem",
    "ym:s:cross_device_lastRecommendationSystem",
    "ym:s:cross_device_firstMessenger",
    "ym:s:cross_device_lastMessenger",
    "ym:s:browserLanguage",
    "ym:s:browserCountry",
    "ym:s:clientTimeZone",
    "ym:s:deviceCategory",
    "ym:s:mobilePhone",
    "ym:s:mobilePhoneModel",
    "ym:s:operatingSystemRoot",
    "ym:s:operatingSystem",
    "ym:s:browser",
    "ym:s:browserMajorVersion",
    "ym:s:browserMinorVersion",
    "ym:s:browserEngine",
    "ym:s:cookieEnabled",
    "ym:s:javascriptEnabled",
    "ym:s:screenFormat",
    "ym:s:parsedParamsKey1",
    "ym:s:parsedParamsKey2",
    "ym:s:parsedParamsKey3",
    "ym:s:parsedParamsKey4",
    "ym:s:parsedParamsKey5",
    "ym:s:parsedParamsKey6",
    "ym:s:parsedParamsKey7",
    "ym:s:parsedParamsKey8",
    "ym:s:parsedParamsKey9",
    "ym:s:parsedParamsKey10",
])

HIT_FIELDS = ",".join([
    "ym:pv:watchID",
    "ym:pv:visitID",            # связь с visits_calculated
    "ym:pv:counterUserIDHash",  # резерв, если visitID пропустит API
    "ym:pv:clientID",
    "ym:pv:dateTime",
    "ym:pv:URL",
    "ym:pv:title",
    "ym:pv:referer",            # источник перехода / внутренний referer
    "ym:pv:goalsID",            # массив достигнутых целей в этом просмотре
    "ym:pv:isPageView",         # 1 = настоящий просмотр страницы, 0 = event-хит
    "ym:pv:notBounce",          # 1 = засчитанный непокинутый просмотр
])
