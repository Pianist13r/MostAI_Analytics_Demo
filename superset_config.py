import os

SQLALCHEMY_DATABASE_URI = (
    f"postgresql+psycopg2://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}"
    f"@db/{os.getenv('POSTGRES_DB')}"
)

PREVENT_UNSAFE_DB_CONNECTIONS = False

_REDIS_URL = "redis://redis:6379/1"

CACHE_CONFIG = {
    "CACHE_TYPE": "RedisCache",
    "CACHE_DEFAULT_TIMEOUT": 86400,
    "CACHE_KEY_PREFIX": "superset_",
    "CACHE_REDIS_URL": _REDIS_URL,
}

DATA_CACHE_CONFIG = {
    "CACHE_TYPE": "RedisCache",
    "CACHE_DEFAULT_TIMEOUT": 3600,
    "CACHE_KEY_PREFIX": "superset_data_",
    "CACHE_REDIS_URL": _REDIS_URL,
}

FILTER_STATE_CACHE_CONFIG = {
    "CACHE_TYPE": "RedisCache",
    "CACHE_DEFAULT_TIMEOUT": 86400,
    "CACHE_KEY_PREFIX": "superset_filter_",
    "CACHE_REDIS_URL": _REDIS_URL,
}

EXPLORE_FORM_DATA_CACHE_CONFIG = {
    "CACHE_TYPE": "RedisCache",
    "CACHE_DEFAULT_TIMEOUT": 86400,
    "CACHE_KEY_PREFIX": "superset_explore_",
    "CACHE_REDIS_URL": _REDIS_URL,
}

ENABLE_PROXY_FIX = True
OVERRIDE_BASE_URL = os.getenv("SUPERSET_BASE_URL", "http://localhost:8088")

TALISMAN_ENABLED = True
TALISMAN_CONFIG = {
    "content_security_policy": {
        "base-uri":        ["'self'"],
        "default-src":     ["'self'"],
        "img-src":         ["'self'", "data:", "blob:", "https://direct.yandex.ru"],
        "worker-src":      ["'self'", "blob:"],
        "connect-src":     ["'self'"],
        "object-src":      ["'none'"],
        "style-src":       ["'self'", "'unsafe-inline'"],
        "script-src":      ["'self'", "'strict-dynamic'", "'unsafe-eval'"],
        "frame-ancestors": ["'self'"],
        "font-src":        ["'self'", "data:"],
    },
    "content_security_policy_nonce_in": ["script-src"],
    "force_https": True,
    "session_cookie_secure": True,
    "session_cookie_samesite": "Lax",
}

RATELIMIT_STORAGE_URI = "redis://redis:6379/0"

PREFERRED_URL_SCHEME = "https"

SUPERSET_WEBSERVER_TIMEOUT = 300
SQLLAB_TIMEOUT = 300

FEATURE_FLAGS = {
    "ENABLE_TEMPLATE_PROCESSING": True,
}

# Отключена для поддержки Handlebars-чартов с кастомным HTML.
HTML_SANITIZATION = False

LANGUAGES = {
    "en": {"flag": "us", "name": "English"},
    "ru": {"flag": "ru", "name": "Russian"},
}
BABEL_DEFAULT_LOCALE = "ru"


def FLASK_APP_MUTATOR(app):
    """Фиксирует dark-режим и скрывает переключатель темы."""
    _DARK_LOCK_HTML = b"""<style>
li.ant-menu-submenu:has([data-menu-id*="theme-sub-menu"]) { display: none !important; }
</style>
<script>
(function () {
  try { localStorage.setItem('superset-theme-mode', 'dark'); } catch (_) {}
})();
</script>
"""

    @app.after_request
    def _inject_dark_lock(response):
        if not (response.content_type and response.content_type.startswith("text/html")):
            return response
        try:
            response.direct_passthrough = False
            data = response.get_data()
            if b"</head>" in data:
                response.set_data(data.replace(b"</head>", _DARK_LOCK_HTML + b"</head>", 1))
        except Exception:
            pass
        return response


EXTRA_CATEGORICAL_COLOR_SCHEMES = [
    {
        "id": "mostai_sources",
        "label": "MostAI Sources",
        "description": "Фирменная схема для поля «Источник» — важные акцентные, остальные приглушены.",
        "isDefault": False,
        "colors": [
            "#B6FF3C",  # lime — Яндекс: Директ (главный)
            "#7C5CFF",  # purple — Реферальная программа
            "#00D4B8",  # teal — Прямой переход
            "#3DD9EB",  # cyan — Органический поиск
            "#9BE61F",  # lime-2 — Яндекс: Директ, Поиск
            "#6FCC0E",  # lime-3 — Яндекс: Директ, Сети
            "#5B8DEF",  # blue — ВКонтакте
            "#3D6FE0",  # blue-2 — ВК реклама
            "#4FB3FF",  # sky — Telegram
            "#2E92E8",  # sky-2 — Telegram main
            "#1B6FBD",  # sky-3 — Telegram community
            "#FF7AA8",  # pink — Социальные сети
            "#FF9F43",  # orange — Дзен
            "#E8842B",  # orange-2 — Дзен продвижение
            "#FFB454",  # warm — Одноклассники
            "#C792EA",  # lilac — Мессенджеры
            "#A78BFA",  # lilac-2 — Рекомендательные системы
            "#88909C",  # neutral — Внутренний переход
            "#6B6F76",  # neutral-2 — Прочие сайты
            "#7C7F88",  # neutral-3 — Прочая реклама
            "#5F636B",  # neutral-4 — Прочие рекламные сети
        ],
    }
]
