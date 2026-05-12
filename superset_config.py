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
        "img-src":         ["'self'", "data:", "blob:"],
        "worker-src":      ["'self'", "blob:"],
        "connect-src":     ["'self'"],
        "object-src":      ["'none'"],
        "style-src":       ["'self'", "'unsafe-inline'"],
        "script-src":      ["'self'", "'strict-dynamic'", "'unsafe-eval'"],
        "frame-ancestors": ["'self'"],
        "font-src":        ["'self'", "data:"],
    },
    "content_security_policy_nonce_in": ["script-src"],
    "force_https": False,
    "session_cookie_secure": False,
    "session_cookie_samesite": "Lax",
}

RATELIMIT_STORAGE_URI = "redis://redis:6379/0"

PREFERRED_URL_SCHEME = "http"

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
        "id": "analytics_sources",
        "label": "Analytics Sources",
        "description": "Color scheme for traffic source breakdowns.",
        "isDefault": False,
        "colors": [
            "#B6FF3C",
            "#7C5CFF",
            "#00D4B8",
            "#3DD9EB",
            "#9BE61F",
            "#6FCC0E",
            "#5B8DEF",
            "#3D6FE0",
            "#4FB3FF",
            "#2E92E8",
            "#1B6FBD",
            "#FF7AA8",
            "#FF9F43",
            "#E8842B",
            "#FFB454",
            "#C792EA",
            "#A78BFA",
            "#88909C",
            "#6B6F76",
            "#7C7F88",
            "#5F636B",
        ],
    }
]
