"""
Точка входа для today-loader. Параллельная ветка к metrika_loader.py:
  - Грузит свежие данные за today из Метрики Reporting API, Директ API,
    MongoDB в отдельные таблицы today_*.
  - Собирает финальную таблицу today_dataset для дашборда «За сегодня».
  - Не модифицирует ни одну существующую таблицу основного ETL.

Запуск:
    docker-compose run --rm loader python metrika_loader_today.py

Запускается через контейнер loader_today циклом каждые 30 мин (while true; do ... sleep 1800).
Работает быстро, поскольку не зависит от Logs API (тяжёлый шаг основного ETL).
"""
from datetime import datetime

from loader_config import log
from loader_today  import sync_today


if __name__ == "__main__":
    started = datetime.now()
    try:
        sync_today()
        log.info("today-loader: успех (за %.1f с).",
                 (datetime.now() - started).total_seconds())
    except Exception:
        log.exception("today-loader: завершился с ошибкой.")
        raise
