"""
Точка входа. Запускается через `docker-compose run --rm loader`.

Порядок шагов намеренно фиксирован:
  1. visits — сырые данные из Метрики (с дедупликацией после докачки)
  2. MongoDB-зеркала (рефереры, маппинг, платежи, пользователи)
  3. Яндекс.Директ — расходы + контент креативов (SCD Type 2)
  4. visits_calculated — зависит от всех предыдущих таблиц
  5. data_quality — подводим итог после полного обновления
  6. cache warming — прогреваем Redis-кэш Superset

Весь прогон обёрнут в общий try/except: статус «success / failed» и текст
последней ошибки записываются в таблицу `loader_state` (ключи last_run_*).
Так оператор видит из БД, чем закончился последний прогон, без чтения логов.
"""
from datetime import datetime

from loader_config       import log, VISIT_FIELDS
from loader_metrika      import (
    download_logs, _deduplicate_by_id, sync_clientid_mapping, migrate_column_types,
    _set_state,
)
from loader_mongo        import sync_referrers, sync_payments, sync_users
from loader_direct       import sync_direct_costs, sync_direct_criteria_costs, update_direct_campaigns
from loader_direct_ads   import sync_direct_ads, sync_direct_ad_images
from loader_visits       import update_visits_calculated
from loader_quality      import update_data_quality
from loader_cache_warmer  import warm_up_superset_cache


def _run_pipeline():
    log.info("=== Начало синхронизации ===")
    _set_state('last_run_started_at', datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    _set_state('last_run_status',     'running')
    _set_state('last_run_error',      '')

    migrate_column_types()

    download_logs('visits', VISIT_FIELDS, 'metrika_visits', '"ym:s:dateTime"')
    _deduplicate_by_id('metrika_visits', '"ym:s:visitID"')

    sync_referrers()
    sync_clientid_mapping()
    sync_payments()
    sync_users()

    sync_direct_costs()
    sync_direct_criteria_costs()
    update_direct_campaigns()
    sync_direct_ads()
    sync_direct_ad_images()

    update_visits_calculated()
    update_data_quality()

    warm_up_superset_cache()

    log.info("=== Синхронизация завершена ===")


if __name__ == "__main__":
    try:
        _run_pipeline()
        _set_state('last_run_status',      'success')
        _set_state('last_run_finished_at', datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    except Exception as e:
        # log.exception сохранит traceback в loader.log; статусы — в БД для дашборда.
        log.exception("ETL завершился с ошибкой.")
        try:
            _set_state('last_run_status',      'failed')
            _set_state('last_run_finished_at', datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
            _set_state('last_run_error',       f'{type(e).__name__}: {e}'[:1000])
        except Exception:
            log.exception("Не удалось записать статус ошибки в loader_state.")
        raise  # ненулевой exit code, чтобы внешний оркестратор увидел сбой
