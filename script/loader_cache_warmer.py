"""
Прогрев кэша Superset после обновления данных.

Вызывает PUT /api/v1/chart/warm_up_cache для всех чартов основного дашборда.
Запускается в конце metrika_loader.py — утром дашборд открывается мгновенно из Redis.
"""
import os
import time
from typing import Optional

import redis
import requests

from loader_config import log

SUPERSET_URL = os.getenv('SUPERSET_INTERNAL_URL', 'http://superset:8088')
REDIS_URL = os.getenv('REDIS_URL', 'redis://redis:6379/1')
DASHBOARD_ID = int(os.getenv('SUPERSET_DASHBOARD_ID', '2'))


def _flush_data_cache() -> int:
    """Удаляет все ключи superset_data_* из Redis — кэш запросов чартов."""
    try:
        r = redis.from_url(REDIS_URL, decode_responses=False)
        keys = r.keys('superset_data_*')
        if keys:
            deleted = r.delete(*keys)
            log.info("Cache invalidation: удалено %d устаревших ключей кэша данных.", deleted)
            return deleted
        log.info("Cache invalidation: кэш данных пуст, нечего удалять.")
        return 0
    except Exception as e:
        log.error("Cache invalidation: ошибка очистки Redis: %s", e)
        return 0


def _init_session() -> Optional[requests.Session]:
    """Возвращает Session с JWT + CSRF, готовую к API-запросам."""
    username = os.getenv('SUPERSET_ADMIN_USER', 'admin')
    password = os.getenv('SUPERSET_ADMIN_PASSWORD', '')
    if not password:
        log.warning("Cache warming: SUPERSET_ADMIN_PASSWORD не задан — пропускаем.")
        return None
    try:
        session = requests.Session()
        resp = session.post(
            f'{SUPERSET_URL}/api/v1/security/login',
            json={'username': username, 'password': password, 'provider': 'db', 'refresh': True},
            timeout=30,
        )
        resp.raise_for_status()
        token = resp.json().get('access_token')
        if not token:
            log.error("Cache warming: нет access_token в ответе: %s", resp.text[:200])
            return None
        session.headers.update({
            'Authorization': f'Bearer {token}',
            'Referer': SUPERSET_URL,
        })
        csrf_resp = session.get(f'{SUPERSET_URL}/api/v1/security/csrf_token/', timeout=30)
        csrf_resp.raise_for_status()
        csrf_token = csrf_resp.json().get('result')
        if not csrf_token:
            log.error("Cache warming: нет CSRF-токена в ответе: %s", csrf_resp.text[:200])
            return None
        session.headers.update({
            'X-CSRFToken': csrf_token,
            'Content-Type': 'application/json',
        })
        return session
    except Exception as e:
        log.error("Cache warming: ошибка авторизации в Superset: %s", e)
        return None


def _fetch_dashboard_chart_ids(session: requests.Session) -> list:
    """Возвращает список chart_id всех чартов основного дашборда через API.

    Раньше список был захардкожен — он быстро устаревал при добавлении/удалении
    чартов. Теперь источник истины — сам Superset.

    Поле в ответе — `id` (Superset 6.x). До этого фигурировал `slice_id`,
    который в новом формате отсутствует, и список схлопывался в пустой.
    """
    try:
        resp = session.get(f'{SUPERSET_URL}/api/v1/dashboard/{DASHBOARD_ID}/charts', timeout=30)
        resp.raise_for_status()
        return sorted({c['id'] for c in resp.json().get('result', []) if c.get('id')})
    except Exception as e:
        log.error("Cache warming: не удалось получить список чартов дашборда %d: %s",
                  DASHBOARD_ID, e)
        return []


def warm_up_superset_cache():
    """Прогревает кэш всех чартов основного дашборда через Superset API."""
    _flush_data_cache()

    session = _init_session()
    if not session:
        return

    chart_ids = _fetch_dashboard_chart_ids(session)
    if not chart_ids:
        log.warning("Cache warming: список чартов пуст — прогрев пропущен.")
        return

    log.info("Cache warming: начинаем прогрев %d чартов дашборда %d...",
             len(chart_ids), DASHBOARD_ID)

    ok = 0
    retry_ids = []

    for chart_id in chart_ids:
        try:
            r = session.put(
                f'{SUPERSET_URL}/api/v1/chart/warm_up_cache',
                json={'chart_id': chart_id, 'dashboard_id': DASHBOARD_ID},
                timeout=120,
            )
            if r.status_code == 200:
                body = r.json().get('result', [{}])
                err = body[0].get('error') if body else None
                if err:
                    log.warning("Cache warm-up: чарт %d — ошибка в ответе: %s", chart_id, err)
                    retry_ids.append(chart_id)
                else:
                    ok += 1
            else:
                log.warning("Cache warm-up: чарт %d — HTTP %d: %s",
                            chart_id, r.status_code, r.text[:100])
                retry_ids.append(chart_id)
        except Exception as e:
            log.warning("Cache warm-up: чарт %d — ошибка: %s", chart_id, e)
            retry_ids.append(chart_id)
        time.sleep(0.3)

    if retry_ids:
        log.info("Cache warming: повторная попытка для %d чартов...", len(retry_ids))
        time.sleep(10)
        retry_ok = retry_failed = 0
        for chart_id in retry_ids:
            try:
                r = session.put(
                    f'{SUPERSET_URL}/api/v1/chart/warm_up_cache',
                    json={'chart_id': chart_id, 'dashboard_id': DASHBOARD_ID},
                    timeout=180,
                )
                if r.status_code == 200:
                    body = r.json().get('result', [{}])
                    err = body[0].get('error') if body else None
                    if err:
                        log.warning("Cache warm-up retry: чарт %d — всё ещё ошибка: %s", chart_id, err)
                        retry_failed += 1
                    else:
                        retry_ok += 1
                else:
                    log.warning("Cache warm-up retry: чарт %d — HTTP %d", chart_id, r.status_code)
                    retry_failed += 1
            except Exception as e:
                log.warning("Cache warm-up retry: чарт %d — ошибка: %s", chart_id, e)
                retry_failed += 1
            time.sleep(0.5)
        ok += retry_ok
        log.info("Cache warming retry: %d ОК, %d всё ещё ошибок.", retry_ok, retry_failed)

    log.info("Cache warming завершён: итого %d ОК, %d ошибок.", ok, len(retry_ids))
