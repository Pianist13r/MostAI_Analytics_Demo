"""
Мониторинг качества данных: таблица data_quality.

Каждый запуск переписывает таблицу полностью. Строки с category='__cnt'
являются служебными — они хранят числовые значения предыдущего запуска
для сравнения (контроль падения строк, заполненности). Дашборд должен
фильтровать category != '__cnt'.
"""
import json
import os
import statistics
from datetime import datetime, timedelta, timezone

import pandas as pd
from sqlalchemy import text

from loader_config import log, engine


def update_data_quality():
    """Считает таблицу data_quality с проверками по всем критическим аспектам данных."""
    log.info("Проверка качества данных...")

    rows = []
    _now_msk = datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(hours=3)
    now  = _now_msk.strftime('%Y-%m-%d %H:%M:%S')

    def _add(category, check_name, status, value, details):
        icon = {'ok': '✅', 'warning': '⚠️', 'error': '❌'}.get(status, '❓')
        rows.append({
            'category':   category,
            'check_name': check_name,
            'status':     status,
            'icon':       icon,
            'value':      str(value),
            'details':    details,
            'checked_at': now,
        })

    # Читаем предыдущие значения до перезаписи таблицы.
    # Служебные строки category='__cnt' хранят числа для межзапусковых сравнений.
    prev_values = {}
    try:
        with engine.connect() as conn:
            for row in conn.execute(text("SELECT check_name, value FROM data_quality")):
                prev_values[row[0]] = row[1]
    except Exception:
        pass  # таблица ещё не существует — начинаем с нуля

    with engine.connect() as conn:

        # ── Дубли ─────────────────────────────────────────────────────────────
        try:
            v_dupes = int(conn.execute(text(
                'SELECT COUNT(*) - COUNT(DISTINCT "ym:s:visitID") FROM metrika_visits'
            )).scalar() or 0)
            status = 'ok' if v_dupes == 0 else 'error'
            _add('Дубли', 'Дубли в сырых данных Метрики', status, v_dupes,
                 'дублей нет' if v_dupes == 0 else f'visits: {v_dupes}')
        except Exception as e:
            _add('Дубли', 'Дубли в сырых данных Метрики', 'error', '—', str(e))

        try:
            p_dupes = conn.execute(text(
                'SELECT COUNT(*) - COUNT(DISTINCT payment_id) FROM mongo_payments'
            )).scalar() or 0
            u_dupes = conn.execute(text(
                'SELECT COUNT(*) - COUNT(DISTINCT mongo_user_id) FROM mongo_users'
            )).scalar() or 0
            total  = int(p_dupes) + int(u_dupes)
            status = 'ok' if total == 0 else 'error'
            _add('Дубли', 'Дубли в сырых данных MongoDB', status, total,
                 'дублей нет' if total == 0 else f'payments: {int(p_dupes)}, users: {int(u_dupes)}')
        except Exception as e:
            _add('Дубли', 'Дубли в сырых данных MongoDB', 'error', '—', str(e))

        try:
            vc_dupes = int(conn.execute(text(
                'SELECT COUNT(*) - COUNT(DISTINCT "ym:s:visitID") FROM visits_calculated'
            )).scalar() or 0)
            status = 'ok' if vc_dupes == 0 else 'error'
            _add('Дубли', 'Дубли в visits_calculated', status, vc_dupes,
                 'дублей нет' if vc_dupes == 0 else f'{vc_dupes} дублирующих visitID')
        except Exception as e:
            _add('Дубли', 'Дубли в visits_calculated', 'error', '—', str(e))

        # ── Строки ────────────────────────────────────────────────────────────
        def _row_check(check_name, *tables):
            """Контролирует, что количество строк не уменьшилось с прошлого запуска."""
            try:
                counts = {
                    t: int(conn.execute(text(f'SELECT COUNT(*) FROM "{t}"')).scalar() or 0)
                    for t in tables
                }
                cnt = sum(counts.values())
                # Берём предыдущее число из служебной строки category='__cnt'
                prev_raw = prev_values.get(check_name + '__cnt')
                status = 'ok'
                value  = 'норма'
                if prev_raw is not None:
                    try:
                        prev_cnt = int(prev_raw.replace(',', ''))
                        if cnt < prev_cnt:
                            status = 'error'
                            value  = f'↓{prev_cnt - cnt:,} строк'
                    except Exception:
                        pass
                if len(tables) > 1:
                    details = ' + '.join(f'{t}: {counts[t]:,}' for t in tables)
                else:
                    details = f'{cnt:,} строк'
                _add('Строки', check_name, status, value, details)
                # Служебная строка для хранения числа при следующем запуске
                _add('__cnt', check_name + '__cnt', 'ok', f'{cnt:,}', '')
            except Exception as e:
                _add('Строки', check_name, 'error', '—', str(e))

        _row_check('Строки: сырые данные Метрики',  'metrika_visits')
        _row_check('Строки: сырые данные MongoDB',  'mongo_payments', 'mongo_users')
        _row_check('Строки: visits_calculated',     'visits_calculated')

        # ── Заполненность ─────────────────────────────────────────────────────
        key_cols = [
            '"ym:s:dateTime"',
            'unified_user_id',
            'last_source',
            'first_source',
            'last_significant_source',
        ]
        try:
            total = conn.execute(text('SELECT COUNT(*) FROM visits_calculated')).scalar() or 0
            if total > 0:
                col_pcts = {}
                for col in key_cols:
                    filled = conn.execute(text(
                        f'SELECT COUNT(*) FROM visits_calculated WHERE {col} IS NOT NULL'
                    )).scalar() or 0
                    col_pcts[col.strip('"')] = filled / total

                min_col  = min(col_pcts, key=col_pcts.get)
                min_pct  = col_pcts[min_col]
                details  = 'мин. ' + '; '.join(f'{c}: {p:.1%}' for c, p in col_pcts.items())

                prev_raw = prev_values.get('Заполненность ключевых колонок__pct')
                status   = 'ok'
                value    = 'норма'
                if prev_raw is not None:
                    try:
                        prev_pct = float(prev_raw.strip('%')) / 100
                        drop     = prev_pct - min_pct
                        if drop > 0.02:
                            status = 'error'
                            value  = f'↓{drop:.1%}'
                    except Exception:
                        pass

                _add('Заполненность', 'Заполненность ключевых колонок', status, value, details)
                _add('__cnt', 'Заполненность ключевых колонок__pct', 'ok', f'{min_pct:.1%}', '')
            else:
                _add('Заполненность', 'Заполненность ключевых колонок', 'error', '—', 'visits_calculated пуст')
        except Exception as e:
            _add('Заполненность', 'Заполненность ключевых колонок', 'error', '—', str(e))

        # ── MongoDB→clientID маппинг — динамика прироста ─────────────────────
        # Старая проверка (% покрытия от mongo_users) давала ложный error при
        # норме ~7–10%: маппинг строится только для пользователей, пришедших
        # ПОСЛЕ внедрения userParams в коде сайта. Заменена на динамическую:
        # ловим а) откат, б) аномально малый прирост за последний запуск,
        # в) снижение темпа роста за последние 5 запусков относительно прежнего.
        # История значений хранится в служебной __cnt-строке как JSON-массив.
        try:
            pairs_total = int(conn.execute(text(
                'SELECT COUNT(*) FROM clientid_to_mongoid'
            )).scalar() or 0)

            history_key = 'MongoDB→clientID маппинг__history'
            history_raw = prev_values.get(history_key)
            try:
                history = json.loads(history_raw) if history_raw else []
                if not isinstance(history, list):
                    history = []
                history = [int(x) for x in history if isinstance(x, (int, float))]
            except Exception:
                history = []

            history.append(pairs_total)
            history = history[-30:]  # храним до 30 последних точек

            status  = 'ok'
            reasons = []
            delta_current = history[-1] - history[-2] if len(history) >= 2 else 0

            if len(history) >= 2:
                deltas = [history[i+1] - history[i] for i in range(len(history) - 1)]

                # а) откат: общее число пар уменьшилось — критично, возможна потеря данных
                if delta_current < 0:
                    status = 'error'
                    reasons.append(f'число пар уменьшилось на {-delta_current:,} с прошлого запуска')

                # б) аномально малый прирост: текущая дельта < 30% медианы прошлых
                #    (нужно ≥ 4 предыдущих дельт, чтобы медиана была устойчивой)
                elif len(deltas) >= 5:
                    median_prev = statistics.median(deltas[:-1])
                    if median_prev > 0 and delta_current < 0.3 * median_prev:
                        status = 'warning' if status == 'ok' else status
                        reasons.append(
                            f'за последний запуск добавилось {delta_current:,} пар '
                            f'(медиана прежних запусков ≈ {int(median_prev):,})'
                        )

                # в) снижение темпа за последний период: ср. за 5 последних дельт
                #    < 50% от ср. за более ранние (нужно ≥ 8 дельт всего)
                if len(deltas) >= 8:
                    recent_window     = deltas[-5:]
                    historical_window = deltas[:-5]
                    avg_recent       = statistics.mean(recent_window)     if recent_window     else 0
                    avg_historical   = statistics.mean(historical_window) if historical_window else 0
                    if avg_historical > 0 and avg_recent < 0.5 * avg_historical:
                        status = 'warning' if status == 'ok' else status
                        reasons.append(
                            f'темп роста за 5 последних запусков ({int(avg_recent):,}/запуск) '
                            f'упал относительно прежних ({int(avg_historical):,}/запуск)'
                        )

            value = f'{pairs_total:,}'
            if len(history) < 2:
                details = f'нет истории для сравнения; всего {pairs_total:,} пар'
            elif status == 'ok':
                details = f'+{delta_current:,} пар за запуск; всего {pairs_total:,}'
            else:
                details = (f'+{delta_current:,} пар за запуск; всего {pairs_total:,}; '
                           + '; '.join(reasons))

            _add('Заполненность', 'MongoDB→clientID маппинг', status, value, details)
            _add('__cnt', history_key, 'ok', json.dumps(history), '')
        except Exception as e:
            _add('Заполненность', 'MongoDB→clientID маппинг', 'error', '—', str(e))

        # ── Актуальность ──────────────────────────────────────────────────────
        try:
            last_dt = conn.execute(text(
                'SELECT MAX("ym:s:dateTime") FROM visits_calculated'
            )).scalar()
            if last_dt:
                # MAX() на колонке TIMESTAMP возвращает datetime-объект в PostgreSQL,
                # но может вернуть строку при других драйверах — обрабатываем оба случая.
                if isinstance(last_dt, datetime):
                    last = last_dt
                else:
                    last = datetime.fromisoformat(str(last_dt)[:19])
                now_msk  = datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(hours=3)
                age_days = (now_msk - last).total_seconds() / 86400
                status   = 'ok' if age_days <= 1.5 else 'warning' if age_days <= 3.0 else 'error'
                _add('Актуальность', 'Свежесть visits_calculated', status,
                     last.strftime('%Y-%m-%d'), f'Последняя запись {age_days:.1f} дн. назад')
            else:
                _add('Актуальность', 'Свежесть visits_calculated', 'error', '—', 'visits_calculated пуст')
        except Exception as e:
            _add('Актуальность', 'Свежесть visits_calculated', 'error', '—', str(e))

        try:
            missing = conn.execute(text("""
                WITH dates AS (
                    SELECT DISTINCT "ym:s:dateTime"::date AS d FROM visits_calculated
                ),
                gaps AS (
                    SELECT d, LAG(d) OVER (ORDER BY d) AS prev_d,
                           d - LAG(d) OVER (ORDER BY d) AS gap_days
                    FROM dates
                )
                SELECT prev_d, d, gap_days FROM gaps WHERE gap_days > 1 ORDER BY gap_days DESC LIMIT 3
            """)).fetchall()
            if not missing:
                _add('Актуальность', 'Пропущенные дни', 'ok', '0', 'пропущенных дней нет, данные непрерывны')
            else:
                worst = missing[0]
                _add('Актуальность', 'Пропущенные дни', 'warning',
                     f'{worst[2]-1} дн.',
                     f'Разрыв {worst[0]} → {worst[1]} ({worst[2]-1} пропущ. дн.)' +
                     (f'; ещё {len(missing)-1} разрыва' if len(missing) > 1 else ''))
        except Exception as e:
            _add('Актуальность', 'Пропущенные дни', 'error', '—', str(e))

        # ── Сверка расходов Директа ───────────────────────────────────────────
        try:
            period = conn.execute(text(
                "SELECT MIN(date), MAX(date) FROM direct_costs"
            )).fetchone()

            if period and period[0]:
                date_from, date_to = period[0], period[1]

                api_total = float(conn.execute(text("""
                    SELECT COALESCE(SUM(cost), 0) FROM direct_costs
                    WHERE date BETWEEN :d1 AND :d2
                """), {"d1": date_from, "d2": date_to}).scalar() or 0)

                # Параметры SQLAlchemy (:d1, :d2) конфликтуют с PostgreSQL cast-синтаксисом (::).
                # Приводим к дате через CAST(...AS date) вместо ::date после параметра.
                attr_total = float(conn.execute(text("""
                    SELECT COALESCE(SUM(ad_cost_per_visit), 0)
                    FROM visits_calculated
                    WHERE CAST("ym:s:dateTime" AS date) BETWEEN CAST(:d1 AS date) AND CAST(:d2 AS date)
                """), {"d1": date_from, "d2": date_to}).scalar() or 0)

                if api_total > 0:
                    diff_abs = api_total - attr_total
                    diff_pct = diff_abs / api_total
                    coverage = attr_total / api_total

                    status = 'ok' if diff_pct <= 0.10 else 'warning' if diff_pct <= 0.25 else 'error'
                    _add('Директ', 'Сверка затрат Директа', status,
                         f'{coverage:.1%}',
                         f'API: {api_total:,.2f}₽ | атрибутировано: {attr_total:,.2f}₽ | '
                         f'разница: {diff_abs:+,.2f}₽ ({diff_pct:.1%}) | '
                         f'период: {date_from} — {date_to}')
                else:
                    _add('Директ', 'Сверка затрат Директа', 'warning', '—',
                         f'direct_costs пуст за {date_from} — {date_to}')
            else:
                _add('Директ', 'Сверка затрат Директа', 'warning', '—', 'direct_costs пуст')
        except Exception as e:
            _add('Директ', 'Сверка затрат Директа', 'error', '—', str(e))

        # ── Ошибки из лога ────────────────────────────────────────────────────
        try:
            log_path      = '/app/data/loader.log'
            error_count   = 0
            warning_count = 0
            last_run_ts   = None
            if os.path.exists(log_path):
                with open(log_path, encoding='utf-8') as f:
                    lines = f.readlines()
                start_idx = 0
                for idx, line in enumerate(lines):
                    if 'Начало синхронизации' in line:
                        start_idx   = idx
                        last_run_ts = line[:19]
                run_lines     = lines[start_idx:]
                error_count   = sum(1 for l in run_lines if '[ERROR]' in l)
                warning_count = sum(1 for l in run_lines if '[WARNING]' in l)

            ts_value = last_run_ts or '—'
            _add('Статус', 'Последний запуск', 'ok', ts_value,
                 f'Начало последней синхронизации: {ts_value}')
            _add('Ошибки', 'ERROR в последнем запуске',
                 'ok' if error_count == 0 else 'error',
                 str(error_count), f'{error_count} ошибок в логе')
            _add('Ошибки', 'WARNING в последнем запуске',
                 'ok' if warning_count == 0 else 'warning',
                 str(warning_count), f'{warning_count} предупреждений в логе')
        except Exception as e:
            _add('Ошибки', 'Парсинг лога', 'error', '—', str(e))

    df = pd.DataFrame(rows)
    with engine.begin() as conn:
        df.to_sql('data_quality', con=conn, if_exists='replace', index=False)
    log.info("data_quality обновлена: %d проверок.", len(rows))
