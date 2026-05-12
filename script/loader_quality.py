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
            v_dupes = conn.execute(text(
                'SELECT COUNT(*) - COUNT(DISTINCT "ym:s:visitID") FROM metrika_visits'
            )).scalar() or 0
            h_dupes = conn.execute(text(
                'SELECT COUNT(*) - COUNT(DISTINCT "ym:pv:watchID") FROM metrika_hits'
            )).scalar() or 0
            total  = int(v_dupes) + int(h_dupes)
            status = 'ok' if total == 0 else 'error'
            _add('Дубли', 'Дубли в сырых данных Метрики', status, total,
                 'дублей нет' if total == 0 else f'visits: {int(v_dupes)}, hits: {int(h_dupes)}')
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

        try:
            hc_dupes = int(conn.execute(text(
                'SELECT COUNT(*) - COUNT(DISTINCT watch_id) FROM hits_calculated'
            )).scalar() or 0)
            status = 'ok' if hc_dupes == 0 else 'error'
            _add('Дубли', 'Дубли в hits_calculated', status, hc_dupes,
                 'дублей нет' if hc_dupes == 0 else f'{hc_dupes} дублирующих watchID')
        except Exception as e:
            _add('Дубли', 'Дубли в hits_calculated', 'error', '—', str(e))

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

        _row_check('Строки: сырые данные Метрики',  'metrika_visits', 'metrika_hits')
        _row_check('Строки: сырые данные MongoDB',  'mongo_payments', 'mongo_users')
        _row_check('Строки: visits_calculated',     'visits_calculated')
        _row_check('Строки: hits_calculated',       'hits_calculated')
        _row_check('Строки: hit_conversions',       'hit_conversions')

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

        # ── Покрытие визитов хитами (orphan-визиты Метрики) ───────────────────
        # Аналог сверки расходов Директа: сколько визитов из visits_calculated
        # имеют хотя бы один хит, и какая доля платежей/регистраций
        # «теряется» на orphan-визитах. На свежих данных orphan-визитов почти
        # нет — это известная проблема первых месяцев счётчика (Метрика
        # не возвращала хиты для части визитов). Свежие визиты без хитов —
        # сигнал к расследованию.
        # Реализация через LEFT JOIN агрегата visit_id'ов (одно сканирование
        # hits_calculated вместо EXISTS-подзапроса на каждую строку).
        try:
            stats = conn.execute(text("""
                WITH hit_visits AS (
                    SELECT DISTINCT visit_id FROM hits_calculated
                ),
                vc_marked AS (
                    SELECT
                      vc."ym:s:visitID"   AS visit_id,
                      vc."ym:s:dateTime"  AS visit_dt,
                      vc.payments_approved_sum,
                      vc.payments_approved_count,
                      vc.registrations_count,
                      (hv.visit_id IS NULL) AS no_hits
                    FROM visits_calculated vc
                    LEFT JOIN hit_visits hv ON hv.visit_id = vc."ym:s:visitID"
                )
                SELECT
                  COUNT(*)                                      AS visits_total,
                  COUNT(*) FILTER (WHERE no_hits)                AS visits_no_hits,
                  COUNT(*) FILTER (WHERE visit_dt::date >=
                      (NOW() AT TIME ZONE 'Europe/Moscow')::date - INTERVAL '14 days'
                  )                                              AS visits_total_recent14d,
                  COUNT(*) FILTER (WHERE no_hits AND visit_dt::date >=
                      (NOW() AT TIME ZONE 'Europe/Moscow')::date - INTERVAL '14 days'
                  )                                              AS visits_no_hits_recent14d,
                  COALESCE(SUM(payments_approved_sum)   FILTER (WHERE no_hits), 0) AS approved_sum_orphan,
                  COALESCE(SUM(payments_approved_count) FILTER (WHERE no_hits), 0) AS approved_cnt_orphan,
                  COALESCE(SUM(registrations_count)     FILTER (WHERE no_hits), 0) AS regs_orphan,
                  COALESCE(SUM(payments_approved_sum), 0)        AS approved_sum_total,
                  COALESCE(SUM(payments_approved_count), 0)      AS approved_cnt_total,
                  COALESCE(SUM(registrations_count), 0)          AS regs_total
                FROM vc_marked
            """)).fetchone()
            visits_total           = int(stats[0] or 0)
            no_hits                 = int(stats[1] or 0)
            visits_total_recent    = int(stats[2] or 0)
            no_hits_recent         = int(stats[3] or 0)
            approved_sum_orph      = float(stats[4] or 0)
            approved_cnt_orph      = int(stats[5] or 0)
            regs_orph              = int(stats[6] or 0)
            approved_sum_total     = float(stats[7] or 0)
            approved_cnt_total     = int(stats[8] or 0)
            regs_total             = int(stats[9] or 0)

            cov_visits   = (visits_total - no_hits) / visits_total if visits_total else 0
            cov_revenue  = (approved_sum_total - approved_sum_orph) / approved_sum_total if approved_sum_total else 1
            cov_payments = (approved_cnt_total - approved_cnt_orph) / approved_cnt_total if approved_cnt_total else 1
            cov_regs     = (regs_total - regs_orph) / regs_total if regs_total else 1
            # Доля orphan-визитов среди СВЕЖИХ (последние 14 дней) — главный индикатор
            # деградации Logs API. Базовый шум ~1–2% (часть визитов в принципе не имеет
            # хитов: event-only сессии, заходы в чат-бот без page-view). Резкий рост —
            # сигнал, что трекер на сайте перестал отдавать хиты.
            recent_orphan_pct = no_hits_recent / visits_total_recent if visits_total_recent else 0

            # Пороги:
            #   • СВЕЖИЕ (recent14d): любой orphan = warning, > 5% = error
            #     В свежих данных хитов вообще не должно «теряться» — если
            #     теряются, значит трекер на сайте перестал отдавать хиты
            #     или Logs API даёт сбои. Любой свежий orphan — повод посмотреть.
            #   • ОБЩАЯ ВЫРУЧКА: > 1% дельта = warning, > 5% = error
            #     На исторической базе ~1% теряется на orphan-визитах сентября-октября
            #     2025 (период раннего внедрения трекера). Это нельзя исправить
            #     задним числом, поэтому warning при 1%, не error.
            status = 'ok'
            reasons = []
            if recent_orphan_pct > 0.05:
                status = 'error'
                reasons.append(f'свежие 14 дн orphan {recent_orphan_pct:.1%} > 5%')
            elif no_hits_recent > 0:
                status = 'warning'
                reasons.append(f'свежие 14 дн orphan: {no_hits_recent} визитов')
            if (1 - cov_revenue) > 0.05:
                status = 'error'
                reasons.append(f'дельта выручки {(1 - cov_revenue):.1%} > 5%')
            elif (1 - cov_revenue) > 0.01 and status != 'error':
                status = 'warning'
                reasons.append(f'дельта выручки {(1 - cov_revenue):.1%} > 1%')

            details = (f"Покрытие: визиты {cov_visits:.1%}, выручка {cov_revenue:.1%}, "
                       f"оплаты {cov_payments:.1%}, регистрации {cov_regs:.1%}. "
                       f"Orphan всего: {no_hits:,} визитов / {approved_sum_orph:,.0f}₽ / "
                       f"{approved_cnt_orph} оплат / {regs_orph} рег. "
                       f"Свежие 14 дн: {no_hits_recent} orphan из {visits_total_recent:,} "
                       f"({recent_orphan_pct:.1%}).")
            if reasons:
                details += " " + "; ".join(reasons) + "."

            _add('Хиты', 'Покрытие визитов хитами Метрики', status,
                 f'{cov_visits:.1%}', details)
        except Exception as e:
            _add('Хиты', 'Покрытие визитов хитами Метрики', 'error', '—', str(e))

        # ── Регрессия атрибуции hits ↔ visits (last-click) ────────────────────
        # SUM(hit_conversions WHERE approved) должен == SUM(visits_calculated.approved_sum)
        # — потому что обе модели формально last-click, единица атрибуции (хит vs визит)
        # не меняет общую сумму конверсий пользователя. Расхождение > 0.5% → проблема:
        # либо потеряны хиты у части пользователей, либо JOIN не докрыл часть платежей.
        try:
            sum_hcv = float(conn.execute(text(
                "SELECT COALESCE(SUM(event_amount), 0) FROM hit_conversions "
                "WHERE event_type = 'payment_approved'"
            )).scalar() or 0)
            sum_vc  = float(conn.execute(text(
                "SELECT COALESCE(SUM(payments_approved_sum), 0) FROM visits_calculated"
            )).scalar() or 0)
            regs_hcv = int(conn.execute(text(
                "SELECT COUNT(*) FROM hit_conversions WHERE event_type = 'registration'"
            )).scalar() or 0)
            regs_vc = int(conn.execute(text(
                "SELECT COALESCE(SUM(registrations_count), 0) FROM visits_calculated"
            )).scalar() or 0)

            if sum_vc > 0:
                diff_pct = abs(sum_hcv - sum_vc) / sum_vc
                status = 'ok' if diff_pct < 0.005 else 'warning' if diff_pct < 0.05 else 'error'
                _add('Атрибуция', 'Регрессия approved-сумм (hits ↔ visits)', status,
                     f'{diff_pct:.2%}',
                     f'hit_conversions: {sum_hcv:,.2f}₽ | visits_calculated: {sum_vc:,.2f}₽ | '
                     f'дельта {sum_hcv - sum_vc:+,.2f}₽')
            else:
                _add('Атрибуция', 'Регрессия approved-сумм (hits ↔ visits)', 'warning',
                     '—', 'visits_calculated.payments_approved_sum = 0')

            if regs_vc > 0:
                diff_pct_r = abs(regs_hcv - regs_vc) / regs_vc
                status_r = 'ok' if diff_pct_r < 0.005 else 'warning' if diff_pct_r < 0.05 else 'error'
                _add('Атрибуция', 'Регрессия регистраций (hits ↔ visits)', status_r,
                     f'{diff_pct_r:.2%}',
                     f'hit_conversions: {regs_hcv:,} | visits_calculated: {regs_vc:,} | '
                     f'дельта {regs_hcv - regs_vc:+,}')
            else:
                _add('Атрибуция', 'Регрессия регистраций (hits ↔ visits)', 'warning',
                     '—', 'visits_calculated.registrations_count = 0')
        except Exception as e:
            _add('Атрибуция', 'Регрессия hits ↔ visits', 'error', '—', str(e))

        # ── Связность hits_calculated ─────────────────────────────────────────
        try:
            orphan = int(conn.execute(text(
                "SELECT COUNT(*) FROM hits_calculated "
                "WHERE visit_id IS NULL OR unified_user_id IS NULL"
            )).scalar() or 0)
            status = 'ok' if orphan == 0 else 'warning'
            _add('Заполненность', 'Hits-orphans (без visit_id/uid)', status, orphan,
                 'все хиты привязаны к визиту' if orphan == 0
                 else f'{orphan:,} хитов без родительского визита/пользователя')
        except Exception as e:
            _add('Заполненность', 'Hits-orphans (без visit_id/uid)', 'error', '—', str(e))

        # ── Свежесть hits_calculated ──────────────────────────────────────────
        try:
            last_dt = conn.execute(text(
                'SELECT MAX(hit_dt) FROM hits_calculated'
            )).scalar()
            if last_dt:
                if isinstance(last_dt, datetime):
                    last = last_dt
                else:
                    last = datetime.fromisoformat(str(last_dt)[:19])
                now_msk  = datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(hours=3)
                age_days = (now_msk - last).total_seconds() / 86400
                status   = 'ok' if age_days <= 1.5 else 'warning' if age_days <= 3.0 else 'error'
                _add('Актуальность', 'Свежесть hits_calculated', status,
                     last.strftime('%Y-%m-%d'),
                     f'Последний хит {age_days:.1f} дн. назад')
            else:
                _add('Актуальность', 'Свежесть hits_calculated', 'error',
                     '—', 'hits_calculated пуст')
        except Exception as e:
            _add('Актуальность', 'Свежесть hits_calculated', 'error', '—', str(e))

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
