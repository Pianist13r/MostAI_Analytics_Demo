"""
Синхронизация данных из MongoDB в PostgreSQL: рефереры, пополнения, пользователи.

Все три функции делают полную перезапись через атомарный swap:
  CREATE _new → DROP old → RENAME _new → old
Это даёт атомарность: если что-то упадёт на середине — старые данные остаются целы.
"""
from datetime import datetime, timedelta
from typing import Optional, Tuple

import pandas as pd
from pymongo import MongoClient
from sqlalchemy import text

from loader_config import log, engine, MONGO_URI, MONGO_DB_NAME, MONGO_COLLECTION


# ---------------------------------------------------------------------------
# Вспомогательные функции
# ---------------------------------------------------------------------------

def _format_datetime(dt) -> Tuple[Optional[datetime], Optional[datetime]]:
    """Возвращает (utc_dt, moscow_dt) для datetime-поля MongoDB.

    Метрика хранит ym:s:dateTime в московском времени, MongoDB — в UTC.
    Конвертируем UTC → UTC+3, чтобы JOIN по времени работал корректно.
    Возвращаем datetime-объекты → pandas сохранит как TIMESTAMP в PostgreSQL.
    pymongo 4.x возвращает timezone-aware datetimes (UTC) — снимаем tzinfo
    перед арифметикой, чтобы не получить aware+naive TypeError.
    """
    if isinstance(dt, datetime):
        naive_utc = dt.replace(tzinfo=None) if dt.tzinfo else dt
        return (naive_utc, naive_utc + timedelta(hours=3))
    return (None, None)


def _swap_table(conn, staging: str, target: str):
    """Атомарно заменяет таблицу target на staging через RENAME."""
    conn.execute(text(f"DROP TABLE IF EXISTS {target}"))
    conn.execute(text(f"ALTER TABLE {staging} RENAME TO {target}"))


# ---------------------------------------------------------------------------
# Реферальная программа
# ---------------------------------------------------------------------------

def sync_referrers():
    """Синхронизирует идентификаторы реферальной программы из MongoDB → mongo_referrers.

    Каждый реферер хранится в двух строках таблицы:
      - строка с ObjectId (для старых ссылок вида ?from=<24-hex>)
      - строка с ref_code (для новых ссылок вида ?from=PROMPT1RU)
    ref_label — человекочитаемое название (ref_code если есть, иначе ObjectId).
    """
    mongo_client = None
    try:
        log.info("Подключение к MongoDB...")
        mongo_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        mongo_client.admin.command('ping')
        log.info("MongoDB: соединение установлено.")

        collection = mongo_client[MONGO_DB_NAME][MONGO_COLLECTION]

        # ObjectId → ref_code для всех пользователей с реферальным кодом
        ref_code_map = {
            str(doc['_id']): str(doc['affiliate_settings']['ref_code']).strip()
            for doc in collection.find(
                {'affiliate_settings.ref_code': {'$exists': True, '$ne': None}},
                {'affiliate_settings.ref_code': 1, '_id': 1},
            )
            if doc.get('affiliate_settings', {}).get('ref_code')
        }

        # ObjectId-ы пользователей, на которых ссылаются другие (реферальные «родители»)
        referrer_obj_ids = [
            str(i) for i in collection.distinct("referrer")
            if i is not None and str(i) != '000000000000000000000000'
        ]

        rows = []
        seen = set()

        # Строки для ObjectId-ссылок: ?from=<24-hex>
        for obj_id in referrer_obj_ids:
            ref_label = ref_code_map.get(obj_id, obj_id)
            if obj_id not in seen:
                rows.append({'identifier': obj_id, 'ref_label': ref_label})
                seen.add(obj_id)

        # Строки для ref_code-ссылок: ?from=PROMPT1RU
        for obj_id, ref_code in ref_code_map.items():
            if ref_code not in seen:
                rows.append({'identifier': ref_code, 'ref_label': ref_code})
                seen.add(ref_code)

        if not rows:
            log.info("Реферальные идентификаторы не найдены.")
            return

        df = pd.DataFrame(rows)
        with engine.begin() as conn:
            conn.execute(text("DROP TABLE IF EXISTS mongo_referrers_new"))
            df.to_sql('mongo_referrers_new', con=conn, if_exists='replace', index=False)
            _swap_table(conn, 'mongo_referrers_new', 'mongo_referrers')
            # UNIQUE-индекс на PK — защита от дублей при будущих изменениях.
            conn.execute(text(
                "CREATE UNIQUE INDEX IF NOT EXISTS uq_mongo_referrers_id "
                "ON mongo_referrers (identifier)"
            ))

        n_obj  = sum(1 for r in rows if len(r['identifier']) == 24)
        n_code = len(rows) - n_obj
        log.info(
            "Синхронизировано %d реферальных идентификаторов (%d ObjectId, %d ref_code).",
            len(df), n_obj, n_code,
        )

    except Exception as e:
        log.error("Ошибка синхронизации рефереров: %s", e)
    finally:
        if mongo_client:
            mongo_client.close()


# ---------------------------------------------------------------------------
# Пополнения
# ---------------------------------------------------------------------------

def sync_payments():
    """Синхронизирует пополнения из MongoDB (коллекция payment) → mongo_payments.

    Полная перезапись при каждом запуске — платежи меняют статус (created → approved).
    """
    mongo_client = None
    try:
        log.info("Синхронизация пополнений из MongoDB...")
        mongo_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        mongo_client.admin.command('ping')

        collection = mongo_client[MONGO_DB_NAME]['payment']
        rows = []
        for doc in collection.find():
            ca_str, ca_moscow = _format_datetime(doc.get('created_at'))
            rows.append({
                'payment_id':        str(doc['_id']),
                'mongo_user_id':     str(doc.get('user', '')),
                'amount':            doc.get('amount', 0.0),
                'status':            doc.get('status', ''),
                'provider':          doc.get('provider', ''),
                'created_at':        ca_str,
                'created_at_moscow': ca_moscow,
            })

        if not rows:
            log.info("Пополнений не найдено.")
            return

        df = pd.DataFrame(rows)
        with engine.begin() as conn:
            conn.execute(text("DROP TABLE IF EXISTS mongo_payments_new"))
            df.to_sql('mongo_payments_new', con=conn, if_exists='replace', index=False)
            _swap_table(conn, 'mongo_payments_new', 'mongo_payments')
            # UNIQUE на payment_id — защита от дублей в зеркале (PK из MongoDB).
            conn.execute(text("CREATE UNIQUE INDEX IF NOT EXISTS uq_payments_id    ON mongo_payments (payment_id)"))
            conn.execute(text("CREATE INDEX        IF NOT EXISTS idx_payments_user   ON mongo_payments (mongo_user_id)"))
            conn.execute(text("CREATE INDEX        IF NOT EXISTS idx_payments_moscow ON mongo_payments (created_at_moscow)"))
            conn.execute(text("CREATE INDEX        IF NOT EXISTS idx_payments_status ON mongo_payments (status)"))
        log.info("Синхронизировано %d пополнений.", len(df))

    except Exception as e:
        log.error("Ошибка синхронизации пополнений: %s", e)
    finally:
        if mongo_client:
            mongo_client.close()


# ---------------------------------------------------------------------------
# Пользователи
# ---------------------------------------------------------------------------

def sync_users():
    """Синхронизирует регистрации пользователей из MongoDB → mongo_users.

    Полная перезапись при каждом запуске.
    """
    mongo_client = None
    try:
        log.info("Синхронизация пользователей из MongoDB...")
        mongo_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        mongo_client.admin.command('ping')

        collection = mongo_client[MONGO_DB_NAME][MONGO_COLLECTION]
        rows = []
        for doc in collection.find({}, {'_id': 1, 'created_at': 1, 'platform': 1}):
            ca_str, ca_moscow = _format_datetime(doc.get('created_at'))
            rows.append({
                'mongo_user_id':     str(doc['_id']),
                'created_at':        ca_str,
                'created_at_moscow': ca_moscow,
                'platform':          doc.get('platform', ''),
            })

        if not rows:
            log.info("Пользователей не найдено.")
            return

        df = pd.DataFrame(rows)
        with engine.begin() as conn:
            conn.execute(text("DROP TABLE IF EXISTS mongo_users_new"))
            df.to_sql('mongo_users_new', con=conn, if_exists='replace', index=False)
            _swap_table(conn, 'mongo_users_new', 'mongo_users')
            # UNIQUE на mongo_user_id — защита от дублей в зеркале (PK из MongoDB).
            conn.execute(text("CREATE UNIQUE INDEX IF NOT EXISTS uq_users_mongo_id ON mongo_users (mongo_user_id)"))
            conn.execute(text("CREATE INDEX        IF NOT EXISTS idx_users_moscow   ON mongo_users (created_at_moscow)"))
        log.info("Синхронизировано %d пользователей.", len(df))

    except Exception as e:
        log.error("Ошибка синхронизации пользователей: %s", e)
    finally:
        if mongo_client:
            mongo_client.close()
