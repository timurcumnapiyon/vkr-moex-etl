import logging
import os
import time
from datetime import UTC, datetime, timedelta

import psycopg2
from psycopg2 import OperationalError

from alorpy.AlorPy.AlorPy import AlorPy

# Токен ALOR лучше хранить в переменной окружения.
ALOR_TOKEN = os.environ.get(
    "ALOR_TOKEN",
    "",
)

# Параметры подключения к PG_RAW.
DB_CONFIG = {
    "host": os.environ.get("PG_RAW_HOST", "pg_raw"),
    "port": int(os.environ.get("PG_RAW_PORT", "5432")),
    "dbname": os.environ.get("PG_RAW_DB", "pg_raw"),
    "user": os.environ.get("PG_RAW_USER", "postgres"),
    "password": os.environ.get("PG_RAW_PASSWORD", "postgres"),
}

# Список инструментов и таймфрейм подписки.
DATANAMES = ("TQBR.SBER", "TQBR.GMKN")
_env_datanames = os.environ.get("ALOR_DATANAMES")
if _env_datanames:
    DATANAMES = tuple(s.strip() for s in _env_datanames.split(",") if s.strip())

TF = "M1"

# Для live-режима стартовый бэкфилл по умолчанию выключен.
DAYS_BACK = int(os.environ.get("ALOR_DAYS_BACK", "0"))

# Пакетный commit для устойчивости под нагрузкой.
DB_COMMIT_EVERY = int(os.environ.get("ALOR_DB_COMMIT_EVERY", "100"))
DB_COMMIT_INTERVAL_SECONDS = float(os.environ.get("ALOR_DB_COMMIT_INTERVAL_SECONDS", "2"))


logger = logging.getLogger("AlorRealtimeETL")
ap_provider: AlorPy | None = None
db_conn: psycopg2.extensions.connection | None = None
pending_writes = 0
last_commit_monotonic = time.monotonic()

# Соответствие подписки GUID -> dataname.
GUID_TO_DATANAME: dict[str, str] = {}


def get_db_connection():
    """Возвращает единственное глобальное подключение к PG."""
    global db_conn
    if db_conn is None or db_conn.closed != 0:
        logger.info("Подключаюсь к PG_RAW...")
        try:
            db_conn = psycopg2.connect(**DB_CONFIG)
            db_conn.autocommit = False
            logger.info("Подключение к PG_RAW установлено.")
        except OperationalError as e:
            logger.error(f"Ошибка подключения к PG_RAW: {e}")
            raise
    return db_conn


def ensure_table_exists():
    """Создает таблицу bars_stocks_alor, если она отсутствует."""
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS bars_stocks_alor (
            dataname  VARCHAR(24)       NOT NULL,
            tf        VARCHAR(5)        NOT NULL,
            datetime  TIMESTAMPTZ       NOT NULL,
            open      DOUBLE PRECISION  NOT NULL,
            high      DOUBLE PRECISION  NOT NULL,
            low       DOUBLE PRECISION  NOT NULL,
            close     DOUBLE PRECISION  NOT NULL,
            volume    DOUBLE PRECISION,
            PRIMARY KEY (dataname, tf, datetime)
        );
        """
    )
    conn.commit()
    cur.close()
    logger.info("Таблица bars_stocks_alor готова.")


def save_bar_to_db(dataname: str, tf: str, dt_utc: datetime, bar: dict):
    """Сохраняет один бар в PG_RAW."""
    global pending_writes, last_commit_monotonic
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO bars_stocks_alor
                (dataname, tf, datetime, open, high, low, close, volume)
            VALUES
                (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (dataname, tf, datetime) DO NOTHING;
            """,
            (
                dataname,
                tf,
                dt_utc,
                bar["open"],
                bar["high"],
                bar["low"],
                bar["close"],
                bar.get("volume"),
            ),
        )
        pending_writes += 1
        elapsed = time.monotonic() - last_commit_monotonic
        if pending_writes >= DB_COMMIT_EVERY or elapsed >= DB_COMMIT_INTERVAL_SECONDS:
            conn.commit()
            pending_writes = 0
            last_commit_monotonic = time.monotonic()
    except Exception as e:
        conn.rollback()
        pending_writes = 0
        last_commit_monotonic = time.monotonic()
        logger.error(f"Ошибка вставки бара ({dataname}, {dt_utc}): {e}")
    finally:
        cur.close()


def on_new_bar(response: dict):
    """Callback, вызываемый AlorPy при получении нового бара."""
    try:
        response_data = response["data"]
        guid = response.get("guid")
        dataname = GUID_TO_DATANAME.get(guid, "UNKNOWN")

        utc_timestamp = response_data["time"]
        dt_utc = datetime.fromtimestamp(utc_timestamp, UTC)

        logger.info(
            f"{dataname} {TF} "
            f"{dt_utc:%d.%m.%Y %H:%M:%S} "
            f"O:{response_data['open']} "
            f"H:{response_data['high']} "
            f"L:{response_data['low']} "
            f"C:{response_data['close']} "
            f"V:{response_data.get('volume')}"
        )

        save_bar_to_db(dataname, TF, dt_utc, response_data)

    except Exception as e:
        logger.error(f"Ошибка в on_new_bar: {e}")


def main():
    global ap_provider

    if not ALOR_TOKEN:
        raise RuntimeError("Переменная окружения ALOR_TOKEN не задана.")

    logging.basicConfig(
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%d.%m.%Y %H:%M:%S",
        level=logging.INFO,
        handlers=[
            logging.FileHandler("fetch_alor_to_pgraw_1min.log", encoding="utf-8"),
            logging.StreamHandler(),
        ],
    )

    logger.info("Запуск ETL потока ALOR real-time...")

    # Инициализация AlorPy с повторными попытками.
    max_attempts = int(os.environ.get("ALOR_INIT_MAX_ATTEMPTS", "10"))
    backoff_seconds = int(os.environ.get("ALOR_INIT_BACKOFF_SECONDS", "10"))
    last_err = None

    for attempt in range(1, max_attempts + 1):
        try:
            ap_provider = AlorPy(ALOR_TOKEN)
            break
        except Exception as e:
            last_err = e
            logger.error(f"Ошибка инициализации ALOR (попытка {attempt}/{max_attempts}): {e}")
            if attempt < max_attempts:
                time.sleep(backoff_seconds)

    if ap_provider is None:
        raise RuntimeError("Не удалось инициализировать ALOR") from last_err

    ensure_table_exists()

    # Логи синхронизации времени (локально/сервер).
    dt_local = datetime.now(ap_provider.tz_msk)
    seconds_from = float(ap_provider.get_time())
    dt_server = datetime.fromtimestamp(seconds_from, ap_provider.tz_msk)
    logger.info(f"Локальное время МСК : {dt_local:%d.%m.%Y %H:%M:%S}")
    logger.info(f"Время на сервере    : {dt_server:%d.%m.%Y %H:%M:%S}")
    logger.info(f"Разница во времени  : {dt_server - dt_local}")

    ap_provider.on_new_bar.subscribe(on_new_bar)

    subscribed = 0
    for dataname in DATANAMES:
        logger.info(f"Открываю подписку {dataname} на бары {TF}...")
        alor_board, symbol = ap_provider.dataname_to_alor_board_symbol(dataname)

        exchange = None
        for attempt in range(1, 4):
            try:
                exchange = ap_provider.get_exchange(alor_board, symbol)
                break
            except Exception as e:
                logger.error(
                    f"Не удалось получить exchange для {dataname} "
                    f"(попытка {attempt}/3): {e}"
                )
                time.sleep(2 * attempt)

        if not exchange:
            logger.error(f"Пропускаю {dataname}: exchange не получен.")
            continue

        alor_tf, intraday = ap_provider.timeframe_to_alor_timeframe(TF)
        _ = intraday

        seconds_from = ap_provider.msk_datetime_to_timestamp(
            datetime.now() - timedelta(days=DAYS_BACK)
        )

        guid = ap_provider.bars_get_and_subscribe(
            exchange,
            symbol,
            alor_tf,
            seconds_from=seconds_from,
            frequency=1_000_000_000,
        )

        GUID_TO_DATANAME[guid] = dataname
        subscribed += 1

        subscription = ap_provider.subscriptions[guid]
        logger.info(
            f"Подписка открыта: {guid} {subscription}. "
            f"Биржа {subscription['exchange']} тикер {subscription['code']} "
            f"TF {subscription['tf']}"
        )

    if subscribed == 0:
        logger.error("Не удалось открыть ни одной подписки. Проверьте настройки ALOR.")
        return

    logger.info("Все подписки открыты. ETL будет писать новые бары по мере поступления.")
    logger.info("Для остановки нажмите Ctrl+C.")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Остановка по запросу пользователя...")
    finally:
        try:
            ap_provider.on_new_bar.unsubscribe(on_new_bar)
        except Exception:
            pass

        try:
            for guid in list(GUID_TO_DATANAME.keys()):
                try:
                    ap_provider.unsubscribe(guid)
                    logger.info(f"Подписка {guid} отключена.")
                except Exception as e:
                    logger.error(f"Ошибка unsubscribe для {guid}: {e}")
        finally:
            try:
                ap_provider.close_web_socket()
            except Exception:
                pass

        global db_conn
        if db_conn is not None and db_conn.closed == 0:
            try:
                if pending_writes > 0:
                    db_conn.commit()
            except Exception as e:
                logger.error(f"Ошибка финального commit при завершении: {e}")
            db_conn.close()
            logger.info("Подключение к PG_RAW закрыто.")

        logger.info("ETL поток ALOR остановлен.")


if __name__ == "__main__":
    main()
