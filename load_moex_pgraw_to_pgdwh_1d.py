import os

import psycopg2
from psycopg2.extras import RealDictCursor


PG_RAW_CONFIG = {
    "host": os.getenv("PG_RAW_HOST", "pg_raw"),
    "port": int(os.getenv("PG_RAW_PORT", "5432")),
    "user": os.getenv("PG_RAW_USER", "postgres"),
    "password": os.getenv("PG_RAW_PASSWORD", "postgres"),
    "dbname": os.getenv("PG_RAW_DB", "pg_raw"),
}

PG_DWH_CONFIG = {
    "host": os.getenv("PG_DWH_HOST", "pg_dwh"),
    "port": int(os.getenv("PG_DWH_PORT", "5432")),
    "user": os.getenv("PG_DWH_USER", "postgres"),
    "password": os.getenv("PG_DWH_PASSWORD", "postgres"),
    "dbname": os.getenv("PG_DWH_DB", "pg_dwh"),
}


def get_last_loaded_date_dwh(conn_dwh):
    """Возвращает последнюю загруженную дату для источника MOEX_ISS в DWH."""
    with conn_dwh.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT max(datetime) AS max_dt
            FROM dwh_bars_1d
            WHERE source = 'MOEX_ISS';
            """
        )
        row = cur.fetchone()
        return row["max_dt"]


def load_new_moex_daily():
    conn_raw = psycopg2.connect(**PG_RAW_CONFIG)
    conn_dwh = psycopg2.connect(**PG_DWH_CONFIG)

    try:
        conn_raw.autocommit = False
        conn_dwh.autocommit = False

        last_dt = get_last_loaded_date_dwh(conn_dwh)

        with conn_raw.cursor(cursor_factory=RealDictCursor) as cur_raw:
            if last_dt is None:
                # Первая загрузка: забираем всю историю 1D.
                cur_raw.execute(
                    """
                    SELECT dataname, datetime, open, high, low, close, volume
                    FROM bars_stocks_moex
                    WHERE tf = '1D'
                    ORDER BY datetime;
                    """
                )
            else:
                # Инкремент: только новые записи.
                cur_raw.execute(
                    """
                    SELECT dataname, datetime, open, high, low, close, volume
                    FROM bars_stocks_moex
                    WHERE tf = '1D'
                      AND datetime > %s
                    ORDER BY datetime;
                    """,
                    (last_dt,),
                )

            rows = cur_raw.fetchall()

        if not rows:
            print("Новых дневных данных MOEX нет.")
            return

        print(f"Найдено {len(rows)} новых дневных строк MOEX, начинаю загрузку в DWH...")

        with conn_dwh.cursor() as cur_dwh:
            for r in rows:
                cur_dwh.execute(
                    """
                    INSERT INTO dwh_bars_1d
                        (dataname, datetime, open, high, low, close, volume, source)
                    VALUES
                        (%s, %s::date, %s, %s, %s, %s, %s, 'MOEX_ISS')
                    ON CONFLICT (dataname, datetime, source) DO NOTHING;
                    """,
                    (
                        r["dataname"],
                        r["datetime"].date(),
                        r["open"],
                        r["high"],
                        r["low"],
                        r["close"],
                        r["volume"],
                    ),
                )

        conn_dwh.commit()
        print("Дневные данные MOEX успешно загружены в DWH.")

    except Exception as e:
        conn_dwh.rollback()
        conn_raw.rollback()
        print("Ошибка:", e)
        raise
    finally:
        conn_raw.close()
        conn_dwh.close()


if __name__ == "__main__":
    load_new_moex_daily()
