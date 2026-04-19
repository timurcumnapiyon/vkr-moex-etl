import os

import psycopg2
from psycopg2.extras import RealDictCursor

PG_RAW_CONFIG = {
    "host": os.getenv("PG_RAW_HOST", "pg_raw"),
    "port": int(os.getenv("PG_RAW_PORT", "5432")),
    "dbname": os.getenv("PG_RAW_DB", "pg_raw"),
    "user": os.getenv("PG_RAW_USER", "postgres"),
    "password": os.getenv("PG_RAW_PASSWORD", "postgres"),
}

PG_DWH_CONFIG = {
    "host": os.getenv("PG_DWH_HOST", "pg_dwh"),
    "port": int(os.getenv("PG_DWH_PORT", "5432")),
    "dbname": os.getenv("PG_DWH_DB", "pg_dwh"),
    "user": os.getenv("PG_DWH_USER", "postgres"),
    "password": os.getenv("PG_DWH_PASSWORD", "postgres"),
}

SOURCE_ALOR = "ALOR"
SOURCE_FALLBACK = "MOEX_ISS_FALLBACK"


def get_last_loaded_datetime(conn_dwh, source: str):
    with conn_dwh.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT max(datetime) AS max_dt
            FROM dwh_bars_1m
            WHERE source = %s;
            """,
            (source,),
        )
        row = cur.fetchone()
        return row["max_dt"]


def table_exists(conn_raw, table_name: str) -> bool:
    with conn_raw.cursor() as cur:
        cur.execute("SELECT to_regclass(%s) IS NOT NULL;", (table_name,))
        return bool(cur.fetchone()[0])


def fetch_rows(conn_raw, table_name: str, last_dt):
    if not table_exists(conn_raw, table_name):
        return []

    with conn_raw.cursor(cursor_factory=RealDictCursor) as cur_raw:
        if last_dt is None:
            cur_raw.execute(
                f"""
                SELECT dataname, datetime, open, high, low, close, volume
                FROM {table_name}
                WHERE tf = 'M1'
                ORDER BY datetime;
                """
            )
        else:
            cur_raw.execute(
                f"""
                SELECT dataname, datetime, open, high, low, close, volume
                FROM {table_name}
                WHERE tf = 'M1'
                  AND datetime > %s
                ORDER BY datetime;
                """,
                (last_dt,),
            )
        return cur_raw.fetchall()


def insert_rows(conn_dwh, rows, source: str, alor_batch_keys=None) -> int:
    if not rows:
        return 0

    if alor_batch_keys is None:
        alor_batch_keys = set()

    inserted = 0
    with conn_dwh.cursor() as cur_dwh:
        for r in rows:
            key = (r["dataname"], r["datetime"])

            # If ALOR for the same minute is present in this run, skip fallback row.
            if source == SOURCE_FALLBACK and key in alor_batch_keys:
                continue

            if source == SOURCE_FALLBACK:
                # Never insert fallback if ALOR already exists in DWH for the same minute.
                cur_dwh.execute(
                    """
                    INSERT INTO dwh_bars_1m
                        (dataname, datetime, open, high, low, close, volume, source)
                    SELECT
                        %s, %s, %s, %s, %s, %s, %s, %s
                    WHERE NOT EXISTS (
                        SELECT 1
                        FROM dwh_bars_1m a
                        WHERE a.dataname = %s
                          AND a.datetime = %s
                          AND a.source = %s
                    )
                    ON CONFLICT (dataname, datetime, source) DO NOTHING;
                    """,
                    (
                        r["dataname"],
                        r["datetime"],
                        r["open"],
                        r["high"],
                        r["low"],
                        r["close"],
                        r["volume"],
                        source,
                        r["dataname"],
                        r["datetime"],
                        SOURCE_ALOR,
                    ),
                )
            else:
                cur_dwh.execute(
                    """
                    INSERT INTO dwh_bars_1m
                        (dataname, datetime, open, high, low, close, volume, source)
                    VALUES
                        (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (dataname, datetime, source) DO NOTHING;
                    """,
                    (
                        r["dataname"],
                        r["datetime"],
                        r["open"],
                        r["high"],
                        r["low"],
                        r["close"],
                        r["volume"],
                        source,
                    ),
                )

            inserted += cur_dwh.rowcount

    return inserted


def remove_fallback_rows_covered_by_alor(conn_dwh) -> int:
    with conn_dwh.cursor() as cur:
        cur.execute(
            """
            DELETE FROM dwh_bars_1m fb
            USING dwh_bars_1m al
            WHERE fb.source = %s
              AND al.source = %s
              AND fb.dataname = al.dataname
              AND fb.datetime = al.datetime;
            """,
            (SOURCE_FALLBACK, SOURCE_ALOR),
        )
        return cur.rowcount


def load_new_alor_1m():
    conn_raw = psycopg2.connect(**PG_RAW_CONFIG)
    conn_dwh = psycopg2.connect(**PG_DWH_CONFIG)

    try:
        conn_raw.autocommit = False
        conn_dwh.autocommit = False

        # 1) ALOR rows
        last_dt_alor = get_last_loaded_datetime(conn_dwh, SOURCE_ALOR)
        rows_alor = fetch_rows(conn_raw, "bars_stocks_alor", last_dt_alor)

        # 2) Fallback rows
        last_dt_fallback = get_last_loaded_datetime(conn_dwh, SOURCE_FALLBACK)
        rows_fallback = fetch_rows(conn_raw, "bars_stocks_moex_fallback_1m", last_dt_fallback)

        if not rows_alor and not rows_fallback:
            print("Новых данных ALOR/FALLBACK 1m нет.")
            return

        alor_batch_keys = {(r["dataname"], r["datetime"]) for r in rows_alor}

        inserted_alor = insert_rows(conn_dwh, rows_alor, SOURCE_ALOR)
        inserted_fallback = insert_rows(conn_dwh, rows_fallback, SOURCE_FALLBACK, alor_batch_keys)

        removed_dupes = remove_fallback_rows_covered_by_alor(conn_dwh)

        conn_dwh.commit()

        print(
            "Загрузка в DWH 1m завершена. "
            f"ALOR fetched={len(rows_alor)} inserted={inserted_alor}; "
            f"FALLBACK fetched={len(rows_fallback)} inserted={inserted_fallback}; "
            f"fallback_removed_by_alor={removed_dupes}"
        )

    except Exception as e:
        conn_dwh.rollback()
        conn_raw.rollback()
        print("Ошибка:", e)
        raise
    finally:
        conn_raw.close()
        conn_dwh.close()


if __name__ == "__main__":
    load_new_alor_1m()
