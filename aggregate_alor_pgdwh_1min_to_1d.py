import os
import psycopg2
from psycopg2.extras import RealDictCursor

PG_DWH = {
    "host": os.getenv("PG_DWH_HOST", "pg_dwh"),
    "port": int(os.getenv("PG_DWH_PORT", "5432")),
    "user": os.getenv("PG_DWH_USER", "postgres"),
    "password": os.getenv("PG_DWH_PASSWORD", "postgres"),
    "dbname": os.getenv("PG_DWH_DB", "pg_dwh"),
}

def load_alor_daily():
    conn = psycopg2.connect(**PG_DWH)
    conn.autocommit = False

    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT COALESCE(MAX(datetime), DATE '1970-01-01') AS max_day
                FROM dwh_bars_1d
                WHERE source = 'ALOR';
                """
            )
            last_day = cur.fetchone()["max_day"]

            cur.execute(
                """
                WITH daily AS (
                    SELECT
                        dataname,
                        (datetime AT TIME ZONE 'UTC')::date AS day,
                        (ARRAY_AGG(open ORDER BY datetime ASC))[1] AS open,
                        MAX(high) AS high,
                        MIN(low) AS low,
                        (ARRAY_AGG(close ORDER BY datetime DESC))[1] AS close,
                        SUM(volume)::double precision AS volume
                    FROM dwh_bars_1m
                    WHERE source = 'ALOR'
                      AND (datetime AT TIME ZONE 'UTC')::date > %s
                    GROUP BY dataname, (datetime AT TIME ZONE 'UTC')::date
                )
                SELECT dataname, day AS datetime, open, high, low, close, volume
                FROM daily
                ORDER BY datetime, dataname;
                """,
                (last_day,),
            )
            rows = cur.fetchall()

            if not rows:
                print("No new ALOR daily rows to aggregate.")
                conn.commit()
                return

            print(f"Found {len(rows)} ALOR daily aggregated rows.")

            for r in rows:
                cur.execute(
                    """
                    INSERT INTO dwh_bars_1d
                    (dataname, datetime, open, high, low, close, volume, source)
                    VALUES (%s, %s::date, %s, %s, %s, %s, %s, 'ALOR')
                    ON CONFLICT (dataname, datetime, source) DO UPDATE
                    SET open = EXCLUDED.open,
                        high = EXCLUDED.high,
                        low = EXCLUDED.low,
                        close = EXCLUDED.close,
                        volume = EXCLUDED.volume;
                    """,
                    (
                        r["dataname"],
                        r["datetime"],
                        r["open"],
                        r["high"],
                        r["low"],
                        r["close"],
                        r["volume"],
                    ),
                )

        conn.commit()
        print("ALOR daily bars upserted into dwh_bars_1d.")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    load_alor_daily()
