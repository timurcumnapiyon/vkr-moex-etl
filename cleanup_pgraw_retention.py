import os
from datetime import UTC, datetime, timedelta

import psycopg2
from psycopg2 import sql

PG_RAW_CONFIG = {
    "host": os.getenv("PG_RAW_HOST", "pg_raw"),
    "port": int(os.getenv("PG_RAW_PORT", "5432")),
    "dbname": os.getenv("PG_RAW_DB", "pg_raw"),
    "user": os.getenv("PG_RAW_USER", "postgres"),
    "password": os.getenv("PG_RAW_PASSWORD", "postgres"),
}

RETENTION_DAYS = int(os.getenv("PG_RAW_RETENTION_DAYS", "90"))
RAW_TABLES = (
    "bars_stocks_moex",
    "bars_stocks_alor",
    "bars_stocks_moex_fallback_1m",
)


def table_exists(conn, table_name: str) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass(%s) IS NOT NULL;", (f"public.{table_name}",))
        return bool(cur.fetchone()[0])


def count_old_rows(conn, table_name: str, cutoff_utc: datetime) -> int:
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL("SELECT count(*) FROM public.{} WHERE datetime < %s;").format(
                sql.Identifier(table_name)
            ),
            (cutoff_utc,),
        )
        return int(cur.fetchone()[0])


def delete_old_rows(conn, table_name: str, cutoff_utc: datetime) -> int:
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL("DELETE FROM public.{} WHERE datetime < %s;").format(
                sql.Identifier(table_name)
            ),
            (cutoff_utc,),
        )
        return int(cur.rowcount)


def main() -> None:
    cutoff_utc = datetime.now(UTC) - timedelta(days=RETENTION_DAYS)
    print(
        f"PG_RAW retention cleanup started. retention_days={RETENTION_DAYS} cutoff_utc={cutoff_utc.isoformat()}"
    )

    conn = psycopg2.connect(**PG_RAW_CONFIG)
    try:
        total_deleted = 0
        for table in RAW_TABLES:
            if not table_exists(conn, table):
                print(f"{table}: skipped (table not found)")
                continue

            old_before = count_old_rows(conn, table, cutoff_utc)
            deleted = delete_old_rows(conn, table, cutoff_utc)
            total_deleted += deleted

            print(f"{table}: old_before={old_before} deleted={deleted}")

        conn.commit()
        print(f"PG_RAW retention cleanup completed. total_deleted={total_deleted}")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
