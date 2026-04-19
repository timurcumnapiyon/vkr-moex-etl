import argparse
import json
import os
from dataclasses import dataclass
from typing import List, Tuple

import psycopg2


PG_DWH_CONFIG = {
    "host": os.getenv("PG_DWH_HOST", "pg_dwh"),
    "port": int(os.getenv("PG_DWH_PORT", "5432")),
    "dbname": os.getenv("PG_DWH_DB", "pg_dwh"),
    "user": os.getenv("PG_DWH_USER", "postgres"),
    "password": os.getenv("PG_DWH_PASSWORD", "postgres"),
}


@dataclass
class CheckResult:
    dataset: str
    check_name: str
    severity: str
    bad_rows: int
    details: dict

    @property
    def status(self) -> str:
        return "fail" if self.bad_rows > 0 else "pass"


def ensure_dq_table(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS public.dq_results (
                id bigserial PRIMARY KEY,
                check_time timestamptz NOT NULL DEFAULT now(),
                dataset text NOT NULL,
                check_name text NOT NULL,
                severity text NOT NULL,
                status text NOT NULL,
                bad_rows bigint NOT NULL,
                details jsonb NOT NULL DEFAULT '{}'::jsonb
            );
            CREATE INDEX IF NOT EXISTS idx_dq_results_check_time
                ON public.dq_results (check_time DESC);
            """
        )
    conn.commit()


def run_count_query(conn, sql: str, params: Tuple = ()) -> int:
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return int(cur.fetchone()[0] or 0)


def run_sample_query(conn, sql: str, params: Tuple = ()) -> List[dict]:
    with conn.cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()
        return [dict(zip([d[0] for d in cur.description], row)) for row in rows]


def check_1m_null_ohlc(conn, lookback_minutes: int) -> CheckResult:
    dataset = "dwh_bars_1m"
    where = (
        "source = 'ALOR' AND datetime >= now() - make_interval(mins => %s) "
        "AND (open IS NULL OR high IS NULL OR low IS NULL OR close IS NULL)"
    )
    bad_rows = run_count_query(conn, f"SELECT COUNT(*) FROM {dataset} WHERE {where};", (lookback_minutes,))
    sample = run_sample_query(
        conn,
        f"""
        SELECT dataname, datetime, open, high, low, close
        FROM {dataset}
        WHERE {where}
        ORDER BY datetime DESC
        LIMIT 5;
        """,
        (lookback_minutes,),
    )
    return CheckResult(dataset, "null_ohlc", "error", bad_rows, {"sample": sample})


def check_1m_high_lt_low(conn, lookback_minutes: int) -> CheckResult:
    dataset = "dwh_bars_1m"
    where = "source = 'ALOR' AND datetime >= now() - make_interval(mins => %s) AND high < low"
    bad_rows = run_count_query(conn, f"SELECT COUNT(*) FROM {dataset} WHERE {where};", (lookback_minutes,))
    sample = run_sample_query(
        conn,
        f"""
        SELECT dataname, datetime, open, high, low, close
        FROM {dataset}
        WHERE {where}
        ORDER BY datetime DESC
        LIMIT 5;
        """,
        (lookback_minutes,),
    )
    return CheckResult(dataset, "high_lt_low", "error", bad_rows, {"sample": sample})


def check_1m_duplicates(conn, lookback_minutes: int) -> CheckResult:
    dataset = "dwh_bars_1m"
    count_sql = f"""
        WITH dups AS (
            SELECT dataname, datetime, source, COUNT(*) AS cnt
            FROM {dataset}
            WHERE source = 'ALOR'
              AND datetime >= now() - make_interval(mins => %s)
            GROUP BY dataname, datetime, source
            HAVING COUNT(*) > 1
        )
        SELECT COUNT(*) FROM dups;
    """
    sample_sql = f"""
        SELECT dataname, datetime, source, COUNT(*) AS cnt
        FROM {dataset}
        WHERE source = 'ALOR'
          AND datetime >= now() - make_interval(mins => %s)
        GROUP BY dataname, datetime, source
        HAVING COUNT(*) > 1
        ORDER BY datetime DESC
        LIMIT 5;
    """
    bad_rows = run_count_query(conn, count_sql, (lookback_minutes,))
    sample = run_sample_query(conn, sample_sql, (lookback_minutes,))
    return CheckResult(dataset, "duplicate_dataname_datetime", "error", bad_rows, {"sample": sample})


def check_1m_time_gaps(
    conn,
    lookback_minutes: int,
    max_gap_minutes: int,
    allowed_gap_rows: int,
    session_start: str,
    session_end: str,
    timezone_name: str,
) -> CheckResult:
    dataset = "dwh_bars_1m"
    count_sql = f"""
        WITH ordered AS (
            SELECT
                dataname,
                source,
                datetime,
                LAG(datetime) OVER (PARTITION BY dataname, source ORDER BY datetime) AS prev_dt,
                (datetime AT TIME ZONE %s) AS local_dt,
                (LAG(datetime) OVER (PARTITION BY dataname, source ORDER BY datetime) AT TIME ZONE %s) AS prev_local_dt
            FROM {dataset}
            WHERE source = 'ALOR'
              AND datetime >= now() - make_interval(mins => %s)
        ),
        gaps AS (
            SELECT dataname, source, prev_dt, datetime,
                   EXTRACT(EPOCH FROM (datetime - prev_dt))/60.0 AS gap_minutes
            FROM ordered
            WHERE prev_dt IS NOT NULL
              AND local_dt::date = prev_local_dt::date
              AND EXTRACT(ISODOW FROM local_dt) BETWEEN 1 AND 5
              AND local_dt::time BETWEEN %s::time AND %s::time
              AND prev_local_dt::time BETWEEN %s::time AND %s::time
              AND datetime - prev_dt > make_interval(mins => %s)
        )
        SELECT COUNT(*) FROM gaps;
    """
    sample_sql = f"""
        WITH ordered AS (
            SELECT
                dataname,
                source,
                datetime,
                LAG(datetime) OVER (PARTITION BY dataname, source ORDER BY datetime) AS prev_dt,
                (datetime AT TIME ZONE %s) AS local_dt,
                (LAG(datetime) OVER (PARTITION BY dataname, source ORDER BY datetime) AT TIME ZONE %s) AS prev_local_dt
            FROM {dataset}
            WHERE source = 'ALOR'
              AND datetime >= now() - make_interval(mins => %s)
        )
        SELECT dataname, source, prev_dt, datetime,
               EXTRACT(EPOCH FROM (datetime - prev_dt))/60.0 AS gap_minutes
        FROM ordered
        WHERE prev_dt IS NOT NULL
          AND local_dt::date = prev_local_dt::date
          AND EXTRACT(ISODOW FROM local_dt) BETWEEN 1 AND 5
          AND local_dt::time BETWEEN %s::time AND %s::time
          AND prev_local_dt::time BETWEEN %s::time AND %s::time
          AND datetime - prev_dt > make_interval(mins => %s)
        ORDER BY datetime DESC
        LIMIT 5;
    """
    params = (
        timezone_name,
        timezone_name,
        lookback_minutes,
        session_start,
        session_end,
        session_start,
        session_end,
        max_gap_minutes,
    )
    raw_bad_rows = run_count_query(conn, count_sql, params)
    sample = run_sample_query(conn, sample_sql, params)
    bad_rows = max(raw_bad_rows - allowed_gap_rows, 0)
    return CheckResult(
        dataset,
        "time_gaps_over_threshold",
        "warn",
        bad_rows,
        {
            "raw_bad_rows": raw_bad_rows,
            "allowed_gap_rows": allowed_gap_rows,
            "max_gap_minutes": max_gap_minutes,
            "timezone": timezone_name,
            "session_start": session_start,
            "session_end": session_end,
            "sample": sample,
        },
    )


def persist_results(conn, results: List[CheckResult]) -> None:
    with conn.cursor() as cur:
        for r in results:
            cur.execute(
                """
                INSERT INTO public.dq_results (dataset, check_name, severity, status, bad_rows, details)
                VALUES (%s, %s, %s, %s, %s, %s::jsonb);
                """,
                (
                    r.dataset,
                    r.check_name,
                    r.severity,
                    r.status,
                    r.bad_rows,
                    json.dumps(r.details, default=str),
                ),
            )
    conn.commit()


def parse_args():
    parser = argparse.ArgumentParser(description="Run DWH data quality checks and write results to dq_results.")
    parser.add_argument("--lookback-minutes", type=int, default=int(os.getenv("DQ_LOOKBACK_MINUTES", "180")))
    parser.add_argument("--max-gap-minutes", type=int, default=int(os.getenv("DQ_MAX_GAP_MINUTES", "2")))
    parser.add_argument("--allowed-gap-rows", type=int, default=int(os.getenv("DQ_ALLOWED_GAP_ROWS", "100")))
    parser.add_argument("--session-start", type=str, default=os.getenv("DQ_SESSION_START", "10:00:00"))
    parser.add_argument("--session-end", type=str, default=os.getenv("DQ_SESSION_END", "23:50:00"))
    parser.add_argument("--timezone", type=str, default=os.getenv("DQ_TIMEZONE", "Europe/Moscow"))
    parser.add_argument(
        "--fail-on-error",
        action="store_true",
        default=str(os.getenv("DQ_FAIL_ON_ERROR", "false")).lower() == "true",
        help="Exit with non-zero code when any error-severity check fails.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    conn = psycopg2.connect(**PG_DWH_CONFIG)
    try:
        ensure_dq_table(conn)
        results = [
            check_1m_null_ohlc(conn, args.lookback_minutes),
            check_1m_high_lt_low(conn, args.lookback_minutes),
            check_1m_duplicates(conn, args.lookback_minutes),
            check_1m_time_gaps(
                conn,
                args.lookback_minutes,
                args.max_gap_minutes,
                args.allowed_gap_rows,
                args.session_start,
                args.session_end,
                args.timezone,
            ),
        ]
        persist_results(conn, results)

        for r in results:
            print(
                f"[DQ] dataset={r.dataset} check={r.check_name} severity={r.severity} "
                f"status={r.status} bad_rows={r.bad_rows}"
            )

        if args.fail_on_error:
            has_error = any(r.status == "fail" and r.severity == "error" for r in results)
            if has_error:
                raise SystemExit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
