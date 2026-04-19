import os
import time

import pandas as pd
import psycopg2
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

API_URL = "https://iss.moex.com/iss/history/engines/stock/markets/shares/boards/TQBR/securities.json"
REQUEST_TIMEOUT = (10, 60)
PAGE_SIZE = 100
PAGE_MAX_RETRIES = 5

PG_CONFIG = {
    "host": os.getenv("PG_RAW_HOST", "pg_raw"),
    "port": int(os.getenv("PG_RAW_PORT", "5432")),
    "user": os.getenv("PG_RAW_USER", "postgres"),
    "password": os.getenv("PG_RAW_PASSWORD", "postgres"),
    "dbname": os.getenv("PG_RAW_DB", "pg_raw"),
}


def build_http_session() -> requests.Session:
    """Build a requests session with retries for transient network issues."""
    retry = Retry(
        total=5,
        connect=5,
        read=5,
        backoff_factor=1.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session = requests.Session()
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def extract_all_pages(api_url: str) -> pd.DataFrame:
    """Fetch all pages from MOEX ISS API with resilient retries."""
    all_rows = []
    start = 0
    session = build_http_session()

    try:
        while True:
            print(f"Fetching page: start={start}")
            raw = None
            last_error = None

            for attempt in range(1, PAGE_MAX_RETRIES + 1):
                try:
                    response = session.get(api_url, params={"start": start}, timeout=REQUEST_TIMEOUT)
                    response.raise_for_status()
                    raw = response.json()
                    break
                except (requests.RequestException, ValueError) as exc:
                    last_error = exc
                    wait_sec = min(30, 2 ** (attempt - 1))
                    print(
                        f"Page start={start} failed (attempt {attempt}/{PAGE_MAX_RETRIES}): {exc}. "
                        f"Waiting {wait_sec}s."
                    )
                    if attempt < PAGE_MAX_RETRIES:
                        time.sleep(wait_sec)

            if raw is None:
                raise RuntimeError(f"MOEX page start={start} could not be fetched") from last_error

            data = raw.get("history", {}).get("data", [])
            columns = raw.get("history", {}).get("columns", [])

            if not data:
                print("No more pages.")
                break

            df = pd.DataFrame(data, columns=columns)
            all_rows.append(df)
            start += PAGE_SIZE
    finally:
        session.close()

    if not all_rows:
        print("No rows fetched from MOEX.")
        return pd.DataFrame()

    full_df = pd.concat(all_rows, ignore_index=True)
    print(f"Total rows fetched: {len(full_df)}")
    return full_df


def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        print("Transform skipped: empty source dataframe.")
        return df

    df = df[["TRADEDATE", "SECID", "OPEN", "HIGH", "LOW", "CLOSE", "VALUE"]].copy()
    df.columns = ["datetime", "dataname", "open", "high", "low", "close", "volume"]

    df["tf"] = "1D"
    df["datetime"] = pd.to_datetime(df["datetime"])
    df.dropna(inplace=True)

    df = df[["dataname", "tf", "datetime", "open", "high", "low", "close", "volume"]]
    print(f"{len(df)} rows transformed.")
    return df


def load_to_pg(df: pd.DataFrame, config: dict):
    if df.empty:
        print("Load skipped: empty dataframe.")
        return

    conn = psycopg2.connect(**config)
    insert_query = """
    INSERT INTO bars_stocks_moex (dataname, tf, datetime, open, high, low, close, volume)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
    ON CONFLICT (dataname, tf, datetime)
    DO UPDATE SET open = EXCLUDED.open,
                  high = EXCLUDED.high,
                  low = EXCLUDED.low,
                  close = EXCLUDED.close,
                  volume = EXCLUDED.volume;
    """

    with conn.cursor() as cur:
        for _, row in df.iterrows():
            cur.execute(insert_query, tuple(row))
        conn.commit()

    conn.close()
    print("MOEX bars successfully saved to PG_RAW.")


def etl_process():
    print("1) Fetching all pages...")
    df_raw = extract_all_pages(API_URL)

    print("2) Transforming...")
    df_clean = transform_data(df_raw)

    print("3) Loading into PG_RAW...")
    load_to_pg(df_clean, PG_CONFIG)

    print("ETL process complete.")


if __name__ == "__main__":
    etl_process()
