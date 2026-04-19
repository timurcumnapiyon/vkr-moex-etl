import os
import psycopg2
import pandas as pd
from datetime import datetime

PG_RAW = {
    "host": os.getenv("PG_RAW_HOST", "pg_raw"), 
    "port": int(os.getenv("PG_RAW_PORT", "5432")),
    "user": os.getenv("PG_RAW_USER", "postgres"),
    "password": os.getenv("PG_RAW_PASSWORD", "postgres"),
    "dbname": os.getenv("PG_RAW_DB", "pg_raw"),
}

PG_DWH = {
    "host": os.getenv("PG_DWH_HOST", "pg_dwh"),
    "port": int(os.getenv("PG_DWH_PORT", "5432")),
    "user": os.getenv("PG_DWH_USER", "postgres"),
    "password": os.getenv("PG_DWH_PASSWORD", "postgres"),
    "dbname": os.getenv("PG_DWH_DB", "pg_dwh"),
}

def _connect(cfg):
    return psycopg2.connect(**cfg)

def get_watermark():
    with _connect(PG_DWH) as conn, conn.cursor() as cur:
        cur.execute("SELECT COALESCE(MAX(ts), '1970-01-01') FROM moex_agg;")
        (wm,) = cur.fetchone()
        print(f"[INFO] Watermark: {wm}")
        return wm

def extract_from_pg_raw(since_ts):
    q = """
      SELECT 
          dataname AS secid,
          datetime AS ts,
          high,
          low,
          volume
      FROM bars_stocks_moex
      WHERE tf = '1D'
        AND datetime > %s
      ORDER BY 1,2
      LIMIT 50000;
    """
    with _connect(PG_RAW) as conn:
        df = pd.read_sql(q, conn, params=[since_ts])
    print(f"[INFO] Extracted RAW rows: {len(df)}")
    return df

def transform(df):
    if df.empty:
        print("[INFO] No rows to transform")
        return df
    df["avg_price"] = (df["high"] + df["low"]) / 2
    df["ts"] = pd.to_datetime(df["ts"], utc=True)
    df["volume"] = df["volume"].fillna(0).astype("int64")
    df = df[["secid", "ts", "avg_price", "volume"]]
    print(f"[INFO] Transformed rows: {len(df)}")
    return df

def load_to_dwh(df):
    if df.empty:
        print("[INFO] Nothing to load.")
        return 0
    with _connect(PG_DWH) as conn, conn.cursor() as cur:
        args = [tuple(r) for r in df.itertuples(index=False, name=None)]
        cur.executemany("""
            INSERT INTO moex_agg (secid, ts, avg_price, volume)
            VALUES (%s,%s,%s,%s)
            ON CONFLICT (secid, ts) DO UPDATE
              SET avg_price = EXCLUDED.avg_price,
                  volume = EXCLUDED.volume;
        """, args)
        conn.commit()
        print(f"[INFO] Loaded to DWH: {len(args)} rows")
        return len(args)

def main():
    wm = get_watermark()
    df_raw = extract_from_pg_raw(wm)
    df_tr = transform(df_raw)
    load_to_dwh(df_tr)
    print("🎉 RAW → DWH ETL COMPLETED")

if __name__ == "__main__":
    main()
