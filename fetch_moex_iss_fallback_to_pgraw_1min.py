import logging
import os
from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo

import psycopg2
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

PG_RAW_CONFIG = {
    "host": os.getenv("PG_RAW_HOST", "pg_raw"),
    "port": int(os.getenv("PG_RAW_PORT", "5432")),
    "dbname": os.getenv("PG_RAW_DB", "pg_raw"),
    "user": os.getenv("PG_RAW_USER", "postgres"),
    "password": os.getenv("PG_RAW_PASSWORD", "postgres"),
}

MSK = ZoneInfo("Europe/Moscow")
ISS_CANDLES_URL = (
    "https://iss.moex.com/iss/engines/stock/markets/shares/boards/TQBR/securities/{symbol}/candles.json"
)

ALOR_DATANAMES = tuple(
    s.strip() for s in os.getenv("ALOR_DATANAMES", "TQBR.SBER,TQBR.GMKN").split(",") if s.strip()
)
FAILOVER_STALE_MINUTES = int(os.getenv("FAILOVER_STALE_MINUTES", "3"))
FALLBACK_LOOKBACK_MINUTES = int(os.getenv("FALLBACK_LOOKBACK_MINUTES", "180"))
SESSION_START = os.getenv("FALLBACK_SESSION_START", "10:00:00")
SESSION_END = os.getenv("FALLBACK_SESSION_END", "23:50:00")
FORCE_RUN = os.getenv("FALLBACK_FORCE_RUN", "false").strip().lower() in {"1", "true", "yes"}

logger = logging.getLogger("MoexFallback1m")


def build_http_session() -> requests.Session:
    retry = Retry(
        total=4,
        connect=4,
        read=4,
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


def parse_session_time(value: str) -> datetime.time:
    return datetime.strptime(value, "%H:%M:%S").time()


def is_session_open(now_msk: datetime) -> bool:
    if now_msk.isoweekday() > 5:
        return False
    start_t = parse_session_time(SESSION_START)
    end_t = parse_session_time(SESSION_END)
    return start_t <= now_msk.time() <= end_t


def ensure_fallback_table(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS bars_stocks_moex_fallback_1m (
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


def get_last_alor_bar_utc(conn) -> datetime | None:
    with conn.cursor() as cur:
        cur.execute("SELECT max(datetime) FROM bars_stocks_alor WHERE tf = 'M1';")
        return cur.fetchone()[0]


def get_last_known_bar_utc(conn, dataname: str) -> datetime | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT greatest(
                COALESCE(
                    (SELECT max(datetime) FROM bars_stocks_alor WHERE dataname = %s AND tf = 'M1'),
                    '-infinity'::timestamptz
                ),
                COALESCE(
                    (SELECT max(datetime) FROM bars_stocks_moex_fallback_1m WHERE dataname = %s AND tf = 'M1'),
                    '-infinity'::timestamptz
                )
            );
            """,
            (dataname, dataname),
        )
        value = cur.fetchone()[0]
        if value is None or str(value) == "-infinity":
            return None
        return value


def dataname_to_symbol(dataname: str) -> str:
    return dataname.split(".", 1)[1] if "." in dataname else dataname


def fetch_iss_1m_rows(
    session: requests.Session,
    dataname: str,
    dt_from_msk: datetime,
    dt_till_msk: datetime,
) -> list[tuple]:
    symbol = dataname_to_symbol(dataname)
    url = ISS_CANDLES_URL.format(symbol=symbol)
    params = {
        "interval": 1,
        "from": dt_from_msk.strftime("%Y-%m-%d %H:%M:%S"),
        "till": dt_till_msk.strftime("%Y-%m-%d %H:%M:%S"),
    }
    response = session.get(url, params=params, timeout=(10, 45))
    response.raise_for_status()
    payload = response.json()

    candles = payload.get("candles", {})
    columns = candles.get("columns", [])
    data = candles.get("data", [])
    if not data:
        return []

    col = {name: idx for idx, name in enumerate(columns)}
    required = ("begin", "open", "high", "low", "close")
    if any(name not in col for name in required):
        return []

    volume_col = "volume" if "volume" in col else ("value" if "value" in col else None)
    rows: list[tuple] = []
    for row in data:
        begin_raw = row[col["begin"]]
        if not begin_raw:
            continue
        dt_msk = datetime.strptime(begin_raw, "%Y-%m-%d %H:%M:%S").replace(tzinfo=MSK)
        dt_utc = dt_msk.astimezone(UTC)
        volume = row[col[volume_col]] if volume_col else None
        rows.append(
            (
                dataname,
                "M1",
                dt_utc,
                row[col["open"]],
                row[col["high"]],
                row[col["low"]],
                row[col["close"]],
                volume,
            )
        )
    return rows


def insert_rows(conn, rows: list[tuple]) -> int:
    if not rows:
        return 0
    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO bars_stocks_moex_fallback_1m
                (dataname, tf, datetime, open, high, low, close, volume)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (dataname, tf, datetime) DO NOTHING;
            """,
            rows,
        )
        inserted = cur.rowcount
    conn.commit()
    return inserted


def main() -> None:
    logging.basicConfig(
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%d.%m.%Y %H:%M:%S",
        level=logging.INFO,
        handlers=[
            logging.FileHandler("fetch_moex_iss_fallback_to_pgraw_1min.log", encoding="utf-8"),
            logging.StreamHandler(),
        ],
    )

    conn = psycopg2.connect(**PG_RAW_CONFIG)
    try:
        ensure_fallback_table(conn)

        now_msk = datetime.now(MSK)
        if not FORCE_RUN and not is_session_open(now_msk):
            logger.info("MOEX out of session (%s). Fallback waiting.", now_msk.strftime("%Y-%m-%d %H:%M:%S"))
            return
        if FORCE_RUN:
            logger.warning("FALLBACK_FORCE_RUN enabled: bypassing session guard for test.")

        last_alor = get_last_alor_bar_utc(conn)
        if last_alor is not None:
            lag = datetime.now(UTC) - last_alor
            if lag <= timedelta(minutes=FAILOVER_STALE_MINUTES):
                logger.info("ALOR is healthy (lag=%s). Fallback not activated.", lag)
                return
            logger.warning("ALOR is stale (last=%s lag=%s). MOEX fallback activated.", last_alor, lag)
        else:
            logger.warning("No ALOR bars found. MOEX fallback activated.")

        total_inserted = 0
        session = build_http_session()
        try:
            now_msk = datetime.now(MSK)
            for dataname in ALOR_DATANAMES:
                last_known_utc = get_last_known_bar_utc(conn, dataname)
                if last_known_utc is None:
                    dt_from_msk = now_msk - timedelta(minutes=FALLBACK_LOOKBACK_MINUTES)
                else:
                    dt_from_msk = last_known_utc.astimezone(MSK) + timedelta(minutes=1)

                if dt_from_msk >= now_msk:
                    continue

                rows = fetch_iss_1m_rows(session, dataname, dt_from_msk, now_msk)
                inserted = insert_rows(conn, rows)
                total_inserted += inserted
                logger.info(
                    "%s fallback bars fetched=%d inserted=%d from=%s to=%s",
                    dataname,
                    len(rows),
                    inserted,
                    dt_from_msk.strftime("%Y-%m-%d %H:%M:%S"),
                    now_msk.strftime("%Y-%m-%d %H:%M:%S"),
                )
        finally:
            session.close()

        logger.info("MOEX fallback completed. total_inserted=%d", total_inserted)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
