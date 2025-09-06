import os
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import List

import pandas as pd
import yfinance as yf
from airflow.decorators import dag, task
from dotenv import load_dotenv

# -----------------------------
# Load env vars
# -----------------------------
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
load_dotenv(os.path.join(ROOT, ".env"))

DB_PATH = os.getenv("DB_PATH")
if not DB_PATH:
    DB_PATH = os.path.join(
        "/home/chethan-kailashnath/Projects/market-data-pipeline",
        "data",
        "market_data.db",
    )

TICKERS = [t.strip() for t in os.getenv("TICKERS", "AAPL,MSFT,BTC-USD").split(",")]
LOOKBACK_MINUTES = int(os.getenv("LOOKBACK_MINUTES", "60"))

os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

# -----------------------------
# SQL schema
# -----------------------------
SQL_CREATE = """
CREATE TABLE IF NOT EXISTS candles (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker TEXT NOT NULL,
    ts INTEGER NOT NULL,
    open REAL, high REAL, low REAL, close REAL,
    volume REAL,
    UNIQUE(ticker, ts)
);
"""
SQL_INSERT = """
INSERT OR IGNORE INTO candles (ticker, ts, open, high, low, close, volume)
VALUES (?, ?, ?, ?, ?, ?, ?)
"""

# -----------------------------
# Helpers
# -----------------------------
def _ensure_db():
    """Create candles table if missing."""
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(SQL_CREATE)
        conn.commit()


def _fetch_recent(ticker: str, minutes: int) -> pd.DataFrame:
    df = yf.download(ticker, period="1d", interval="1m", progress=False)

    # Case 1: MultiIndex (older yfinance)
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [c[1].lower() if c[1] else c[0].lower() for c in df.columns]

    # Case 2: New style where every column == ticker
    elif len(df.columns) == 5 and all(str(c).upper() == ticker.upper() for c in df.columns):
        df.columns = ["open", "high", "low", "close", "volume"]

    # Case 3: Already clean
    else:
        df.columns = [c.lower() for c in df.columns]

    print(f"[DEBUG] {ticker} normalized columns: {df.columns.tolist()}")

    # Only keep last X minutes
    cutoff = datetime.now(timezone.utc) - timedelta(minutes=minutes)
    df = df[df.index >= cutoff]

    expected_cols = ["open", "high", "low", "close", "volume"]
    for col in expected_cols:
        if col not in df.columns:
            df[col] = 0.0  # fill missing

    return df[expected_cols] if not df.empty else pd.DataFrame(columns=expected_cols)




def _upsert(ticker: str, df: pd.DataFrame):
    """Insert rows into SQLite."""
    if df.empty:
        print(f"[DEBUG] No rows to insert for {ticker}")
        return 0

    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.cursor()
        for ts, row in df.iterrows():
            try:
                epoch = int(pd.to_datetime(ts).timestamp())
                cur.execute(SQL_INSERT, (
                    ticker, epoch,
                    float(row["open"]), float(row["high"]), float(row["low"]),
                    float(row["close"]), float(row["volume"])
                ))
            except Exception as e:
                print(f"[ERROR] Failed to insert row for {ticker}: {e}")
        conn.commit()
    return len(df)


# -----------------------------
# Airflow DAG
# -----------------------------
default_args = {"owner": "CK", "retries": 1}

@dag(
    dag_id="market_data_pipeline",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule="*/5 * * * *",  # every 5 minutes
    catchup=False,
    tags=["fintech", "demo"],
)
def pipeline():
    @task
    def ensure_db():
        _ensure_db()
        return "DB ready"

    @task
    def list_tickers() -> List[str]:
        return TICKERS

    @task
    def ingest_one(ticker: str):
        df = _fetch_recent(ticker, LOOKBACK_MINUTES)
        print(f"[DEBUG] {ticker} fetched {len(df)} rows")
        if not df.empty:
            print(df.head())
        rows = _upsert(ticker, df)
        print(f"[DEBUG] {ticker} inserted {rows} rows")
        return f"{ticker}: {rows} rows"

    @task
    def show_counts():
        with sqlite3.connect(DB_PATH) as conn:
            cur = conn.cursor()
            cur.execute("SELECT ticker, COUNT(*) FROM candles GROUP BY ticker;")
            results = cur.fetchall()
        print("[DEBUG] Row counts:", results)
        return {t: c for t, c in results}

    ensure = ensure_db()
    tickers = list_tickers()
    inserted = ingest_one.expand(ticker=tickers)
    counts = show_counts()

    ensure >> tickers >> inserted >> counts


pipeline()
