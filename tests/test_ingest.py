import sqlite3
import pandas as pd
from datetime import datetime, timedelta, timezone
import yfinance as yf

DB_PATH = "./data/market_data.db"

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

def _ensure_db():
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(SQL_CREATE)
        conn.commit()

def _fetch_recent(ticker: str, minutes: int) -> pd.DataFrame:
    df = yf.download(ticker, period="1d", interval="1m", progress=False)

    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [c[1].lower() for c in df.columns]
    elif all(str(c).upper() == ticker.upper() for c in df.columns):
        df.columns = ["open", "high", "low", "close", "volume"]
    else:
        df.columns = [c.lower() for c in df.columns]

    print(f"[DEBUG] {ticker} normalized columns: {df.columns.tolist()}")
    print(df.head())

    cutoff = datetime.now(timezone.utc) - timedelta(minutes=minutes)
    df = df[df.index >= cutoff]

    expected_cols = ["open", "high", "low", "close", "volume"]
    for col in expected_cols:
        if col not in df.columns:
            df[col] = 0.0

    return df[expected_cols] if not df.empty else pd.DataFrame(columns=expected_cols)

def _upsert(ticker: str, df: pd.DataFrame):
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
                    float(row.get("open", 0) or 0),
                    float(row.get("high", 0) or 0),
                    float(row.get("low", 0) or 0),
                    float(row.get("close", 0) or 0),
                    float(row.get("volume", 0) or 0),
                ))
            except Exception as e:
                print(f"[ERROR] Row failed at {ts}: {e}")
        conn.commit()
    return len(df)

if __name__ == "__main__":
    _ensure_db()
    df = _fetch_recent("BTC-USD", 5)
    print(f"Fetched {len(df)} rows after filtering")
    rows = _upsert("BTC-USD", df)
    print(f"Inserted {rows} rows into DB")
