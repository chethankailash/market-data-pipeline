import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta, timezone

def _fetch_recent(ticker: str, minutes: int) -> pd.DataFrame:
    df = yf.download(ticker, period="1d", interval="1m", progress=False)

    # Flatten yfinance’s MultiIndex columns if present
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [c[1] if c[1] else c[0] for c in df.columns]

    # Normalize to lowercase
    df.columns = [c.lower() for c in df.columns]

    print(f"[DEBUG] {ticker} columns: {df.columns.tolist()}")
    print(df.head())

    # Filter recent rows
    cutoff = datetime.now(timezone.utc) - timedelta(minutes=minutes)
    df = df[df.index >= cutoff]

    expected_cols = ["open", "high", "low", "close", "volume"]
    for col in expected_cols:
        if col not in df.columns:
            df[col] = 0.0  # add missing with default

    return df[expected_cols] if not df.empty else pd.DataFrame(columns=expected_cols)

if __name__ == "__main__":
    df = _fetch_recent("BTC-USD", 5)
    print(f"Fetched {len(df)} rows")
    print(df.tail())
