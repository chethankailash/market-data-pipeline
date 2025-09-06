import sqlite3
import pandas as pd
import streamlit as st
import altair as alt
import os

# Resolve DB path relative to project root
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
DB_PATH = os.path.join(PROJECT_ROOT, "data", "market_data.db")

st.set_page_config(page_title="Market Data Dashboard", layout="wide")
st.title("Market Data Dashboard (Airflow + Streamlit)")

# Sidebar controls
st.sidebar.header("Controls")
with sqlite3.connect(DB_PATH) as conn:
    tickers = [row[0] for row in conn.execute("SELECT DISTINCT ticker FROM candles")]

ticker = st.sidebar.selectbox("Select Ticker", tickers)
limit = st.sidebar.slider("Show last N candles", 50, 2000, 500)

# Load data
with sqlite3.connect(DB_PATH) as conn:
    df = pd.read_sql(
        """
        SELECT ts, open, high, low, close, volume 
        FROM candles 
        WHERE ticker = ? 
        ORDER BY ts DESC LIMIT ?
        """,
        conn,
        params=(ticker, limit),
    )

df["ts"] = pd.to_datetime(df["ts"], unit="s")
df = df.sort_values("ts")

# Add moving averages for trend lines
df["ma20"] = df["close"].rolling(window=20).mean()
df["ma50"] = df["close"].rolling(window=50).mean()

# ==============================
# Summary Metrics
# ==============================
if not df.empty:
    latest_price = df["close"].iloc[-1]
    prev_price = df["close"].iloc[0]
    pct_change = ((latest_price - prev_price) / prev_price) * 100 if prev_price else 0
    avg_vol = df["volume"].mean()

    c1, c2, c3 = st.columns(3)
    c1.metric("Last Price", f"{latest_price:.2f}")
    c2.metric("Change (%)", f"{pct_change:.2f} %")
    c3.metric("Avg Volume", f"{avg_vol:,.0f}")

# ==============================
# Charts
# ==============================
if df.empty:
    st.warning("⚠️ No valid candles available. Try another symbol or wait for fresh data.")
else:
    # Candlestick chart
    base = alt.Chart(df).encode(x="ts:T")

    candles = (
        base.mark_rule()
        .encode(y="low:Q", y2="high:Q")
        .encode(tooltip=["ts", "open", "high", "low", "close", "volume"])
        + base.mark_bar()
        .encode(
            y="open:Q",
            y2="close:Q",
            color=alt.condition("datum.close >= datum.open", alt.value("green"), alt.value("red")),
        )
    )

    # Moving averages overlay
    ma20 = base.mark_line(color="blue").encode(y="ma20:Q")
    ma50 = base.mark_line(color="orange").encode(y="ma50:Q")

    st.altair_chart(
        (candles + ma20 + ma50).properties(width=1000, height=400), use_container_width=True
    )

    # Volume chart
    volume = (
        alt.Chart(df)
        .mark_bar(opacity=0.5)
        .encode(x="ts:T", y="volume:Q", tooltip=["volume"])
        .properties(width=1000, height=150)
    )
    st.altair_chart(volume, use_container_width=True)

    # ==============================
    # Data Preview
    # ==============================
    st.subheader("📑 Latest Candle Data")
    st.dataframe(df.tail(20).reset_index(drop=True), use_container_width=True)

    # ==============================
    # Extra Stats
    # ==============================
    st.subheader("📈 Additional Statistics")
    col1, col2, col3 = st.columns(3)
    col1.metric("High", f"{df['high'].max():.2f}")
    col2.metric("Low", f"{df['low'].min():.2f}")
    col3.metric("Volatility (Std Dev)", f"{df['close'].std():.2f}")
