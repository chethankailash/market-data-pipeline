# Real-Time Market Data Pipeline

A demo project by CK using **Apache Airflow** + **Streamlit**.

## Features
- Fetches 1-minute candles for AAPL, MSFT, TSLA, BTC-USD via yfinance
- Stores into SQLite
- Airflow DAG runs every 5 minutes
- Streamlit dashboard shows live charts

## Setup

### 1. Create venv
```bash
python3 -m venv .venv
source .venv/bin/activate
