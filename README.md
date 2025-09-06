# 📊 Market Data Pipeline & Dashboard

An **end-to-end data pipeline** that ingests crypto & stock candlestick data with **Apache Airflow**, stores it in **SQLite**, and visualizes it in an interactive **Streamlit dashboard**.

---

## 🚀 Features
- **Airflow DAG**: fetches OHLCV data (BTC-USD, ETH-USD, SOL-USD, AAPL, MSFT) via yfinance → stores in `data/market_data.db`.
- **Streamlit Dashboard**: 
  - Candlestick & volume charts  
  - Ticker dropdown + candle count slider  
  - Stats panel (latest price, % change, volatility, moving averages)  
- **Extensible**: add more tickers easily in the DAG, dashboard auto-detects.

---

## ⚙️ Setup

### Local (Airflow + Streamlit)
```bash
git clone https://github.com/<your-username>/market-data-pipeline.git
cd market-data-pipeline
pip install -r requirements.txt

# Run Airflow
export AIRFLOW_HOME=~/airflow
airflow db init
airflow dags test market_data_pipeline $(date +%Y-%m-%d)

# Run Streamlit dashboard
cd dashboard
streamlit run app.py

## Future Ideas

- Add more tickers / assets  
- Switch to PostgreSQL for production  
- Alerts when thresholds are crossed  
- Deploy Airflow on Kubernetes  
