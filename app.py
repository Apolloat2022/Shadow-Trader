import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import requests

st.set_page_config(page_title="Shadow Trader Showcase", layout="wide")

# SIDEBAR: Project Info
st.sidebar.title("🚀 Project Showcase")
st.sidebar.info("""
**Architecture:** Medallion (Bronze/Silver/Gold)  
**Stack:** Python, AWS S3, Lambda, Streamlit  
**Goal:** Trend-following optimization for BTC.
""")

st.title("🏹 Shadow Trader: Live Market Intelligence")

# 🛰️ Public Coinbase Feed
def get_live_price():
    try:
        url = "https://api.coinbase.com/v2/prices/BTC-USD/spot"
        response = requests.get(url, timeout=5)
        return float(response.json()['data']['amount'])
    except:
        return None

# Load Gold Data
df = pd.read_parquet('gold_features.parquet')
df['date'] = pd.to_datetime(df['date'])

# Strategy Controls
st.sidebar.header("Strategy Settings")
window = st.sidebar.slider("SMA Window (Days)", 5, 200, 50)

# Logic
df['dynamic_sma'] = df['price'].rolling(window=window).mean()
live_px = get_live_price() or df.iloc[-1]['price']
is_bullish = live_px > df['dynamic_sma'].iloc[-1]
signal_text = "🟢 BULLISH" if is_bullish else "🔴 BEARISH"

# UI
col1, col2, col3 = st.columns(3)
col1.metric("Live Price (Coinbase)", f"${live_px:,.2f}")
col2.metric(f"{window}-Day SMA", f"${df['dynamic_sma'].iloc[-1]:,.2f}")
col3.metric("Current Signal", signal_text)

# Charting
fig = go.Figure()
fig.add_trace(go.Scatter(x=df['date'], y=df['price'], name='Historical', line=dict(color='gold', width=1)))
fig.add_trace(go.Scatter(x=df['date'], y=df['dynamic_sma'], name='SMA', line=dict(color='cyan', width=2)))
fig.update_layout(template='plotly_dark', height=500)
st.plotly_chart(fig, use_container_width=True)

with st.expander("📝 View Pipeline Methodology"):
    st.write("""
    1. **Bronze**: Raw AlphaVantage JSON ingested via AWS Lambda.
    2. **Silver**: Data cleaned, standardized, and stored as Parquet.
    3. **Gold**: Feature engineering (SMA) applied for signal generation.
    """)
