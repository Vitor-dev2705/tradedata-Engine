import os
import sys
import subprocess

try:
    from streamlit_autorefresh import st_autorefresh
except ImportError:
    subprocess.check_call([sys.executable, "-m", "pip", "install", "streamlit-autorefresh"])
    from streamlit_autorefresh import st_autorefresh

import duckdb
import pandas as pd
import streamlit as st
import plotly.graph_objects as go
from pathlib import Path

st.set_page_config(page_title="TradeData Engine", layout="wide")

st.markdown("""
    <style>
    [data-testid="stMetricValue"] { font-size: 28px; }
    [data-testid="stMetric"] {
        background-color: rgba(28, 131, 225, 0.1);
        padding: 15px;
        border-radius: 10px;
        border: 1px solid rgba(28, 131, 225, 0.2);
    }
    </style>
    """, unsafe_allow_html=True)

@st.cache_resource
def get_connection(db_path):
    return duckdb.connect(str(db_path), read_only=True)

def load_data(symbol):
    if os.path.exists('/app'):
        db_path = Path('/app/data/silver/trading.db')
    else:
        db_path = Path(r'C:\Users\Micro\Desktop\PORTIFÓLIO\tradedata-Engine\data\silver\trading.db')
    
    if not db_path.exists():
        return pd.DataFrame(), pd.DataFrame()

    con = get_connection(db_path)
    try:
        df = con.execute(f"SELECT * FROM daily_metrics WHERE symbol = '{symbol}' ORDER BY time").df()
        df.columns = [c.lower() for c in df.columns]

        try:
            zones = con.execute(f"SELECT * FROM price_action_zones WHERE symbol = '{symbol}'").df()
            zones.columns = [c.lower() for c in zones.columns]
        except:
            zones = pd.DataFrame()
            
        return df, zones
    except:
        return pd.DataFrame(), pd.DataFrame()

st_autorefresh(interval=30000, key="datarefresh")

st.sidebar.header("Filtros")
ativo = st.sidebar.selectbox("Ativo:", ["BTC-USD", "ETH-USD", "SOL-USD"])

df, zones = load_data(ativo)

if not df.empty:
    ultimo_preco = float(df['close_price'].iloc[-1])
    st.title(f"Analise: {ativo}")
    
    fig = go.Figure()

    fig.add_trace(go.Candlestick(
        x=df['time'], 
        open=df['open_price'], high=df['high_price'],
        low=df['low_price'], close=df['close_price'], 
        name="Preco"
    ))

    if not zones.empty:
        max_s = zones['strength'].max() if 'strength' in zones.columns and zones['strength'].max() > 0 else 1
        
        for _, row in zones.iterrows():
            is_sup = 'suporte' in str(row['type']).lower()
            cor = "#00f2ff" if is_sup else "#ff9900"
            
            strength = row['strength'] if 'strength' in row else 1
            dash_style = "solid" if strength >= (max_s * 0.5) else "dash"
            
            fig.add_hline(
                y=float(row['price']), 
                line_dash=dash_style,
                line_color=cor, 
                line_width=2,
                annotation_text=f"{row['type']} (S:{strength})",
                annotation_position="right"
            )

    fig.update_layout(template="plotly_dark", height=700, xaxis_rangeslider_visible=False)
    st.plotly_chart(fig, width='stretch')
    
    m1, m2, m3 = st.columns(3)
    m1.metric("Preco Atual", f"$ {ultimo_preco:,.2f}")
    m2.metric("Zonas Detectadas", len(zones))
    m3.metric("Status", "Online")
else:
    st.error("Dados nao encontrados. Execute o pipeline primeiro.")