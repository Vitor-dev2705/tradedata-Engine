import os
import sys
import subprocess
import duckdb
import pandas as pd
import streamlit as st
import plotly.graph_objects as go
from pathlib import Path

try:
    from streamlit_autorefresh import st_autorefresh
except ImportError:
    subprocess.check_call([sys.executable, "-m", "pip", "install", "streamlit-autorefresh"])
    from streamlit_autorefresh import st_autorefresh

from extract import extract_data, get_crypto_list
from transform import process_data
from gerar_linhas import fix_gold_layer

st.set_page_config(page_title="TradeData Engine", layout="wide")

st.markdown("""
    <style>
    [data-testid="stMetricValue"] { font-size: 28px; color: #00f2ff; }
    [data-testid="stMetric"] {
        background-color: rgba(28, 131, 225, 0.05);
        padding: 15px;
        border-radius: 10px;
        border: 1px solid rgba(28, 131, 225, 0.1);
    }
    </style>
    """, unsafe_allow_html=True)

def run_full_pipeline():
    st.cache_resource.clear() 
    ativos = get_crypto_list()
    extract_data(ativos)
    process_data()
    fix_gold_layer()
    st.rerun()

def get_db_path():
    relative_path = Path(__file__).parent.parent / "data" / "silver" / "trading.db"
    if relative_path.exists():
        return relative_path
    return Path(r'C:\Users\Micro\Desktop\PORTIFÓLIO\tradedata-Engine\data\silver\trading.db')

def get_all_symbols(db_path):
    if not db_path.exists():
        return []
    try:
        with duckdb.connect(str(db_path), read_only=True) as con:
            df_symbols = con.execute("SELECT DISTINCT UPPER(symbol) as symbol FROM daily_metrics ORDER BY symbol").df()
        return df_symbols['symbol'].tolist()
    except:
        return []

def load_data(db_path, symbol):
    if not db_path.exists():
        return pd.DataFrame(), pd.DataFrame(), str(db_path)
    
    try:
        with duckdb.connect(str(db_path), read_only=True) as con:
            df = con.execute(f"SELECT * FROM daily_metrics WHERE UPPER(symbol) = '{symbol.upper()}' ORDER BY time").df()
            df.columns = [c.lower() for c in df.columns]
            try:
                zones = con.execute(f"SELECT * FROM price_action_zones WHERE UPPER(symbol) = '{symbol.upper()}'").df()
                zones.columns = [c.lower() for c in zones.columns]
            except:
                zones = pd.DataFrame()
        return df, zones, str(db_path)
    except:
        return pd.DataFrame(), pd.DataFrame(), "Erro"

db_current = get_db_path()
st.sidebar.header("TradeData Engine")

if st.sidebar.button("Rodar Pipeline Completo"):
    run_full_pipeline()

lista_ativos = get_all_symbols(db_current)

if lista_ativos:
    ativo_selecionado = st.sidebar.selectbox("Ativo", lista_ativos)
    st_autorefresh(interval=900000, key="datarefresh")
    
    df, zones, debug_info = load_data(db_current, ativo_selecionado)

    if not df.empty:
        ultimo_preco = float(df['close_price'].iloc[-1])
        st.title(f"Analise: {ativo_selecionado}")
        
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
                    line_width=1.5,
                    opacity=0.7,
                    annotation_text=f"{row['type'].upper()} (S:{strength})",
                    annotation_position="right"
                )

        fig.update_layout(
            template="plotly_dark", 
            height=650, 
            xaxis_rangeslider_visible=False,
            margin=dict(l=10, r=10, t=30, b=10)
        )
        st.plotly_chart(fig, use_container_width=True)
        
        col1, col2, col3 = st.columns(3)
        col1.metric("Preco Atual", f"$ {ultimo_preco:,.2f}")
        col2.metric("Zonas", len(zones))
        col3.metric("Status", "Online")
    else:
        st.warning("Sem dados.")
else:
    st.error("Banco de dados nao encontrado.")