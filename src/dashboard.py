import duckdb
import pandas as pd
import streamlit as st
import plotly.graph_objects as go
import os
from streamlit_autorefresh import st_autorefresh

@st.cache_resource
def get_connection(db_path):
    return duckdb.connect(db_path, read_only=True)

def load_data(symbol):
    if os.path.exists('/app'):
        db_path = '/app/data/silver/trading.db'
    else:
        db_path = r'C:\Users\Micro\Desktop\PORTIFÓLIO\tradedata-Engine\data\silver\trading.db'
    
    if not os.path.exists(db_path):
        return pd.DataFrame(), pd.DataFrame(), None

    con = get_connection(db_path)
    try:
        df = con.execute(f"SELECT * FROM daily_metrics WHERE symbol = '{symbol}' ORDER BY time").df()
        if df.empty: return pd.DataFrame(), pd.DataFrame(), None
        df.columns = [c.lower() for c in df.columns]
        
        mapping = {'open': 'open_price', 'high': 'high_price', 'low': 'low_price', 'close': 'close_price', 'time': 'time'}

        try:
            zones = con.execute(f"SELECT * FROM price_action_zones WHERE symbol = '{symbol}'").df()
            if not zones.empty: zones.columns = [c.lower() for c in zones.columns]
        except Exception:
            zones = pd.DataFrame()
            
        return df, zones, mapping
    except Exception:
        return pd.DataFrame(), pd.DataFrame(), None

st.set_page_config(page_title="TradeData Engine | 2026 Pro", layout="wide")

st_autorefresh(interval=30000, key="datarefresh")

st.sidebar.header("Filtros de Mercado")
ativo_selecionado = st.sidebar.selectbox("Ativo:", ["BTC-USD", "ETH-USD", "SOL-USD"])
sensibilidade = st.sidebar.slider("Alerta de Proximidade (%)", 0.1, 5.0, 1.5)

df, zones, mapping = load_data(ativo_selecionado)

if not df.empty:
    df[mapping['time']] = pd.to_datetime(df[mapping['time']])
    ultimo_preco = float(df[mapping['close']].iloc[-1])

    col_a, col_b = st.columns([3, 1], gap="medium")
    with col_a:
        st.title(f"Analise em Tempo Real: {ativo_selecionado}")
    
    fig = go.Figure()

    fig.add_trace(go.Histogram(
        y=df[mapping['close']], 
        name="Perfil de Volume",
        orientation='h',
        marker_color='rgba(100, 200, 255, 0.1)',
        hoverinfo='skip',
        nbinsy=40,
        xaxis='x2'
    ))

    fig.add_trace(go.Candlestick(
        x=df[mapping['time']], open=df[mapping['open']], high=df[mapping['high']],
        low=df[mapping['low']], close=df[mapping['close']], name="Preco"
    ))

    fig.add_hline(
        y=ultimo_preco,
        line_dash="dot",
        line_color="white",
        line_width=1,
        annotation_text=f" ATUAL ${ultimo_preco:,.2f}",
        annotation_position="left",
        annotation_font_color="white"
    )
    
    if not zones.empty:
        max_strength = zones['strength'].max() if zones['strength'].max() > 0 else 1
        
        for _, row in zones.iterrows():
            is_sup = 'suporte' in str(row['type']).lower()
            cor = "#00f2ff" if is_sup else "#ff9900"
            espessura = 1 + (float(row['strength']) / max_strength) * 4
            
            fig.add_hline(
                y=float(row['price']), 
                line_dash="dash" if row['strength'] < (max_strength * 0.5) else "solid",
                line_color=cor, 
                line_width=espessura,
                opacity=0.8,
                annotation_text=f" {'SUP' if is_sup else 'RES'} (Forca: {row['strength']})",
                annotation_position="right", 
                annotation_font_color=cor
            )

    fig.update_layout(
        template="plotly_dark", height=750, xaxis_rangeslider_visible=False,
        margin=dict(l=10, r=100, t=30, b=10),
        xaxis2=dict(overlaying='x', side='top', showgrid=False, showticklabels=False, range=[0, len(df)*3]),
        hovermode="x unified"
    )
    
    st.plotly_chart(fig, width="stretch", theme="streamlit")
    
    m1, m2, m3, m4 = st.columns(4, gap="small")
    m1.metric("Preco Atual", f"$ {ultimo_preco:,.2f}")
    
    if not zones.empty:
        dist_min = min([abs(ultimo_preco - p) for p in zones['price']])
        dist_pct = (dist_min / ultimo_preco) * 100
        m2.metric("Distancia do Nivel", f"{dist_pct:.2f}%")
    
    m3.metric("Zonas Ativas", len(zones))
    m4.metric("Status", "Em Execucao")

else:
    st.error(f"Aguardando dados de {ativo_selecionado}...")