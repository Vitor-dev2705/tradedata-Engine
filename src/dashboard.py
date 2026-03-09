import duckdb
import pandas as pd
import streamlit as st
import plotly.graph_objects as go
import os

@st.cache_data(ttl=5) # Reduzi o TTL para 5s para refletir atualizações rápidas do pipeline
def load_data(symbol):
    db_path = r'C:\Users\Micro\Desktop\PORTIFÓLIO\tradedata-Engine\data\silver\trading.db'
    
    if not os.path.exists(db_path):
        return pd.DataFrame(), pd.DataFrame(), None

    con = duckdb.connect(db_path, read_only=True)
    try:
        df = con.execute(f"SELECT * FROM daily_metrics WHERE symbol = '{symbol}' ORDER BY time").df()
        
        if df.empty:
            return pd.DataFrame(), pd.DataFrame(), None
            
        df.columns = [c.lower() for c in df.columns]
        
        mapping = {
            'open': 'open_price' if 'open_price' in df.columns else 'open',
            'high': 'high_price' if 'high_price' in df.columns else 'high',
            'low': 'low_price' if 'low_price' in df.columns else 'low',
            'close': 'close_price' if 'close_price' in df.columns else 'close'
        }

        try:
            zones = con.execute(f"SELECT * FROM price_action_zones WHERE symbol = '{symbol}'").df()
            if not zones.empty:
                zones.columns = [c.lower() for c in zones.columns]
        except:
            zones = pd.DataFrame()
            
        return df, zones, mapping
    finally:
        con.close()

st.set_page_config(page_title="TradeData Engine | Dashboard", layout="wide")

st.sidebar.header(" Configurações")
ativo_selecionado = st.sidebar.selectbox(
    "Escolha o Ativo:",
    ["BTC-USD", "ETH-USD", "SOL-USD"] 
)

st.title(f" Análise de Mercado: {ativo_selecionado}")

df, zones, mapping = load_data(ativo_selecionado)

if not df.empty:
    fig = go.Figure(data=[go.Candlestick(
        x=df['time'], 
        open=df[mapping['open']], 
        high=df[mapping['high']],
        low=df[mapping['low']], 
        close=df[mapping['close']],
        name="Price"
    )])
    
    if not zones.empty:
        for _, row in zones.iterrows():
            cor = "cyan" if row['type'].lower() == 'suporte' else "orange"
            fig.add_hline(
                y=row['price'], 
                line_dash="dash", 
                line_color=cor, 
                annotation_text=f"{row['type']}: {row['price']:.2f}",
                annotation_position="bottom right"
            )

    fig.update_layout(
        template="plotly_dark", 
        height=650, 
        xaxis_rangeslider_visible=False,
        margin=dict(l=20, r=20, t=20, b=20)
    )
    
    st.plotly_chart(fig, use_container_width=True)
    
    col1, col2, col3 = st.columns(3)
    ultimo_preco = df[mapping['close']].iloc[-1]
    col1.metric("Último Preço", f"$ {ultimo_preco:,.2f}")
    col2.metric("Zonas Identificadas", len(zones))
    col3.metric("Status do Pipeline", " Sincronizado")
    
else:
    st.error(f" Sem dados para {ativo_selecionado}.")
    st.info("Execute o pipeline completo para gerar este banco de dados: `python src/run_pipeline.py`")