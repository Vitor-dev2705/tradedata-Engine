import duckdb
import pandas as pd
import os

def detect_levels(df):
    levels = []
    for i in range(2, len(df) - 2):
        if df['high_price'][i] > df['high_price'][i-1] and \
           df['high_price'][i] > df['high_price'][i-2] and \
           df['high_price'][i] > df['high_price'][i+1] and \
           df['high_price'][i] > df['high_price'][i+2]:
            levels.append((df['symbol'][i], df['time'][i], df['high_price'][i], 'Resistencia'))

        # Detecção de Suporte (Fundo)
        if df['low_price'][i] < df['low_price'][i-1] and \
           df['low_price'][i] < df['low_price'][i-2] and \
           df['low_price'][i] < df['low_price'][i+1] and \
           df['low_price'][i] < df['low_price'][i+2]:
            levels.append((df['symbol'][i], df['time'][i], df['low_price'][i], 'Suporte'))
    return levels

def get_market_analysis():
    db_path = r'C:\Users\Micro\Desktop\PORTIFÓLIO\tradedata-Engine\data\silver\trading.db'
    
    if not os.path.exists(db_path):
        print(f"Erro: Banco não encontrado em {db_path}")
        return

    con = duckdb.connect(database=db_path)
    
    try:
        con.execute("CREATE OR REPLACE TABLE price_action_zones (symbol VARCHAR, time TIMESTAMP, price DOUBLE, type VARCHAR)")
        
        symbols = [s[0] for s in con.execute("SELECT DISTINCT symbol FROM daily_metrics").fetchall()]
        
        total_zones = 0
        for sym in symbols:
            df = con.execute(f"SELECT * FROM daily_metrics WHERE symbol = '{sym}' ORDER BY time ASC").df()
            
            if len(df) < 5:
                continue
                
            zones = detect_levels(df)
            for zone in zones:
                con.execute("INSERT INTO price_action_zones VALUES (?, ?, ?, ?)", (zone[0], zone[1], zone[2], zone[3]))
            
            print(f"✅ {sym}: {len(zones)} zonas identificadas.")
            total_zones += len(zones)
            
        print(f"🏁 MarketBrain: Total de {total_zones} zonas processadas.")
        
    finally:
        con.close()

if __name__ == "__main__":
    get_market_analysis()