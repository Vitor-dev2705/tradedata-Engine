import os
import sys

# Adiciona a raiz do projeto ao path para localizar o modulo 'src'
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
if project_root not in sys.path:
    sys.path.insert(0, project_root)

import duckdb
import pandas as pd
import numpy as np
from src.alerts import send_alert

def detect_levels(df, window=5):
    levels = []
    for i in range(window, len(df) - window):
        is_high = df['high_price'].iloc[i] == max(df['high_price'].iloc[i-window:i+window+1])
        is_low = df['low_price'].iloc[i] == min(df['low_price'].iloc[i-window:i+window+1])
        
        if is_high:
            levels.append((df['symbol'].iloc[i], df['time'].iloc[i], float(df['high_price'].iloc[i]), 'Resistencia'))
        if is_low:
            levels.append((df['symbol'].iloc[i], df['time'].iloc[i], float(df['low_price'].iloc[i]), 'Suporte'))
    return levels

def calculate_zone_strength(df, price, tolerance=0.01):
    upper_bound = price * (1 + tolerance)
    lower_bound = price * (1 - tolerance)
    touches = df[(df['high_price'] >= lower_bound) & (df['low_price'] <= upper_bound)]
    return len(touches)

def process_group(group_list, df_historico):
    group_df = pd.DataFrame(group_list)
    mean_price = group_df['price'].mean()
    predominant_type = group_df['type'].mode()[0]
    strength = calculate_zone_strength(df_historico, mean_price)
    
    return {
        'symbol': group_df['symbol'].iloc[0],
        'time': group_df['time'].iloc[0],
        'price': mean_price,
        'type': predominant_type,
        'strength': strength
    }

def group_and_filter_zones(levels_df, df_historico, threshold_percent=1.5):
    if levels_df.empty:
        return levels_df
    
    sorted_df = levels_df.sort_values(by='price').reset_index(drop=True)
    grouped_levels = []
    current_group = [sorted_df.iloc[0].to_dict()]
    
    for i in range(1, len(sorted_df)):
        current_row = sorted_df.iloc[i].to_dict()
        last_in_group = current_group[-1]
        price_diff_pct = ((current_row['price'] / last_in_group['price']) - 1) * 100
        
        if price_diff_pct <= threshold_percent:
            current_group.append(current_row)
        else:
            grouped_levels.append(process_group(current_group, df_historico))
            current_group = [current_row]
            
    if current_group:
        grouped_levels.append(process_group(current_group, df_historico))
        
    return pd.DataFrame(grouped_levels)

def process_market_analysis():
    db_path = os.path.join(project_root, 'data', 'silver', 'trading.db').replace('\\', '/')
    
    if not os.path.exists(db_path):
        print(f"Erro: Banco de dados nao encontrado em {db_path}")
        return

    con = duckdb.connect(db_path)
    
    try:
        symbols_df = con.execute("SELECT DISTINCT symbol FROM daily_metrics").df()
        
        con.execute("""
            CREATE OR REPLACE TABLE price_action_zones (
                symbol VARCHAR, 
                time TIMESTAMP, 
                price DOUBLE, 
                type VARCHAR,
                strength INTEGER
            )
        """)
        
        total_zones = 0
        for sym in symbols_df['symbol']:
            df = con.execute(f"SELECT * FROM daily_metrics WHERE symbol = '{sym}' ORDER BY time").df()
            
            if len(df) > 30:
                raw_levels = detect_levels(df, window=5)
                levels_df = pd.DataFrame(raw_levels, columns=['symbol', 'time', 'price', 'type'])
                
                final_zones_df = group_and_filter_zones(levels_df, df, threshold_percent=1.5)
                
                if not final_zones_df.empty:
                    con.execute("INSERT INTO price_action_zones SELECT * FROM final_zones_df")
                    total_zones += len(final_zones_df)
                    
                    ultimo_preco = df['close_price'].iloc[-1]
                    print(f"{sym}: {len(final_zones_df)} zonas processadas.")
            else:
                print(f"{sym}: Dados insuficientes.")
                
        print(f"\nMarketBrain Gold: {total_zones} zonas estrategicas prontas.")
        
    except Exception as e:
        print(f"Erro na analise Gold: {e}")
    finally:
        con.close()

if __name__ == "__main__":
    process_market_analysis()