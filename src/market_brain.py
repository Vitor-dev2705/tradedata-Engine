import duckdb
import pandas as pd
import numpy as np
import os
from src.alerts import send_alert

def detect_levels(df, window=10):
    levels = []
    for i in range(window, len(df) - window):
        is_high = df['high_price'][i] == max(df['high_price'][i-window:i+window+1])
        is_low = df['low_price'][i] == min(df['low_price'][i-window:i+window+1])
        
        if is_high:
            levels.append((df['symbol'][i], df['time'][i], float(df['high_price'][i]), 'Resistencia'))
        if is_low:
            levels.append((df['symbol'][i], df['time'][i], float(df['low_price'][i]), 'Suporte'))
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

def group_and_filter_zones(levels_df, df_historico, threshold_percent=2.0):
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
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(current_dir)
    db_path = os.path.join(project_root, 'data', 'silver', 'trading.db').replace('\\', '/')
    
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
            
            if len(df) > 50:
                raw_levels = detect_levels(df, window=10)
                levels_df = pd.DataFrame(raw_levels, columns=['symbol', 'time', 'price', 'type'])
                
                final_zones_df = group_and_filter_zones(levels_df, df, threshold_percent=2.5)
                
                if not final_zones_df.empty:
                    con.execute("INSERT INTO price_action_zones SELECT * FROM final_zones_df")
                    total_zones += len(final_zones_df)
                    
                    ultimo_preco = df['close_price'].iloc[-1]
                    for _, zona in final_zones_df.iterrows():
                        distancia = abs(ultimo_preco - zona['price']) / zona['price']
                        
                        if distancia <= 0.005 and zona['strength'] >= 10:
                            msg = f"🚀 *ALERTA TRADE: {sym}*\n\n" \
                                  f"Preço Atual: `${ultimo_preco:,.2f}`\n" \
                                  f"Zona Detectada: {zona['type']}\n" \
                                  f"Valor da Zona: `${zona['price']:,.2f}`\n" \
                                  f"Score de Força: `{zona['strength']}`"
                            send_alert(msg)

                    print(f" {sym}: {len(final_zones_df)} zonas processadas.")
            else:
                print(f" {sym}: Dados insuficientes.")
                
        print(f"\n MarketBrain Gold: {total_zones} zonas estratégicas prontas.")
        
    except Exception as e:
        print(f" Erro na análise Gold: {e}")
    finally:
        con.close()

if __name__ == "__main__":
    process_market_analysis()