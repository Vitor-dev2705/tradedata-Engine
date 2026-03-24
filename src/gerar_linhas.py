import duckdb
import os
import pandas as pd
from pathlib import Path

def fix_gold_layer():
    base_path = Path(__file__).parent.parent
    db_path = base_path / "data" / "silver" / "trading.db"
    
    if not db_path.exists():
        db_path = Path(r'C:\Users\Micro\Desktop\PORTIFÓLIO\tradedata-Engine\data\silver\trading.db')
    
    con = duckdb.connect(str(db_path))
    try:
        con.execute("CREATE OR REPLACE TABLE price_action_zones (symbol VARCHAR, price DOUBLE, type VARCHAR, strength INTEGER)")
        symbols = [s[0] for s in con.execute("SELECT DISTINCT symbol FROM daily_metrics").fetchall()]
        
        for sym in symbols:
            df = con.execute(f"SELECT high_price, low_price, close_price FROM daily_metrics WHERE symbol = '{sym}'").df()
            if df.empty: continue

            window = 30
            df['max_local'] = df['high_price'].rolling(window=window, center=True).max()
            df['min_local'] = df['low_price'].rolling(window=window, center=True).min()

            potential_res = df[df['high_price'] == df['max_local']]['high_price'].unique()
            potential_sup = df[df['low_price'] == df['min_local']]['low_price'].unique()

            def get_strength(price):
                margin = price * 0.012
                touches = df[(df['high_price'] >= price - margin) & (df['low_price'] <= price + margin)]
                return len(touches)

            all_zones = []
            for p in potential_res: all_zones.append({'p': float(p), 't': 'Resistencia', 's': get_strength(p)})
            for p in potential_sup: all_zones.append({'p': float(p), 't': 'Suporte', 's': get_strength(p)})

            all_zones = sorted(all_zones, key=lambda x: x['s'], reverse=True)

            final_zones = []
            min_dist = (df['high_price'].max() - df['low_price'].min()) * 0.15

            for zone in all_zones:
                if not any(abs(zone['p'] - f['p']) < min_dist for f in final_zones):
                    final_zones.append(zone)
                if len(final_zones) >= 5: break

            for z in final_zones:
                con.execute("INSERT INTO price_action_zones VALUES (?, ?, ?, ?)", (sym, z['p'], z['t'], z['s']))
            
            print(f"Gold: {sym} | {len(final_zones)} zonas filtradas")
        
    except Exception as e:
        print(f"Erro: {e}")
    finally:
        con.close()

if __name__ == "__main__":
    fix_gold_layer()