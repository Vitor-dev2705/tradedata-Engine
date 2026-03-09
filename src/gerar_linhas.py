import duckdb
import os
import pandas as pd

def fix_gold_layer():
    db_path = r'C:\Users\Micro\Desktop\PORTIFÓLIO\tradedata-Engine\data\silver\trading.db'
    
    if not os.path.exists(db_path):
        print(f" Erro: O banco não foi encontrado em: {db_path}")
        return

    con = duckdb.connect(db_path)
    try:
        query = "SELECT high_price, low_price FROM daily_metrics"
        data = con.execute(query).df()
        
        if data.empty:
            print(" A tabela daily_metrics está vazia. Execute o transform.py primeiro.")
            return

        resistencia = data['high_price'].max()
        suporte = data['low_price'].min()
        
        con.execute("CREATE OR REPLACE TABLE price_action_zones (price DOUBLE, type VARCHAR)")
        
        con.execute(f"INSERT INTO price_action_zones VALUES ({resistencia}, 'Resistencia')")
        con.execute(f"INSERT INTO price_action_zones VALUES ({suporte}, 'Suporte')")
        
        print(f" Camada Gold Atualizada com Sucesso!")
        print(f" Resistência calculada: {resistencia:.2f}")
        print(f" Suporte calculado: {suporte:.2f}")
        
    except Exception as e:
        print(f" Ocorreu um erro durante o processamento: {e}")
    finally:
        con.close()

if __name__ == "__main__":
    fix_gold_layer()
    
def fix_gold_layer():
    db_path = r'C:\Users\Micro\Desktop\PORTIFÓLIO\tradedata-Engine\data\silver\trading.db'
    con = duckdb.connect(db_path)
    try:
        con.execute("CREATE OR REPLACE TABLE price_action_zones (symbol VARCHAR, price DOUBLE, type VARCHAR)")
        
        symbols = [s[0] for s in con.execute("SELECT DISTINCT symbol FROM daily_metrics").fetchall()]
        
        for sym in symbols:
            data = con.execute(f"SELECT high_price, low_price FROM daily_metrics WHERE symbol = '{sym}'").df()
            res = data['high_price'].max()
            sup = data['low_price'].min()
            
            con.execute(f"INSERT INTO price_action_zones VALUES ('{sym}', {res}, 'Resistencia'), ('{sym}', {sup}, 'Suporte')")
            print(f"✅ Zonas para {sym} calculadas!")
            
    finally:
        con.close()