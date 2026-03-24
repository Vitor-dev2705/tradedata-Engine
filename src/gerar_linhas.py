import duckdb
import os
import pandas as pd

def fix_gold_layer():
    if os.path.exists('/app'):
        db_path = '/app/data/silver/trading.db'
    else:
        db_path = r'C:\Users\Micro\Desktop\PORTIFÓLIO\tradedata-Engine\data\silver\trading.db'
    
    if not os.path.exists(db_path):
        print(f"Erro: O banco não foi encontrado em: {db_path}")
        return

    con = duckdb.connect(db_path)
    try:
        con.execute("""
            CREATE OR REPLACE TABLE price_action_zones (
                symbol VARCHAR, 
                price DOUBLE, 
                type VARCHAR, 
                strength INTEGER
            )
        """)
        
        symbols = [s[0] for s in con.execute("SELECT DISTINCT symbol FROM daily_metrics").fetchall()]
        
        if not symbols:
            print("A tabela daily_metrics está vazia. Execute o transform.py primeiro.")
            return

        for sym in symbols:
            df = con.execute(f"SELECT high_price, low_price, close_price FROM daily_metrics WHERE symbol = '{sym}'").df()
            
            res = float(df['high_price'].max())
            sup = float(df['low_price'].min())
            
            def get_strength(price):
                upper = price * 1.01
                lower = price * 0.99
                touches = df[(df['high_price'] >= lower) & (df['low_price'] <= upper)]
                return len(touches)

            strength_res = get_strength(res)
            strength_sup = get_strength(sup)

            con.execute(f"""
                INSERT INTO price_action_zones VALUES 
                ('{sym}', {res}, 'Resistencia', {strength_res}),
                ('{sym}', {sup}, 'Suporte', {strength_sup})
            """)
            
            print(f"Camada Gold: {sym} | Res: ${res:,.2f} (F:{strength_res}) | Sup: ${sup:,.2f} (F:{strength_sup})")
        
        print("\nProcessamento concluido com sucesso!")
        
    except Exception as e:
        print(f"Erro no processamento: {e}")
    finally:
        con.close()

if __name__ == "__main__":
    fix_gold_layer()