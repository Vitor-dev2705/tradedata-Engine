import duckdb
import os

db_path = r'C:\Users\Micro\Desktop\PORTIFÓLIO\tradedata-Engine\data\silver\trading.db'

if not os.path.exists(db_path):
    print(f" Banco não encontrado em: {db_path}")
else:
    con = duckdb.connect(db_path)
    try:
        print(" --- Contagem por Ativo (Silver) ---")
        print(con.execute("SELECT symbol, count(*) FROM daily_metrics GROUP BY symbol").df())
        
        print("\n --- Contagem por Ativo (Gold) ---")
        print(con.execute("SELECT symbol, count(*) FROM price_action_zones GROUP BY symbol").df())
    except Exception as e:
        print(f" Erro ao ler tabelas: {e}")
    finally:
        con.close()