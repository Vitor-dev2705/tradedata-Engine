import duckdb
import os
import glob

def process_data():
    if os.path.exists('/app'):
        project_root = '/app'
    else:
        current_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.dirname(current_dir)
    
    path_raw = os.path.join(project_root, 'data', 'raw', '*.json').replace('\\', '/')
    path_silver = os.path.join(project_root, 'data', 'silver', 'trading.db').replace('\\', '/')
    
    files = glob.glob(path_raw)
    if not files:
        print(f"AVISO: Nenhum arquivo JSON encontrado em: {os.path.dirname(path_raw)}")
        return

    os.makedirs(os.path.dirname(path_silver), exist_ok=True)
    
    con = duckdb.connect(database=path_silver, read_only=False)
    
    try:
        con.execute(f"""
        CREATE OR REPLACE TABLE daily_metrics AS 
        SELECT 
            upper(split_part(regexp_extract(filename, '([^\\\\/]+)$', 1), '_', 1)) as symbol,
            strptime(Datetime, '%Y-%m-%d %H:%M:%S%z')::TIMESTAMP as time,
            Open as open_price,
            High as high_price,
            Low as low_price,
            Close as close_price,
            Volume as volume
        FROM read_json_auto('{path_raw}', filename=True)
        WHERE volume > 0
    """)
        
        ativos = con.execute("SELECT DISTINCT symbol FROM daily_metrics").fetchall()
        print(f"SUCESSO: {len(files)} arquivos processados.")
        
    except Exception as e:
        print(f"Erro na Camada Silver: {e}")
    finally:
        con.close()

if __name__ == "__main__":
    process_data()