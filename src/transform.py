import duckdb
import os
import glob
import pandas as pd
from pathlib import Path

def get_paths():
    base_path = Path(__file__).parent.parent
    path_raw = base_path / "data" / "raw"
    path_silver = base_path / "data" / "silver" / "trading.db"
    
    os.makedirs(path_raw, exist_ok=True)
    os.makedirs(path_silver.parent, exist_ok=True)
    
    return path_raw, path_silver

def process_data():
    path_raw, path_silver = get_paths()
    
    json_files = list(path_raw.glob("*.json"))
    if not json_files:
        print(" Camada Silver: Nenhum arquivo JSON encontrado em data/raw.")
        return

    print(f" Camada Silver: Processando {len(json_files)} arquivos ativos...")

    con = duckdb.connect(str(path_silver), read_only=False)

    try:
        con.execute(f"""
            CREATE OR REPLACE TEMP TABLE stage_raw AS 
            SELECT * FROM read_json_auto('{path_raw}/*.json')
        """)

        con.execute("""
            CREATE TABLE IF NOT EXISTS daily_metrics (
                symbol VARCHAR,
                time TIMESTAMP,
                open_price DOUBLE,
                high_price DOUBLE,
                low_price DOUBLE,
                close_price DOUBLE,
                volume DOUBLE
            )
        """)

        con.execute("TRUNCATE TABLE daily_metrics")
        
        con.execute("""
            INSERT INTO daily_metrics
            SELECT 
                UPPER(REPLACE(split_part(filename, '_', 1), 'data/raw/', '')) as symbol,
                CAST(Datetime AS TIMESTAMP) as time,
                Open as open_price,
                High as high_price,
                Low as low_price,
                Close as close_price,
                Volume as volume
            FROM (
                SELECT *, current_setting('file_name') as filename 
                FROM stage_raw
            )
        """)

        count = con.execute("SELECT count(*) FROM daily_metrics").fetchone()[0]
        print(f" Camada Silver: {count} registros normalizados no DuckDB.")

    except Exception as e:
        print(f" Erro na Transformação Silver: {e}")
    finally:
        con.close()

if __name__ == "__main__":
    process_data()