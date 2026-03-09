import duckdb
import os

def run_quality_checks():
    db_path = r'C:\Users\Micro\Desktop\PORTIFÓLIO\tradedata-Engine\data\silver\trading.db'
    
    if not os.path.exists(db_path):
        print(f" Erro: O banco de dados não foi encontrado em: {db_path}")
        exit(1)

    con = duckdb.connect(database=db_path, read_only=True)
    
    try:
        null_count = con.execute("SELECT count(*) FROM daily_metrics WHERE close_price IS NULL").fetchone()[0]
        
        negative_prices = con.execute("SELECT count(*) FROM daily_metrics WHERE close_price <= 0").fetchone()[0]
        
        missing_symbols = con.execute("SELECT count(*) FROM daily_metrics WHERE symbol IS NULL OR symbol = ''").fetchone()[0]

        if null_count == 0 and negative_prices == 0 and missing_symbols == 0:
            print(" Data Quality: Sucesso! Dados limpos e consistentes.")
            
            assets = con.execute("SELECT DISTINCT symbol FROM daily_metrics").fetchall()
            print(f" Ativos validados: {[a[0] for a in assets]}")
        else:
            print(f" Data Quality: Falha detectada!")
            if null_count > 0: print(f"   - {null_count} valores nulos encontrados.")
            if negative_prices > 0: print(f"   - {negative_prices} preços inválidos (<= 0).")
            if missing_symbols > 0: print(f"   - {missing_symbols} registros sem identificação de símbolo.")
            exit(1)
            
    finally:
        con.close()

if __name__ == "__main__":
    run_quality_checks()