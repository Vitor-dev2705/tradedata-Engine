import yfinance as yf
import os
import glob
from datetime import datetime

def clear_raw_data():
    files = glob.glob("data/raw/*.json")
    for f in files:
        try:
            os.remove(f)
        except Exception as e:
            print(f" ❌ Erro ao limpar arquivo antigo {f}: {e}")

def get_crypto_list():
    """
    Retorna uma lista das principais criptomoedas.
    """
    return [
        "BTC-USD", "ETH-USD", "SOL-USD", "BNB-USD", "XRP-USD", 
        "ADA-USD", "AVAX-USD", "DOGE-USD", "DOT-USD", "LINK-USD",
        "MATIC-USD", "SHIB-USD", "LTC-USD", "UNI-USD", "ATOM-USD",
        "XLM-USD", "XMR-USD", "BCH-USD", "ALGO-USD", "NEAR-USD"
    ]

def extract_data(symbols, interval="1h", period="90d"):
    os.makedirs("data/raw", exist_ok=True)
    clear_raw_data()
    
    total_sucesso = 0
    
    for symbol in symbols:
        try:
            print(f" Extraindo {symbol}...")
            ticker = yf.Ticker(symbol)
            
            df = ticker.history(period=period, interval=interval)
            
            if df.empty:
                df = ticker.history(period="30d", interval=interval)

            if not df.empty:
                df = df.reset_index()
                
                time_col = 'Datetime' if 'Datetime' in df.columns else 'Date'
                df[time_col] = df[time_col].dt.strftime('%Y-%m-%d %H:%M:%S%z')
                
                if time_col != 'Datetime':
                    df = df.rename(columns={time_col: 'Datetime'})
                
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                filename = f"data/raw/{symbol.replace('-', '_').lower()}_{timestamp}.json"
                
                df.to_json(filename, orient="records")
                print(f" {symbol}: {len(df)} linhas salvas.")
                total_sucesso += 1
            else:
                print(f" {symbol}: Nenhum dado encontrado.")

        except Exception as e:
            print(f" Erro em {symbol}: {e}")
        
    print(f"\n--- Extração Finalizada: {total_sucesso}/{len(symbols)} ativos processados. ---")

def main():
    meus_ativos = get_crypto_list()
    
    extract_data(meus_ativos, interval="1h", period="60d")

if __name__ == "__main__":
    main()