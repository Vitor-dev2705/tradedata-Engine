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
            print(f" Erro ao limpar arquivo antigo {f}: {e}")

def extract_data(interval="1h", period="90d"):
    symbols = ["BTC-USD", "ETH-USD", "SOL-USD"]
    
    os.makedirs("data/raw", exist_ok=True)
    
    clear_raw_data()
    
    for symbol in symbols:
        try:
            print(f" Extraindo {symbol} (Período: {period}, Intervalo: {interval})...")
            ticker = yf.Ticker(symbol)
            
            df = ticker.history(period=period, interval=interval)
            
            if df.empty:
                print(f" Falha ao obter dados para {symbol}. Tentando período menor...")
                df = ticker.history(period="30d", interval=interval)

            if not df.empty:
                df = df.reset_index()
                
                if 'Datetime' in df.columns:
                    df['Datetime'] = df['Datetime'].dt.strftime('%Y-%m-%d %H:%M:%S%z')
                elif 'Date' in df.columns:
                    df = df.rename(columns={'Date': 'Datetime'})
                    df['Datetime'] = df['Datetime'].dt.strftime('%Y-%m-%d %H:%M:%S%z')
                
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                filename = f"data/raw/{symbol.lower()}_{timestamp}.json"
                
                df.to_json(filename, orient="records")
                print(f" {symbol} salvo com sucesso! ({len(df)} linhas)")
            
        except Exception as e:
            print(f" Erro ao processar {symbol}: {e}")

def main():
   
    extract_data(interval="1h", period="90d")

if __name__ == "__main__":
    main()