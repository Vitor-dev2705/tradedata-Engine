import yfinance as yf
import pandas as pd
import json
import os
from datetime import datetime

def fetch_crypto_data(ticker="BTC-USD"):
    print(f"Iniciando extração de {ticker}...")
    
    df = yf.download(ticker, period="7d", interval="1h")
    df = df.reset_index()
    
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [col[0] for col in df.columns]
    else:
        df.columns = [str(col) for col in df.columns]
        
    raw_data = df.to_dict(orient="records")
    
    path = 'data/raw'
    if os.path.exists(path) and not os.path.isdir(path):
        os.remove(path)
        
    os.makedirs(path, exist_ok=True)
    
    asset_prefix = ticker.split('-')[0].lower()
    filename = f"{path}/{asset_prefix}_{datetime.now().strftime('%Y%m%d_%H%M')}.json"
    
    with open(filename, 'w') as f:
        json.dump(raw_data, f, default=str)
    
    print(f"Dados brutos salvos em: {filename}")
    return filename

if __name__ == "__main__":
    import sys
    target = sys.argv[1] if len(sys.argv) > 1 else "BTC-USD"
    fetch_crypto_data(target)