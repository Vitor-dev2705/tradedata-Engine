from fastapi import FastAPI, Query, HTTPException
import duckdb
import os
import pandas as pd

app = FastAPI(title="TradeData API - Engine")

def get_db_path():
    if os.path.exists('/app'):
        return '/app/data/silver/trading.db'
    return r'C:\Users\Micro\Desktop\PORTIFÓLIO\tradedata-Engine\data\silver\trading.db'

DB_PATH = get_db_path()

@app.get("/")
def read_root():
    return {"message": "TradeData Engine API is online", "database": DB_PATH}

@app.get("/market-data")
def get_data(symbol: str = Query("BTC-USD", description="O par de ativos para filtrar")):
    if not os.path.exists(DB_PATH):
        raise HTTPException(status_code=404, detail="Banco de dados não encontrado")

    con = duckdb.connect(database=DB_PATH, read_only=True)
    try:
        query = f"SELECT * FROM daily_metrics WHERE symbol = '{symbol.upper()}' ORDER BY time DESC LIMIT 100"
        df = con.execute(query).df()
        
        if df.empty:
            return {"status": "success", "symbol": symbol, "count": 0, "data": []}
            
        df['time'] = pd.to_datetime(df['time']).dt.strftime('%Y-%m-%d %H:%M:%S')
            
        result = df.to_dict(orient="records")
        return {
            "status": "success", 
            "symbol": symbol.upper(),
            "count": len(result),
            "data": result
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao consultar dados: {str(e)}")
    finally:
        con.close()

@app.get("/zones")
def get_zones(symbol: str = Query("BTC-USD", description="Filtrar zonas por símbolo")):
    if not os.path.exists(DB_PATH):
        raise HTTPException(status_code=404, detail="Banco de dados não encontrado")

    con = duckdb.connect(database=DB_PATH, read_only=True)
    try:
        zones = con.execute(f"SELECT * FROM price_action_zones WHERE symbol = '{symbol.upper()}'").df()
        
        if zones.empty:
            return {"status": "success", "symbol": symbol, "data": []}
            
        if 'time' in zones.columns:
            zones['time'] = pd.to_datetime(zones['time']).dt.strftime('%Y-%m-%d %H:%M:%S')
            
        return {
            "status": "success", 
            "symbol": symbol.upper(),
            "data": zones.to_dict(orient="records")
        }
    except Exception as e:
        return {"status": "warning", "message": "Camada Gold ainda não processada", "data": []}
    finally:
        con.close()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)