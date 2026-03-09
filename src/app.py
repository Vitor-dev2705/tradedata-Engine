from fastapi import FastAPI, Query
import duckdb
import os

app = FastAPI(title="TradeData API - Engine")

DB_PATH = r'C:\Users\Micro\Desktop\PORTIFÓLIO\tradedata-Engine\data\silver\trading.db'

@app.get("/market-data")
def get_data(symbol: str = Query("BTC-USD", description="O par de ativos para filtrar")):
    if not os.path.exists(DB_PATH):
        return {"status": "error", "message": "Banco de dados nao encontrado"}

    con = duckdb.connect(database=DB_PATH, read_only=True)
    try:
        query = f"SELECT * FROM daily_metrics WHERE symbol = '{symbol}' ORDER BY time DESC LIMIT 100"
        df = con.execute(query).df()
        
        if not df.empty:
            df['time'] = df['time'].dt.strftime('%Y-%m-%d %H:%M:%S')
            
        result = df.to_dict(orient="records")
        return {
            "status": "success", 
            "symbol": symbol,
            "count": len(result),
            "data": result
        }
    finally:
        con.close()

@app.get("/zones")
def get_zones(symbol: str = "BTC-USD"):
    con = duckdb.connect(database=DB_PATH, read_only=True)
    try:
        zones = con.execute(f"SELECT * FROM price_action_zones WHERE symbol = '{symbol}'").df()
        if not zones.empty:
            zones['time'] = zones['time'].dt.strftime('%Y-%m-%d %H:%M:%S')
        return {"status": "success", "data": zones.to_dict(orient="records")}
    finally:
        con.close()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)