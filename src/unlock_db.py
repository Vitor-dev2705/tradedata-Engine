import os
import psutil

def force_release():
    db_path = "/app/data/silver/trading.db" if os.path.exists('/app') else r'C:\Users\Micro\Desktop\PORTIFÓLIO\tradedata-Engine\data\silver\trading.db'
    
    for proc in psutil.process_iter(['pid', 'name']):
        try:
            for file in proc.open_files():
                if os.path.abspath(db_path) == os.path.abspath(file.path):
                    print(f"Encerrando processo bloqueador: {proc.info['name']} (PID: {proc.info['pid']})")
                    proc.kill()
        except:
            continue

if __name__ == "__main__":
    force_release()