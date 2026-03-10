import os
import subprocess
import sys
import time
from datetime import datetime

def clear_raw_data(root_dir):
    raw_path = os.path.join(root_dir, 'data', 'raw')
    if os.path.exists(raw_path):
        for filename in os.listdir(raw_path):
            file_path = os.path.join(raw_path, filename)
            try:
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.unlink(file_path)
            except Exception:
                pass

def run_full_pipeline():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    root_dir = os.path.dirname(current_dir)
    python_exe = sys.executable 
    
    scripts = [
        ("Extracao (Bronze)", "src/extract.py"),
        ("Transformacao (Silver)", "src/transform.py"),
        ("Analise de Precos (Gold)", "src/market_brain.py"), 
        ("Validacao (Qualidade)", "src/quality_check.py")
    ]

    while True:
        start_time = datetime.now()
        print(f"[{start_time.strftime('%H:%M:%S')}] Iniciando Ciclo...")

        success = True
        for nome, script_rel in scripts:
            script_path = os.path.join(root_dir, script_rel)
            
            if os.path.exists(script_path):
                try:
                    subprocess.run([python_exe, script_path], check=True, cwd=root_dir)
                except subprocess.CalledProcessError:
                    success = False
                    if "quality_check" not in script_rel:
                        break
            
        if success:
            clear_raw_data(root_dir)

        end_time = datetime.now()
        duracao = (end_time - start_time).total_seconds()
        print(f"Ciclo finalizado em {duracao:.2f}s. Aguardando 60s...")
        time.sleep(60)

if __name__ == "__main__":
    try:
        run_full_pipeline()
    except KeyboardInterrupt:
        sys.exit(0)