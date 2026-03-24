import time
import sys
from pathlib import Path

# Adiciona a pasta 'src' ao caminho de busca do Python
sys.path.append(str(Path(__file__).parent / "src"))

# Agora o Python consegue encontrar os módulos dentro de src
try:
    from extract import extract_data, get_crypto_list
    from transform import process_data
except ImportError as e:
    print(f"Erro ao importar modulos: {e}")
    sys.exit(1)

def run_loop():
    while True:
        try:
            print(f"[{time.strftime('%H:%M:%S')}] Iniciando ciclo de atualizacao...")
            
            ativos = get_crypto_list()
            extract_data(ativos, interval="1h", period="60d")
            process_data()

            print("Ciclo finalizado. Aguardando 5 minutos...")
            time.sleep(300) 
            
        except Exception as e:
            print(f"Erro critico no loop: {e}")
            time.sleep(60)

if __name__ == "__main__":
    run_loop()