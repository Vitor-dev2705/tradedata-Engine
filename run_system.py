import time
from src.extract import extract_data as run_extraction
from src.market_brain import process_market_analysis

def start_engine():
    print(" TradeData Engine: Sentinela Iniciada...")
    
    while True:
        try:
            print(f"\n[{time.strftime('%H:%M:%S')}] Iniciando ciclo de atualização...")
            
            run_extraction()
            
            process_market_analysis()
            
            print(f"[{time.strftime('%H:%M:%S')}] Ciclo concluído. Aguardando próximo turno...")
            
            time.sleep(300) 
            
        except KeyboardInterrupt:
            print("\nSistema pausado pelo usuário.")
            break
        except Exception as e:
            print(f"Erro crítico no loop: {e}")
            time.sleep(60) 

if __name__ == "__main__":
    start_engine()