import os
import subprocess
import sys

def run_full_pipeline():
    root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    python_exe = sys.executable  # Usa o Python que está rodando este script
    
    ativos = ["BTC-USD", "ETH-USD", "SOL-USD"]
    
    print(f"🚀 Iniciando Pipeline em: {root_dir}")

    scripts = [
        ("Transformação", "src/transform.py"),
        ("Análise", "src/gerar_linhas.py"),
        ("Qualidade", "src/quality_check.py")
    ]

    for ativo in ativos:
        print(f" Extraindo {ativo}...")
        subprocess.run([python_exe, os.path.join(root_dir, "src/extract.py"), ativo], check=True)

    for nome, script_rel in scripts:
        script_path = os.path.join(root_dir, script_rel)
        if os.path.exists(script_path):
            print(f" Executando {nome}...")
            subprocess.run([python_exe, script_path], check=True)
        else:
            print(f" Aviso: Script {script_rel} não encontrado.")

    print("\n Pipeline concluído!")

if __name__ == "__main__":
    run_full_pipeline()