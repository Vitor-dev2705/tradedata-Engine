import subprocess
import time
import sys
import os
import webbrowser
import socket

def check_docker():
    print("🐳 Checando status do Docker Desktop...")
    try:
        subprocess.run(["docker", "info"], check=True, capture_output=True, timeout=10)
        return True
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired, FileNotFoundError):
        print("❌ ERRO: O Docker Desktop não está rodando ou não foi encontrado.")
        return False

def wait_for_dashboard(port=8501, timeout=30):
    print(f"🌐 Aguardando Dashboard em http://localhost:{port}...")
    start_time = time.time()
    while time.time() - start_time < timeout:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            if sock.connect_ex(('localhost', port)) == 0:
                return True
        time.sleep(1)
    return False

def run_step(command, desc):
    print(f"🚀 {desc}...")
    process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    stdout, stderr = process.communicate()
    
    if process.returncode != 0:
        print(f"❌ Falha em {desc}: {stderr.strip()}")
        return False
    
    if stdout:
        print(f"📝 Logs: {stdout.strip()}")
        
    print(f"✅ {desc} finalizado.")
    return True

def main():
    os.chdir(os.path.dirname(os.path.abspath(__file__)))

    print("\n" + "="*45)
    print("      TRADE-DATA ENGINE v1.0 - DOCKER MODE")
    print("="*45 + "\n")

    if not check_docker():
        input("\nPressione Enter para sair...")
        sys.exit(1)

    if not run_step("docker compose up -d", "Subindo Infraestrutura (Docker)"):
        input("\nErro ao subir Docker. Pressione Enter...")
        sys.exit(1)

    time.sleep(5)

    container = "trade_engine_container"


    steps = [
        (f"docker exec {container} python src/extract.py", "Camada Bronze: Extração"),
        (f"docker exec {container} python src/transform.py", "Camada Silver: Limpeza"),
        (f"docker exec {container} python src/gerar_linhas.py", "Camada Gold: Price Action"),
        (f"docker exec {container} python src/quality_check.py", "Data Quality: Validação Final")
        
    ]

    for cmd, desc in steps:
        if not run_step(cmd, desc):
            print(f"\n O pipeline foi interrompido no passo: {desc}")
            input("Pressione Enter para fechar...")
            return

    print("\n" + "="*45)
    if wait_for_dashboard(8501):
        print("✨ SUCESSO: Pipeline Concluído e Dashboard Online!")
        print("📈 Abrindo: http://localhost:8501")
        webbrowser.open("http://localhost:8501")
    else:
        print("⚠️ Dashboard demorou para responder. Tente acessar manualmente.")
    print("="*45)
    
    input("\nSistema rodando. Pressione Enter para encerrar...")

if __name__ == "__main__":
    main()