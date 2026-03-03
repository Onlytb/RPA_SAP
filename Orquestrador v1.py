import schedule
import time
import subprocess
import os
from datetime import datetime

DIRETORIO_ATUAL = os.path.dirname(os.path.abspath(__file__))

def executar_script_horario():
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Iniciando script de EWM TU + Remessa...")
    try:
        caminho_script_1 = os.path.join(DIRETORIO_ATUAL, "RPA_Download_EWM.py")
        caminho_script_2 = os.path.join(DIRETORIO_ATUAL, "salvar_remessa_com_tu.py")
        subprocess.run(["python", caminho_script_1], check=True)
        subprocess.run(["python", caminho_script_2], check=True)    
        print("Script de EWM finalizado com sucesso!")
    except subprocess.CalledProcessError as e:
        print(f"Erro ao executar o script EWM TU: {e}")

def executar_script_diario():
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Iniciando script Base KNAPP...")
    try:
        caminho_script = os.path.join(DIRETORIO_ATUAL, "BaixarKanpp.py")
        subprocess.run(["python", caminho_script], check=True)
        print("Script diário finalizado com sucesso!")
    except subprocess.CalledProcessError as e:
        print(f"Erro ao executar o script Base KNAPP: {e}")


#rules

print("===Orquestrador Iniciado===")
print("Iniciando script EWM TU pela primeira vez agora...")

executar_script_horario()

# 2. Agora sim, fazemos os agendamentos para o futuro
schedule.every(1).hours.do(executar_script_horario)
schedule.every().day.at("00:05").do(executar_script_diario)

print(f"[{datetime.now().strftime('%H:%M:%S')}] Agendamentos concluidos. Script em modo de espera....")

# 3. Entra no loop infinito esperando a hora dos próximos agendamentos bater
while True:
    schedule.run_pending()
    time.sleep(1)