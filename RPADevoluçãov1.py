import os
import time
import subprocess
import pandas as pd
import xml.etree.ElementTree as ET
import re
import csv
from datetime import datetime
from playwright.sync_api import sync_playwright

# ================= CONFIGURAÇÕES =================
PORTA_DEBUG = "9222"
DIR_PERFIL_CHROME = r"C:\temp\chrome_dev_session"
CAMINHO_CHROME = r"C:\Program Files\Google\Chrome\Application\chrome.exe"
PASTA_BASE = r"U:\Drives compartilhados\CD PR - CGS\Dados\BaseRPA"
ARQUIVO_LOG = os.path.join(PASTA_BASE, "log_processamento.csv")

def registrar_log(dados):
    """Registra uma linha no arquivo de log CSV."""
    file_exists = os.path.isfile(ARQUIVO_LOG)
    with open(ARQUIVO_LOG, 'a', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=["Data_Hora", "BP", "NF", "Status", "Detalhes"])
        if not file_exists:
            writer.writeheader()
        dados["Data_Hora"] = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        writer.writerow(dados)

def extrair_dados_xml(caminho_xml):
    try:
        tree = ET.parse(caminho_xml)
        root = tree.getroot()
        ns = {'nfe': 'http://www.portalfiscal.inf.br/nfe'}
        def get_text(path):
            node = root.find(path, ns)
            return node.text if node is not None else ""
        
        dados = {"dhEmi": get_text(".//nfe:dhEmi"), "vNFTot": get_text(".//nfe:vNFTot"), "infCpl": get_text(".//nfe:infAdic/nfe:infCpl")}
        obs = dados["infCpl"]
        campos_busca = ['Valor de ICMS ST', 'Valor FCP ST', 'Valor IPI']
        extraidos = {}
        for campo in campos_busca:
            match = re.search(fr"{campo}[:\s]*R?\$?\s*([\d.,]+)", obs, re.IGNORECASE)
            if match: extraidos[campo] = match.group(1)
        dados["valores_adicionais"] = extraidos
        return dados
    except Exception as e:
        print(f"Erro ao ler XML: {e}"); return None

def iniciar_automacao():
    caminho_csv = os.path.join(PASTA_BASE, "BaseRPA.csv")
    pasta_xml = os.path.join(PASTA_BASE, "XMLs Referencia")
    
    # 1. Carregar CSV completo para o Loop
    try:
        df = pd.read_csv(caminho_csv, sep=',', encoding='utf-8')
        print(f"Total de documentos para processar: {len(df)}")
    except Exception as e:
        print(f"Erro ao ler CSV: {e}"); return

    # 2. Garantir que o Chrome está aberto
    subprocess.Popen([CAMINHO_CHROME, f"--remote-debugging-port={PORTA_DEBUG}", f"--user-data-dir={DIR_PERFIL_CHROME}", "--disable-blink-features=AutomationControlled", "--start-maximized"])
    time.sleep(5)

    with sync_playwright() as p:
        try:
            browser = p.chromium.connect_over_cdp(f"http://localhost:{PORTA_DEBUG}")
            contexto = browser.contexts[0]
            page = contexto.new_page()

            # --- INÍCIO DO LOOP DE DOCUMENTOS ---
            for index, linha in df.iterrows():
                valor_bp = str(linha.iloc[0]).strip()
                chave_acesso = str(linha.iloc[1]).strip()
                num_nf = str(linha.iloc[3]).strip().split('.')[0].zfill(9)
                serie_nf = str(linha.iloc[4]).strip().split('.')[0].zfill(3)
                string_sap = f"{num_nf}-{serie_nf}"

                print(f"\n--- Processando {index+1}/{len(df)}: BP {valor_bp} ---")

                try:
                    # ETAPA 1: MEU DANFE
                    page.goto("https://meudanfe.com.br/ver-danfe#?", wait_until="load")
                    page.fill("#searchTxt", chave_acesso)
                    page.click("#searchBtn")
                    page.wait_for_selector("#verifying-text", state="hidden", timeout=90000)
                    
                    xpath_download = "xpath=/html/body/section[1]/div/div/div[2]/div[1]/a"
                    page.wait_for_selector(xpath_download, state="visible", timeout=30000)
                    
                    with page.expect_download() as download_info:
                        page.click(xpath_download)
                    download = download_info.value
                    caminho_xml_baixado = os.path.join(pasta_xml, download.suggested_filename)
                    download.save_as(caminho_xml_baixado)
                    
                    # ETAPA 2: SAP
                    page.goto("https://s4prd.sap.grupoboticario.digital/sap/bc/ui2/flp?appState=lean#SalesDocument-change?sap-ui-tech-hint=GUI")
                    
                    # Busca de Frame
                    target_frame = None
                    for _ in range(15):
                        for frame in page.frames:
                            if frame.locator('input[title="Referência de cliente como campo matchcode"]').is_visible(timeout=500):
                                target_frame = frame
                                break
                        if target_frame: break
                        time.sleep(1)

                    if target_frame:
                        target_frame.fill('input[title="Referência de cliente como campo matchcode"]', string_sap)
                        target_frame.click('div[title="Exec.pesquisa"]')
                        
                        target_frame.wait_for_selector('input[title="Encontrar entrada na lista"]', state="visible", timeout=30000)
                        target_frame.fill('input[title="Encontrar entrada na lista"]', valor_bp)
                        target_frame.click('div[title="Procurar"]')
                        
                        # --- VERIFICAÇÃO DE "NÃO ENCONTRADO" NO POP-UP ---
                        time.sleep(2)
                        xpath_erro = "xpath=/html/body/table/tbody/tr/td/div/div/div/div[11]/div/section/div/div[3]/span/span"
                        
                        if target_frame.locator(xpath_erro).is_visible(timeout=5000):
                            msg = target_frame.locator(xpath_erro).inner_text()
                            print(f"Resultado: {msg}")
                            registrar_log({"BP": valor_bp, "NF": string_sap, "Status": "Não Encontrado", "Detalhes": msg})
                            
                            # Redirecionar para criação
                            page.goto("https://s4prd.sap.grupoboticario.digital/sap/bc/ui2/flp?appState=lean#SalesDocument-create?sap-ui-tech-hint=GUI")
                        else:
                            print("Resultado: Documento encontrado.")
                            registrar_log({"BP": valor_bp, "NF": string_sap, "Status": "Encontrado", "Detalhes": "Seguiu para modificação"})
                    else:
                        registrar_log({"BP": valor_bp, "NF": string_sap, "Status": "Erro", "Detalhes": "Frame SAP não carregou"})

                except Exception as e_linha:
                    print(f"Erro na linha {index}: {e_linha}")
                    registrar_log({"BP": valor_bp, "NF": string_sap, "Status": "Erro", "Detalhes": str(e_linha)})

            print("\n>>> TODOS OS DOCUMENTOS PROCESSADOS <<<")
            input("Pressione Enter para encerrar...")

        except Exception as e:
            print(f"\nERRO FATAL: {e}")

if __name__ == "__main__":
    iniciar_automacao()