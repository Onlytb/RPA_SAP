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

    try:
        df = pd.read_csv(caminho_csv, sep=',', encoding='utf-8')
        print(f"Total de documentos para processar: {len(df)}")
    except Exception as e:
        print(f"Erro ao ler CSV: {e}"); return

    # Inicia o Chrome
    subprocess.Popen([
        CAMINHO_CHROME, 
        f"--remote-debugging-port={PORTA_DEBUG}", 
        f"--user-data-dir={DIR_PERFIL_CHROME}", 
        "--disable-blink-features=AutomationControlled", 
        "--start-maximized"
    ])
    time.sleep(5)

    with sync_playwright() as p:
        try:
            browser = p.chromium.connect_over_cdp(f"http://localhost:{PORTA_DEBUG}")
            contexto = browser.contexts[0]
            
            # Aba principal do loop (usada para Danfe e Pesquisa)
            page = contexto.new_page()
            page.bring_to_front()

            for index, linha in df.iterrows():
                valor_bp = str(linha.iloc[0]).strip()
                chave_acesso = str(linha.iloc[1]).strip()
                tp_pedido = str(linha.iloc[2]).strip()
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
                    
                    # Extrair e formatar a data do XML
                    dados_nfe = extrair_dados_xml(caminho_xml_baixado)
                    data_formatada = ""
                    if dados_nfe and dados_nfe.get("dhEmi"):
                        data_xml_bruta = dados_nfe["dhEmi"]
                        data_obj = datetime.strptime(data_xml_bruta[:10], "%Y-%m-%d")
                        data_formatada = data_obj.strftime("%d.%m.%Y")
                    
                    # ETAPA 2: PESQUISA SAP
                    page.goto("https://s4prd.sap.grupoboticario.digital/sap/bc/ui2/flp?appState=lean#SalesDocument-change?sap-ui-tech-hint=GUI", wait_until="load")
                    
                    target_frame = None
                    for _ in range(15):
                        for frame in page.frames:
                            try:
                                if frame.locator('input[title="Referência de cliente como campo matchcode"]').is_visible(timeout=500):
                                    target_frame = frame
                                    break
                            except: continue
                        if target_frame: break
                        time.sleep(1)

                    if target_frame:
                        target_frame.fill('input[title="Referência de cliente como campo matchcode"]', string_sap)
                        target_frame.click('div[title="Exec.pesquisa"]')
                        
                        target_frame.wait_for_selector('input[title="Encontrar entrada na lista"]', state="visible", timeout=30000)
                        target_frame.fill('input[title="Encontrar entrada na lista"]', valor_bp)
                        target_frame.click('div[title="Procurar"]')
                        
                        time.sleep(2)
                        xpath_erro = "xpath=/html/body/table/tbody/tr/td/div/div/div/div[11]/div/section/div/div[3]/span/span"
                        
                        # =========================================================
                        # FORK: DOCUMENTO NÃO ENCONTRADO -> CRIAÇÃO
                        # =========================================================
                        if target_frame.locator(xpath_erro).is_visible(timeout=5000):
                            msg = target_frame.locator(xpath_erro).inner_text()
                            print(f"Resultado: {msg}")
                            
                            print("Iniciando Criação de Ordem (VA01) em uma NOVA ABA isolada...")
                            # O PULO DO GATO: Abre uma aba isolada só para a criação, evitando conflitos de Fiori
                            aba_criacao = contexto.new_page()
                            aba_criacao.bring_to_front()
                            aba_criacao.goto("https://s4prd.sap.grupoboticario.digital/sap/bc/ui2/flp?appState=lean#SalesDocument-create?sap-ui-tech-hint=GUI", wait_until="load")
                            
                            # Sincroniza com a classe do Fiori Title enviada por você
                            print("Aguardando carregamento da interface Shell Fiori...")
                            aba_criacao.wait_for_selector('.sapUshellAppTitleText', state="visible", timeout=60000)
                            
                            print("Aguardando formulário interno do SAP liberar o cursor (8s)...")
                            time.sleep(8)
                            
                            # --- NAVEGAÇÃO POR TECLADO ---
                            print(f"Digitando Tipo de Ordem: {tp_pedido}")
                            aba_criacao.keyboard.type(tp_pedido, delay=100)
                            
                            time.sleep(1)
                            print("Pressionando TAB...")
                            aba_criacao.keyboard.press("Tab")
                            
                            time.sleep(1)
                            print("Digitando Organização de Vendas: BR03")
                            aba_criacao.keyboard.type("BR03", delay=100)
                            
                            time.sleep(1)
                            print("Pressionando Enter (Avançar)...")
                            aba_criacao.keyboard.press("Enter")
                            
                            # --- TELA DE SÍNTESE ---
                            print("Aguardando carregamento da tela de Síntese...")
                            time.sleep(5)
                            
                            sel_emissor = 'input[title="Emissor da ordem"]'
                            sel_receb = "xpath=/html/body/table/tbody/tr/td/div/form/div/div[4]/div/div[1]/div/div[6]/div/div[5]/table/tbody/tr/td[1]/input"
                            sel_ref_cliente = "xpath=/html/body/table/tbody/tr/td/div/form/div/div[4]/div/div[1]/div/div[8]/table/tbody/tr/td[1]/input"
                            sel_dt_ref_cliente = 'input[title="Data de referência do cliente"]'
                            sel_btn_gravar = 'div[title="Gravar (Ctrl+S)"]'

                            # Busca o frame interno de síntese na NOVA aba
                            create_frame = None
                            for _ in range(20):
                                for frame in aba_criacao.frames:
                                    try:
                                        if frame.locator(sel_emissor).is_visible(timeout=500):
                                            create_frame = frame
                                            break
                                    except: continue
                                if create_frame: break
                                time.sleep(1)
                                
                            if create_frame:
                                print("Preenchendo detalhes da Ordem...")
                                create_frame.click(sel_emissor)
                                create_frame.fill(sel_emissor, valor_bp)
                                
                                create_frame.click(sel_receb)
                                create_frame.fill(sel_receb, valor_bp)
                                
                                create_frame.click(sel_ref_cliente)
                                create_frame.fill(sel_ref_cliente, num_nf)
                                
                                create_frame.click(sel_dt_ref_cliente)
                                create_frame.fill(sel_dt_ref_cliente, data_formatada)
                                
                                print("Pressionando botão Gravar...")
                                create_frame.click(sel_btn_gravar)
                                
                                # ==========================================
                                # TRATAMENTO DO POP-UP: ÁREA DE VENDAS
                                # ==========================================
                                print("Verificando pop-up de Área de Vendas...")
                                sel_btn_selecionar = 'div[title="Selecionar (Entrada)"]'
                                
                                try:
                                    # Aguarda até 8 segundos para ver se o botão de confirmação do pop-up aparece
                                    if create_frame.locator(sel_btn_selecionar).is_visible(timeout=8000):
                                        print("Pop-up detectado! Procurando a linha 'Produtos'...")
                                        
                                        # Busca dinamicamente qualquer item da lista que contenha "Produtos"
                                        linha_produtos = create_frame.locator('div.lsAbapListText--text', has_text=re.compile(r"Produtos", re.IGNORECASE)).first
                                        
                                        if linha_produtos.is_visible(timeout=3000):
                                            linha_produtos.click() # Seleciona a linha
                                            print("Opção 'Produtos' selecionada na tabela.")
                                        else:
                                            print("Aviso: A string 'Produtos' não foi encontrada nas opções.")
                                            
                                        print("Pressionando o botão 'Selecionar' (Check verde)...")
                                        create_frame.click(sel_btn_selecionar)
                                        
                                        # Aguarda a tela fechar e o SAP processar a criação
                                        time.sleep(3) 
                                    else:
                                        print("Nenhum pop-up exibido. O SAP aceitou os dados diretamente.")
                                except Exception as popup_err:
                                    print(f"Erro ao manipular o pop-up (ignorando): {popup_err}")

                                # ==========================================
                                
                                time.sleep(5) # Espera o backend do SAP finalizar o carregamento geral
                                registrar_log({"BP": valor_bp, "NF": string_sap, "Status": "Criado", "Detalhes": "Ordem criada com sucesso."})
                                print(f"Ordem para BP {valor_bp} finalizada com sucesso!")
                            else:
                                msg_erro_frame = "Frame da tela de Síntese não carregou a tempo."
                                print(f"Erro: {msg_erro_frame}")
                                registrar_log({"BP": valor_bp, "NF": string_sap, "Status": "Erro", "Detalhes": msg_erro_frame})
                            
                            # Fecha a aba temporária para manter o navegador limpo
                            aba_criacao.close()
                            page.bring_to_front() # Foca de volta na aba principal para a próxima repetição
                                
                        else:
                            print("Documento encontrado. Fluxo de Modificação...")
                            registrar_log({"BP": valor_bp, "NF": string_sap, "Status": "Encontrado", "Detalhes": "Pendente de lógica de modificação"})

                    else:
                        msg_erro_pesquisa = "Frame de Pesquisa SAP não carregou"
                        print(f"Erro: {msg_erro_pesquisa}")
                        registrar_log({"BP": valor_bp, "NF": string_sap, "Status": "Erro", "Detalhes": msg_erro_pesquisa})

                except Exception as e_linha:
                    print(f"Erro ao processar documento {valor_bp}: {e_linha}")
                    registrar_log({"BP": valor_bp, "NF": string_sap, "Status": "Erro", "Detalhes": str(e_linha)})

            print("\n>>> TODOS OS DOCUMENTOS PROCESSADOS <<<")
            input("Pressione Enter para encerrar...")

        except Exception as e:
            print(f"\nERRO FATAL: {e}")

if __name__ == "__main__":
    iniciar_automacao()