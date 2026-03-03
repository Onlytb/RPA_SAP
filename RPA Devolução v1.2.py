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
    """Extrai cabeçalho e lista de produtos do XML."""
    try:
        tree = ET.parse(caminho_xml)
        root = tree.getroot()
        ns = {'nfe': 'http://www.portalfiscal.inf.br/nfe'}
        
        def get_text(path, parent=root):
            node = parent.find(path, ns)
            return node.text if node is not None else ""
        
        # Dados do Cabeçalho
        dados = {
            "dhEmi": get_text(".//nfe:dhEmi"), 
            "vNFTot": get_text(".//nfe:vNFTot"), 
            "infCpl": get_text(".//nfe:infAdic/nfe:infCpl")
        }
        
        # Lógica de Extração de Múltiplos Produtos
        produtos = []
        for det in root.findall(".//nfe:det", ns):
            cProd = get_text(".//nfe:cProd", det)
            qCom_raw = get_text(".//nfe:qCom", det)
            
            if cProd and qCom_raw:
                # Converte "10.0000" para "10"
                qCom_int = str(int(float(qCom_raw))) 
                produtos.append({"codigo": cProd, "quantidade": qCom_int})
                
        dados["produtos"] = produtos
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
                    
                    dados_nfe = extrair_dados_xml(caminho_xml_baixado)
                    data_formatada = ""
                    if dados_nfe and dados_nfe.get("dhEmi"):
                        data_xml_bruta = dados_nfe["dhEmi"]
                        data_formatada = datetime.strptime(data_xml_bruta[:10], "%Y-%m-%d").strftime("%d.%m.%Y")
                    
                    # ETAPA 2: PESQUISA SAP
                    page.goto("https://s4prd.sap.grupoboticario.digital/sap/bc/ui2/flp?appState=lean#SalesDocument-change?sap-ui-tech-hint=GUI", wait_until="load")
                    
                    target_frame = None
                    for _ in range(15):
                        for frame in page.frames:
                            try:
                                if frame.locator('input[title="Referência de cliente como campo matchcode"]').is_visible(timeout=500):
                                    target_frame = frame; break
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
                            aba_criacao = contexto.new_page()
                            aba_criacao.bring_to_front()
                            aba_criacao.goto("https://s4prd.sap.grupoboticario.digital/sap/bc/ui2/flp?appState=lean#SalesDocument-create?sap-ui-tech-hint=GUI", wait_until="load")
                            
                            aba_criacao.wait_for_selector('.sapUshellAppTitleText', state="visible", timeout=60000)
                            time.sleep(8)
                            
                            print(f"Digitando Tipo de Ordem: {tp_pedido}")
                            aba_criacao.keyboard.type(tp_pedido, delay=100)
                            time.sleep(1)
                            aba_criacao.keyboard.press("Tab")
                            time.sleep(1)
                            aba_criacao.keyboard.type("BR03", delay=100)
                            time.sleep(1)
                            aba_criacao.keyboard.press("Enter")
                            
                            # --- TELA DE SÍNTESE ---
                            print("Aguardando carregamento da tela de Síntese...")
                            time.sleep(5)
                            
                            sel_emissor = 'input[title="Emissor da ordem"]'
                            sel_receb = "xpath=/html/body/table/tbody/tr/td/div/form/div/div[4]/div/div[1]/div/div[6]/div/div[5]/table/tbody/tr/td[1]/input"
                            sel_ref_cliente = "xpath=/html/body/table/tbody/tr/td/div/form/div/div[4]/div/div[1]/div/div[8]/table/tbody/tr/td[1]/input"
                            sel_dt_ref_cliente = 'input[title="Data de referência do cliente"]'

                            create_frame = None
                            for _ in range(20):
                                for frame in aba_criacao.frames:
                                    try:
                                        if frame.locator(sel_emissor).is_visible(timeout=500):
                                            create_frame = frame; break
                                    except: continue
                                if create_frame: break
                                time.sleep(1)
                                
                            if create_frame:
                                print("Preenchendo detalhes iniciais da Ordem...")
                                create_frame.click(sel_emissor)
                                create_frame.fill(sel_emissor, valor_bp)
                                create_frame.click(sel_receb)
                                create_frame.fill(sel_receb, valor_bp)
                                create_frame.click(sel_ref_cliente)
                                create_frame.fill(sel_ref_cliente, num_nf)
                                create_frame.click(sel_dt_ref_cliente)
                                create_frame.fill(sel_dt_ref_cliente, data_formatada)
                                
                                # CONFIRMAR DADOS (Isso aciona o pop-up de Área de Vendas)
                                print("Validando parceiros (Enter)...")
                                aba_criacao.keyboard.press("Enter")
                                time.sleep(3)
                                
                                # --- 1. POP-UP ÁREA DE VENDAS ---
                                sel_btn_ok_popup = 'div[title="Selecionar (Entrada)"]'
                                if create_frame.locator(sel_btn_ok_popup).is_visible(timeout=5000):
                                    print("Pop-up de Área de Vendas detectado. Buscando 'Produtos'...")
                                    linha_produtos = create_frame.locator('div.lsAbapListText--text', has_text=re.compile(r"Produtos", re.IGNORECASE)).first
                                    
                                    if linha_produtos.is_visible(timeout=2000):
                                        linha_produtos.click()
                                        create_frame.click(sel_btn_ok_popup)
                                        time.sleep(2)
                                    else:
                                        msg_sem_produtos = "Opção 'Produtos' não encontrada na Área de Vendas."
                                        print(msg_sem_produtos)
                                        registrar_log({"BP": valor_bp, "NF": string_sap, "Status": "Erro", "Detalhes": msg_sem_produtos})
                                        aba_criacao.close()
                                        page.bring_to_front()
                                        continue # Pula para o próximo documento
                                
                                # --- 2. PREENCHIMENTO DE CONDIÇÕES E MOTIVO ---
                                print("Preenchendo Condições e Motivo...")
                                create_frame.fill('input[title="Chave de condições de pagamento"]', "AV01")
                                create_frame.fill('input[title="Incoterms parte 1"]', "CIF")
                                create_frame.fill('input[title="Local incoterms 1"]', "CURITIBA - PR")
                                time.sleep(1)
                                
                                # Dropdown Motivo da Ordem
                                create_frame.click('input[title="Motivo da ordem (motivo da transação comercial)"]')
                                create_frame.locator('div[role="option"]', has_text="Devolução Mercadoria p/ AVALIAÇÃO").click()
                                time.sleep(1)

                                print("Enviando confirmações (Enter) para validar os dados do cabeçalho...")
                                aba_criacao.keyboard.press("Enter")
                                time.sleep(2) # Pausa para a primeira validação do SAP
                                aba_criacao.keyboard.press("Enter")
                                time.sleep(3)

                                # --- 3. INSERÇÃO DOS PRODUTOS ---# --- 3. INSERÇÃO DOS PRODUTOS ---
                                print(f"Inserindo {len(dados_nfe['produtos'])} produtos da NFe...")
                                
                                for i, produto in enumerate(dados_nfe['produtos']):
                                    # Usamos o atributo 'lsdata' que sempre contém a referência técnica do campo,
                                    # aliado à classe '.lsField__input' para não confundir com o cabeçalho da tabela.
                                    seletor_material = '.lsField__input[lsdata*="RV45A-MABNR"]'
                                    seletor_qtd = '.lsField__input[lsdata*="RV45A-KWMENG"]'
                                    
                                    try:
                                        # 1. MATERIAL
                                        celula_mat = create_frame.locator(seletor_material).nth(i)
                                        celula_mat.scroll_into_view_if_needed()
                                        celula_mat.click() # Clica para focar e forçar o SAP a virar um Input
                                        time.sleep(0.5)
                                        # Digita via teclado para evitar erros de elemento que acabou de "mutar" no DOM
                                        aba_criacao.keyboard.type(produto['codigo'], delay=50)
                                        
                                        # 2. QUANTIDADE
                                        celula_qtd = create_frame.locator(seletor_qtd).nth(i)
                                        celula_qtd.click() # Clica na quantidade
                                        time.sleep(0.5)
                                        # O SAP costuma pré-preencher com zeros, então selecionamos tudo (Ctrl+A) e apagamos antes de digitar
                                        aba_criacao.keyboard.press("Control+A")
                                        aba_criacao.keyboard.press("Backspace")
                                        aba_criacao.keyboard.type(produto['quantidade'], delay=50)
                                        
                                    except Exception as e_prod:
                                        print(f"Aviso: Erro ao tentar preencher o produto {i+1} - {e_prod}")

                                print("Validando produtos inseridos (Enter)...")
                                aba_criacao.keyboard.press("Enter")
                                time.sleep(4) # Pausa para o SAP calcular impostos e validar materiais

                                # ==========================================
                                # NOVA VERIFICAÇÃO: BARRA DE STATUS (RODAPÉ)
                                # ==========================================
                                print("Verificando a barra de status do SAP por erros de material...")
                                seletor_barra_status = '[id="wnd[0]/sbar_msg-txt"]'
                                
                                # Verifica se a barra de mensagens está visível e contém o texto
                                if create_frame.locator(seletor_barra_status).is_visible(timeout=3000):
                                    texto_status = create_frame.locator(seletor_barra_status).inner_text()
                                    
                                    # Checa se a mensagem contém o nosso erro de "ctg.item" (ignora maiúsculas/minúsculas)
                                    if "ctg.item" in texto_status.lower() or "não existe" in texto_status.lower():
                                        msg_aviso = f"Produto sem cadastro. Msg SAP: {texto_status}"
                                        print(f"Alerta: {msg_aviso}")
                                        registrar_log({"BP": valor_bp, "NF": string_sap, "Status": "Erro", "Detalhes": msg_aviso})
                                        
                                        # Fecha a aba de criação e pula para o próximo cliente
                                        aba_criacao.close()
                                        page.bring_to_front()
                                        continue 

                                # --- 4. EXIBIR DETALHES CABEÇALHO ---
                                print("Acessando Detalhes do Cabeçalho...")
                                create_frame.click('div[title="Exibir detalhes p/cabeç.doc."]')
                                time.sleep(3)
                                
                                # --- 5. ABAS DO CABEÇALHO ---
                                print("Preenchendo aba 'Documento de faturamento'...")
                                # Clica na aba pelo texto exato
                                create_frame.get_by_text("Documento de faturamento", exact=True).click()
                                time.sleep(1.5)
                                
                                # O SAP reutiliza IDs entre as abas. Usar .filter(has=...) garante que pegaremos o que está visível na tela atual.
                                create_frame.locator('input[title="Local incoterms 1"]').filter(has=create_frame.locator("visible=true")).fill("CURITIBA - PR")
                                create_frame.locator('input[title="Data do faturamento"]').filter(has=create_frame.locator("visible=true")).fill(data_formatada)
                                create_frame.locator('input[title="Data na qual os serviços são prestados"]').filter(has=create_frame.locator("visible=true")).fill(data_formatada)

                                print("Preenchendo aba 'Contabilidade'...")
                                create_frame.get_by_text("Contabilidade", exact=True).click()
                                time.sleep(1.5)
                                create_frame.locator('input[title="Nº documento de referência"]').filter(has=create_frame.locator("visible=true")).fill(string_sap)

                                print("Acessando aba 'Condições'...")
                                create_frame.get_by_text("Condições", exact=True).click()
                                time.sleep(1.5)
                                
                                # --- FIM DO BLOCO ATUAL ---
                                print(f"Pronto para a próxima fase na aba de Condições!")
                                registrar_log({"BP": valor_bp, "NF": string_sap, "Status": "Em Andamento", "Detalhes": "Chegou na aba Condições."})
                                
                            else:
                                msg_erro_frame = "Frame da tela de Síntese não carregou a tempo."
                                print(f"Erro: {msg_erro_frame}")
                                registrar_log({"BP": valor_bp, "NF": string_sap, "Status": "Erro", "Detalhes": msg_erro_frame})
                            
                            aba_criacao.close()
                            page.bring_to_front() 
                                
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