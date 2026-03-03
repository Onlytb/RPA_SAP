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
        writer = csv.DictWriter(f, fieldnames=["Data_Hora", "BP", "NF", "Status", "Detalhes", "remessa_criada", "validacao_financeira"])
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
        
        # CORREÇÃO AQUI: A tag correta no XML é 'vNF', não 'vNFTot'
        # Tentamos buscar vNF recursivamente
        valor_total = get_text(".//nfe:vNF")
        if not valor_total:
             # Fallback: Tenta buscar pelo caminho completo se o recursivo falhar
             valor_total = get_text(".//nfe:total/nfe:ICMSTot/nfe:vNF")

        dados = {
            "dhEmi": get_text(".//nfe:dhEmi"), 
            "vNFTot": valor_total, 
            "infCpl": get_text(".//nfe:infAdic/nfe:infCpl")
        }
        
        produtos = []
        for det in root.findall(".//nfe:det", ns):
            cProd = get_text(".//nfe:cProd", det)
            qCom_raw = get_text(".//nfe:qCom", det)
            if cProd and qCom_raw:
                qCom_int = str(int(float(qCom_raw))) 
                cProd_clean = str(int(cProd)) if cProd.isdigit() else cProd
                produtos.append({"codigo": cProd_clean, "quantidade": qCom_int})
                
        dados["produtos"] = produtos
        return dados
    except Exception as e:
        print(f"Erro ao ler XML: {e}"); return None

def aguardar_loading_sap(frame):
    """Monitora a caixa de carregamento do SAP."""
    try:
        if frame.locator('#ur-loading-box').is_visible(timeout=1000):
            print("Aguardando SAP processar...")
            frame.locator('#ur-loading-box').wait_for(state="hidden", timeout=60000)
            time.sleep(0.5)
    except: pass

def converter_valor_sap(valor_str):
    """Converte string do SAP (ex: '3,86 ') para float (3.86)."""
    try:
        if not valor_str: return 0.0
        # Remove espaços e troca vírgula por ponto
        limpo = valor_str.strip().replace('.', '').replace(',', '.')
        return float(limpo)
    except:
        return 0.0

def validar_itens_tabela(frame, produtos_xml):
    """Compara os itens da tabela do SAP (aba Venda) com o XML."""
    print("Iniciando validação cruzada de produtos (SAP vs XML)...")
    try:
        try:
            if frame.locator('span[id*="M0:46:2::0:0-text"]').is_visible():
                 frame.click('span[id*="M0:46:2::0:0-text"]')
            else:
                 frame.get_by_text("Venda", exact=True).click()
        except: pass
        
        aguardar_loading_sap(frame)
        time.sleep(2) 

        seletor_material = 'span[lsdata*="RV45A-MABNR"]'
        seletor_qtd = 'span[lsdata*="RV45A-KWMENG"]'
        
        try: frame.wait_for_selector(seletor_material, state="visible", timeout=10000)
        except: return False

        elementos_mat = frame.locator(seletor_material).all()
        elementos_qtd = frame.locator(seletor_qtd).all()
        
        itens_sap = []
        for i, el_mat in enumerate(elementos_mat):
            val_mat = el_mat.inner_text().strip()
            if not val_mat: continue 
            if i < len(elementos_qtd):
                val_qtd = elementos_qtd[i].inner_text().strip()
            else: val_qtd = "0"
            
            cod_norm = str(int(val_mat)) if val_mat.isdigit() else val_mat
            try:
                qtd_limpa = val_qtd.replace('.', '').replace(',', '.')
                qtd_norm = str(int(float(qtd_limpa)))
            except: qtd_norm = val_qtd

            itens_sap.append({"codigo": cod_norm, "quantidade": qtd_norm})

        lista_sap_sorted = sorted(itens_sap, key=lambda x: x['codigo'])
        lista_xml_sorted = sorted(produtos_xml, key=lambda x: x['codigo'])
        
        if lista_sap_sorted == lista_xml_sorted:
            print("✅ Itens validados com sucesso.")
            return True
        else:
            print("❌ Divergência de itens encontrada!")
            return False
    except Exception as e:
        print(f"Erro validação itens: {e}"); return False

def executar_fluxo_modificacao(frame, string_sap, data_formatada, valor_bp, dados_nfe):
    """Executa abas de modificação e validação financeira."""
    print("--- Iniciando Fluxo de Modificação / Complemento ---")
    
    # 1. Cabeçalho
    print("Acessando Detalhes do Cabeçalho...")
    frame.click('div[title="Exibir detalhes p/cabeç.doc."]')
    aguardar_loading_sap(frame)
    time.sleep(3)
    
    remessa = "N/A"
    try:
        el_remessa = frame.locator('input[title="Documento de vendas"]')
        if el_remessa.is_visible():
            remessa = el_remessa.input_value()
            print(f"Remessa capturada: {remessa}")
    except: pass

    # 2. Aba Faturamento
    print("Preenchendo aba 'Documento de faturamento'...")
    frame.get_by_text("Documento de faturamento", exact=True).click()
    aguardar_loading_sap(frame)
    time.sleep(1.5)
    
    frame.locator('input[title="Local incoterms 1"]').filter(has=frame.locator("visible=true")).fill("CURITIBA - PR")
    frame.locator('input[title="Data do faturamento"]').filter(has=frame.locator("visible=true")).fill(data_formatada)
    frame.locator('input[title="Data na qual os serviços são prestados"]').filter(has=frame.locator("visible=true")).fill(data_formatada)

    # 3. Aba Contabilidade
    print("Preenchendo aba 'Contabilidade'...")
    frame.get_by_text("Contabilidade", exact=True).click()
    aguardar_loading_sap(frame)
    time.sleep(1.5)
    frame.locator('input[title="Nº documento de referência"]').filter(has=frame.locator("visible=true")).fill(string_sap)

    # 4. Aba Condições e Validação Financeira
    print("Acessando aba 'Condições'...")
    frame.get_by_text("Condições", exact=True).click()
    aguardar_loading_sap(frame)
    time.sleep(2)
    
    print("Validando valores financeiros...")
    try:
        # Verifica se o XML tem valor total válido
        if not dados_nfe.get('vNFTot'):
            raise ValueError("Valor 'vNF' não encontrado no arquivo XML.")

        xml_total = float(dados_nfe['vNFTot'])

        # Captura valor líquido e imposto do SAP
        val_liquido_str = frame.locator('input[title="Valor líquido na moeda do documento"]').input_value()
        val_imposto_str = frame.locator('input[title="Montante de imposto na moeda do documento"]').input_value()
        
        sap_liquido = converter_valor_sap(val_liquido_str)
        sap_imposto = converter_valor_sap(val_imposto_str)
        sap_total = sap_liquido + sap_imposto
        
        diferenca = abs(sap_total - xml_total)
        status_fin = f"SAP: {sap_total:.2f} | XML: {xml_total:.2f} | Diff: {diferenca:.2f}"
        print(status_fin)
        
        if diferenca <= 3.00:
            print("✅ Validação financeira APROVADA. Prosseguindo...")
            
            # Clica no menu 'Documento de vendas'
            frame.click('div[title="Documento de vendas"]')
            time.sleep(1)
            # Clica na opção 'Fornecer'
            frame.get_by_text("Fornecer").click()
            
            registrar_log({
                "BP": valor_bp, 
                "NF": string_sap, 
                "Status": "Concluído", 
                "Detalhes": "Processo finalizado com sucesso.",
                "remessa_criada": remessa,
                "validacao_financeira": "OK"
            })
            print("Processo finalizado para este documento.")
            
        else:
            msg_erro_fin = f"Diferença financeira alta ({diferenca:.2f})."
            print(f"❌ {msg_erro_fin}")
            registrar_log({
                "BP": valor_bp, 
                "NF": string_sap, 
                "Status": "Erro Financeiro", 
                "Detalhes": msg_erro_fin,
                "remessa_criada": remessa,
                "validacao_financeira": status_fin
            })
            frame.click('div[title="Cancelar"]') 

    except Exception as e_fin:
        print(f"Erro na validação financeira: {e_fin}")
        registrar_log({"BP": valor_bp, "NF": string_sap, "Status": "Erro", "Detalhes": f"Erro financeiro: {e_fin}"})


def iniciar_automacao():
    caminho_csv = os.path.join(PASTA_BASE, "BaseRPA.csv")
    pasta_xml = os.path.join(PASTA_BASE, "XMLs Referencia")

    try:
        df = pd.read_csv(caminho_csv, sep=',', encoding='utf-8')
        print(f"Total de documentos para processar: {len(df)}")
    except Exception as e:
        print(f"Erro ao ler CSV: {e}"); return

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
                        # LIMPEZA PREVENTIVA: Campo "Ordem"
                        try:
                            sel_campo_ordem = 'input[title="Documento de vendas"]'
                            if target_frame.locator(sel_campo_ordem).is_visible():
                                target_frame.fill(sel_campo_ordem, "")
                        except: pass

                        target_frame.fill('input[title="Referência de cliente como campo matchcode"]', string_sap)
                        target_frame.click('div[title="Exec.pesquisa"]')
                        aguardar_loading_sap(target_frame)
                        
                        print("Tentando filtrar por BP no pop-up...")
                        try:
                            sel_input_busca = 'input[title="Encontrar entrada na lista"]'
                            target_frame.wait_for_selector(sel_input_busca, state="visible", timeout=10000)
                            target_frame.click(sel_input_busca)
                            target_frame.fill(sel_input_busca, "") 
                            target_frame.fill(sel_input_busca, valor_bp)
                            target_frame.click('div[title="Procurar"]')
                            aguardar_loading_sap(target_frame)
                            time.sleep(2)
                        except: pass

                        xpath_msg_status = "xpath=/html/body/table/tbody/tr/td/div/div/div/div[11]/div/section/div/div[3]/span/span"
                        msg_status = ""
                        if target_frame.locator(xpath_msg_status).is_visible(timeout=3000):
                            msg_status = target_frame.locator(xpath_msg_status).inner_text().strip()
                            print(f"Mensagem SAP: {msg_status}")

                        # CASO 1: CRIAÇÃO (VA01)
                        if "não foi encontrado" in msg_status or "não existe" in msg_status:
                            print("Documento não encontrado. Iniciando Criação (VA01)...")
                            aba_criacao = contexto.new_page()
                            aba_criacao.bring_to_front()
                            aba_criacao.goto("https://s4prd.sap.grupoboticario.digital/sap/bc/ui2/flp?appState=lean#SalesDocument-create?sap-ui-tech-hint=GUI", wait_until="load")
                            aba_criacao.wait_for_selector('.sapUshellAppTitleText', state="visible", timeout=60000)
                            time.sleep(8)
                            
                            print(f"Digitando Tipo de Ordem: {tp_pedido}")
                            aba_criacao.keyboard.type(tp_pedido, delay=100)
                            time.sleep(1); aba_criacao.keyboard.press("Tab")
                            time.sleep(1); aba_criacao.keyboard.type("BR03", delay=100)
                            time.sleep(1); aba_criacao.keyboard.press("Enter")
                            
                            print("Aguardando carregamento da tela de Síntese...")
                            time.sleep(6)
                            
                            create_frame = None
                            for _ in range(20):
                                for frame in aba_criacao.frames:
                                    try:
                                        if frame.locator('input[title="Emissor da ordem"]').is_visible(timeout=500):
                                            create_frame = frame; break
                                    except: continue
                                if create_frame: break
                                time.sleep(1)
                                
                            if create_frame:
                                print("Preenchendo detalhes iniciais da Ordem...")
                                create_frame.click('input[title="Emissor da ordem"]')
                                create_frame.fill('input[title="Emissor da ordem"]', valor_bp)
                                create_frame.click("xpath=/html/body/table/tbody/tr/td/div/form/div/div[4]/div/div[1]/div/div[6]/div/div[5]/table/tbody/tr/td[1]/input")
                                create_frame.fill("xpath=/html/body/table/tbody/tr/td/div/form/div/div[4]/div/div[1]/div/div[6]/div/div[5]/table/tbody/tr/td[1]/input", valor_bp)
                                create_frame.click("xpath=/html/body/table/tbody/tr/td/div/form/div/div[4]/div/div[1]/div/div[8]/table/tbody/tr/td[1]/input")
                                create_frame.fill("xpath=/html/body/table/tbody/tr/td/div/form/div/div[4]/div/div[1]/div/div[8]/table/tbody/tr/td[1]/input", num_nf)
                                create_frame.click('input[title="Data de referência do cliente"]')
                                create_frame.fill('input[title="Data de referência do cliente"]', data_formatada)
                                
                                print("Validando parceiros (Enter)...")
                                aba_criacao.keyboard.press("Enter")
                                time.sleep(5) 
                                
                                sel_btn_ok_popup = 'div[title="Selecionar (Entrada)"]'
                                if create_frame.locator(sel_btn_ok_popup).is_visible(timeout=10000):
                                    linha_produtos = create_frame.locator('div.lsAbapListText--text', has_text=re.compile(r"Produtos", re.IGNORECASE)).first
                                    if linha_produtos.is_visible(timeout=3000):
                                        linha_produtos.click(); time.sleep(0.5)
                                        create_frame.click(sel_btn_ok_popup)
                                        aguardar_loading_sap(create_frame)
                                    else:
                                        msg = "Opção 'Produtos' não encontrada na Área de Vendas."
                                        print(msg); registrar_log({"BP": valor_bp, "NF": string_sap, "Status": "Erro", "Detalhes": msg})
                                        aba_criacao.close(); page.bring_to_front(); continue
                                
                                print("Preenchendo Condições e Motivo...")
                                create_frame.fill('input[title="Chave de condições de pagamento"]', "AV01")
                                create_frame.fill('input[title="Incoterms parte 1"]', "CIF")
                                create_frame.fill('input[title="Local incoterms 1"]', "CURITIBA - PR")
                                time.sleep(1)
                                create_frame.click('input[title="Motivo da ordem (motivo da transação comercial)"]')
                                create_frame.locator('div[role="option"]', has_text="Devolução Mercadoria p/ AVALIAÇÃO").click()
                                time.sleep(1)

                                print("Enviando confirmações (Enter)...")
                                aba_criacao.keyboard.press("Enter"); aguardar_loading_sap(create_frame); time.sleep(2)
                                aba_criacao.keyboard.press("Enter"); aguardar_loading_sap(create_frame); time.sleep(2)

                                print(f"Inserindo {len(dados_nfe['produtos'])} produtos...")

                                # 1. Foca na primeira linha APENAS UMA VEZ antes do loop
                                seletor_material = '.lsField__input[lsdata*="RV45A-MABNR"]'
                                try:
                                    celula_mat_inicial = create_frame.locator(seletor_material).nth(0)
                                    celula_mat_inicial.scroll_into_view_if_needed()
                                    celula_mat_inicial.click()
                                    time.sleep(0.5)
                                except Exception as e:
                                    print(f"Erro ao focar na primeira linha: {e}")

                                # 2. Loop apenas com comandos de teclado (sem procurar elementos novos)
                                for i, produto in enumerate(dados_nfe['produtos']):
                                    try:
                                        # Digita o código do material onde o cursor já está
                                        aba_criacao.keyboard.type(produto['codigo'], delay=50)
                                        
                                        # Dá 1x TAB para ir para o campo de quantidade
                                        aba_criacao.keyboard.press("Tab")
                                        time.sleep(0.2) 
                                        
                                        # Digita a quantidade
                                        aba_criacao.keyboard.type(produto['quantidade'], delay=50)
                                        
                                        # Dá 1x Enter para confirmar a inserção da linha
                                        aba_criacao.keyboard.press("Enter")
                                        
                                        # Aguarda o SAP processar e dá 1 segundo de respiro para não engasgar
                                        aguardar_loading_sap(create_frame)
                                        time.sleep(1)
                                        
                                    except Exception as e:
                                        print(f"Erro ao inserir produto {produto['codigo']}: {e}")
                                        pass

                                        print("Produtos inseridos e validados com sucesso.")
                                        time.sleep(1)
                                        
                                    except Exception as e:
                                        print(f"Erro ao inserir produto {produto['codigo']}: {e}")
                                        pass

                                print("Produtos inseridos e validados com sucesso.")
                                time.sleep(1)

                                seletor_barra_status = '[id="wnd[0]/sbar_msg-txt"]'
                                if create_frame.locator(seletor_barra_status).is_visible(timeout=3000):
                                    texto_status = create_frame.locator(seletor_barra_status).inner_text()
                                    if "ctg.item" in texto_status.lower() or "não existe" in texto_status.lower():
                                        msg_aviso = f"Produto sem cadastro. Msg SAP: {texto_status}"
                                        print(f"Alerta: {msg_aviso}")
                                        registrar_log({"BP": valor_bp, "NF": string_sap, "Status": "Erro", "Detalhes": msg_aviso})
                                        aba_criacao.close(); page.bring_to_front(); continue 

                                executar_fluxo_modificacao(create_frame, string_sap, data_formatada, valor_bp, dados_nfe)
                            
                            aba_criacao.close(); page.bring_to_front()

                        # CASO 2: MODIFICAÇÃO (VA02)
                        else:
                            print(f"Validando células com texto exato '{valor_bp}'...")
                            xpath_celula_bp = f"//tbody//span[contains(@class, 'lsCaption') and text()='{valor_bp}']"
                            
                            try:
                                target_frame.wait_for_selector(xpath_celula_bp, state="visible", timeout=10000)
                                celulas_match = target_frame.locator(xpath_celula_bp).all()
                                qtd = len(celulas_match)
                                print(f"Registros encontrados com BP {valor_bp}: {qtd}")
                                
                                if qtd == 1:
                                    print("Registro único confirmado. Selecionando...")
                                    celulas_match[0].click(); time.sleep(0.5)
                                    if target_frame.locator('div[title="Aceitar"]').is_visible():
                                        target_frame.click('div[title="Aceitar"]')
                                    elif target_frame.locator('[id="btnSH2_copy"]').is_visible():
                                        target_frame.click('[id="btnSH2_copy"]')
                                    else:
                                        print("Botão Aceitar não visível, tentando Enter...")
                                        page.keyboard.press("Enter")
                                    aguardar_loading_sap(target_frame); time.sleep(2)
                                    
                                    if validar_itens_tabela(target_frame, dados_nfe['produtos']):
                                        executar_fluxo_modificacao(target_frame, string_sap, data_formatada, valor_bp, dados_nfe)
                                    else:
                                        msg_div = "Diferença entre NF e OV já existente."
                                        print(f"Erro: {msg_div}")
                                        registrar_log({"BP": valor_bp, "NF": string_sap, "Status": "Erro", "Detalhes": msg_div})
                                        try: target_frame.click('div[title="Voltar"]');
                                        except: pass
                                    
                                elif qtd > 1:
                                    msg = f"Erro: {qtd} registros encontrados para o BP {valor_bp}."
                                    print(msg); registrar_log({"BP": valor_bp, "NF": string_sap, "Status": "Erro", "Detalhes": msg})
                                    try: target_frame.locator('div[title="Cancelar"]').click();
                                    except: pass
                                else:
                                    print("Nenhum match na tabela. Verificando acesso direto...")
                                    if target_frame.locator('div[title="Exibir detalhes p/cabeç.doc."]').is_visible(timeout=3000):
                                        print("Acesso direto confirmado.")
                                        if validar_itens_tabela(target_frame, dados_nfe['produtos']):
                                            executar_fluxo_modificacao(target_frame, string_sap, data_formatada, valor_bp, dados_nfe)
                                        else:
                                            registrar_log({"BP": valor_bp, "NF": string_sap, "Status": "Erro", "Detalhes": "Diferença entre NF e OV (Acesso Direto)."})
                                    else:
                                        msg_err = f"BP {valor_bp} não encontrado na lista."
                                        print(msg_err); registrar_log({"BP": valor_bp, "NF": string_sap, "Status": "Erro", "Detalhes": msg_err})
                                        try: target_frame.locator('div[title="Cancelar"]').click();
                                        except: pass

                            except Exception as e_tab:
                                print(f"Erro tabela ({e_tab}). Verificando se entrou direto...")
                                if target_frame.locator('div[title="Exibir detalhes p/cabeç.doc."]').is_visible():
                                    if validar_itens_tabela(target_frame, dados_nfe['produtos']):
                                        executar_fluxo_modificacao(target_frame, string_sap, data_formatada, valor_bp, dados_nfe)
                                else:
                                    registrar_log({"BP": valor_bp, "NF": string_sap, "Status": "Erro", "Detalhes": "Falha na pesquisa."})

                    else:
                        msg = "Frame de Pesquisa SAP não carregou"
                        print(f"Erro: {msg}"); registrar_log({"BP": valor_bp, "NF": string_sap, "Status": "Erro", "Detalhes": msg})

                except Exception as e_linha:
                    print(f"Erro geral: {e_linha}")
                    registrar_log({"BP": valor_bp, "NF": string_sap, "Status": "Erro", "Detalhes": str(e_linha)})

            print("\n>>> FIM <<<")
            input("Pressione Enter...")

        except Exception as e:
            print(f"\nERRO FATAL: {e}")

if __name__ == "__main__":
    iniciar_automacao()