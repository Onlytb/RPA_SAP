import os
import time
import subprocess
import socket
from datetime import datetime, timedelta
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

# ================= CONFIGURAÇÕES =================
URL_SAP = "https://ewmprd.sap.grupoboticario.digital/sap/bc/ui2/flp/FioriLaunchpad.html?sap-client=200#Shell-home"
CAMINHO_SALVAR = r"U:\Drives compartilhados\CD PR - CGS\Dados\Pedidos com Remessa e TU"
DATA_INICIO_VARREDURA = datetime(2026, 1, 1)
DIAS_RETROATIVOS_ATUALIZACAO = 10

PORTA_DEBUG = "9222"
DIR_PERFIL_CHROME = r"C:\ChromeProfile_RPA_SAP" 

# --- XPATHS / IDs ---
XPATH_BOTAO_EXECUTAR = "/html/body/table/tbody/tr/td/div/div/div/div[11]/div/footer/div[1]/div/div/div[1]/span[3]/div"
XPATH_BOTAO_EXPORTAR = "/html/body/table/tbody/tr/td/div/form/div/div[4]/div/div/div/table/tbody/tr/td[3]/div/div/div/div/table/tbody/tr[1]/td/div/div/div/table/thead/tr[3]/th/div/div/div[1]/span[27]/div"
XPATH_BOTAO_OK = "/html/body/table/tbody/tr/td/div/div/div[2]/footer/div[1]/div"
# XPath fornecido para a Árvore (Caixa de seleção inicial)
XPATH_ARVORE_ITEM = "/html/body/table/tbody/tr/td/div/form/div/div[4]/div/div/div/table/tbody/tr/td[1]/div/div/div/div/div/table/tbody[1]/tr/td/div/div[2]/table/tbody/tr[5]/td/table/tbody/tr/td[3]/div/table/tbody/tr/td[3]/div/span/span"
ID_ARVORE_TECNICO = "tree#C121#5#1#1#i"

# ================= FUNÇÕES AUXILIARES =================

def garantir_navegador_aberto():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    result = sock.connect_ex(('127.0.0.1', int(PORTA_DEBUG)))
    sock.close()
    if result == 0: return True
    
    print(f"Iniciando Chrome na porta {PORTA_DEBUG}...")
    caminho_chrome = encontrar_executavel_chrome()
    if not caminho_chrome: raise Exception("Chrome não encontrado.")

    cmd = [
        caminho_chrome,
        f"--remote-debugging-port={PORTA_DEBUG}",
        f"--user-data-dir={DIR_PERFIL_CHROME}",
        "--start-maximized",
        "--no-default-browser-check",
        "--no-first-run"
    ]
    subprocess.Popen(cmd)
    time.sleep(5)
    return True

def encontrar_executavel_chrome():
    caminhos = [
        r"C:\Program Files\Google\Chrome\Application\chrome.exe",
        r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
        os.path.expanduser(r"~\AppData\Local\Google\Chrome\Application\chrome.exe")
    ]
    for c in caminhos:
        if os.path.exists(c): return c
    return None

def aguardar_carregamento_sap(contexto):
    selector_id = '[id="ur-loading-box"]'
    print(" -> Verificando status de carregamento...")
    
    try:
        if contexto.locator(selector_id).count() > 0:
            contexto.locator(selector_id).wait_for(state="visible", timeout=5000)
    except:
        if not contexto.locator(selector_id).is_visible():
            print("   [Nenhum loading longo detectado, seguindo...]")
            return

    print("   [Loading detectado! Entrando em modo de espera longa...]")
    tempo_limite = datetime.now() + timedelta(minutes=60)
    
    while datetime.now() < tempo_limite:
        try:
            visivel = contexto.locator(selector_id).is_visible()
            if not visivel:
                print("   [Processamento concluído!]")
                break
            time.sleep(2)
        except Exception as e:
            print(f"   [Aviso no loop de espera: {e}]")
            time.sleep(2)

    time.sleep(2)

def obter_contexto_sap(page):
    seletores = ["iframe[id*='application']", "iframe[id*='canvas']"]
    for sel in seletores:
        if page.locator(sel).count() > 0:
            return page.frame_locator(sel)
    return page

def limpar_e_preencher(contexto, id_selector, valor):
    loc = contexto.locator(f'[id="{id_selector}"]')
    try:
        loc.wait_for(state="visible", timeout=10000)
        loc.click()
        time.sleep(0.1)
        loc.press("Control+A")
        loc.press("Delete")
        loc.fill(str(valor))
        return True
    except Exception as e:
        print(f"Erro input {id_selector}: {e}")
        return False

def clicar_item_arvore_forca_bruta(contexto, texto_item, id_tecnico, xpath_usuario):
    print(f"Reiniciando filtro via Árvore...")
    
    try:
        loc_xpath = contexto.locator(f"xpath={xpath_usuario}")
        if loc_xpath.count() > 0:
            print(" -> Clicando via XPath...")
            loc_xpath.scroll_into_view_if_needed()
            loc_xpath.click()
            time.sleep(0.5)
            loc_xpath.dblclick()
            return True
    except: pass

    try:
        el = contexto.get_by_text(texto_item, exact=True)
        if el.count() > 0:
            print(" -> Clicando via Texto...")
            el.scroll_into_view_if_needed()
            el.click()
            time.sleep(0.5)
            el.dblclick()
            return True
    except: pass
    
    try:
        loc_id = contexto.locator(f'[id="{id_tecnico}"]')
        if loc_id.count() > 0:
            print(" -> Clicando via ID...")
            loc_id.dblclick()
            return True
    except: pass

    print(" -> Tentando injeção de JS no elemento...")
    try:
        target_loc = contexto.locator(f'[id="{id_tecnico}"]')
        if target_loc.count() == 0:
            target_loc = contexto.locator(f"xpath={xpath_usuario}")
        
        if target_loc.count() > 0:
            target_loc.evaluate("element => element.click()")
            time.sleep(0.2)
            target_loc.evaluate("""element => { 
                var ev = new MouseEvent('dblclick', {
                    'view': window, 
                    'bubbles': true, 
                    'cancelable': true
                });
                element.dispatchEvent(ev);
            }""")
            return True
    except Exception as e:
        print(f"Erro na injeção JS: {e}")
    
    return False

def verificar_necessidade_download(data_alvo):
    nome_arquivo = f"Pedidos_CGS {data_alvo.strftime('%d.%m.%Y')}.xlsx"
    caminho_completo = os.path.join(CAMINHO_SALVAR, nome_arquivo)
    if not os.path.exists(caminho_completo): return True, nome_arquivo
    diferenca = (datetime.now() - data_alvo).days
    if diferenca <= DIAS_RETROATIVOS_ATUALIZACAO: return True, nome_arquivo
    return False, nome_arquivo

# ================= FLUXO PRINCIPAL =================

def run():
    try:
        garantir_navegador_aberto()
    except Exception as e:
        print(f"Erro: {e}")
        return

    with sync_playwright() as p:
        browser = None
        try:
            browser = p.chromium.connect_over_cdp(f"http://localhost:{PORTA_DEBUG}")
            context = browser.contexts[0]
            
            page = context.new_page() 
            page.bring_to_front()     
            
            print(f"Acessando SAP na nova aba ativa...")
            page.goto(URL_SAP)

            # 1. Tile Inicial
            try:
                page.wait_for_selector('[id="__content1"]', state="visible", timeout=60000)
                page.click('[id="__content1"]')
            except: pass
            
            time.sleep(5)
            app_frame = obter_contexto_sap(page)
            aguardar_carregamento_sap(app_frame)

            # 2. Configuração Inicial
            print("Configurando ambiente inicial...")
            try:
                loc_deposito = app_frame.locator('[id="M1:46:::0:34"]')
                
                if loc_deposito.is_visible(timeout=5000):
                    print("Preenchendo Depósito: E001")
                    limpar_e_preencher(app_frame, "M1:46:::0:34", "E001")
                    time.sleep(0.5)
                    loc_deposito.click()
                    page.keyboard.press("Enter") 
                    aguardar_carregamento_sap(app_frame)
                    
                    print("Preenchendo Monitor: SAP")
                    loc_monitor = app_frame.locator('[id="M1:46:::1:34"]')
                    loc_monitor.wait_for(state="visible", timeout=10000)
                    
                    limpar_e_preencher(app_frame, "M1:46:::1:34", "SAP")
                    time.sleep(0.5)
                    loc_monitor.click()
                    page.keyboard.press("Enter") 
                    aguardar_carregamento_sap(app_frame)
                    
                    print("Confirmando entrada (F8)...")
                    try:
                        btn_exec = app_frame.locator('[id="M1:50::btn[8]"]')
                        if btn_exec.is_visible(timeout=2000):
                            btn_exec.click()
                        else:
                            page.keyboard.press("F8")
                    except:
                        page.keyboard.press("F8")
                        
                    aguardar_carregamento_sap(app_frame)
            except Exception as e:
                print(f"Erro na configuração inicial (pode já estar preenchido): {e}")

            # 3. LOOP DIA A DIA
            data_atual = DATA_INICIO_VARREDURA
            data_hoje = datetime.now()

            while data_atual <= data_hoje:
                precisa, nome_arq = verificar_necessidade_download(data_atual)
                str_data = data_atual.strftime("%d.%m.%Y")
                
                if precisa:
                    print(f"\n>>> INICIANDO: {str_data}")
                    
                    if not clicar_item_arvore_forca_bruta(app_frame, "Ordem de entrega", ID_ARVORE_TECNICO, XPATH_ARVORE_ITEM):
                        print("ALERTA: Falha na árvore. Tentando recuperar frame...")
                        app_frame = obter_contexto_sap(page)
                        clicar_item_arvore_forca_bruta(app_frame, "Ordem de entrega", ID_ARVORE_TECNICO, XPATH_ARVORE_ITEM)

                    try:
                        print("2. Aguardando parâmetros...")
                        input_data = app_frame.locator('[id="M1:46:::40:34"]')
                        input_data.wait_for(state="visible", timeout=20000)
                        
                        limpar_e_preencher(app_frame, "M1:46:::40:34", str_data)
                        limpar_e_preencher(app_frame, "M1:46:::40:62", str_data)
                        limpar_e_preencher(app_frame, "M1:46:::40:73", "23:59:59")
                        
                        print("3. Executando consulta...")
                        executou = False
                        try:
                            if app_frame.locator(f"xpath={XPATH_BOTAO_EXECUTAR}").is_visible(timeout=2000):
                                app_frame.locator(f"xpath={XPATH_BOTAO_EXECUTAR}").click()
                                executou = True
                        except: pass
                        if not executou: page.keyboard.press("F8")
                        
                        aguardar_carregamento_sap(app_frame)

                        print("4. Verificando resultados...")
                        botao_exportar = None
                        
                        try:
                            loc_xpath = app_frame.locator(f"xpath={XPATH_BOTAO_EXPORTAR}")
                            loc_xpath.wait_for(state="visible", timeout=10000)
                            botao_exportar = loc_xpath
                        except:
                            try:
                                loc_id = app_frame.locator('[id="_MB_EXPORT125"]')
                                loc_id.wait_for(state="visible", timeout=3000)
                                botao_exportar = loc_id
                            except: pass

                        if botao_exportar:
                            print(" -> Botão Exportar encontrado.")
                            botao_exportar.click()
                            
                            try:
                                app_frame.get_by_text("Planilha eletrônica").click(timeout=2000)
                            except:
                                try:
                                    page.get_by_text("Planilha eletrônica").click(timeout=2000)
                                except:
                                    page.keyboard.press("ArrowDown")
                                    page.keyboard.press("Enter")

                            print(" -> Confirmando download...")
                            with page.expect_download(timeout=180000) as download_info:
                                clicado_ok = False
                                try:
                                    btn_ok = app_frame.locator(f"xpath={XPATH_BOTAO_OK}")
                                    btn_ok.wait_for(state="visible", timeout=5000)
                                    btn_ok.click()
                                    clicado_ok = True
                                except: pass
                                
                                if not clicado_ok:
                                    try:
                                        if app_frame.locator('[id="UpDownDialogChoose"]').is_visible():
                                            app_frame.click('[id="UpDownDialogChoose"]')
                                            clicado_ok = True
                                    except: pass
                                
                                if not clicado_ok:
                                    page.keyboard.press("Enter")

                            download = download_info.value
                            dest = os.path.join(CAMINHO_SALVAR, nome_arq)
                            if os.path.exists(dest): os.remove(dest)
                            download.save_as(dest)
                            print(f"Sucesso: {nome_arq} salvo.")
                            time.sleep(2)
                        else:
                            print("Nenhum dado para exportar.")
                            time.sleep(2)

                    except Exception as e:
                        print(f"Erro no dia {str_data}: {e}")

                else:
                    print(f"Pulado: {str_data}")

                data_atual += timedelta(days=1)

            print("Processo finalizado.")
            
            # =========================================================
            # NOVO: FORÇA O FECHAMENTO DO CHROME VIA PROTOCOLO CDP
            # =========================================================
            print("Encerrando o navegador...")
            try:
                # Inicia uma sessão nativa para se comunicar com o motor do Chrome
                cdp_session = context.new_cdp_session(page)
                # Envia a ordem de fechamento da aplicação
                cdp_session.send("Browser.close")
            except Exception as cdp_error:
                print(f"Aviso ao tentar fechar o Chrome: {cdp_error}")

        except Exception as e:
            print(f"Erro Fatal: {e}")
            
        finally:
            # Garante que a desconexão ocorra mesmo se der erro no meio
            if browser:
                try:
                    browser.disconnect()
                except: pass

if __name__ == "__main__":
    run()