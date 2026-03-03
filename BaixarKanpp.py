import time
import os
import shutil
import json
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from playwright.sync_api import sync_playwright, Playwright

# --- 1. Constantes de Configuração (Download) ---
URL_LOGIN = "https://vmpwin1348.boticario.net/APP/web/Login?ReturnUrl=%2fAPP%2fweb%2fDashboardOnlyPTW"
URL_RELATORIO = "https://vmpwin1348.boticario.net/APP/web/Report?IdReport=508"
USUARIO = "diego.wergenski@grupoboticario.com.br"
SENHA = "#Ddn@log3034"

# Atualizado para a pasta principal solicitada
PASTA_DOWNLOADS = os.path.join("U:\\", "Drives compartilhados", "CD PR - CGS", "Hitórico Registros KNAPP")

# --- 2. Constantes de Configuração (ETL para Dashboard) ---
FILE_PATH_UCS = "U:/Meu Drive/UCsDesvios/UCs.csv" 
JSON_OUTPUT_PATH = os.path.join(PASTA_DOWNLOADS, "dashboard_data.json")

TAGS_MAP = [
    ('Peso incorreto.', 'Peso'),
    ('ordem de transporte pendente.', 'Ordem Pendente'),
    ('Conferência obrigatória', 'Conf. Obrigatória'),
    ('Short Picking', 'Corte'),
    ('picking pendente.', 'Picking Pendente')
]
RAW_TAGS = [item[0] for item in TAGS_MAP]
TAGS_REGEX = '|'.join(RAW_TAGS)


# --- 3. Função de Processamento de Dados (ETL) ---
def load_and_process_data_etl(caminho_xlsx):
    print(f"Verificando dados: Lendo {caminho_xlsx} e {FILE_PATH_UCS}...")
    try:
        df_original = pd.read_excel(caminho_xlsx) 
        df_original['Mensagem'] = df_original['Mensagem'].astype(str)
        df_original['Data'] = pd.to_datetime(df_original['Data'])
        
        DF_UCS_LOOKUP = pd.read_csv(FILE_PATH_UCS)
        DF_UCS_LOOKUP['UC'] = DF_UCS_LOOKUP['UC'].astype(str)
        DF_UCS_LOOKUP['Material'] = DF_UCS_LOOKUP['Material'].astype(str) 
        print(f"Lookup de materiais carregado: {len(DF_UCS_LOOKUP)} linhas.")

        max_time = df_original['Data'].max()
        start_time = max_time - pd.Timedelta(hours=8)
        df_original = df_original[df_original['Data'] >= start_time].copy()
        print(f"Base de dados filtrada para 8h. {len(df_original)} registos restantes.")
        
    except FileNotFoundError as e:
        print(f"ERRO: Ficheiro não encontrado: {e.filename}")
        return None 
    except Exception as e:
        print(f"Erro ao ler os ficheiros: {e}")
        return None

    # Lógica de KPIs 
    df_w02_all = df_original[df_original['Ponto de decisão'] == 'W02'].copy()
    kpi_w02_count_total = df_w02_all['Leitura'].nunique()
    
    df_l05 = df_original[df_original['Ponto de decisão'] == 'L05'].copy()
    df_l05_limpo = df_l05[~df_l05['Mensagem'].str.contains(r'\[FULL STATION\] L05', case=False, na=False)].copy()
    df_l05_limpo = df_l05_limpo.sort_values(by=['Leitura', 'Data'])
    df_l05_limpo['time_diff'] = df_l05_limpo.groupby('Leitura')['Data'].diff()
    
    minutos_tolerancia = 2
    mask_evento_clone = df_l05_limpo['time_diff'] >= pd.Timedelta(minutes=minutos_tolerancia)
    df_eventos_clones = df_l05_limpo[mask_evento_clone].copy()
    
    lista_clones_unicos_8h = df_eventos_clones['Leitura'].unique()
    kpi_clones_8h = len(lista_clones_unicos_8h)
    
    time_one_hour_ago = max_time - pd.Timedelta(hours=1)
    df_clones_1h_events = df_eventos_clones[df_eventos_clones['Data'] >= time_one_hour_ago]
    kpi_clones_1h = df_clones_1h_events['Leitura'].nunique()
    
    clones_list_data = []
    if kpi_clones_8h > 0:
        for leitura_id in lista_clones_unicos_8h:
            registros_da_caixa = df_l05_limpo[df_l05_limpo['Leitura'] == leitura_id]
            total_passagens = len(registros_da_caixa)
            ultima_hora = registros_da_caixa['Data'].max()
            
            clones_list_data.append({
                'Leitura': str(leitura_id),
                'Qtd': int(total_passagens),
                'Hora': ultima_hora.strftime('%H:%M')
            })
        clones_list_data.sort(key=lambda x: x['Hora'], reverse=True)

    df_w02_com_tags = df_w02_all[df_w02_all['Mensagem'].str.contains(TAGS_REGEX, case=False, na=False)].copy()
    df_first_seen_tags = df_w02_com_tags.drop_duplicates(subset=['Leitura'], keep='first').copy()
    kpi_total_com_tags = len(df_first_seen_tags) 
    
    conds = []
    choices = []
    for raw_tag, display_name in TAGS_MAP:
        conds.append(df_first_seen_tags['Mensagem'].str.contains(raw_tag, case=False, na=False))
        choices.append(display_name)
    
    df_first_seen_tags['Tag_Unica'] = np.select(condlist=conds, choicelist=choices, default='Outros')
    tag_counts_series = df_first_seen_tags['Tag_Unica'].value_counts()
    
    kpi_tags_counts = {}
    for _, display_name in TAGS_MAP:
        kpi_tags_counts[display_name] = tag_counts_series.get(display_name, 0)

    top_10_list = [] 
    raw_peso_tag = 'Peso incorreto.'
    df_w02_peso_apenas = df_w02_all[df_w02_all['Mensagem'].str.contains(raw_peso_tag, case=False, na=False)]

    if not df_w02_peso_apenas.empty and not DF_UCS_LOOKUP.empty:
        bad_uc_list_peso = df_w02_peso_apenas['Leitura'].astype(str).unique()
        df_bad_materials_peso = DF_UCS_LOOKUP[DF_UCS_LOOKUP['UC'].isin(bad_uc_list_peso)]
        df_bad_materials_grouped = df_bad_materials_peso.groupby(['Material', 'des_material']).size()
        top_10_series = df_bad_materials_grouped.nlargest(10)
        top_10_series_reset = top_10_series.reset_index(name='count')
        top_10_series_reset['count'] = top_10_series_reset['count'].astype(int)
        top_10_list = top_10_series_reset.to_dict('records')

    df_resampled = df_first_seen_tags.set_index('Data').resample('15min').size()
    df_plot_full_history = df_resampled.cumsum().reset_index() 
    df_plot_full_history.columns = ['Timestamp', 'Contagem_Acumulada_Tags']
    df_plot = df_plot_full_history[df_plot_full_history['Timestamp'] >= start_time].copy()

    kpi_hourly_comparison_dict = {
        'previous': "Total Acumulado (Anterior): N/A",
        'current': "Total Acumulado (Atual): N/A",
        'percent_text': "Sem dados suficientes."
    }
    percent_change_value = 0.0 

    if not df_plot_full_history.empty and len(df_plot_full_history) > 1:
        current_total = df_plot_full_history['Contagem_Acumulada_Tags'].iloc[-1] 
        time_now = df_plot_full_history['Timestamp'].iloc[-1]
        time_one_hour_ago = time_now - pd.Timedelta(hours=1)
        
        df_hour_ago = df_plot_full_history[df_plot_full_history['Timestamp'] <= time_one_hour_ago]
        previous_total = 0
        previous_time_obj = None 
        if not df_hour_ago.empty:
            previous_total = df_hour_ago['Contagem_Acumulada_Tags'].iloc[-1]
            previous_time_obj = df_hour_ago['Timestamp'].iloc[-1] 
        
        increase = current_total - previous_total
        percent_text = ""
        current_total = int(current_total)
        previous_total = int(previous_total)
        
        if previous_total > 0:
            percent_change_value = (increase / previous_total) * 100 
            if percent_change_value > 0: percent_text = f"▲ Aumento de {percent_change_value:.1f}%"
            elif percent_change_value < 0: percent_text = f"▼ Queda de {abs(percent_change_value):.1f}%"
            else: percent_text = "Sem alteração (0.0%)"
        elif current_total > 0: 
            percent_text = f"▲ Aumento (de 0 para {current_total})"
            percent_change_value = 100.0
        else: 
            percent_text = "Sem alteração (0)"
        
        current_time_str = time_now.strftime('%H:%M')
        previous_time_str = "Início" 
        if previous_time_obj:
            previous_time_str = previous_time_obj.strftime('%H:%M')
            
        kpi_hourly_comparison_dict = {
            'previous': f"Total Acumulado Até ({previous_time_str}): {previous_total}",
            'current': f"Total Acumulado Até ({current_time_str}): {current_total}",
            'percent_text': percent_text
        }
    
    total_frames = 0
    xaxis_range_list = [start_time.isoformat(), max_time.isoformat()] 
    yaxis_range_list = [0, 10]
    
    if not df_plot.empty:
        yaxis_min = df_plot['Contagem_Acumulada_Tags'].min()
        yaxis_max = df_plot['Contagem_Acumulada_Tags'].max()
        yaxis_range_list = [0, float(yaxis_max) * 1.05] 
        total_frames = len(df_plot)
    
    df_plot['Contagem_Acumulada_Tags'] = df_plot['Contagem_Acumulada_Tags'].astype(int)
    df_plot_dict = df_plot.to_dict('records')
    for record in df_plot_dict:
        record['Timestamp'] = record['Timestamp'].isoformat()
        
    kpi_tags_counts_serializable = {
        key: int(value) for key, value in kpi_tags_counts.items()
    }
    
    return {
        'kpis': {
            'total': int(kpi_w02_count_total),          
            'por_tag': kpi_tags_counts_serializable,    
            'total_com_tags': int(kpi_total_com_tags),
            'clones': int(kpi_clones_8h),
            'clones_1h': int(kpi_clones_1h),
            'clones_detail_list': clones_list_data, 
            'hourly_comparison_dict': kpi_hourly_comparison_dict,
            'percent_change': float(percent_change_value),
            'top_10_materials': top_10_list 
        },
        'graph_data': df_plot_dict,
        'graph_config': {
            'xaxis_range': xaxis_range_list,
            'yaxis_range': yaxis_range_list, 
            'total_frames': int(total_frames)
        }
    }


# --- 4. Função Wrapper para Rodar o ETL ---
def run_etl_processing(caminho_xlsx):
    print("\n" + "="*20 + " INICIANDO PÓS-PROCESSAMENTO (ETL) " + "="*20)
    try:
        data_dict = load_and_process_data_etl(caminho_xlsx)
        if data_dict is None:
            print("Processamento de dados falhou.")
            return

        with open(JSON_OUTPUT_PATH, 'w', encoding='utf-8') as f:
            json.dump(data_dict, f, ensure_ascii=False) 
        print(f"Ficheiro JSON do dashboard guardado com sucesso em: {JSON_OUTPUT_PATH}")
        print("="*61)

    except Exception as e:
        print(f"\n--- ERRO DURANTE O PÓS-PROCESSAMENTO (ETL) ---")
        print(f"Erro: {e}\n")


# --- 5. Função para Converter XLSX para CSV no Padrão do BigQuery ---
def gerar_csv_historico_bq(caminho_xlsx, caminho_csv, data_referencia):
    time.sleep(2)
    print(f"\nA iniciar a conversão para formato do BigQuery...")
    try:
        if not os.path.exists(caminho_xlsx):
            print("Ficheiro XLSX não encontrado para conversão.")
            return

        df = pd.read_excel(caminho_xlsx)
        
        DE_PARA = {
            'Data': 'dt_hora',
            'Leitura': 'leitura',
            'Mensagem': 'mensagem',
            'Ponto de decisão': 'ponto_decisão'
        }
        
        df.rename(columns=DE_PARA, inplace=True)
        
        # Adiciona a data do registo baseada na data de extração solicitada (ontem)
        data_registro_sql = data_referencia.strftime("%Y-%m-%d")
        df['dt_registro'] = data_registro_sql
        
        colunas_finais_bq = ['dt_hora', 'leitura', 'mensagem', 'ponto_decisão', 'dt_registro']
        
        for col in colunas_finais_bq:
            if col not in df.columns:
                print(f"  -> Aviso: Coluna '{col}' não encontrada. A preencher com vazio.")
                df[col] = ""
                
        df_export = df[colunas_finais_bq]
        
        df_export.to_csv(caminho_csv, sep=';', index=False, encoding='utf-8-sig', date_format='%Y-%m-%d %H:%M:%S')
        
        print(f"Ficheiro CSV de histórico guardado com sucesso em: {caminho_csv}")
    except Exception as e:
        print(f"Erro ao formatar/guardar CSV: {e}")


# --- 6. Função Principal de Download ---
def run(playwright: Playwright):
    browser = playwright.chromium.launch(headless=False, slow_mo=250)
    context = browser.new_context(accept_downloads=True)
    page = context.new_page()

    try:
        # --- CÁLCULO DAS DATAS E NOMES DOS FICHEIROS ---
        agora = datetime.now()
        ontem = agora - timedelta(days=1)
        
        # Nome base no formato DDMMYYYY - Pontos de Decisão KNAPP
        nome_base_arquivo = f"{ontem.strftime('%d%m%Y')} - Pontos de Decisão KNAPP"
        
        caminho_xlsx = os.path.join(PASTA_DOWNLOADS, f"{nome_base_arquivo}.xlsx")
        caminho_csv = os.path.join(PASTA_DOWNLOADS, f"{nome_base_arquivo}.csv")
        
        data_hora_inicial = f"{ontem.strftime('%d/%m/%Y')} 00:00:00"
        data_hora_final = f"{ontem.strftime('%d/%m/%Y')} 23:59:59"

        print(f"A navegar para a página de login: {URL_LOGIN}")
        page.goto(URL_LOGIN)
        page.locator("#UserName").fill(USUARIO)
        page.locator("#UserPassword").fill(SENHA)
        page.get_by_role("button", name="LOGIN").click()
        page.wait_for_load_state("networkidle")

        if "DynamicDashboard" not in page.url:
            raise Exception("Falha no login.")
        
        page.goto(URL_RELATORIO)
        page.wait_for_load_state("networkidle")

        print(f"A procurar dados de: {data_hora_inicial} até {data_hora_final}")
        page.locator("#TIMESTAMP_START_I").fill(data_hora_inicial)
        page.locator("#TIMESTAMP_END_I").fill(data_hora_final)
        
        page.get_by_role("button", name="Pesquisar").click()
        page.wait_for_load_state("networkidle", timeout=120000) 

        seletor_tabela = "#GridView"
        page.locator(seletor_tabela).click(button="right")

        id_menu_export = "#GridView_DXContextMenu_Rows_DXI8_T"
        page.wait_for_timeout(500) 
        page.locator(id_menu_export).click()

        with page.expect_download(timeout=240000) as download_info: 
            time.sleep(2)
            page.get_by_text("Export to XLSX", exact=True).click(no_wait_after=True)
        
        download = download_info.value
        temp_path = download.path()

        os.makedirs(PASTA_DOWNLOADS, exist_ok=True)
        
        # Remove o ficheiro antigo se por acaso já existir um com o mesmo nome
        try:
            if os.path.exists(caminho_xlsx):
                os.remove(caminho_xlsx)
        except OSError as e:
            if os.path.exists(temp_path): os.remove(temp_path)
            raise e 

        try:
            # Move e renomeia o ficheiro baixado
            shutil.move(temp_path, caminho_xlsx)
            print(f"Ficheiro Excel transferido e guardado como: {caminho_xlsx}")
            
            # Executa o ETL do dashboard
            #run_etl_processing(caminho_xlsx)
            
            # Gera o CSV para o histórico do BigQuery passando a data (ontem)
            gerar_csv_historico_bq(caminho_xlsx, caminho_csv, ontem)
            
            time.sleep(5)
        except Exception as e:
            if os.path.exists(temp_path): os.remove(temp_path)
            raise e

    except Exception as e:
        print(f"\nOcorreu um erro durante a automação: {e}")
    finally:
        browser.close()

# --- EXECUÇÃO DO SCRIPT ---
if __name__ == "__main__":
    with sync_playwright() as playwright:
        run(playwright)
        print("\n--- Execução do Script Concluída ---")