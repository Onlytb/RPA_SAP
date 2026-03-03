import pandas as pd
import os
import glob
from datetime import datetime

def converter_excels_para_csv():
    # --- CONFIGURAÇÕES ---
    # Caminho da pasta onde você salva os arquivos XLSX originais
    PASTA_ORIGEM = r"U:\Drives compartilhados\CD PR - CGS\Hitórico Registros KNAPP"
    
    # Caminho da pasta sincronizada com o Google Drive onde os CSVs serão salvos
    # IMPORTANTE: Altere para o caminho da sua pasta do Google Drive no PC
    PASTA_SAIDA_DRIVE = r"U:\Drives compartilhados\CD PR - CGS\Hitórico Registros KNAPP" 
    
    # Mapeamento: Nome EXATO no Excel -> Nome EXATO no BigQuery
    # Baseado no seu arquivo de exemplo:
    DE_PARA = {
        'Data': 'dt_hora',              # Coluna A do Excel
        'Leitura': 'leitura',           # Coluna B
        'Mensagem': 'mensagem',         # Coluna C
        'Ponto de decisão': 'ponto_decisão' # Coluna E
        # A coluna 'Comando' não está aqui, então será ignorada
    }

    # Cria a pasta de saída se não existir
    if not os.path.exists(PASTA_SAIDA_DRIVE):
        try:
            os.makedirs(PASTA_SAIDA_DRIVE)
        except OSError as e:
            print(f"Erro ao criar pasta de saída: {e}")
            return

    arquivos_xlsx = glob.glob(os.path.join(PASTA_ORIGEM, "*.xlsx"))
    print(f"Encontrados {len(arquivos_xlsx)} arquivos na origem.")

    for arquivo in arquivos_xlsx:
        nome_arquivo = os.path.basename(arquivo)
        
        # Ignora arquivos temporários do Excel (~$)
        if nome_arquivo.startswith("~$"): continue

        # 1. Extrair data do nome do arquivo (Ex: 16102025 -> 2025-10-16)
        try:
            data_str = nome_arquivo[:8] # Pega os primeiros 8 dígitos
            # Converte para formato YYYY-MM-DD (Padrão SQL DATE)
            data_registro_sql = datetime.strptime(data_str, "%d%m%Y").strftime("%Y-%m-%d")
        except:
            print(f"AVISO: Arquivo pulado (data não identificada no nome): {nome_arquivo}")
            continue

        # Nome do arquivo CSV de saída
        nome_csv = nome_arquivo.replace('.xlsx', '.csv')
        caminho_csv = os.path.join(PASTA_SAIDA_DRIVE, nome_csv)

        # Verifica se o CSV já existe para não refazer trabalho
        if not os.path.exists(caminho_csv):
            print(f"Processando: {nome_arquivo} ...")
            
            try:
                # Ler o Excel
                df = pd.read_excel(arquivo)
                
                # Verifica se as colunas essenciais existem antes de continuar
                # Isso ajuda a pegar erros se o layout mudar no futuro
                colunas_existentes = df.columns.tolist()
                
                # Renomeia as colunas usando o dicionário DE_PARA
                # O parâmetro 'errors="ignore"' permite que o script continue mesmo se não achar 'Comando' para renomear (já que vamos filtrar depois)
                df.rename(columns=DE_PARA, inplace=True)
                
                # Adiciona a coluna de data de registro
                df['dt_registro'] = data_registro_sql
                
                # Lista final de colunas que queremos no BigQuery
                colunas_finais_bq = ['dt_hora', 'leitura', 'mensagem', 'ponto_decisão', 'dt_registro']
                
                # Verifica se todas as colunas necessárias estão presentes após renomear
                # Se faltar alguma (ex: se o Excel vier sem 'Ponto de decisão'), cria ela vazia para não quebrar o BQ
                for col in colunas_finais_bq:
                    if col not in df.columns:
                        print(f"  -> Aviso: Coluna '{col}' não encontrada no arquivo. Preenchendo com vazio.")
                        df[col] = ""
                
                # Filtra o DataFrame para ter APENAS as colunas finais (isso remove a coluna 'Comando')
                df_export = df[colunas_finais_bq]

                # Salva o CSV
                # sep=';' -> Usa ponto e vírgula (melhor para textos com vírgulas)
                # index=False -> Não salva o número da linha
                # date_format -> Garante formato padrão para datas
                df_export.to_csv(caminho_csv, sep=';', index=False, encoding='utf-8-sig', date_format='%Y-%m-%d %H:%M:%S')
                
                print(f"  -> Sucesso! Salvo em: {nome_csv}")
                
            except Exception as e:
                print(f"  -> ERRO ao ler/salvar {nome_arquivo}: {e}")
        else:
            # Arquivo já existe, pula
            pass

    print("\n--- Processo Finalizado ---")

if __name__ == "__main__":
    converter_excels_para_csv()