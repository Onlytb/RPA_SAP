import pandas as pd
import glob
import os

# Caminhos
caminho_entrada = r'U:\Drives compartilhados\CD PR - CGS\Dados\Simulador Volumes\Tarefas CSVs'
pasta_saida = r'U:\Drives compartilhados\CD PR - CGS\Dados\Simulador Volumes'
caminho_final = os.path.join(pasta_saida, 'base_consolidada_tarefas.csv')

all_files = glob.glob(os.path.join(caminho_entrada, "extração_*.csv"))

df_list = []

for filename in all_files:
    # 1. Verifica o tamanho do arquivo antes de abrir
    # Se o arquivo tiver 0 bytes, ele pula
    if os.path.getsize(filename) == 0:
        print(f"Pulando arquivo vazio: {os.path.basename(filename)}")
        continue
        
    try:
        # 2. Tenta ler o arquivo
        df = pd.read_csv(filename, sep=',', encoding='utf-8', low_memory=False)
        
        # 3. Verifica se o DataFrame resultante está vazio (ex: só tinha cabeçalho)
        if df.empty:
            print(f"Arquivo sem dados (apenas cabeçalho): {os.path.basename(filename)}")
            continue
            
        df_list.append(df)
        
    except pd.errors.EmptyDataError:
        print(f"Erro de dados vazios no arquivo: {os.path.basename(filename)} - Pulando.")
    except Exception as e:
        print(f"Erro inesperado ao ler {os.path.basename(filename)}: {e}")

# Concatena e salva
if df_list:
    df_final = pd.concat(df_list, ignore_index=True)
    
    # Opcional: Remover duplicados caso haja sobreposição de datas
    df_final = df_final.drop_duplicates()
    
    df_final.to_csv(caminho_final, index=False, sep=';', encoding='utf-8-sig')
    print(f"\nSucesso! {len(df_list)} arquivos unificados.")
    print(f"Salvo em: {caminho_final}")
else:
    print("Nenhum dado válido foi encontrado para unificar.")