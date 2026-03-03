import pandas as pd
import glob
import os

origem_path = r'U:\Drives compartilhados\CD PR - CGS\Hitórico Registros KNAPP'
destino_path = r'C:\Users\diego.wergenski@grupoboticario.com.br\Desktop\Projetos\Simulador Volumes\backend'
arquivo_final = os.path.join(destino_path, 'Base_KNAPP_FULL.csv')

def consolidar_bases():
    # 1. Localizar todos os arquivos .xlsx na pasta de origem
    arquivos = glob.glob(os.path.join(origem_path, "*.xlsx"))
    
    if not arquivos:
        print("Nenhum arquivo .xlsx encontrado na pasta de origem.")
        return

    lista_df = []

    print(f"Iniciando processamento de {len(arquivos)} arquivos...")

    for arquivo in arquivos:
        try:
            # Ler o arquivo Excel
            df = pd.read_excel(arquivo)
            
            # Garantir que as colunas necessárias existam
            colunas_esperadas = ['Data', 'Leitura', 'Mensagem', 'Ponto de decisão']
            if not all(col in df.columns for col in colunas_esperadas):
                print(f"Aviso: Arquivo {os.path.basename(arquivo)} ignorado por falta de colunas esperadas.")
                continue

            # Selecionar e renomear as colunas
            df_fmt = df[colunas_esperadas].copy()
            df_fmt.columns = ['dt_hora', 'uc', 'mensagem', 'ponto_decisao']
            
            # Formatar a coluna de data para o padrão do exemplo (DD/MM/YYYY HH:MM:SS)
            df_fmt['dt_hora'] = pd.to_datetime(df_fmt['dt_hora']).dt.strftime('%d/%m/%Y %H:%M:%S')
            
            lista_df.append(df_fmt)
            print(f"Processado: {os.path.basename(arquivo)}")
            
        except Exception as e:
            print(f"Erro ao processar {os.path.basename(arquivo)}: {e}")

    if lista_df:
        # 2. Unir todos os DataFrames
        df_final = pd.concat(lista_df, ignore_index=True)
        
        # 3. Criar a pasta de destino caso não exista
        if not os.path.exists(destino_path):
            os.makedirs(destino_path)
            
        # 4. Salvar como CSV (separado por vírgula, sem índice, codificação utf-8)
        df_final.to_csv(arquivo_final, index=False, sep=',', encoding='utf-8')
        print(f"\nSucesso! Base consolidada salva em: {arquivo_final}")
        print(f"Total de registros: {len(df_final)}")
    else:
        print("Nenhum dado válido foi processado.")

if __name__ == "__main__":
    consolidar_bases()