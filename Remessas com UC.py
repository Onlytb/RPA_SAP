import pandas as pd
import os
import time
import datetime
from pathlib import Path

# --- CONFIGURAÇÕES ---
# Ajuste os caminhos conforme sua necessidade
DIRETORIO_ORIGEM = r"U:\Drives compartilhados\CD PR - CGS\Dados\Remessas Com UC"
DIRETORIO_DESTINO = r"U:\Drives compartilhados\CD PR - CGS\Dados\Remessas Com UC\vscs"

def limpar_formato_hora(valor):
    """Função auxiliar para garantir formato de hora HH:MM:SS"""
    if pd.isna(valor) or valor == '':
        return ''
    try:
        if isinstance(valor, (datetime.time, datetime.datetime)):
            return valor.strftime('%H:%M:%S')
        return str(valor).split('.')[0] 
    except:
        return str(valor)

def limpar_formato_data(valor):
    """Função auxiliar para garantir formato de data YYYY-MM-DD"""
    if pd.isna(valor) or valor == '':
        return ''
    try:
        if isinstance(valor, (datetime.date, datetime.datetime)):
            return valor.strftime('%Y-%m-%d')
        return pd.to_datetime(valor).strftime('%Y-%m-%d')
    except:
        return str(valor)

def processar_arquivo(caminho_arquivo_xlsx):
    nome_arquivo = os.path.basename(caminho_arquivo_xlsx)
    nome_arquivo_csv = os.path.splitext(nome_arquivo)[0] + ".csv"
    caminho_arquivo_csv = os.path.join(DIRETORIO_DESTINO, nome_arquivo_csv)

    print(f"Processando: {nome_arquivo}...")

    try:
        # 1. Ler o arquivo Excel como texto
        df = pd.read_excel(caminho_arquivo_xlsx, dtype=str)

        # --- ALTERAÇÃO AQUI: Renomear a coluna específica ---
        # Verifica se a coluna antiga existe e renomeia para manter coerência
        if 'Unidade med.altern.' in df.columns:
            df.rename(columns={'Unidade med.altern.': 'Unid.medida_básica'}, inplace=True)
            print("  -> Coluna 'Unidade med.altern' renomeada para 'Unid.medida_básica'")
        
        # 2. Renomear todas as colunas (Espaço -> Underline)
        # Isso transformará "Unid.medida básica" (se houver espaço) ou manterá o nome corrigido acima
        df.columns = df.columns.str.replace(' ', '_')

        # 3. Tratamento de Tipos de Dados (Índices baseados em 0)
        # G=6, H=7, I=8 (Inteiros) | K=10, M=12 (Datas) | L=11, N=13 (Horas)

        # Colunas Inteiras (G, H, I)
        cols_int = [6, 7, 8]
        for idx in cols_int:
            if idx < len(df.columns):
                col_name = df.columns[idx]
                df[col_name] = pd.to_numeric(df[col_name], errors='coerce').fillna(0).astype(int)

        # Colunas de Data (K, M)
        cols_data = [10, 12]
        for idx in cols_data:
            if idx < len(df.columns):
                col_name = df.columns[idx]
                df[col_name] = df[col_name].apply(limpar_formato_data)

        # Colunas de Hora (L, N)
        cols_hora = [11, 13]
        for idx in cols_hora:
            if idx < len(df.columns):
                col_name = df.columns[idx]
                df[col_name] = df[col_name].apply(limpar_formato_hora)

        # 4. Salvar como CSV
        df.to_csv(caminho_arquivo_csv, index=False, sep=',', encoding='utf-8-sig')
        
        print(f"Sucesso: {nome_arquivo_csv} criado/atualizado.")

    except Exception as e:
        print(f"Erro ao processar {nome_arquivo}: {e}")

def verificar_pastas():
    if not os.path.exists(DIRETORIO_DESTINO):
        os.makedirs(DIRETORIO_DESTINO)
        print(f"Diretório criado: {DIRETORIO_DESTINO}")

    for arquivo in os.listdir(DIRETORIO_ORIGEM):
        if arquivo.lower().endswith(".xlsx") and not arquivo.startswith("~$"):
            caminho_xlsx = os.path.join(DIRETORIO_ORIGEM, arquivo)
            nome_csv = os.path.splitext(arquivo)[0] + ".csv"
            caminho_csv = os.path.join(DIRETORIO_DESTINO, nome_csv)

            processar = False

            if not os.path.exists(caminho_csv):
                processar = True
            else:
                tempo_mod_xlsx = os.path.getmtime(caminho_xlsx)
                tempo_mod_csv = os.path.getmtime(caminho_csv)
                if tempo_mod_xlsx > tempo_mod_csv:
                    processar = True

            if processar:
                processar_arquivo(caminho_xlsx)

def main():
    print("Monitorando arquivos... Pressione Ctrl+C para parar.")
    while True:
        verificar_pastas()
        time.sleep(60)

if __name__ == "__main__":
    main()