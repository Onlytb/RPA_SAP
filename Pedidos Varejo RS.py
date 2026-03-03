import os
import pandas as pd
import numpy as np

# --- Configurações ---
DIRETORIO_ORIGEM = r"U:\Drives compartilhados\CD PR - CGS\Dados\Pedidos Varejo RS"
DIRETORIO_DESTINO = os.path.join(DIRETORIO_ORIGEM, "csvs")

NOVOS_NOMES_COLUNAS = [
    "ordem_venda",
    "dt_criacao",
    "nro_pedido",
    "dt_pedido",
    "centro",
    "remessa",
    "pedido_sto",    
    "remessa_calaminho"
]

def converter_varejo_para_csv():
    if not os.path.exists(DIRETORIO_DESTINO):
        os.makedirs(DIRETORIO_DESTINO)
        print(f"Pasta criada: {DIRETORIO_DESTINO}")

    print(f"Monitorando Varejo: {DIRETORIO_ORIGEM}")
    print(f"Salvando em: {DIRETORIO_DESTINO}\n")

    arquivos = os.listdir(DIRETORIO_ORIGEM)
    contador_convertidos = 0

    for arquivo in arquivos:
        if arquivo.lower().endswith(".xlsx") and not arquivo.startswith("~$"):
            
            caminho_xlsx = os.path.join(DIRETORIO_ORIGEM, arquivo)
            nome_csv = os.path.splitext(arquivo)[0] + ".csv"
            caminho_csv = os.path.join(DIRETORIO_DESTINO, nome_csv)

            processar = False

            if not os.path.exists(caminho_csv):
                print(f"[NOVO] Encontrado: {arquivo}")
                processar = True
            else:
                if os.path.getmtime(caminho_xlsx) > os.path.getmtime(caminho_csv):
                    print(f"[ATUALIZAÇÃO] Modificado: {arquivo}")
                    processar = True

            if processar:
                try:
                    # Carrega o Excel
                    df = pd.read_excel(caminho_xlsx)

                    qtd_colunas_excel = len(df.columns)
                    qtd_colunas_novas = len(NOVOS_NOMES_COLUNAS)

                    if qtd_colunas_excel == qtd_colunas_novas:
                        # 1. Renomeia as colunas
                        df.columns = NOVOS_NOMES_COLUNAS
                        
                        # --- CORREÇÃO DO .0 ---
                        # Tratamos 'remessa_calaminho' e outras colunas de ID que não devem ter decimais
                        colunas_para_limpar = ["remessa_calaminho", "remessa", "nro_pedido", "ordem_venda", "pedido_sto"]
                        
                        for col in colunas_para_limpar:
                            if col in df.columns:
                                # Converte para string e remove o ".0" do final se existir
                                df[col] = df[col].astype(str).str.replace(r'\.0$', '', regex=True)
                                # Limpa valores que ficaram como 'nan' (vazios no Excel)
                                df[col] = df[col].replace('nan', '')

                        # 2. Adiciona a coluna com o nome do arquivo original
                        df['nome_arquivo'] = arquivo

                        # 3. REORDENAÇÃO PARA O BIGQUERY
                        # Mantém 'remessa_calaminho' como a última coluna conforme sua lista
                        cols = list(df.columns)
                        col_nome_arquivo = cols.pop(-1) # Remove 'nome_arquivo' do fim
                        cols.insert(len(cols)-1, col_nome_arquivo) # Insere antes da última
                        
                        df = df[cols]

                        # 4. Salva o CSV
                        df.to_csv(caminho_csv, index=False, sep=';', encoding='utf-8-sig')
                        
                        contador_convertidos += 1
                        print(f" -> Sucesso! {arquivo} convertido (formatado sem .0).")
                    
                    else:
                        print(f" [ERRO] O arquivo {arquivo} tem {qtd_colunas_excel} colunas (esperava {qtd_colunas_novas}).")

                except Exception as e:
                    print(f" [ERRO CRÍTICO] Falha ao processar {arquivo}: {e}\n")

    print("\n--- Processo Finalizado ---")
    if contador_convertidos > 0:
        print(f"Total de arquivos processados: {contador_convertidos}")
    else:
        print("Tudo atualizado.")

if __name__ == "__main__":
    converter_varejo_para_csv()
    input("\nPressione ENTER para sair...")