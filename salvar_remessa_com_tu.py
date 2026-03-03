import os
import pandas as pd
import time

# --- Configurações ---
DIRETORIO_ORIGEM = r"U:\Drives compartilhados\CD PR - CGS\Dados\Pedidos com Remessa e TU"
DIRETORIO_DESTINO = os.path.join(DIRETORIO_ORIGEM, "csvs")

# Lista das colunas vindas do Excel (17 colunas)
# A ordem aqui deve refletir EXATAMENTE a ordem do seu arquivo Excel
NOVOS_NOMES_COLUNAS = [
    "unidade_de_transporte",
    "documento",
    "status_picking",
    "status_sm",
    "status_dep",
    "tp_doc",
    "transportadora",
    "desc_rec_merc",
    "dt_criacao",
    "hr_criacao",
    "concluído",
    "ordem_prod",
    "dt_planejada",
    "hr_planejada",
    "dt_definitiva",
    "hr_definitiva",
    "recebedor" 
]

def converter_xlsx_para_csv():
    if not os.path.exists(DIRETORIO_DESTINO):
        os.makedirs(DIRETORIO_DESTINO)
        print(f"Pasta criada: {DIRETORIO_DESTINO}")

    print(f"Monitorando: {DIRETORIO_ORIGEM}")
    print(f"Salvando em: {DIRETORIO_DESTINO}\n")

    arquivos = os.listdir(DIRETORIO_ORIGEM)
    contador_convertidos = 0

    for arquivo in arquivos:
        if arquivo.lower().endswith(".xlsx") and not arquivo.startswith("~$"):
            
            caminho_xlsx = os.path.join(DIRETORIO_ORIGEM, arquivo)
            nome_csv = os.path.splitext(arquivo)[0] + ".csv"
            caminho_csv = os.path.join(DIRETORIO_DESTINO, nome_csv)

            processar = False

            # Lógica de Data (só processa se necessário)
            if not os.path.exists(caminho_csv):
                print(f"[NOVO] Encontrado: {arquivo}")
                processar = True
            else:
                if os.path.getmtime(caminho_xlsx) > os.path.getmtime(caminho_csv):
                    print(f"[ATUALIZAÇÃO] Modificado: {arquivo}")
                    processar = True

            if processar:
                try:
                    df = pd.read_excel(caminho_xlsx)

                    qtd_colunas_excel = len(df.columns)
                    qtd_colunas_novas = len(NOVOS_NOMES_COLUNAS) # Esperado: 17

                    if qtd_colunas_excel == qtd_colunas_novas:
                        # 1. Renomeia as colunas originais
                        df.columns = NOVOS_NOMES_COLUNAS
                        
                        # 2. Cria a coluna nome_arquivo (o Pandas joga ela pro final automaticamente)
                        # Neste momento a ordem fica: [..., recebedor, nome_arquivo]
                        df['nome_arquivo'] = arquivo

                        # 3. REORDENAÇÃO FORÇADA
                        # Queremos que 'recebedor' seja a ÚLTIMA coluna para bater com o BigQuery.
                        # Então a ordem deve ser: [..., nome_arquivo, recebedor]
                        
                        cols = list(df.columns)
                        
                        # Move 'nome_arquivo' (que é o último, index -1) para antes de 'recebedor' (index -2)
                        # Remove 'nome_arquivo' da lista e insere na penúltima posição
                        col_nome_arquivo = cols.pop(-1) # Tira 'nome_arquivo' do fim
                        cols.insert(len(cols)-1, col_nome_arquivo) # Insere antes do último ('recebedor')
                        
                        # Aplica a nova ordem ao DataFrame
                        df = df[cols]

                        # 4. Salva o CSV
                        df.to_csv(caminho_csv, index=False, sep=';', encoding='utf-8-sig')
                        
                        contador_convertidos += 1
                        print(f" -> Sucesso! Ordem final salva: ..., {df.columns[-2]}, {df.columns[-1]}")
                    
                    else:
                        print(f" [ERRO] O arquivo {arquivo} tem {qtd_colunas_excel} colunas (esperava {qtd_colunas_novas}).")

                except Exception as e:
                    print(f" [ERRO CRÍTICO] Falha ao ler {arquivo}: {e}\n")

    print("--- Processo Finalizado ---")
    if contador_convertidos > 0:
        print(f"Total processado: {contador_convertidos}")
    else:
        print("Tudo atualizado.")

if __name__ == "__main__":
    converter_xlsx_para_csv()
    print("Processo de conversão para csv finalizado.")
    