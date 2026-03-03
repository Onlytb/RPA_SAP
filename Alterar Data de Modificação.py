import os

def forcar_sincronizacao(caminho_pasta):
    """
    Abre cada arquivo, lê o primeiro byte e o reescreve.
    Isso força o sistema operacional a notificar uma alteração de conteúdo
    sem realmente mudar os dados, engatilhando a sincronização do Google Drive.
    """
    
    contador = 0
    erros = 0

    print(f"--- Iniciando 'Touch' físico nos arquivos em: {caminho_pasta} ---")

    for raiz, diretorios, arquivos in os.walk(caminho_pasta):
        for arquivo in arquivos:
            caminho_completo = os.path.join(raiz, arquivo)
            
            try:
                # Verificamos se o arquivo não está vazio (arquivos vazios dão erro ao ler byte)
                if os.path.getsize(caminho_completo) > 0:
                    
                    # Abre o arquivo em modo de leitura e escrita binária (r+b)
                    with open(caminho_completo, 'r+b') as f:
                        # 1. Lê apenas o primeiro caractere/byte
                        byte_inicial = f.read(1)
                        
                        # 2. Volta o cursor para o início do arquivo
                        f.seek(0)
                        
                        # 3. Reescreve exatamente o mesmo byte que leu
                        f.write(byte_inicial)
                        
                        # O fechamento do arquivo (com o 'with') salva a alteração
                    
                    print(f"[OK] Atualizado: {arquivo}")
                    contador += 1
                else:
                    print(f"[PULADO] Arquivo vazio: {arquivo}")

            except PermissionError:
                print(f"[ERRO] Sem permissão (arquivo aberto?): {arquivo}")
                erros += 1
            except Exception as e:
                print(f"[ERRO] Falha em {arquivo}: {e}")
                erros += 1

    print("-" * 30)
    print(f"Concluído!")
    print(f"Arquivos tocados: {contador}")
    print(f"Erros encontrados: {erros}")

# --- CONFIGURAÇÃO ---
PASTA_ALVO = r"U:\Drives compartilhados\CD PR - CGS\Dados\Remessas Com UC\vscs" 

if __name__ == "__main__":
    if os.path.exists(PASTA_ALVO):
        # AVISO DE SEGURANÇA
        print("AVISO: Este script abrirá e salvará novamente todos os arquivos.")
        print("Recomenda-se fechar outros programas que estejam usando esses arquivos.")
        confirmacao = input("Digite 'S' para continuar: ")
        
        if confirmacao.lower() == 's':
            forcar_sincronizacao(PASTA_ALVO)
        else:
            print("Operação cancelada.")
    else:
        print(f"Erro: A pasta '{PASTA_ALVO}' não existe.")