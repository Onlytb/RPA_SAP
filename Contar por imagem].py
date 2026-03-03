import cv2
import numpy as np
import os

# --- Configurações ---
nome_arquivo = 'testelerfoto.jpg' # Certifique-se que este arquivo está na pasta!

# --- Parâmetros de Detecção (Ajuste estes se não detectar nada) ---
# DICA: Para a foto da tela, tive que aumentar a minDist e o param2
# para evitar detectar o ruído da tela.
p_minDist = 40    # Distância mínima entre centros
p_param2 = 35     # Sensibilidade (menor = detecta mais, maior = mais rigoroso)
p_minRadius = 15  # Raio mínimo
p_maxRadius = 45  # Raio máximo

# 1. Verificação e Carregamento
print(f"Tentando ler: {os.path.abspath(nome_arquivo)}")
if not os.path.exists(nome_arquivo):
    print(f"\nERRO CRÍTICO: O arquivo '{nome_arquivo}' NÃO existe nesta pasta.")
    exit()

img = cv2.imread(nome_arquivo)
if img is None:
    print("\nERRO CRÍTICO: Arquivo encontrado, mas o OpenCV não conseguiu ler. Formato inválido?")
    exit()

print("Imagem carregada com sucesso. Processando...")

# 2. Processamento
output = img.copy()
gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
# Blur um pouco mais forte ajuda se for foto de tela
gray = cv2.medianBlur(gray, 7) 

# 3. Detecção
circles = cv2.HoughCircles(
    gray, 
    cv2.HOUGH_GRADIENT, 
    dp=1.2, 
    minDist=p_minDist,
    param1=50, 
    param2=p_param2,
    minRadius=p_minRadius, 
    maxRadius=p_maxRadius
)

count = 0
if circles is not None:
    circles = np.round(circles[0, :]).astype("int")
    count = len(circles)
    print(f"\n>>> SUCESSO: Foram detectados {count} produtos. <<<")

    for (x, y, r) in circles:
        # Círculo verde ao redor
        cv2.circle(output, (x, y), r, (0, 255, 0), 2)
        # Ponto vermelho no centro
        cv2.circle(output, (x, y), 2, (0, 0, 255), 3)
else:
    print("\n>>> AVISO: Nenhum círculo foi detectado com os parâmetros atuais. <<<")
    print("Tente diminuir o 'p_param2' ou ajustar o 'p_minDist'.")

# 4. Exibição (Usando OpenCV nativo)
print("Abrindo janela de resultado...")
cv2.imshow('Resultado da Contagem (Pressione Q para sair)', output)

# --- O PASSO MAIS IMPORTANTE ---
# O código espera você apertar uma tecla. Sem isso, a janela fecha na hora.
print("Pressione a tecla 'Q' na janela da imagem para fechar o programa.")
cv2.waitKey(0) 
cv2.destroyAllWindows()