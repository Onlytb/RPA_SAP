import os
import shutil
import http.server
import socketserver
import socket

# --- CONFIGURAÇÕES ---
PORTA = 8099
PASTA_PARA_ENVIAR = r'C:\Users\diego.wergenski@grupoboticario.com.br\Desktop\Projetos\Simulador Volumes' 
NOME_DO_ARQUIVO = "tudo_em_um.zip"
# ---------------------

class CustomHandler(http.server.SimpleHTTPRequestHandler):
    def do_GET(self):
        if self.path == '/':
            # Página HTML simples com o botão de download
            self.send_response(200)
            self.send_header("Content-type", "text/html; charset=utf-8")
            self.end_headers()
            html = f"""
            <html>
                <body style="font-family: sans-serif; text-align: center; padding-top: 50px;">
                    <h2>Servidor de Transferência</h2>
                    <p>Pasta: <b>{os.path.basename(PASTA_PARA_ENVIAR)}</b></p>
                    <a href="/download" style="padding: 20px; background: #28a745; color: white; text-decoration: none; border-radius: 5px; font-weight: bold;">
                        🚀 CLIQUE AQUI PARA BAIXAR A PASTA COMPLETA (.ZIP)
                    </a>
                </body>
            </html>
            """
            self.wfile.write(html.encode())
            
        elif self.path == '/download':
            # Verifica se o ZIP já existe, se não, cria (para poupar tempo)
            zip_path = NOME_DO_ARQUIVO
            if not os.path.exists(zip_path):
                print(f"📦 Compactando pasta... Aguarde.")
                shutil.make_archive(NOME_DO_ARQUIVO.replace('.zip', ''), 'zip', PASTA_PARA_ENVIAR)
            
            # Serve o arquivo ZIP
            self.send_response(200)
            self.send_header("Content-type", "application/octet-stream")
            self.send_header("Content-Disposition", f'attachment; filename="{NOME_DO_ARQUIVO}"')
            self.send_header("Content-Length", str(os.path.getsize(zip_path)))
            self.end_headers()
            
            with open(zip_path, 'rb') as f:
                shutil.copyfileobj(f, self.wfile)
            print("✅ Transferência concluída!")

# Pega o IP da rede local
hostname = socket.gethostname()
ip_local = socket.gethostbyname(hostname)

with socketserver.TCPServer(("", PORTA), CustomHandler) as httpd:
    print(f"🔥 Servidor Ativo!")
    print(f"📢 No outro PC, acesse: http://{ip_local}:{PORTA}")
    httpd.serve_forever()