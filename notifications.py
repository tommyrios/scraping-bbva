import os
import time
import urllib.request
import urllib.parse

class MensajeSender:
    def __init__(self):
        self.api_key = os.environ.get('WHATSAPP_API_KEY')
        self.destinatarios = []
        
        if 'WHATSAPP_PHONE' in os.environ:
            self.destinatarios.append(os.environ['WHATSAPP_PHONE'])

    def agregar_destinatario(self, numero):
        if numero not in self.destinatarios:
            self.destinatarios.append(numero)

    def enviar_difusion(self, mensaje):
        if not self.api_key:
            print("Error: No hay API KEY de WhatsApp configurada.")
            return

        if not self.destinatarios:
            print("Error: No hay destinatarios configurados.")
            return

        print(f"Iniciando difusión a {len(self.destinatarios)} destinatarios...")

        url = "https://api.callmebot.com/whatsapp.php"

        for telefono in self.destinatarios:
            try:
                params = {
                    'phone': telefono,
                    'text': mensaje,
                    'apikey': self.api_key
                }
                
                data = urllib.parse.urlencode(params).encode('utf-8')
                
                req = urllib.request.Request(url, data=data, method='POST')
                
                req.add_header('User-Agent', 'Python-Urllib/3.9')
                
                with urllib.request.urlopen(req) as response:
                    resultado = response.read().decode('utf-8')
                    if "ERROR" in resultado:
                         print(f"⚠️ Error reportado por API en envío a {telefono}: {resultado}")
                    else:
                         print(f"✅ Enviado a {telefono}: {resultado}")
                
                time.sleep(2)
            except Exception as e:
                print(f"❌ Error de conexión enviando a {telefono}: {e}")
