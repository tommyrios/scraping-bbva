import os
import time
import requests 

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
                payload = {
                    'phone': telefono,
                    'apikey': self.api_key,
                    'text': mensaje
                }
                
                response = requests.post(url, data=payload, timeout=10)
                
                if response.status_code == 200 and "ERROR" not in response.text:
                    print(f"✅ Enviado a {telefono}: {response.text}")
                else:
                    print(f"⚠️ Servidor respondió con error a {telefono}: {response.text}")
                
                time.sleep(2)
                
            except Exception as e:
                print(f"❌ Error crítico enviando a {telefono}: {e}")
