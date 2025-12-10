import os
import time
import requests # <--- Usamos la librería profesional

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
                # ESTRATEGIA HÍBRIDA CON REQUESTS
                # 1. params: Van a la URL (Para que el servidor encuentre el teléfono)
                params_url = {
                    'phone': telefono,
                    'apikey': self.api_key
                }
                
                # 2. data: Va al cuerpo/body (Para que entre el texto largo)
                body_data = {
                    'text': mensaje
                }
                
                # Enviamos POST. 'requests' combina URL params + Body data automáticamente.
                response = requests.post(url, params=params_url, data=body_data, timeout=20)
                
                if response.status_code == 200 and "ERROR" not in response.text:
                    print(f"✅ Enviado a {telefono}: {response.text}")
                else:
                    print(f"⚠️ Servidor respondió con error a {telefono}: {response.text}")
                    
                    # PLAN B: Si falla el POST, intentamos GET (puede cortar el mensaje, pero llega)
                    print("🔄 Intentando reenvío con método alternativo (GET)...")
                    params_url['text'] = mensaje
                    requests.get(url, params=params_url, timeout=20)
                
                time.sleep(2)
                
            except Exception as e:
                print(f"❌ Error crítico enviando a {telefono}: {e}")
