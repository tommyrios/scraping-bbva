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

        texto_codificado = urllib.parse.quote(mensaje)

        print(f"Iniciando difusión a {len(self.destinatarios)} destinatarios...")

        for telefono in self.destinatarios:
            try:
                url = f"https://api.callmebot.com/whatsapp.php?phone={telefono}&text={texto_codificado}&apikey={self.api_key}"
                req = urllib.request.Request(url)
                with urllib.request.urlopen(req) as response:
                    print(f"Enviado a {telefono}: {response.read().decode('utf-8')}")
                time.sleep(2)
            except Exception as e:
                print(f"Error enviando a {telefono}: {e}")
