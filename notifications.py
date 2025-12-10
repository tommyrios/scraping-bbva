import os
import time
import requests
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

    def dividir_mensaje(self, texto, limite=800):
        """Divide un texto largo en fragmentos más pequeños para que pasen por la URL"""
        if len(texto) <= limite:
            return [texto]
        
        partes = []
        while texto:
            corte = texto[:limite]
            ultimo_salto = corte.rfind('\n')
            
            if ultimo_salto > 0 and len(texto) > limite:
                partes.append(texto[:ultimo_salto])
                texto = texto[ultimo_salto:].strip()
            else:
                partes.append(corte)
                texto = texto[limite:].strip()
        return partes

    def enviar_difusion(self, mensaje):
        if not self.api_key:
            print("Error: No hay API KEY de WhatsApp configurada.")
            return

        if not self.destinatarios:
            print("Error: No hay destinatarios configurados.")
            return

        print(f"Iniciando difusión a {len(self.destinatarios)} destinatarios...")
        
        base_url = "https://api.callmebot.com/whatsapp.php"

        fragmentos = self.dividir_mensaje(mensaje, limite=800)

        for telefono in self.destinatarios:
            try:
                for i, fragmento in enumerate(fragmentos):
                    
                    texto_final = fragmento
                    if len(fragmentos) > 1:
                        texto_final = f"📄 *Parte {i+1}/{len(fragmentos)}*\n\n{fragmento}"

                    params = {
                        'phone': telefono,
                        'apikey': self.api_key,
                        'text': texto_final
                    }
                    
                    response = requests.get(base_url, params=params, timeout=20)
                    
                    if response.status_code == 200 and "ERROR" not in response.text:
                        print(f"✅ Enviado a {telefono} (Parte {i+1}): {response.text}")
                    else:
                        print(f"⚠️ Error enviando parte {i+1} a {telefono}: {response.text}")
                    
                    time.sleep(2)

            except Exception as e:
                print(f"❌ Error crítico enviando a {telefono}: {e}")
