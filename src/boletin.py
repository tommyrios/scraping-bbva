import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import urllib3
import time
import random # Necesario para el "wait" variable

# Desactivar advertencias de SSL
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class ScrapearBoletin:
    def __init__(self):
        self.api_url = "https://www.boletinoficial.gob.ar/v2/normas/secciones/primera"
        
        # Headers "mágicos" para simular Chrome y evitar bloqueos
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'application/json, text/plain, */*',
            'Referer': 'https://www.boletinoficial.gob.ar/seccion/primera',
            'X-Requested-With': 'XMLHttpRequest', # CLAVE: Indica que es una petición interna de la web
            'Connection': 'keep-alive'
        }
        self.session = requests.Session()
        self.session.verify = False

    def obtener_texto_completo(self, id_aviso, fecha_norma):
        url_detalle = f"https://www.boletinoficial.gob.ar/detalleAviso/primera/{id_aviso}/{fecha_norma}"
        try:
            # --- PAUSA DE SEGURIDAD (WAIT) ---
            # Esperamos entre 0.5 y 1.5 segundos entre cada lectura de texto.
            # Esto evita saturar el servidor y reduce riesgo de bloqueo.
            time.sleep(random.uniform(0.5, 1.5))
            
            response = self.session.get(url_detalle, headers=self.headers, timeout=10)
            
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')
                contenido = soup.find('div', {'id': 'avisodetalle'}) or soup.find('div', class_='detalle-aviso')
                
                if contenido:
                    texto = contenido.get_text(separator='\n', strip=True)
                    texto = texto.replace("Boletín Oficial de la República Argentina", "")
                    return texto[:12000] # Limitamos caracteres para la IA
            return ""
        except:
            return ""

    def scrape(self):
        print(">>> Iniciando scraping Boletín Oficial (Modo Lento y Seguro)...")
        
        try:
            # 1. Visita previa a la home para cargar cookies (importante anti-bloqueo)
            try:
                self.session.get("https://www.boletinoficial.gob.ar/", headers=self.headers, timeout=10)
            except:
                pass # Si falla la home, intentamos seguir igual con la API

            # 2. Definir fecha
            fecha_hoy = datetime.now().strftime("%Y%m%d")
            # SI ESTÁS PROBANDO EL ESCENARIO DEL PDF, DESCOMENTA ESTO:
            # fecha_hoy = "20260108" 
            
            params = {'fecha': fecha_hoy}
            
            print(f"   📡 Consultando API para: {fecha_hoy}...")
            response = self.session.get(self.api_url, headers=self.headers, params=params, timeout=15)

            if response.status_code != 200:
                print(f"⚠️ API Error {response.status_code}. Saltando.")
                return pd.DataFrame()

            try:
                data_json = response.json()
            except:
                print("⚠️ API devolvió HTML (Bloqueo). Saltando.")
                return pd.DataFrame()

            lista_normas = data_json.get('data', [])
            
            if not lista_normas:
                print("📭 No hay normas hoy.")
                return pd.DataFrame()

            print(f"   ✅ Encontradas {len(lista_normas)} normas. Descargando textos (tardará un poco)...")
            
            datos_procesados = []

            for i, item in enumerate(lista_normas):
                titulo_corto = item.get('detalle', 'Sin título')
                id_norma = item.get('idAviso')
                organismo = item.get('organismo', 'Poder Ejecutivo')
                
                # Armar referencia
                tipo = item.get('tipoNorma', '')
                numero = item.get('numeroNorma', '')
                anio = item.get('anioNorma', '')
                ref = f"{tipo} {numero}/{anio}".strip()
                
                # Feedback visual de progreso
                print(f"      [{i+1}/{len(lista_normas)}] Leyendo {ref}...", end="\r")

                # Intentamos obtener el texto completo
                texto_full = self.obtener_texto_completo(id_norma, fecha_hoy)
                
                if texto_full:
                    contenido_ia = f"TITULO: {titulo_corto}\n\n--- TEXTO OFICIAL ---\n{texto_full}"
                else:
                    contenido_ia = f"TITULO: {titulo_corto}\n(Texto completo no disponible)"

                link_web = f"https://www.boletinoficial.gob.ar/detalleAviso/primera/{id_norma}/{fecha_hoy}"

                datos_procesados.append({
                    "ID": f"BO{id_norma}",
                    "Origen": "Boletin Oficial",
                    "Expediente": ref,
                    "Autor": organismo,
                    "Fecha de inicio": datetime.now().strftime("%d/%m/%Y"),
                    "Proyecto": contenido_ia, # Texto completo para la IA
                    "Comisiones": link_web,   # Link visible en Excel
                    # ELIMINADOS: "Partido Político" y "Provincia"
                })

            print(f"\n   ✨ Finalizado: {len(datos_procesados)} normas procesadas.")
            return pd.DataFrame(datos_procesados)

        except Exception as e:
            print(f"❌ Error en BO: {e}")
            return pd.DataFrame()

if __name__ == "__main__":
    s = ScrapearBoletin()
    print(s.scrape())
