import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import urllib3
import time
import json
import re

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class ScrapearBoletin:
    def __init__(self):
        self.base_url = "https://www.boletinoficial.gob.ar"
        self.api_url = "https://www.boletinoficial.gob.ar/v2/normas/secciones/primera"
        
        # Headers copiados de una navegación real en Chrome
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'es-ES,es;q=0.9',
            'Referer': 'https://www.boletinoficial.gob.ar/seccion/primera',
            'Connection': 'keep-alive',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin',
        }
        
        self.session = requests.Session()
        self.session.headers.update(self.headers)
        self.session.verify = False

    def obtener_texto_completo(self, id_aviso, fecha_norma):
        # URL de la web visual (a veces carga mejor que la API de detalle)
        url_detalle = f"https://www.boletinoficial.gob.ar/detalleAviso/primera/{id_aviso}/{fecha_norma}"
        try:
            time.sleep(0.5) # Pausa respetuosa
            response = self.session.get(url_detalle, timeout=10)
            
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # Buscamos el div específico del texto
                contenido = soup.find('div', {'id': 'avisodetalle'}) or soup.find('div', class_='detalle-aviso')
                
                if contenido:
                    texto = contenido.get_text(separator='\n', strip=True)
                    # Limpieza de basura común del BO
                    texto = texto.replace("Boletín Oficial de la República Argentina", "")
                    texto = re.sub(r'Referencia:.*', '', texto) # Limpiar metadatos al final
                    return texto[:15000]
            return ""
        except:
            return ""

    def scrape(self):
        print(">>> Iniciando scraping Boletín Oficial...")
        
        try:
            # 1. VISITA DE "CALENTAMIENTO" (Simular usuario entrando a la home)
            print("   🍪 Generando sesión válida...")
            self.session.get("https://www.boletinoficial.gob.ar/", timeout=10)
            time.sleep(1)
            
            # 2. DEFINIR FECHA
            # IMPORTANTE: Usamos la fecha del 08/01/2026 para asegurar que hay datos y probar el script.
            # Si quieres volver a "HOY", cambia esto a: datetime.now().strftime("%Y%m%d")
            fecha_target = "20260108" 
            
            print(f"   📡 Buscando normas del día: {fecha_target}...")
            
            params = {'fecha': fecha_target}
            response = self.session.get(self.api_url, params=params, timeout=15)

            # 3. VERIFICACIÓN Y EXTRACCIÓN
            if response.status_code != 200:
                print(f"❌ Error HTTP {response.status_code}")
                return pd.DataFrame()

            # Intentamos leer JSON. Si falla, el bloqueo persiste.
            try:
                data_json = response.json()
            except json.JSONDecodeError:
                print("❌ ERROR DE BLOQUEO: El sitio devolvió HTML (Anti-bot activado).")
                return pd.DataFrame()

            lista_normas = data_json.get('data', [])
            
            if not lista_normas:
                print(f"📭 No hay normas cargadas para la fecha {fecha_target}.")
                return pd.DataFrame()

            print(f"   ✅ Se encontraron {len(lista_normas)} normas. Descargando textos...")
            datos_procesados = []

            for i, item in enumerate(lista_normas):
                # Feedback de progreso visual
                print(f"      Processando {i+1}/{len(lista_normas)}...", end="\r")
                
                id_norma = item.get('idAviso')
                titulo_corto = item.get('detalle', 'Sin título')
                organismo = item.get('organismo', 'Poder Ejecutivo')
                
                # Armamos referencia y link
                numero = item.get('numeroNorma', 'S/N')
                anio = item.get('anioNorma', '')
                tipo = item.get('tipoNorma', '')
                ref = f"{tipo} {numero}/{anio}"
                link_web = f"https://www.boletinoficial.gob.ar/detalleAviso/primera/{id_norma}/{fecha_target}"

                # 4. DESCARGA DEL TEXTO REAL
                texto_full = self.obtener_texto_completo(id_norma, fecha_target)
                
                # Preparamos el paquete para la IA
                contenido_ia = f"TITULO: {titulo_corto}\n\n--- TEXTO OFICIAL ---\n{texto_full}"

                datos_procesados.append({
                    "ID": f"BO{id_norma}",
                    "Origen": "Boletin Oficial",
                    "Expediente": ref,
                    "Autor": organismo,
                    "Fecha de inicio": datetime.now().strftime("%d/%m/%Y"), # Fecha de proceso
                    "Proyecto": contenido_ia, # Texto completo para Analisis.py
                    "Comisiones": link_web,   # Link para el usuario
                })

            print(f"\n   ✨ Éxito: {len(datos_procesados)} normas descargadas.")
            return pd.DataFrame(datos_procesados)

        except Exception as e:
            print(f"❌ Error crítico BO: {e}")
            return pd.DataFrame()

if __name__ == "__main__":
    # Test rápido al ejecutar el archivo
    s = ScrapearBoletin()
    df = s.scrape()
    print(df.head())
