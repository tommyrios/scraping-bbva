import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import urllib3
import time
import json

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class ScrapearBoletin:
    def __init__(self):
        self.base_url = "https://www.boletinoficial.gob.ar"
        self.api_url = "https://www.boletinoficial.gob.ar/v2/normas/secciones/primera" 
        # Headers completos para parecer un navegador real y evitar el error JSON
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'es-ES,es;q=0.9,en;q=0.8',
            'Referer': 'https://www.boletinoficial.gob.ar/seccion/primera/20260108',
            'Origin': 'https://www.boletinoficial.gob.ar',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin',
            'X-Requested-With': 'XMLHttpRequest'
        }

    def obtener_texto_completo(self, id_aviso):
        url_detalle = f"https://www.boletinoficial.gob.ar/detalleAviso/primera/{id_aviso}"
        try:
            time.sleep(0.3)
            response = requests.get(url_detalle, headers=self.headers, verify=False, timeout=10)
            
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')
                contenido = soup.find('div', {'id': 'avisodetalle'}) or soup.find('div', class_='detalle-aviso')
                
                if contenido:
                    texto = contenido.get_text(separator='\n', strip=True)
                    texto = texto.replace("Boletín Oficial de la República Argentina", "")
                    return texto[:15000]
            return ""
        except:
            return ""

    def scrape(self):
        print(">>> Iniciando scraping Boletín Oficial (con texto completo)...")
        try:
            fecha_hoy = datetime.now().strftime("%Y%m%d")
            params = {'fecha': fecha_hoy}
            
            # Petición a la API
            response = requests.get(self.api_url, headers=self.headers, params=params, verify=False, timeout=15)
            
            # Diagnóstico de error si no es 200
            if response.status_code != 200:
                print(f"❌ Error HTTP {response.status_code}: {response.text[:100]}")
                return pd.DataFrame()

            # Intentamos parsear JSON con manejo de error específico
            try:
                data_json = response.json()
            except json.JSONDecodeError:
                print("❌ Error: La web devolvió HTML en lugar de JSON (Posible bloqueo o mantenimiento).")
                # Imprimimos un poco del texto para ver qué devolvió realmente
                print(f"   Respuesta recibida: {response.text[:200]}...")
                return pd.DataFrame()

            lista_normas = data_json.get('data', [])
            
            if not lista_normas:
                print("📭 Sin normas publicadas hoy en la API.")
                return pd.DataFrame()

            datos_procesados = []
            print(f"   🔍 Procesando {len(lista_normas)} normas...")

            for item in lista_normas:
                id_norma = item.get('idAviso')
                numero = item.get('numeroNorma', 'S/N')
                anio = item.get('anioNorma', '')
                tipo = item.get('tipoNorma', '')
                organismo = item.get('organismo', 'Poder Ejecutivo')
                titulo_corto = item.get('detalle', '')
                
                ref = f"{tipo} {numero}/{anio}"
                link = f"https://www.boletinoficial.gob.ar/detalleAviso/primera/{id_norma}/{fecha_hoy}"

                # Descarga del texto real
                texto_full = self.obtener_texto_completo(id_norma)
                
                # Combinamos para la IA
                contenido_ia = f"TITULO: {titulo_corto}\n\n--- TEXTO OFICIAL ---\n{texto_full}"

                datos_procesados.append({
                    "ID": f"BO{id_norma}",
                    "Origen": "Boletin Oficial",
                    "Expediente": ref,
                    "Autor": organismo,
                    "Fecha de inicio": datetime.now().strftime("%d/%m/%Y"),
                    "Proyecto": contenido_ia,
                    "Comisiones": link, # Link para el usuario
                })

            return pd.DataFrame(datos_procesados)

        except Exception as e:
            print(f"❌ Error crítico BO: {e}")
            return pd.DataFrame()