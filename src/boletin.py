import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import urllib3
import time

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class ScrapearBoletin:
    def __init__(self):
        self.base_url = "https://www.boletinoficial.gob.ar"
        self.api_url = "https://www.boletinoficial.gob.ar/v2/normas/secciones/primera" 
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'application/json, text/plain, */*',
            'Referer': 'https://www.boletinoficial.gob.ar/seccion/primera'
        }

    def obtener_texto_completo(self, id_aviso):
        url_detalle = f"https://www.boletinoficial.gob.ar/detalleAviso/primera/{id_aviso}"
        try:
            # Pausa técnica para estabilidad
            time.sleep(0.3)
            response = requests.get(url_detalle, headers=self.headers, verify=False, timeout=10)
            
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')
                contenido = soup.find('div', {'id': 'avisodetalle'})
                if not contenido:
                    contenido = soup.find('div', class_='detalle-aviso')
                
                if contenido:
                    texto = contenido.get_text(separator='\n', strip=True)
                    texto = texto.replace("Boletín Oficial de la República Argentina", "")
                    return texto[:15000] # Limite generoso para asegurar nombres
            return ""
        except:
            return ""

    def scrape(self):
        print(">>> Iniciando scraping Boletín Oficial (con texto completo)...")
        try:
            fecha_hoy = datetime.now().strftime("%Y%m%d")
            params = {'fecha': fecha_hoy}
            
            response = requests.get(self.api_url, headers=self.headers, params=params, verify=False, timeout=15)
            if response.status_code != 200: return pd.DataFrame()

            data_json = response.json()
            lista_normas = data_json.get('data', [])
            
            if not lista_normas:
                print("📭 Sin normas hoy.")
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
                
                # Combinamos para la IA, pero mantenemos estructura
                contenido_ia = f"TITULO: {titulo_corto}\n\n--- TEXTO OFICIAL ---\n{texto_full}"

                datos_procesados.append({
                    "ID": f"BO{id_norma}",
                    "Origen": "Boletin Oficial",
                    "Expediente": ref,
                    "Autor": organismo,
                    "Fecha de inicio": datetime.now().strftime("%d/%m/%Y"),
                    "Proyecto": contenido_ia, # Esto lee la IA
                    "Comisiones": link,
                    "Partido Político": "Oficialismo",
                    "Provincia": "Nacional"
                })

            return pd.DataFrame(datos_procesados)

        except Exception as e:
            print(f"❌ Error BO: {e}")
            return pd.DataFrame()