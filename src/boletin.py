import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import urllib3
import time
import json

# Desactivar advertencias de certificado SSL
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class ScrapearBoletin:
    def __init__(self):
        self.base_url = "https://www.boletinoficial.gob.ar"
        self.api_url = "https://www.boletinoficial.gob.ar/v2/normas/secciones/primera" 
        
        # Headers estándar de Chrome
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'application/json, text/plain, */*',
            'Referer': 'https://www.boletinoficial.gob.ar/seccion/primera/20260108',
        }
        
        # CREAMOS UNA SESIÓN (Esto guarda las cookies automáticamente)
        self.session = requests.Session()
        self.session.headers.update(self.headers)
        self.session.verify = False # Ignorar SSL en toda la sesión

    def inicializar_cookies(self):
        """
        Visita la portada para obtener las cookies de seguridad antes de llamar a la API.
        """
        try:
            print("   🍪 Obteniendo cookies de sesión...")
            # Visitamos la home primero
            self.session.get(self.base_url, timeout=10)
            # Visitamos la sección primera para "calentar" la navegación
            self.session.get(f"{self.base_url}/seccion/primera", timeout=10)
            return True
        except Exception as e:
            print(f"⚠️ Error inicializando cookies: {e}")
            return False

    def obtener_texto_completo(self, id_aviso):
        url_detalle = f"https://www.boletinoficial.gob.ar/detalleAviso/primera/{id_aviso}"
        try:
            # Usamos self.session en lugar de requests directo
            response = self.session.get(url_detalle, timeout=10)
            
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
        print(">>> Iniciando scraping Boletín Oficial...")
        
        # 1. Inicializamos la sesión (CLAVE para evitar bloqueo)
        self.inicializar_cookies()
        
        try:
            fecha_hoy = datetime.now().strftime("%Y%m%d")
            params = {'fecha': fecha_hoy}
            
            print(f"   📡 Consultando API para fecha: {fecha_hoy}...")
            # Usamos la sesión con las cookies ya cargadas
            response = self.session.get(self.api_url, params=params, timeout=15)
            
            if response.status_code != 200:
                print(f"❌ Error HTTP {response.status_code}")
                return pd.DataFrame()

            try:
                data_json = response.json()
            except json.JSONDecodeError:
                print("❌ Bloqueo persistente: La web sigue devolviendo HTML.")
                return pd.DataFrame()

            lista_normas = data_json.get('data', [])
            
            if not lista_normas:
                print("📭 No se encontraron normas hoy.")
                return pd.DataFrame()

            datos_procesados = []
            print(f"   🔍 Descargando textos de {len(lista_normas)} normas...")

            for item in lista_normas:
                id_norma = item.get('idAviso')
                numero = item.get('numeroNorma', 'S/N')
                anio = item.get('anioNorma', '')
                tipo = item.get('tipoNorma', '')
                organismo = item.get('organismo', 'Poder Ejecutivo')
                titulo_corto = item.get('detalle', '')
                
                ref = f"{tipo} {numero}/{anio}"
                link = f"https://www.boletinoficial.gob.ar/detalleAviso/primera/{id_norma}/{fecha_hoy}"

                # Descargamos texto completo usando la misma sesión
                texto_full = self.obtener_texto_completo(id_norma)
                
                contenido_ia = f"TITULO: {titulo_corto}\n\n--- TEXTO OFICIAL ---\n{texto_full}"

                datos_procesados.append({
                    "ID": f"BO{id_norma}",
                    "Origen": "Boletin Oficial",
                    "Expediente": ref,
                    "Autor": organismo,
                    "Fecha de inicio": datetime.now().strftime("%d/%m/%Y"),
                    "Proyecto": contenido_ia,
                    "Comisiones": link,
                    "Partido Político": "Oficialismo",
                    "Provincia": "Nacional"
                })

            return pd.DataFrame(datos_procesados)

        except Exception as e:
            print(f"❌ Error crítico: {e}")
            return pd.DataFrame()

if __name__ == "__main__":
    s = ScrapearBoletin()
    print(s.scrape())
