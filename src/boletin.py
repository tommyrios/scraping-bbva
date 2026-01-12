import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import urllib3
import time
import random
import re

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class ScrapearBoletin:
    def __init__(self):
        self.base_url = "https://www.boletinoficial.gob.ar"
        self.fecha_target_str = "20260108" 
        
        if self.fecha_target_str:
            self.url_seccion = f"https://www.boletinoficial.gob.ar/seccion/primera/{self.fecha_target_str}"
        else:
            self.url_seccion = "https://www.boletinoficial.gob.ar/seccion/primera"

        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Referer': 'https://www.boletinoficial.gob.ar/',
            'Accept-Language': 'es-ES,es;q=0.9',
            'Upgrade-Insecure-Requests': '1'
        }
        self.session = requests.Session()
        self.session.verify = False

    def procesar_detalle_completo(self, link_detalle):
        """
        Entra al aviso y extrae: Texto, Organismo y Número de Norma real.
        """
        data = {
            "texto": "",
            "organismo": "Poder Ejecutivo", # Valor por defecto
            "norma": "S/N"
        }
        
        try:
            time.sleep(random.uniform(0.5, 1.5))
            response = self.session.get(link_detalle, headers=self.headers, timeout=15)
            response.encoding = 'utf-8'

            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # --- 1. EXTRAER METADATA (Organismo y Norma) ---
                header = soup.find('div', {'id': 'tituloDetalleAviso'})
                if header:
                    # El H1 suele ser el Organismo (ej: BANCO CENTRAL...)
                    h1 = header.find('h1')
                    if h1: data["organismo"] = h1.get_text(strip=True)
                    
                    # El H2 suele ser la Norma (ej: Comunicación "A" 1234)
                    h2 = header.find('h2')
                    if h2: data["norma"] = h2.get_text(strip=True)

                # --- 2. EXTRAER TEXTO ---
                contenido = soup.find('div', {'id': 'cuerpoDetalleAviso'}) or \
                            soup.find('div', {'id': 'avisodetalle'}) or \
                            soup.find('div', class_='detalle-aviso')

                if contenido:
                    for tag in contenido(['script', 'style']): tag.decompose()
                    
                    texto = contenido.get_text(separator='\n', strip=True)
                    texto = texto.replace("Boletín Oficial de la República Argentina", "")
                    texto = re.sub(r'e\.\s+\d{2}/\d{2}/.*', '', texto)
                    
                    data["texto"] = texto[:15000]
                    
            return data
            
        except Exception as e:
            return data # Devuelve defaults si falla

    def scrape(self):
        print(f">>> Iniciando scraping ({self.fecha_target_str or 'HOY'})...")
        
        try:
            response = self.session.get(self.url_seccion, headers=self.headers, timeout=15)
            if response.status_code != 200: return pd.DataFrame()

            soup = BeautifulSoup(response.text, 'html.parser')
            links_brutos = soup.find_all('a', href=re.compile(r'/detalleAviso/primera/'))
            
            unique_norms = []
            seen_ids = set()
            
            for a in links_brutos:
                href = a.get('href')
                match = re.search(r'/primera/(\d+)/', href)
                if match:
                    id_aviso = match.group(1)
                    if id_aviso in seen_ids: continue
                    seen_ids.add(id_aviso)
                    
                    # Título preliminar del listado
                    titulo_listado = a.get_text(" ", strip=True)
                    if len(titulo_listado) < 5:
                        parent = a.find_parent('div', class_='row')
                        if parent: titulo_listado = parent.get_text(" ", strip=True)

                    unique_norms.append({'id': id_aviso, 'href': href, 'titulo_listado': titulo_listado})

            if not unique_norms:
                print("📭 No hay normas.")
                return pd.DataFrame()

            print(f"   ✅ Procesando {len(unique_norms)} normas...")
            datos_procesados = []
            
            fecha_excel = datetime.now().strftime("%d/%m/%Y")
            if self.fecha_target_str:
                fecha_excel = datetime.strptime(self.fecha_target_str, "%Y%m%d").strftime("%d/%m/%Y")

            for i, item in enumerate(unique_norms):
                id_aviso = item['id']
                link_completo = self.base_url + item['href']
                
                print(f"      [{i+1}/{len(unique_norms)}] ID {id_aviso}...", end="\r")

                info_detalle = self.procesar_detalle_completo(link_completo)
                
                titulo_final = item['titulo_listado'].replace("Ver", "").strip()
                
                if info_detalle["texto"]:
                    contenido_ia = f"NORMA: {info_detalle['norma']}\nORGANISMO: {info_detalle['organismo']}\nTITULO: {titulo_final}\n\n--- TEXTO ---\n{info_detalle['texto']}"
                else:
                    contenido_ia = f"TITULO: {titulo_final}\n(Texto no disponible)"

                datos_procesados.append({
                    "ID": f"BO{id_aviso}",
                    "Origen": "Boletin Oficial",
                    "Expediente": info_detalle["norma"], 
                    "Autor": info_detalle["organismo"], 
                    "Fecha de inicio": fecha_excel,
                    "Proyecto": contenido_ia,
                    "Comisiones": link_completo
                })

            print(f"\n   ✨ {len(datos_procesados)} normas listas.")
            return pd.DataFrame(datos_procesados)

        except Exception as e:
            print(f"❌ Error: {e}")
            return pd.DataFrame()
