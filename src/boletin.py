import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import urllib3
import time
import random
import re

# Desactivar advertencias de SSL
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class ScrapearBoletin:
    def __init__(self):
        # En lugar de la API, vamos a la página visual que ven los humanos
        self.url_seccion = "https://www.boletinoficial.gob.ar/seccion/primera"
        self.base_url = "https://www.boletinoficial.gob.ar"
        
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Referer': 'https://www.boletinoficial.gob.ar/',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        }
        self.session = requests.Session()
        self.session.verify = False

    def obtener_texto_completo(self, link_detalle):
        try:
            # Pausa de seguridad aleatoria
            time.sleep(random.uniform(0.5, 1.5))
            
            response = self.session.get(link_detalle, headers=self.headers, timeout=10)
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')
                # Buscamos el div del texto
                contenido = soup.find('div', {'id': 'avisodetalle'}) or soup.find('div', class_='detalle-aviso')
                
                if contenido:
                    texto = contenido.get_text(separator='\n', strip=True)
                    texto = texto.replace("Boletín Oficial de la República Argentina", "")
                    # Limpiamos referencias finales que ensucian
                    texto = re.sub(r'e\.\s+\d{2}/\d{2}/.*', '', texto)
                    return texto[:12000]
            return ""
        except:
            return ""

    def scrape(self):
        print(">>> Iniciando scraping Boletín Oficial (Método Visual)...")
        
        try:
            # 1. Obtenemos la página HTML de la lista (como un usuario normal)
            print("   📡 Accediendo a la portada de Primera Sección...")
            response = self.session.get(self.url_seccion, headers=self.headers, timeout=15)
            
            if response.status_code != 200:
                print(f"⚠️ Error al acceder a la web: {response.status_code}")
                return pd.DataFrame()

            # 2. Analizamos el HTML para encontrar los links de las normas
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Buscamos todos los links que lleven a un detalle de aviso
            # Los links suelen ser: <a href="/detalleAviso/primera/123456/20260112">...</a>
            links_normas = soup.find_all('a', href=re.compile(r'/detalleAviso/primera/'))
            
            # Filtramos duplicados (el sitio a veces pone el mismo link en el título y en el botón)
            urls_unicas = []
            seen = set()
            for a in links_normas:
                href = a.get('href')
                if href and href not in seen:
                    seen.add(href)
                    # A veces el título está dentro del <a> o en un div hijo
                    titulo = a.get_text(" ", strip=True)
                    # Si el título es muy corto (ej: "Ver más"), buscamos el contexto
                    if len(titulo) < 5:
                         # Intentamos buscar un h6 o p cercano
                         parent = a.find_parent('div')
                         if parent: titulo = parent.get_text(" ", strip=True)
                    
                    urls_unicas.append((href, titulo))

            if not urls_unicas:
                print("📭 No se encontraron normas en la portada hoy (¿Es feriado o fin de semana?).")
                return pd.DataFrame()

            print(f"   ✅ Se encontraron {len(urls_unicas)} normas visibles. Procesando...")
            
            datos_procesados = []
            fecha_hoy_str = datetime.now().strftime("%d/%m/%Y")

            for i, (href_relativo, titulo_raw) in enumerate(urls_unicas):
                link_completo = self.base_url + href_relativo
                
                # Extraemos ID del link (ej: .../primera/315123/2025...)
                try:
                    parts = href_relativo.split('/')
                    id_aviso = parts[3] 
                except:
                    id_aviso = f"desc_{i}"

                print(f"      [{i+1}/{len(urls_unicas)}] Leyendo norma {id_aviso}...", end="\r")

                # Descargamos el texto
                texto_full = self.obtener_texto_completo(link_completo)
                
                # Limpieza del título (a veces trae basura del HTML)
                titulo_limpio = titulo_raw.replace("Boletín Oficial", "").strip()
                if len(titulo_limpio) > 300: titulo_limpio = titulo_limpio[:300] + "..."

                if texto_full:
                    contenido_ia = f"TITULO: {titulo_limpio}\n\n--- TEXTO OFICIAL ---\n{texto_full}"
                else:
                    contenido_ia = f"TITULO: {titulo_limpio}\n(Texto completo no disponible - Solo Título)"

                datos_procesados.append({
                    "ID": f"BO{id_aviso}",
                    "Origen": "Boletin Oficial",
                    "Expediente": f"Norma {id_aviso}", # Referencia genérica si no parseamos numero
                    "Autor": "Poder Ejecutivo",
                    "Fecha de inicio": fecha_hoy_str,
                    "Proyecto": contenido_ia,
                    "Comisiones": link_completo,
                    # Columnas eliminadas: Partido, Provincia
                })

            print(f"\n   ✨ Finalizado: {len(datos_procesados)} normas procesadas.")
            return pd.DataFrame(datos_procesados)

        except Exception as e:
            print(f"❌ Error crítico en BO: {e}")
            return pd.DataFrame()

if __name__ == "__main__":
    s = ScrapearBoletin()
    print(s.scrape())
