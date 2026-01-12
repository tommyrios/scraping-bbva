import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import urllib3
import time
import random
import re

# Desactivar advertencias de SSL (necesario para sitios del gobierno)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class ScrapearBoletin:
    def __init__(self):
        # Usamos la vista visual de la "Primera Sección" (Leyes, Decretos, Resoluciones)
        self.url_seccion = "https://www.boletinoficial.gob.ar/seccion/primera/20260108"
        self.base_url = "https://www.boletinoficial.gob.ar"
        
        # Headers para simular un navegador real y evitar bloqueos
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Referer': 'https://www.boletinoficial.gob.ar/',
            'Accept-Language': 'es-ES,es;q=0.9',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        }
        self.session = requests.Session()
        self.session.verify = False

    def obtener_texto_completo(self, link_detalle):
        """
        Entra al link del aviso, busca el contenedor correcto y limpia estilos CSS basura.
        """
        try:
            # Pausa aleatoria para comportamiento humano
            time.sleep(random.uniform(0.5, 1.5))
            
            response = self.session.get(link_detalle, headers=self.headers, timeout=15)
            response.encoding = 'utf-8' # Forzar UTF-8

            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # --- ESTRATEGIA DE BÚSQUEDA ---
                contenido = None
                
                # 1. Buscamos por el ID que mostraste en tu ejemplo HTML
                contenido = soup.find('div', {'id': 'cuerpoDetalleAviso'})
                
                # 2. Si no está, buscamos el ID clásico de otras secciones
                if not contenido: 
                    contenido = soup.find('div', {'id': 'avisodetalle'})
                
                # 3. Fallback genérico por clase
                if not contenido: 
                    contenido = soup.find('div', class_='detalle-aviso')

                if contenido:
                    # --- LIMPIEZA CRÍTICA ---
                    # Eliminamos etiquetas <style> y <script> para que no salga código CSS en el texto
                    for tag in contenido(['script', 'style']):
                        tag.decompose()

                    # Obtenemos texto limpio
                    texto = contenido.get_text(separator='\n', strip=True)
                    
                    # Limpiezas finales de "basura" común del BO
                    texto = texto.replace("Boletín Oficial de la República Argentina", "")
                    texto = re.sub(r'e\.\s+\d{2}/\d{2}/.*', '', texto) # Borra firma de fecha al final
                    
                    # Limitamos a 15k caracteres para no saturar a Gemini
                    return texto[:15000]
            
            return ""
        except Exception as e:
            # Si falla, devolvemos vacío para no romper el flujo principal
            # print(f"      ⚠️ Error leyendo texto: {e}") 
            return ""

    def scrape(self):
        print(">>> Iniciando scraping Boletín Oficial (Visual + Texto Completo)...")
        
        try:
            print("   📡 Accediendo a la portada...")
            response = self.session.get(self.url_seccion, headers=self.headers, timeout=15)
            
            if response.status_code != 200:
                print(f"⚠️ Error al acceder a la web: {response.status_code}")
                return pd.DataFrame()

            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Buscamos todos los links que lleven a un detalle de aviso en la primera sección
            links_brutos = soup.find_all('a', href=re.compile(r'/detalleAviso/primera/'))
            
            unique_norms = []
            seen_ids = set() # Set para controlar duplicados por ID numérico
            
            for a in links_brutos:
                href = a.get('href')
                
                # Extraemos el ID numérico del link (ej: .../337351/...)
                match = re.search(r'/primera/(\d+)/', href)
                
                if match:
                    id_aviso = match.group(1)
                    
                    # Si ya procesamos este ID, lo saltamos (evita duplicados de botones/títulos)
                    if id_aviso in seen_ids:
                        continue
                    
                    seen_ids.add(id_aviso)
                    
                    # Intentamos obtener el título del link
                    titulo = a.get_text(" ", strip=True)
                    
                    # Si el link es un botón "Ver" o vacío, buscamos el título en el contenedor padre
                    if len(titulo) < 5:
                        row_parent = a.find_parent('div', class_='row')
                        if row_parent:
                            titulo = row_parent.get_text(" ", strip=True)
                    
                    unique_norms.append({
                        'id': id_aviso,
                        'href': href,
                        'titulo': titulo
                    })

            if not unique_norms:
                print("📭 No se encontraron normas hoy en la portada.")
                return pd.DataFrame()

            print(f"   ✅ Se encontraron {len(unique_norms)} normas ÚNICAS. Descargando textos...")
            
            datos_procesados = []
            fecha_hoy_str = datetime.now().strftime("%d/%m/%Y")

            for i, item in enumerate(unique_norms):
                id_aviso = item['id']
                href = item['href']
                titulo_raw = item['titulo']
                
                link_completo = self.base_url + href
                
                # Feedback visual de progreso
                print(f"      [{i+1}/{len(unique_norms)}] Procesando ID {id_aviso}...", end="\r")

                # Descargamos el texto real
                texto_full = self.obtener_texto_completo(link_completo)
                
                # Limpieza del título
                titulo_limpio = titulo_raw.replace("Boletín Oficial", "").replace("Ver", "").replace("Descargar", "").strip()
                titulo_limpio = " ".join(titulo_limpio.split()) # Quita espacios dobles y saltos
                if len(titulo_limpio) > 300: titulo_limpio = titulo_limpio[:300] + "..."

                # Armamos el contenido para la IA
                if texto_full:
                    contenido_ia = f"TITULO: {titulo_limpio}\n\n--- TEXTO OFICIAL ---\n{texto_full}"
                else:
                    contenido_ia = f"TITULO: {titulo_limpio}\n(Texto completo no disponible - Solo Título)"

                datos_procesados.append({
                    "ID": f"BO{id_aviso}",
                    "Origen": "Boletin Oficial",
                    "Expediente": f"Norma {id_aviso}",
                    "Autor": "Poder Ejecutivo",
                    "Fecha de inicio": fecha_hoy_str,
                    "Proyecto": contenido_ia,     # Esto va a la IA
                    "Comisiones": link_completo,  # Esto va al Excel (Link)
                    # No incluimos Partido ni Provincia
                })

            print(f"\n   ✨ Finalizado: {len(datos_procesados)} normas procesadas.")
            return pd.DataFrame(datos_procesados)

        except Exception as e:
            print(f"❌ Error crítico en BO: {e}")
            return pd.DataFrame()

if __name__ == "__main__":
    # Test rápido al ejecutar el archivo
    s = ScrapearBoletin()
    df = s.scrape()
    print(df.head())
