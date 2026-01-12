import cloudscraper
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import time
import json
import re

class ScrapearBoletin:
    def __init__(self):
        self.base_url = "https://www.boletinoficial.gob.ar"
        self.api_url = "https://www.boletinoficial.gob.ar/v2/normas/secciones/primera"
        
        # Cloudscraper crea un navegador falso que resuelve los desafíos JS/Cloudflare
        self.scraper = cloudscraper.create_scraper(
            browser={
                'browser': 'chrome',
                'platform': 'windows',
                'desktop': True
            }
        )

    def obtener_texto_completo(self, id_aviso, fecha_norma):
        # Intentamos acceder a la vista web directa
        url_detalle = f"https://www.boletinoficial.gob.ar/detalleAviso/primera/{id_aviso}/{fecha_norma}"
        try:
            time.sleep(0.5) # Pausa para no ser agresivos
            response = self.scraper.get(url_detalle, timeout=15)
            
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # Buscamos el contenedor del texto
                contenido = soup.find('div', {'id': 'avisodetalle'}) or soup.find('div', class_='detalle-aviso')
                
                if contenido:
                    texto = contenido.get_text(separator='\n', strip=True)
                    # Limpieza básica
                    texto = texto.replace("Boletín Oficial de la República Argentina", "")
                    texto = re.sub(r'Referencia:.*', '', texto) 
                    return texto[:15000]
            return ""
        except Exception as e:
            print(f"      ⚠️ No se pudo leer texto de {id_aviso}: {e}")
            return ""

    def scrape(self):
        print(">>> Iniciando scraping Boletín Oficial (Modo Anti-Bloqueo)...")
        
        try:
            # 1. Definir fecha
            # IMPORTANTE: Si estás en una PC real en 2025/2026, usa una fecha PASADA válida para probar.
            # Si pones una fecha futura real, la API te dará lista vacía (pero no error 403).
            # Para tu prueba del "escenario", usa la fecha que mencionaste antes:
            fecha_target = "20260108" 
            # SI ESTO DEVUELVE 0 RESULTADOS EN TU PC REAL, CAMBIA A UNA FECHA REAL DE AYER (ej: "20241020")
            
            print(f"   📡 Buscando normas para fecha: {fecha_target}...")
            
            # Parametros de la API
            params = {'fecha': fecha_target}
            
            # Usamos el scraper en lugar de requests normal
            response = self.scraper.get(self.api_url, params=params, timeout=15)

            # Diagnóstico
            if response.status_code != 200:
                print(f"❌ Error HTTP {response.status_code}")
                # Si falla, imprimimos un trozo para ver qué devolvió
                print(f"   Respuesta: {response.text[:100]}...")
                return pd.DataFrame()

            try:
                data_json = response.json()
            except json.JSONDecodeError:
                print("❌ ERROR CRÍTICO: El sitio sigue devolviendo HTML.")
                print("   Intenta: pip install --upgrade cloudscraper")
                return pd.DataFrame()

            lista_normas = data_json.get('data', [])
            
            if not lista_normas:
                print(f"📭 La API respondió correctamente (JSON), pero no hay normas para el {fecha_target}.")
                print("   (Esto es normal si estás consultando una fecha futura en el servidor real).")
                return pd.DataFrame()

            print(f"   ✅ ¡Conexión exitosa! Encontradas {len(lista_normas)} normas.")
            datos_procesados = []

            for i, item in enumerate(lista_normas):
                print(f"      Procesando {i+1}/{len(lista_normas)}...", end="\r")
                
                id_norma = item.get('idAviso')
                titulo_corto = item.get('detalle', 'Sin título')
                organismo = item.get('organismo', 'Poder Ejecutivo')
                
                # Datos de referencia
                numero = item.get('numeroNorma', 'S/N')
                anio = item.get('anioNorma', '')
                tipo = item.get('tipoNorma', '')
                ref = f"{tipo} {numero}/{anio}"
                link_web = f"https://www.boletinoficial.gob.ar/detalleAviso/primera/{id_norma}/{fecha_target}"

                # 2. Descarga del texto completo con el mismo scraper
                texto_full = self.obtener_texto_completo(id_norma, fecha_target)
                
                # Armado del paquete para IA
                contenido_ia = f"TITULO: {titulo_corto}\n\n--- TEXTO OFICIAL ---\n{texto_full}"

                datos_procesados.append({
                    "ID": f"BO{id_norma}",
                    "Origen": "Boletin Oficial",
                    "Expediente": ref,
                    "Autor": organismo,
                    "Fecha de inicio": datetime.now().strftime("%d/%m/%Y"),
                    "Proyecto": contenido_ia,
                    "Comisiones": link_web,
                    "Partido Político": "Oficialismo",
                    "Provincia": "Nacional"
                })

            print(f"\n   ✨ Éxito: {len(datos_procesados)} normas descargadas.")
            return pd.DataFrame(datos_procesados)

        except Exception as e:
            print(f"❌ Error inesperado: {e}")
            return pd.DataFrame()

if __name__ == "__main__":
    s = ScrapearBoletin()
    print(s.scrape())
