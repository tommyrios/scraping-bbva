import time
import traceback
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select # Importante para el dropdown
from bs4 import BeautifulSoup

class ScrapearSenado:

    def __init__(self):
        print("Inicializando robot Senado...")
        options = Options()
        
        # --- CONFIGURACIÓN HEADLESS ---
        options.add_argument('--headless') 
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        # ------------------------------
        
        options.add_argument('--window-size=1920,1080')
        options.add_argument('--ignore-certificate-errors')
        options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36")

        self.driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=options)
        self.data = []

    def limpiar_texto(self, texto):
        if not texto: return "S/D"
        # Elimina espacios dobles y saltos de línea
        return " ".join(texto.split())

    def extraer_detalle_proyecto(self, url):
        """Entra a la ficha para sacar: Primer Autor, Fecha, Comisiones y Título completo"""
        try:
            self.driver.get(url)
            # Esperamos que cargue el título principal
            WebDriverWait(self.driver, 15).until(
                EC.presence_of_element_located((By.TAG_NAME, "h1"))
            )
            
            soup = BeautifulSoup(self.driver.page_source, 'html.parser')

            # 1. PROYECTO (TÍTULO / EXTRACTO)
            proyecto_texto = "S/D"
            try:
                tabla_encabezado = soup.find('table', class_='table-bordered')
                if tabla_encabezado:
                    filas = tabla_encabezado.find('tbody').find_all('tr')
                    for f in filas:
                        cols = f.find_all('td')
                        if len(cols) >= 4:
                            proyecto_texto = self.limpiar_texto(cols[3].text)
            except: pass

            # 2. AUTOR (SOLO EL PRIMERO / FIRMANTE)
            autor_principal = "S/D"
            try:
                div_autores = soup.find('div', id='Autores')
                if div_autores:
                    # Usamos .find() en lugar de .find_all() para traer solo el primero
                    link_autor = div_autores.find('a', href=True)
                    if link_autor:
                        autor_principal = self.limpiar_texto(link_autor.text)
            except: pass

            # 3. FECHA DE INGRESO
            fecha = "S/D"
            try:
                div_tramite = soup.find('div', id='tramiteLegislativo')
                if div_tramite:
                    tabla_fechas = div_tramite.find('table', attrs={'summary': 'Fechas en Mesa de Entradas'})
                    if tabla_fechas:
                        fecha = self.limpiar_texto(tabla_fechas.find('tbody').find('tr').find_all('td')[0].text)
            except: pass

            # 4. COMISIONES (GIROS)
            lista_comisiones = []
            try:
                div_tramite = soup.find('div', id='tramiteLegislativo')
                if div_tramite:
                    tabla_giros = div_tramite.find('table', attrs={'summary': 'Giros del Expediente a Comisiones'})
                    if tabla_giros:
                        filas_giros = tabla_giros.find('tbody').find_all('tr')
                        for fila in filas_giros:
                            cols = fila.find_all('td')
                            if cols:
                                texto_raw = cols[0].get_text(separator=' ', strip=True)
                                nombre_comision = texto_raw.split('ORDEN DE GIRO')[0].strip()
                                lista_comisiones.append(nombre_comision)
            except: pass

            comisiones_final = ", ".join(lista_comisiones) if lista_comisiones else "Sin giros"

            return {
                'Proyecto': proyecto_texto,
                'Autor': autor_principal,
                'Fecha de inicio': fecha,
                'Comisiones': comisiones_final
            }

        except Exception:
            return None

    def scrape(self):
        # URL Directa a búsqueda avanzada
        url_avanzada = "https://www.senado.gob.ar/parlamentario/parlamentaria/avanzada"
        print(f"Entrando a {url_avanzada}")
        
        try:
            self.driver.get(url_avanzada)
            wait = WebDriverWait(self.driver, 30)

            # --- LÓGICA DE PAGINACIÓN (100 REGISTROS) ---
            print("Configurando filtro a 100 resultados...")
            try:
                # Buscamos el selector <select name="cantRegistros">
                select_element = wait.until(EC.presence_of_element_located((By.NAME, "cantRegistros")))
                select = Select(select_element)
                select.select_by_value("100")
                
                # Como tiene onchange="submit()", la página se recarga sola.
                # Esperamos que la tabla se vuelva "stale" (desaparezca) o que reaparezca.
                print("Esperando recarga de tabla tras selección...")
                time.sleep(4) 
                wait.until(EC.presence_of_element_located((By.TAG_NAME, "table")))
                
            except Exception as e:
                print(f"⚠️ No se pudo cambiar a 100 registros (se usará el default): {e}")

            # --- OBTENCIÓN DE LINKS Y ARMADO DE ID ---
            soup = BeautifulSoup(self.driver.page_source, 'html.parser')
            filas = soup.find_all('tr')
            
            items_a_procesar = []

            print(f"Analizando {len(filas)} filas encontradas...")

            for fila in filas:
                cols = fila.find_all('td')
                # Estructura esperada en listado:
                # Col 0: <a href="LINK">NUMERO (2042/25)</a>
                # Col 1: TIPO (PD)
                
                if len(cols) >= 2:
                    enlace = cols[0].find('a', href=True)
                    if enlace and 'verExp' in enlace['href']:
                        url_completa = f"https://www.senado.gob.ar{enlace['href']}"
                        
                        # --- LÓGICA DE FORMATEO DE ID (2042-PD-25) ---
                        texto_numero = self.limpiar_texto(enlace.text) # Ej: "2042/25"
                        texto_tipo = self.limpiar_texto(cols[1].text)  # Ej: "PD"
                        
                        id_formateado = texto_numero # Fallback por si falla el split
                        try:
                            if "/" in texto_numero:
                                partes = texto_numero.split('/')
                                numero = partes[0]
                                anio = partes[1]
                                # Resultado: 2042-PD-25
                                id_formateado = f"{numero}-{texto_tipo}-{anio}"
                        except:
                            pass
                        
                        # Guardamos la info preliminar para procesar luego
                        items_a_procesar.append({
                            'url': url_completa,
                            'id': id_formateado
                        })

            # Eliminamos duplicados por URL
            # (Usamos un diccionario auxiliar para filtrar)
            items_unicos = {item['url']: item for item in items_a_procesar}.values()
            
            print(f"✅ Se encontraron {len(items_unicos)} proyectos únicos. Iniciando extracción profunda...")

        except Exception:
            print("❌ Error CRÍTICO obteniendo lista de proyectos.")
            print(traceback.format_exc())
            self.driver.quit()
            return pd.DataFrame()

        # --- RECORRIDO DE PROYECTOS ---
        for i, item in enumerate(items_unicos): 
            print(f"[{i+1}/{len(items_unicos)}] Procesando {item['id']}...")
            
            info_detalle = self.extraer_detalle_proyecto(item['url'])
            
            if info_detalle:
                self.data.append({
                    'Cámara de Origen': 'Senado',
                    'Expediente': item['id'], # Usamos el ID formateado que calculamos arriba
                    'Autor': info_detalle['Autor'],
                    'Fecha de inicio': info_detalle['Fecha de inicio'],
                    'Proyecto': info_detalle['Proyecto'],
                    'Comisiones': info_detalle['Comisiones'],
                    'Estado': '',
                    'Probabilidad': '',
                    'Partido Político': '',
                    'Provincia': '',
                    'Observaciones': ''
                })
                time.sleep(1)

        self.driver.quit()
        return pd.DataFrame(self.data)
