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
from selenium.webdriver.support.ui import Select
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
        return " ".join(texto.split())

    def extraer_detalle_proyecto(self, url):
        """Entra a la ficha para sacar: Primer Autor, Fecha, Comisiones y Título completo"""
        try:
            self.driver.get(url)
            WebDriverWait(self.driver, 15).until(
                EC.presence_of_element_located((By.TAG_NAME, "h1"))
            )
            
            soup = BeautifulSoup(self.driver.page_source, 'html.parser')

            # 1. PROYECTO (TÍTULO)
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
                    # .find() devuelve solo el primer elemento que encuentra
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
        # Volvemos a la URL raíz que sabemos que funciona
        url_inicio = "https://www.senado.gob.ar/parlamentario/parlamentaria/"
        print(f"Entrando a {url_inicio}")
        
        try:
            self.driver.get(url_inicio)
            wait = WebDriverWait(self.driver, 30)

            # --- PASO 1: REALIZAR BÚSQUEDA INICIAL ---
            print("1. Desplegando búsqueda avanzada...")
            boton_avanzada = wait.until(EC.element_to_be_clickable((By.XPATH, "//a[contains(@href, '#collapse113')]")))
            self.driver.execute_script("arguments[0].click();", boton_avanzada)
            time.sleep(1)

            print("2. Enviando formulario vacío (Traer todo)...")
            formulario = wait.until(EC.presence_of_element_located((By.NAME, "ingreso2")))
            formulario.submit()

            print("3. Esperando resultados iniciales...")
            wait.until(EC.presence_of_element_located((By.TAG_NAME, "table")))

            # --- PASO 2: CAMBIAR A 100 RESULTADOS ---
            # Ahora sí estamos en la pantalla de resultados, así que el dropdown existe
            print("4. Aplicando filtro de 100 resultados...")
            try:
                select_element = wait.until(EC.presence_of_element_located((By.NAME, "cantRegistros")))
                select = Select(select_element)
                select.select_by_value("100")
                
                print("   Esperando recarga de tabla...")
                # Esperamos a que la tabla vieja desaparezca o pase un tiempo seguro
                time.sleep(5) 
                wait.until(EC.presence_of_element_located((By.TAG_NAME, "table")))
            except Exception as e:
                print(f"⚠️ No se pudo cambiar a 100 (se usará default): {e}")

            # --- PASO 3: PARSEO DE LA TABLA ---
            soup = BeautifulSoup(self.driver.page_source, 'html.parser')
            filas = soup.find_all('tr')
            
            items_a_procesar = []
            print(f"Analizando {len(filas)} filas en tabla de resultados...")

            for fila in filas:
                cols = fila.find_all('td')
                # Buscamos filas con al menos 2 columnas (Expediente y Tipo)
                if len(cols) >= 2:
                    enlace = cols[0].find('a', href=True)
                    if enlace and 'verExp' in enlace['href']:
                        url_completa = f"https://www.senado.gob.ar{enlace['href']}"
                        
                        # --- LÓGICA DE ID: 2042-PD-25 ---
                        texto_numero = self.limpiar_texto(enlace.text) # "2042/25"
                        texto_tipo = self.limpiar_texto(cols[1].text)  # "PD"
                        
                        id_formateado = texto_numero 
                        try:
                            if "/" in texto_numero:
                                partes = texto_numero.split('/')
                                num = partes[0]
                                anio = partes[1]
                                id_formateado = f"{num}-{texto_tipo}-{anio}"
                        except: pass
                        
                        items_a_procesar.append({
                            'url': url_completa,
                            'id': id_formateado
                        })

            # Eliminar duplicados
            items_unicos = {item['url']: item for item in items_a_procesar}.values()
            
            print(f"✅ Se encontraron {len(items_unicos)} proyectos únicos.")

        except Exception:
            print("❌ Error CRÍTICO en la navegación.")
            print(traceback.format_exc())
            self.driver.quit()
            return pd.DataFrame()

        # --- PASO 4: EXTRACCIÓN DETALLADA ---
        # Si quieres probar rápido, deja el slice [:5] un momento más
        for i, item in enumerate(items_unicos): 
            print(f"[{i+1}/{len(items_unicos)}] {item['id']}...")
            
            info_detalle = self.extraer_detalle_proyecto(item['url'])
            
            if info_detalle:
                self.data.append({
                    'Cámara de Origen': 'Senado',
                    'Expediente': item['id'],
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
                # Pausa breve para no saturar
                time.sleep(0.5)

        self.driver.quit()
        return pd.DataFrame(self.data)
