import time
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup

class ScrapearSenado:

    def __init__(self):
        print("Inicializando robot Senado...")
        options = Options()
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--window-size=1920,1080')
        options.add_argument('--ignore-certificate-errors')
        options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36")

        self.driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=options)
        self.data = []

    def limpiar_texto(self, texto):
        if not texto: return "S/D"
        return " ".join(texto.split())

    def extraer_detalle_proyecto(self, url):
        try:
            self.driver.get(url)
            WebDriverWait(self.driver, 15).until(
                EC.presence_of_element_located((By.TAG_NAME, "h1"))
            )
            
            soup = BeautifulSoup(self.driver.page_source, 'html.parser')

            expediente = "S/D"
            try:
                h1 = soup.find('h1')
                if h1:
                    expediente = h1.text.replace("Número de Expediente", "").strip()
            except: pass

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

            autores = "S/D"
            try:
                div_autores = soup.find('div', id='Autores')
                if div_autores:
                    nombres = []
                    links_autores = div_autores.find_all('a', href=True)
                    for a in links_autores:
                        if "senador" in a['href']:
                            nombres.append(self.limpiar_texto(a.text))
                    
                    if nombres:
                        autores = ", ".join(nombres)
            except: pass

            fecha = "S/D"
            try:
                div_tramite = soup.find('div', id='tramiteLegislativo')
                if div_tramite:
                    tabla_fechas = div_tramite.find('table', attrs={'summary': 'Fechas en Mesa de Entradas'})
                    if tabla_fechas:
                        fecha = self.limpiar_texto(tabla_fechas.find('tbody').find('tr').find_all('td')[0].text)
            except: pass

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
                'Expediente': expediente,
                'Proyecto': proyecto_texto,
                'Autor': autores,
                'Fecha de inicio': fecha,
                'Comisiones': comisiones_final
            }

        except Exception as e:
            print(f"Error en detalle {url}: {e}")
            return None

    def scrape(self):
        url_busqueda = "https://www.senado.gob.ar/parlamentario/parlamentaria/"
        print(f"Entrando a {url_busqueda}")
        
        try:
            self.driver.get(url_busqueda)
            wait = WebDriverWait(self.driver, 20)

            boton_avanzada = wait.until(EC.element_to_be_clickable((By.XPATH, "//a[contains(@href, '#collapse113')]")))
            self.driver.execute_script("arguments[0].click();", boton_avanzada)
            time.sleep(1)

            boton_buscar = wait.until(EC.element_to_be_clickable((By.ID, "type_image2")))
            self.driver.execute_script("arguments[0].click();", boton_buscar)

            wait.until(EC.presence_of_element_located((By.TAG_NAME, "table")))
            time.sleep(2) 

            soup = BeautifulSoup(self.driver.page_source, 'html.parser')
            links_proyectos = []
            
            filas = soup.find_all('tr')
            for fila in filas:
                enlace = fila.find('a', href=True)
                if enlace and 'verExp' in enlace['href']:
                    url_completa = f"https://www.senado.gob.ar{enlace['href']}"
                    links_proyectos.append(url_completa)
            
            links_proyectos = list(set(links_proyectos))
            print(f"Se encontraron {len(links_proyectos)} proyectos.")

        except Exception as e:
            print(f"Error búsqueda inicial: {e}")
            self.driver.quit()
            return pd.DataFrame()

        for i, link in enumerate(links_proyectos): 
            print(f"[{i+1}/{len(links_proyectos)}] Procesando: {link}")
            
            info = self.extraer_detalle_proyecto(link)
            
            if info:
                self.data.append({
                    'Cámara de Origen': 'Senado',
                    'Expediente': info['Expediente'],
                    'Autor': info['Autor'],
                    'Fecha de inicio': info['Fecha de inicio'],
                    'Proyecto': info['Proyecto'],
                    'Comisiones': info['Comisiones'],
                    'Estado': '',
                    'Probabilidad': '',
                    'Partido Político': '',
                    'Provincia': '',
                    'Observaciones': ''
                })
                time.sleep(1)

        self.driver.quit()
        return pd.DataFrame(self.data)
