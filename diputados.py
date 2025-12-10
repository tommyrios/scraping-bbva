import time
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

class ScrapearDiputados:

    def __init__(self):
        print("Inicializando robot Diputados...")
        options = Options()
        options.add_argument('--headless')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--window-size=1920,1080')
        options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36")

        self.driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=options)
        self.data = []

    def get_expediente(self, soup):
        try:
            spans = soup.find_all('span')
            for s in spans:
                if "Expediente" in s.text:
                    return s.text.split(":")[-1].strip()
            return "S/D"
        except: return "S/D"

    def get_autor_info(self, soup):
        autor = "S/D"; bloque = "S/D"; provincia = "S/D"
        try:
            contenedor = soup.parent
            h5 = contenedor.find('h5', string=lambda x: x and 'FIRMANTES' in x)
            if h5:
                tabla = h5.find_next('table')
                if tabla:
                    fila = tabla.find('tbody').find('tr')
                    cols = fila.find_all('td')
                    if len(cols) >= 3:
                        autor = cols[0].text.strip()
                        provincia = cols[1].text.strip()
                        bloque = cols[2].text.strip()
                    elif len(cols) == 1:
                        autor = cols[0].text.strip()
            return autor, bloque, provincia
        except: return autor, bloque, provincia

    def get_fechaInicio(self, soup):
        try:
            spans = soup.find_all('span')
            for s in spans:
                if "Fecha" in s.text:
                    return s.text.split(":")[-1].strip()
            return "S/D"
        except: return "S/D"

    def get_proyecto(self, soup):
        try:
            contenedor = soup.parent
            div = contenedor.find('div', class_='dp-texto')
            return div.text.strip() if div else "S/D"
        except: return "S/D"

    def get_comisiones(self, soup):
        try:
            contenedor = soup.parent
            h5 = contenedor.find('h5', string=lambda x: x and 'GIRO' in x)
            if h5:
                tabla = h5.find_next('table')
                filas = tabla.find('tbody').find_all('tr')
                nombres = [f.text.strip().replace('\n', '') for f in filas]
                return ", ".join(nombres)
            return "S/D"
        except: return "S/D"

    def scrape(self, url):
        print(f"Entrando a {url}")
        self.driver.get(url)

        try:
            wait = WebDriverWait(self.driver, 30)
            
            print("Configurando filtro a 100 resultados...")
            dropdown = wait.until(EC.presence_of_element_located((By.ID, "strCantPagina")))
            select = Select(dropdown)
            select.select_by_value("100")

            time.sleep(3) 

            print("Buscando botón...")
            boton = wait.until(EC.element_to_be_clickable((By.XPATH, "//input[@value='Buscar']")))
            self.driver.execute_script("arguments[0].click();", boton)
            
            print("Esperando que cargue la tabla de resultados...")
            
            try:
                wait.until(EC.presence_of_element_located((By.CLASS_NAME, "dp-metadata")))
                time.sleep(2)
            except:
                print("⚠️ Alerta: Pasaron 30 segundos y no aparecieron proyectos. Puede que no haya resultados o el sitio esté muy lento.")
            
        except Exception as e:
            print(f"❌ Error interactuando con la página: {e}")
            self.driver.quit()
            return None

        soup = BeautifulSoup(self.driver.page_source, 'html.parser')
        bloques = soup.find_all('div', class_='dp-metadata')

        print(f"Procesando {len(bloques)} proyectos encontrados...")

        if len(bloques) == 0:
            print("El scraping no encontró bloques de proyectos (len=0).")
            self.driver.quit()
            return pd.DataFrame(self.data)

        for bloque in bloques:
            autor, partido, provincia = self.get_autor_info(bloque)

            self.data.append({
                'Cámara de Origen': 'Diputados',
                'Expediente': self.get_expediente(bloque),
                'Autor': autor,
                'Fecha de inicio': self.get_fechaInicio(bloque),
                'Proyecto': self.get_proyecto(bloque),
                'Comisiones': self.get_comisiones(bloque),
                'Estado': '',
                'Probabilidad': '',
                'Partido Político': partido,
                'Provincia': provincia,
                'Observaciones': ''
            })

        self.driver.quit()
        return pd.DataFrame(self.data)
