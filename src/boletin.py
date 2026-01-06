import time
import pandas as pd
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

class ScrapearBoletin:
    def __init__(self):
        self.filtros_organismo = [
            "BANCO CENTRAL", "CNV", "COMISIÓN NACIONAL DE VALORES", 
            "ECONOMÍA", "HACIENDA", "FINANZAS", "AFIP", "ARCA", "UIF", 
            "PODER LEGISLATIVO", "PODER EJECUTIVO", "CAPITAL HUMANO"
        ]
        self.filtros_norma = ["LEY", "DECRETO", "DNU", "RESOLUCIÓN GENERAL"]

    def configurar_browser(self):
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument('--disable-blink-features=AutomationControlled')
        chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
        return webdriver.Chrome(options=chrome_options)

    def es_relevante(self, organismo, norma_tipo):
        org_upper = organismo.upper()
        norma_upper = norma_tipo.upper()
        
        for f in self.filtros_norma:
            if f in norma_upper:
                return True
        
        for f in self.filtros_organismo:
            if f in org_upper:
                return True
        return False

    def scrape(self):
        print(">>> 📜 Iniciando Scraper Boletín Oficial...")
        driver = self.configurar_browser()
        datos = []
        fecha_hoy = datetime.now().strftime("%d/%m/%Y")
        
        try:
            url = "https://www.boletinoficial.gob.ar/seccion/primera"
            driver.get(url)
            
            wait = WebDriverWait(driver, 20)
            wait.until(EC.presence_of_element_located((By.CLASS_NAME, "linea-aviso")))
            time.sleep(3)

            avisos_elements = driver.find_elements(By.XPATH, "//div[contains(@class, 'linea-aviso')]/..")
            
            for aviso in avisos_elements:
                try:
                    texto_completo = aviso.text.split('\n')
                    organismo = texto_completo[0] if len(texto_completo) > 0 else ""
                    norma = texto_completo[1] if len(texto_completo) > 1 else ""
                    sintesis = texto_completo[2] if len(texto_completo) > 2 else ""
                    link = aviso.get_attribute("href")

                    if self.es_relevante(organismo, norma):
                        datos.append({
                            'Expediente': norma,       
                            'Autor': organismo,        
                            'Fecha de inicio': fecha_hoy, 
                            'Proyecto': sintesis,      
                            'Comisiones': link         
                        })

                except Exception:
                    continue

        except Exception as e:
            print(f"❌ Error Boletín: {e}")
        finally:
            driver.quit()

        return pd.DataFrame(datos)