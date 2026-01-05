import time
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

class ScrapearBoletin:
    def __init__(self):
        # Palabras clave para filtrar lo que nos interesa
        self.filtros_organismo = [
            "BANCO CENTRAL", "CNV", "COMISIÓN NACIONAL DE VALORES", 
            "ECONOMÍA", "HACIENDA", "FINANZAS", "AFIP", "ARCA", "UIF", 
            "PODER LEGISLATIVO", "PODER EJECUTIVO"
        ]
        # Si la norma es Ley o Decreto, pasa siempre, sin importar el organismo
        self.filtros_norma = ["LEY", "DECRETO", "DNU"]

    def configurar_browser(self):
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument('--disable-blink-features=AutomationControlled')
        # User agent real para que no nos bloqueen
        chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
        return webdriver.Chrome(options=chrome_options)

    def es_relevante(self, organismo, norma_tipo):
        org_upper = organismo.upper()
        norma_upper = norma_tipo.upper()

        # 1. Prioridad: Leyes y Decretos pasan siempre
        for f in self.filtros_norma:
            if f in norma_upper:
                return True
        
        # 2. Organismos financieros clave
        for f in self.filtros_organismo:
            if f in org_upper:
                return True
        
        return False

    def scrape_test(self):
        print(">>> 📜 Iniciando Test del Boletín Oficial...")
        driver = self.configurar_browser()
        
        try:
            url = "https://www.boletinoficial.gob.ar/seccion/primera"
            print(f"Navegando a: {url}")
            driver.get(url)
            
            # Esperamos que aparezca al menos una línea de aviso (la clase que vimos en tu HTML)
            wait = WebDriverWait(driver, 20)
            print("Esperando carga de avisos...")
            wait.until(EC.presence_of_element_located((By.CLASS_NAME, "linea-aviso")))
            
            # Pequeña pausa para asegurar renderizado completo
            time.sleep(3)

            # Buscamos todos los elementos con la clase "linea-aviso"
            # Ojo: El HTML muestra que 'linea-aviso' está DENTRO del <a>
            # Vamos a buscar los <a> que contengan esa clase.
            avisos_elements = driver.find_elements(By.XPATH, "//div[contains(@class, 'linea-aviso')]/..")
            
            print(f"✅ Se detectaron {len(avisos_elements)} avisos en total en la primera página.")
            print("--------------------------------------------------")

            contador_relevantes = 0

            for i, aviso in enumerate(avisos_elements):
                try:
                    # Extraemos el texto crudo
                    texto_completo = aviso.text.split('\n')
                    # Estructura usual del texto según tu HTML:
                    # [0] Organismo (p.item)
                    # [1] Tipo Norma (p.item-detalle small 1)
                    # [2] Síntesis (p.item-detalle small 2)
                    
                    organismo = texto_completo[0] if len(texto_completo) > 0 else "Desconocido"
                    norma_tipo = texto_completo[1] if len(texto_completo) > 1 else ""
                    sintesis = texto_completo[2] if len(texto_completo) > 2 else ""
                    
                    link = aviso.get_attribute("href")

                    # Aplicamos filtro
                    if self.es_relevante(organismo, norma_tipo):
                        contador_relevantes += 1
                        print(f"🔹 [RELEVANTE] {norma_tipo}")
                        print(f"   🏛️ Org: {organismo}")
                        print(f"   📄 Tema: {sintesis[:100]}...") # Cortamos para que no ensucie log
                        print(f"   🔗 Link: {link}")
                        print("-" * 20)
                    else:
                        # Descomenta esto si quieres ver lo que descarta
                        # print(f"❌ [Descartado] {organismo} - {norma_tipo}")
                        pass

                except Exception as e:
                    print(f"⚠️ Error leyendo aviso #{i}: {e}")
                    continue

            print(f"✅ Test finalizado. Se encontraron {contador_relevantes} normas relevantes para el Banco.")

        except Exception as e:
            print(f"❌ ERROR CRÍTICO: {e}")
            try:
                print(f"Título de página al fallar: {driver.title}")
            except: pass
        finally:
            driver.quit()

if __name__ == "__main__":
    bot = ScrapearBoletin()
    bot.scrape_test()