import os
import json
import pandas as pd
import time
import gspread
from google.oauth2.service_account import Credentials
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup

class ScrapearDiputados:
    def __init__(self):
        print("Inicializando robot...")
        options = Options()
        options.add_argument('--headless')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--window-size=1920,1080')
        
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
            wait = WebDriverWait(self.driver, 20)
            print("Buscando botón...")
            boton = wait.until(EC.element_to_be_clickable((By.XPATH, "//input[@value='Buscar']")))
            self.driver.execute_script("arguments[0].click();", boton)
            time.sleep(8)
        except Exception as e:
            print(f"Error buscando: {e}")
            self.driver.quit()
            return None

        soup = BeautifulSoup(self.driver.page_source, 'html.parser')
        bloques = soup.find_all('div', class_='dp-metadata')

        print(f"Procesando {len(bloques)} proyectos...")

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

if __name__ == "__main__":
    url_objetivo = "https://www.diputados.gov.ar/proyectos/"
    bot = ScrapearDiputados()
    df_resultado = bot.scrape(url_objetivo)

    print("Autenticando con Google (Service Account)...")
    
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
    
    json_creds = json.loads(os.environ['GCP_CREDENTIALS'])
    creds = Credentials.from_service_account_info(json_creds, scopes=scopes)
    gc = gspread.authorize(creds)

    URL_PLANILLA = "https://docs.google.com/spreadsheets/d/16aksCoBrIFB6Vy8JpiuVBEpfGNHdUNJcsCKb2k33tsQ/edit?usp=sharing"
    NOMBRE_HOJA = "Proyectos"

    try:
        print(f"Abriendo planilla...")
        wb = gc.open_by_url(URL_PLANILLA)
        sheet = wb.worksheet(NOMBRE_HOJA)

        datos_existentes = sheet.get_all_records()
        df_sheet = pd.DataFrame(datos_existentes)
        
        filas_nuevas = []
        contador_actualizados = 0

        if not df_sheet.empty and 'ID' in df_sheet.columns:
            ids_numericos = pd.to_numeric(df_sheet['ID'], errors='coerce')
            max_id = ids_numericos.fillna(0).max()
            proximo_id = int(max_id) + 1
        else:
            proximo_id = 1

        if df_resultado is not None and not df_resultado.empty:
            for index, row in df_resultado.iterrows():
                expediente_nuevo = str(row['Expediente']).strip()
                fecha_nueva = str(row['Fecha de inicio']).strip()
                
                match = pd.DataFrame()
                if not df_sheet.empty:
                    match = df_sheet[df_sheet['Expediente'].astype(str) == expediente_nuevo]

                if match.empty:
                    row['ID'] = proximo_id
                    proximo_id += 1
                    fila_ordenada = [
                        row['ID'], 'Diputados', row['Expediente'], row['Autor'], 
                        row['Fecha de inicio'], row['Proyecto'], row['Comisiones'], 
                        '', '', row['Partido Político'], row['Provincia'], ''
                    ]
                    fila_limpia = [str(x) if pd.notna(x) else "" for x in fila_ordenada]
                    filas_nuevas.append(fila_limpia)
                else:
                    idx_existente = match.index[0]
                    fecha_existente = str(match.iloc[0]['Fecha de inicio']).strip()
                    id_existente = match.iloc[0]['ID']

                    if fecha_nueva != fecha_existente:
                        print(f"Actualizando {expediente_nuevo}...")
                        fila_sheet_num = idx_existente + 2
                        fila_actualizada = [
                            id_existente, 'Diputados', row['Expediente'], row['Autor'], 
                            row['Fecha de inicio'], row['Proyecto'], row['Comisiones'], 
                            match.iloc[0]['Estado'], match.iloc[0]['Probabilidad'], 
                            row['Partido Político'], row['Provincia'], match.iloc[0]['Observaciones']
                        ]
                        fila_limpia = [str(x) if pd.notna(x) else "" for x in fila_actualizada]
                        rango = f"A{fila_sheet_num}:L{fila_sheet_num}"
                        sheet.update(range_name=rango, values=[fila_limpia])
                        contador_actualizados += 1

            if filas_nuevas:
                print(f"Cargando {len(filas_nuevas)} filas nuevas...")
                sheet.append_rows(filas_nuevas)
            
            print(f"Proceso terminado. Nuevos: {len(filas_nuevas)}, Actualizados: {contador_actualizados}")
        else:
            print("No se encontraron datos en el scraping.")

    except Exception as e:
        print(f"Error crítico: {e}")
        exit(1)
