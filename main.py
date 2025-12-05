import os
import json
import time
import pandas as pd
import gspread
import urllib.request
import urllib.parse
from google.oauth2.service_account import Credentials
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
            
            dropdown = wait.until(EC.presence_of_element_located((By.ID, "strCantPagina")))
            
            select = Select(dropdown)
            select.select_by_value("100") 
            
            print("Buscando botón...")
            boton = wait.until(EC.element_to_be_clickable((By.XPATH, "//input[@value='Buscar']")))
            
            time.sleep(2) 
            
            self.driver.execute_script("arguments[0].click();", boton)
            
            print("Esperando resultados...")
            time.sleep(10) 
            
        except Exception as e:
            print(f"Error interactuando con la página: {e}")
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

def enviar_whatsapp(mensaje):
    if 'WHATSAPP_PHONE' in os.environ and 'WHATSAPP_API_KEY' in os.environ:
        try:
            phone = os.environ['WHATSAPP_PHONE']
            apikey = os.environ['WHATSAPP_API_KEY']
            texto_codificado = urllib.parse.quote(mensaje)
            url = f"https://api.callmebot.com/whatsapp.php?phone={phone}&text={texto_codificado}&apikey={apikey}"
            
            req = urllib.request.Request(url)
            with urllib.request.urlopen(req) as response:
                print(f"Estado WhatsApp: {response.read().decode('utf-8')}")
                
        except Exception as e:
            print(f"Error enviando WhatsApp: {e}")
    else:
        print("Credenciales de WhatsApp no configuradas.")

if __name__ == "__main__":
    
    msg_final = ""
    
    try:
        url_objetivo = "https://www.diputados.gov.ar/proyectos/"
        bot = ScrapearDiputados()
        df_resultado = bot.scrape(url_objetivo)

        print("-" * 50)
        print("Iniciando proceso de sincronización con Google Sheets...")

        if 'GCP_CREDENTIALS' in os.environ:
            json_creds = json.loads(os.environ['GCP_CREDENTIALS'])
            scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
            creds = Credentials.from_service_account_info(json_creds, scopes=scopes)
            gc = gspread.authorize(creds)
        else:
            raise Exception("No se encontró la variable de entorno GCP_CREDENTIALS")

        URL_PLANILLA = "https://docs.google.com/spreadsheets/d/16aksCoBrIFB6Vy8JpiuVBEpfGNHdUNJcsCKb2k33tsQ/edit?gid=0#gid=0"
        NOMBRE_HOJA = "Proyectos"

        print(f"Abriendo planilla...")
        wb = gc.open_by_url(URL_PLANILLA)
        sheet = wb.worksheet(NOMBRE_HOJA)

        datos_existentes = sheet.get_all_records()
        df_sheet = pd.DataFrame(datos_existentes)

        filas_nuevas = []
        contador_actualizados = 0
        contador_omitidos = 0

        proximo_id = 1
        if not df_sheet.empty and 'ID' in df_sheet.columns:
            max_id_numeric = 0
            max_id_pl_format = 0

            pl_ids_series = df_sheet['ID'][df_sheet['ID'].astype(str).str.startswith('PL', na=False)]
            if not pl_ids_series.empty:
                numeric_parts = pl_ids_series.str.replace('PL', '', regex=False).astype(str).str.extract(r'^(\d+)$', expand=False)
                max_pl_id_val = pd.to_numeric(numeric_parts, errors='coerce').max()
                if pd.notna(max_pl_id_val): max_id_pl_format = int(max_pl_id_val)

            numeric_ids_series = df_sheet['ID'][~df_sheet['ID'].astype(str).str.startswith('PL', na=False)]
            if not numeric_ids_series.empty:
                max_numeric_id_val = pd.to_numeric(numeric_ids_series, errors='coerce').max()
                if pd.notna(max_numeric_id_val): max_id_numeric = int(max_numeric_id_val)

            proximo_id = max(max_id_numeric, max_id_pl_format) + 1

        if df_resultado is not None and not df_resultado.empty:

            print(f"Analizando {len(df_resultado)} proyectos scrapeados...")

            for index, row in df_resultado.iterrows():
                expediente_nuevo = str(row['Expediente']).strip()
                fecha_nueva = str(row['Fecha de inicio']).strip()

                match = pd.DataFrame()
                if not df_sheet.empty:
                    match = df_sheet[df_sheet['Expediente'].astype(str) == expediente_nuevo]

                if match.empty:
                    formatted_id = f"PL{proximo_id:03d}"

                    fila_ordenada = [
                        formatted_id, 'Diputados', row['Expediente'], row['Autor'],
                        row['Fecha de inicio'], row['Proyecto'], row['Comisiones'],
                        '', '', row['Partido Político'], row['Provincia'], ''
                    ]
                    fila_limpia = [str(x) if pd.notna(x) else "" for x in fila_ordenada]
                    filas_nuevas.append(fila_limpia)
                    proximo_id += 1

                else:
                    idx_existente = match.index[0]
                    fecha_existente = str(match.iloc[0]['Fecha de inicio']).strip()
                    id_existente = str(match.iloc[0]['ID']).strip() 

                    if fecha_nueva != fecha_existente:
                        print(f"Actualizando Exp: {expediente_nuevo}")
                        
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
                    else:
                        contador_omitidos += 1

            if filas_nuevas:
                print(f"Cargando {len(filas_nuevas)} proyectos NUEVOS...")
                sheet.append_rows(filas_nuevas)

            print("-" * 50)
            print("RESUMEN DE OPERACIÓN:")
            print(f"Nuevos cargados: {len(filas_nuevas)}")
            print(f"Actualizados: {contador_actualizados}")
            print(f"Omitidos: {contador_omitidos}")
            print("-" * 50)
            
            msg_final = f"Resumen Ejecucion Diputados: Nuevos: {len(filas_nuevas)}. Actualizados: {contador_actualizados}. Omitidos: {contador_omitidos}."

        else:
            msg = "El scraping no trajo datos."
            print(msg)
            msg_final = f"Alerta Diputados: {msg}"

        enviar_whatsapp(msg_final)

    except Exception as e:
        err_msg = f"Error Critico Diputados: {str(e)}"
        print(err_msg)
        enviar_whatsapp(err_msg)
        exit(1)
