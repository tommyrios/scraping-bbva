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
        options.add_argument('--headless') 
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--window-size=1920,1080')
        options.add_argument('--ignore-certificate-errors')
        options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36")

        self.driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=options)
        self.data = []
        self.mapa_datos_senadores = {} 

    def limpiar_texto(self, texto):
        if not texto: return "S/D"
        return " ".join(texto.split())

    def obtener_diccionario_partidos(self):
        url_lista = "https://www.senado.gob.ar/senadores/listados/listaSenadoRes"
        print(f"Obteniendo datos de senadores desde {url_lista}")
        
        try:
            self.driver.get(url_lista)
            WebDriverWait(self.driver, 15).until(EC.presence_of_element_located((By.TAG_NAME, "table")))
            
            soup = BeautifulSoup(self.driver.page_source, 'html.parser')
            tabla = soup.find('table', id='senadoresTabla')
            
            if not tabla:
                print("❌ [DEBUG] No se encontró la tabla con id='senadoresTabla'")
                return

            filas = tabla.find('tbody').find_all('tr')
            print(f"ℹ️ [DEBUG] Filas encontradas en la lista: {len(filas)}")
            
            for i, fila in enumerate(filas):
                cols = fila.find_all('td')
                if len(cols) >= 4:
                    nombre = "S/D"
                    link_nombre = cols[1].find('a', href=True)
                    if link_nombre and link_nombre.has_attr('title'):
                        # ACA ESTA LA CLAVE: ¿Como viene el nombre del title?
                        nombre = link_nombre['title'].strip().upper()
                    
                    provincia = self.limpiar_texto(cols[2].text)
                    partido = self.limpiar_texto(cols[3].text).upper()
                    
                    if nombre != "S/D":
                        self.mapa_datos_senadores[nombre] = {
                            'partido': partido,
                            'provincia': provincia
                        }
                        # Imprimimos los primeros 3 para ver el formato
                        if i < 3:
                            print(f"   💾 [GUARDADO] Clave: '{nombre}' -> Partido: {partido}")

            print(f"✅ [DEBUG] Diccionario cargado con {len(self.mapa_datos_senadores)} senadores.")

        except Exception as e:
            print(f"⚠️ Error obteniendo lista de senadores: {e}")

    def extraer_detalle_proyecto(self, url):
        try:
            self.driver.get(url)
            WebDriverWait(self.driver, 15).until(
                EC.presence_of_element_located((By.TAG_NAME, "h1"))
            )
            
            soup = BeautifulSoup(self.driver.page_source, 'html.parser')

            proyecto_texto = "S/D"
            try:
                tabla_encabezado = soup.find('table', class_='table-bordered')
                if tabla_encabezado:
                    filas = tabla_encabezado.find('tbody').find_all('tr')
                    for f in filas:
                        cols = f.find_all('td')
                        if len(cols) >= 4:
                            texto_crudo = self.limpiar_texto(cols[3].text)
                            if ":" in texto_crudo:
                                partes = texto_crudo.split(":", 1)
                                if len(partes) > 1:
                                    proyecto_texto = partes[1].strip()
                                else:
                                    proyecto_texto = texto_crudo
                            else:
                                proyecto_texto = texto_crudo
            except: pass

            autor_principal = "S/D"
            try:
                div_autores = soup.find('div', id='Autores')
                if div_autores:
                    link_autor = div_autores.find('a', href=True)
                    if link_autor:
                        if link_autor.has_attr('title') and link_autor['title']:
                            autor_principal = link_autor['title'].strip().upper()
                        else:
                            raw_text = self.limpiar_texto(link_autor.text)
                            autor_principal = raw_text.replace(" ,", ",").upper()
                    else:
                        td = div_autores.find('td')
                        if td:
                            raw_text = self.limpiar_texto(td.text)
                            autor_principal = raw_text.replace(" ,", ",").upper()
            except: pass

            fecha = "S/D"
            try:
                div_tramite = soup.find('div', id='tramiteLegislativo')
                if div_tramite:
                    tabla_fechas = div_tramite.find('table', attrs={'summary': 'Fechas en Mesa de Entradas'})
                    if tabla_fechas:
                        texto_fecha = tabla_fechas.find('tbody').find('tr').find_all('td')[0].text
                        fecha = self.limpiar_texto(texto_fecha).replace('-', '/')
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
                'Proyecto': proyecto_texto,
                'Autor': autor_principal,
                'Fecha de inicio': fecha,
                'Comisiones': comisiones_final
            }

        except Exception:
            return None

    def scrape(self):
        self.obtener_diccionario_partidos()

        url_inicio = "https://www.senado.gob.ar/parlamentario/parlamentaria/"
        print(f"Entrando a {url_inicio}")
        
        try:
            self.driver.get(url_inicio)
            wait = WebDriverWait(self.driver, 30)

            print("1. Desplegando búsqueda...")
            boton_avanzada = wait.until(EC.element_to_be_clickable((By.XPATH, "//a[contains(@href, '#collapse113')]")))
            self.driver.execute_script("arguments[0].click();", boton_avanzada)
            time.sleep(1)

            print("2. Enviando formulario...")
            formulario = wait.until(EC.presence_of_element_located((By.NAME, "ingreso2")))
            formulario.submit()

            print("3. Esperando resultados...")
            wait.until(EC.presence_of_element_located((By.TAG_NAME, "table")))

            # Comentado para test rápido
            # print("4. Filtrando 100 resultados...")
            # try:
            #     select_element = wait.until(EC.presence_of_element_located((By.NAME, "cantRegistros")))
            #     select = Select(select_element)
            #     select.select_by_value("100")
            #     time.sleep(5) 
            #     wait.until(EC.presence_of_element_located((By.TAG_NAME, "table")))
            # except: pass

            soup = BeautifulSoup(self.driver.page_source, 'html.parser')
            filas = soup.find_all('tr')
            items_a_procesar = []

            for fila in filas:
                cols = fila.find_all('td')
                if len(cols) < 2: continue

                enlace = cols[0].find('a', href=True)
                if not enlace or 'verExp' not in enlace['href']: continue

                url_completa = f"https://www.senado.gob.ar{enlace['href']}"
                
                texto_numero = self.limpiar_texto(enlace.text)
                texto_tipo = self.limpiar_texto(cols[1].text)
                
                id_formateado = texto_numero 
                try:
                    if "/" in texto_numero:
                        partes = texto_numero.split('/')
                        id_formateado = f"{partes[0]}-{texto_tipo}-{partes[1]}"
                except: pass
                
                items_a_procesar.append({'url': url_completa, 'id': id_formateado})

            items_unicos = {item['url']: item for item in items_a_procesar}.values()
            print(f"Se encontraron {len(items_unicos)} proyectos únicos.")

        except Exception:
            print("Error critico en navegacion.")
            print(traceback.format_exc())
            self.driver.quit()
            return pd.DataFrame()

        for i, item in enumerate(items_unicos): 
            print(f"[{i+1}/{len(items_unicos)}] {item['id']}...")
            info = self.extraer_detalle_proyecto(item['url'])
            
            if info:
                # --- ZONA DE DEBUGGING DE CRUCE ---
                autor_proyecto = info['Autor']
                datos_extra = self.mapa_datos_senadores.get(autor_proyecto, {'partido': '', 'provincia': ''})
                
                # CHIVATO: Si no encontro partido, mostramos por que
                if datos_extra['partido'] == "":
                    print(f"   ⚠️ [FALLO CRUCE] Autor Proyecto: '{autor_proyecto}'")
                    # Mostramos si existe alguna clave parecida en el mapa (para ver si es tema de formato)
                    match_cercano = [k for k in self.mapa_datos_senadores.keys() if k.split(',')[0] in autor_proyecto]
                    if match_cercano:
                        print(f"      -> Posible coincidencia en Diccionario: '{match_cercano[0]}'")
                    else:
                        print(f"      -> No se encontró nada parecido en el diccionario.")
                else:
                    print(f"   ✅ [CRUCE OK] {autor_proyecto} -> {datos_extra['partido']}")
                # ----------------------------------

                self.data.append({
                    'Cámara de Origen': 'Senado',
                    'Expediente': item['id'],
                    'Autor': info['Autor'],
                    'Fecha de inicio': info['Fecha de inicio'],
                    'Proyecto': info['Proyecto'],
                    'Comisiones': info['Comisiones'],
                    'Estado': '',
                    'Probabilidad': '',
                    'Partido Político': datos_extra['partido'],
                    'Provincia': datos_extra['provincia'],
                    'Observaciones': ''
                })
                time.sleep(0.5)

        self.driver.quit()
        return pd.DataFrame(self.data)
