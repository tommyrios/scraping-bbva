import time
import traceback
import re
import requests
from urllib.parse import urljoin, urlparse
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
        options.add_argument("user-agent=Mozilla/5.0 (Windows...37.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36")

        self.driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=options)
        self.data = []
        self.mapa_datos_senadores = {} 
        self._session = requests.Session()
        self._session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"
        })

    BASE_SENADO = "https://www.senado.gob.ar"

    def _es_pdf(self, resp) -> bool:
        try:
            ctype = (resp.headers.get("Content-Type") or "").lower()
            cdisp = (resp.headers.get("Content-Disposition") or "").lower()
            return ("application/pdf" in ctype) or ("pdf" in cdisp) or ctype.endswith("/pdf")
        except Exception:
            return False

    def _extraer_url_desde_onclick(self, onclick: str) -> str:
        if not onclick:
            return ""
        m = re.search(r"""['"]([^'"]+)['"]""", onclick)
        return m.group(1).strip() if m else ""

    def _buscar_link_texto_original_en_html(self, soup: BeautifulSoup) -> str:
        for a in soup.find_all("a"):
            label = " ".join(a.stripped_strings).strip().lower()
            if "texto original" not in label:
                continue
            href = (a.get("href") or "").strip()
            if href:
                return href
            onclick = (a.get("onclick") or "").strip()
            if onclick:
                u = self._extraer_url_desde_onclick(onclick)
                if u:
                    return u

        for el in soup.find_all(["button", "input", "span", "div"]):
            onclick = (el.get("onclick") or "").strip()
            if onclick and ("texto" in onclick.lower() and "original" in onclick.lower()):
                u = self._extraer_url_desde_onclick(onclick)
                if u:
                    return u

        for a in soup.find_all("a", href=True):
            href = (a.get("href") or "").strip()
            if href.lower().endswith(".pdf"):
                return href
        return ""

    def _candidatos_descarga_desde_verexp(self, verexp_url: str) -> list:
        try:
            path = urlparse(verexp_url).path.strip("/")
            parts = path.split("/")
            if "verExp" not in parts:
                return []
            i = parts.index("verExp")
            if len(parts) <= i + 3:
                return []
            exp_id = parts[i + 1]
            cam = parts[i + 2]
            tipo = parts[i + 3]
            base = self.BASE_SENADO
            return [
                f"{base}/parlamentario/comisiones/verTextoOriginal/{exp_id}/{cam}/{tipo}",
                f"{base}/parlamentario/comisiones/verTextoOriginalPdf/{exp_id}/{cam}/{tipo}",
                f"{base}/parlamentario/comisiones/verTexto/{exp_id}/{cam}/{tipo}",
                f"{base}/parlamentario/comisiones/textoOriginal/{exp_id}/{cam}/{tipo}",
                f"{base}/parlamentario/comisiones/descargarTextoOriginal/{exp_id}/{cam}/{tipo}",
                f"{base}/parlamentario/comisiones/downloadTextoOriginal/{exp_id}/{cam}/{tipo}",
            ]
        except Exception:
            return []

    def get_link_texto_original(self, verexp_url: str, soup_detalle: BeautifulSoup = None) -> str:
        """Devuelve un link al PDF (texto original) si puede; fallback: verExp_url."""
        if not verexp_url:
            return ""

        # 1) Desde HTML (href u onclick)
        try:
            if soup_detalle is None:
                r = self._session.get(verexp_url, timeout=25)
                if r.status_code == 200:
                    soup_detalle = BeautifulSoup(r.text, "html.parser")

            if soup_detalle is not None:
                raw = self._buscar_link_texto_original_en_html(soup_detalle)
                if raw:
                    candidato = urljoin(self.BASE_SENADO, raw)
                    try:
                        rr = self._session.get(candidato, timeout=25, allow_redirects=True)
                        if rr.status_code == 200 and self._es_pdf(rr):
                            return rr.url
                    except Exception:
                        return candidato
        except Exception:
            pass

        for cand in self._candidatos_descarga_desde_verexp(verexp_url):
            try:
                rr = self._session.get(cand, timeout=25, allow_redirects=True)
                if rr.status_code == 200 and self._es_pdf(rr):
                    return rr.url
            except Exception:
                continue

        return verexp_url

    def limpiar_texto(self, texto):
        if not texto: return "S/D"
        return " ".join(texto.split())

    def obtener_diccionario_partidos(self):
        url_lista = "https://www.senado.gob.ar/senadores/listados/listaSenadoRes"
        print(f"Mapeando senadores desde {url_lista}")
        
        try:
            self.driver.get(url_lista)
            WebDriverWait(self.driver, 15).until(EC.presence_of_element_located((By.TAG_NAME, "table")))
            
            soup = BeautifulSoup(self.driver.page_source, 'html.parser')
            tabla = soup.find('table', id='senadoresTabla')
            
            if not tabla: return

            filas = tabla.find('tbody').find_all('tr')
            
            for fila in filas:
                cols = fila.find_all('td')
                if len(cols) < 5: continue

                nombre = self.limpiar_texto(cols[0].text)
                provincia = self.limpiar_texto(cols[1].text)
                partido = self.limpiar_texto(cols[2].text)
                
                self.mapa_datos_senadores[nombre] = {'provincia': provincia, 'partido': partido}

        except Exception as e:
            print(f"Error al obtener diccionario de senadores: {e}")

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
                div_autores = soup.find('div', class_='tab-pane', id='autores')
                
                if div_autores:
                    link_autor = div_autores.find('a', href=True)
                    
                    raw_text = ""
                    if link_autor:
                        if link_autor.has_attr('title') and link_autor['title']:
                            raw_text = link_autor['title'].strip()
                        else:
                            raw_text = self.limpiar_texto(link_autor.text)
                    else:
                        td = div_autores.find('td')
                        if td:
                            raw_text = self.limpiar_texto(td.text)
                    
                    autor_principal = raw_text if raw_text else "S/D"
            except: pass

            fecha = "S/D"
            try:
                tabla_encabezado = soup.find('table', class_='table-bordered')
                if tabla_encabezado:
                    filas = tabla_encabezado.find('tbody').find_all('tr')
                    for f in filas:
                        cols = f.find_all('td')
                        if len(cols) >= 2:
                            etiqueta = self.limpiar_texto(cols[0].text).lower()
                            if "fecha" in etiqueta:
                                fecha = self.limpiar_texto(cols[1].text)
            except: pass

            comisiones_final = "S/D"
            try:
                div_giro = soup.find('div', class_='tab-pane', id='giro')
                if div_giro:
                    tabla = div_giro.find('table')
                    if tabla:
                        filas = tabla.find_all('tr')
                        comisiones = []
                        for f in filas:
                            cols = f.find_all('td')
                            if cols:
                                com = self.limpiar_texto(cols[0].text)
                                if com and com != "S/D":
                                    comisiones.append(com)
                        if comisiones:
                            comisiones_final = ", ".join(comisiones)
            except: pass

            link_texto = self.get_link_texto_original(url, soup_detalle=soup)

            return {
                'Proyecto': proyecto_texto,
                'Autor': autor_principal,
                'Fecha de inicio': fecha,
                'Comisiones': comisiones_final,
                'Link Texto': link_texto
            }

        except Exception:
            return None

    def scrape(self):
        self.obtener_diccionario_partidos()

        url_inicio = "https://www.senado.gob.ar/parlamentario/comisiones/comisiones"
        print(f"Entrando a {url_inicio}")

        try:
            self.driver.get(url_inicio)

            wait = WebDriverWait(self.driver, 20)
            dropdown = wait.until(EC.presence_of_element_located((By.ID, "strCantPagina")))
            select = Select(dropdown)
            select.select_by_value("100")

            time.sleep(2)

            boton = wait.until(EC.element_to_be_clickable((By.XPATH, "//input[@value='Buscar']")))
            self.driver.execute_script("arguments[0].click();", boton)

            wait.until(EC.presence_of_element_located((By.TAG_NAME, "table")))
            time.sleep(2)

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

        except Exception as e:
            print(f"Error critico en navegacion: {e}")
            self.driver.quit()
            return pd.DataFrame()

        for i, item in enumerate(items_unicos): 
            info = self.extraer_detalle_proyecto(item['url'])
            
            if info:
                autor_para_mostrar = info['Autor']
                autor_para_buscar = autor_para_mostrar

                if "," in autor_para_mostrar:
                    partes = autor_para_mostrar.split(",")
                    if len(partes) == 2:
                        autor_para_buscar = f"{partes[1].strip()} {partes[0].strip()}"
                
                datos_extra = self.mapa_datos_senadores.get(autor_para_buscar, {'partido': '', 'provincia': ''})

                self.data.append({
                    'Cámara de Origen': 'Senado',
                    'Expediente': item['id'],
                    'Autor': autor_para_mostrar,
                    'Fecha de inicio': info['Fecha de inicio'],
                    'Proyecto': info['Proyecto'],
                    'Comisiones': info['Comisiones'],
                    'Link Texto': info.get('Link Texto', ''),
                    'Estado': '',
                    'Probabilidad': '',
                    'Partido Político': datos_extra['partido'],
                    'Provincia': datos_extra['provincia'],
                    'Observaciones': ''
                })
                time.sleep(0.5)

        self.driver.quit()
        return pd.DataFrame(self.data)