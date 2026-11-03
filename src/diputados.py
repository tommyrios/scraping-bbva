import time
import re
import urllib3
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from urllib.parse import urljoin

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

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class ScrapearDiputados:
    BASE_URL = "https://www.diputados.gov.ar"

    def __init__(self):
        print("Inicializando robot Diputados...")
        options = Options()
        options.add_argument('--headless=new')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-gpu')
        options.add_argument('--ignore-certificate-errors')
        options.add_argument('--window-size=1920,1080')
        options.add_argument(
            "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        )

        try:
            self.driver = webdriver.Chrome(
                service=ChromeService(ChromeDriverManager().install()),
                options=options,
            )
        except Exception:
            self.driver = webdriver.Chrome(options=options)

        self.data = []

        self._session = requests.Session()
        self._session.verify = False
        self._session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                          "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        })

        retry_strategy = Retry(
            total=2,
            connect=2,
            read=2,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS"],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self._session.mount("https://", adapter)
        self._session.mount("http://", adapter)

    def _cerrar_driver(self):
        try:
            if self.driver:
                self.driver.quit()
        except Exception:
            pass

    def _normalizar_origen(self, origen_raw: str) -> str:
        if not origen_raw:
            return "Diputados"
        o = " ".join(origen_raw.split()).lower()
        if "senado" in o:
            return "Senado"
        if "diput" in o:
            return "Diputados"
        return "Diputados"

    def _inferir_origen_por_expediente(self, expediente: str) -> str:
        """Inferencia por sigla del expediente."""
        exp = " ".join((expediente or "").strip().upper().split())
        if not exp or exp == "S/D":
            return "Diputados"

        siglas_senado = {"S", "PE", "PC", "PL", "PD", "CO", "CC"}
        siglas_diputados = {"D", "JGM"}

        m = re.search(r"-\s*([A-Z]{1,3})\s*-", exp)
        if m:
            sigla = m.group(1)
            if sigla in siglas_senado:
                return "Senado"
            if sigla in siglas_diputados:
                return "Diputados"

        return "Diputados"

    def get_origen(self, soup):
        """Obtiene el origen con estrategia robusta."""
        try:
            spans = soup.find_all('span')
            for s in spans:
                txt = " ".join(s.stripped_strings)
                if not txt:
                    continue
                if re.search(r"\b(iniciado en|c[aá]mara de origen|origen)\b", txt, re.IGNORECASE):
                    origen_raw = txt.split(":")[-1].strip()
                    return self._normalizar_origen(origen_raw)

            exp = self.get_expediente(soup)
            return self._inferir_origen_por_expediente(exp)
        except Exception:
            return "Diputados"

    def _elegir_mejor_link_detalle(self, contenedor):
        """En el listado, intenta elegir el link al detalle del proyecto."""
        anchors = contenedor.find_all("a", href=True)
        if not anchors:
            return ""

        candidatos = []
        for a in anchors:
            href = (a.get("href") or "").strip()
            if not href or href == "#":
                continue
            href_low = href.lower()
            score = 0
            if "proyecto" in href_low or "proyectos" in href_low:
                score += 3
            if "exped" in href_low:
                score += 2
            if href_low.endswith(".pdf"):
                score -= 5
            candidatos.append((score, href))

        if not candidatos:
            return ""

        candidatos.sort(key=lambda x: x[0], reverse=True)
        return urljoin(self.BASE_URL, candidatos[0][1])

    def get_link_detalle(self, bloque):
        try:
            contenedor = bloque.parent
            return self._elegir_mejor_link_detalle(contenedor)
        except Exception:
            return ""

    def _extraer_link_texto_desde_html(self, soup_detalle):
        """Busca un link al PDF/texto en el detalle."""
        try:
            keywords = ["texto original", "texto completo", "texto", "pdf", "descargar", "descarga", "documento"]
            candidatos = []
            for a in soup_detalle.find_all("a", href=True):
                label = " ".join(a.stripped_strings).strip().lower()
                href = (a.get("href") or "").strip()
                href_low = href.lower()

                score = 0
                if href_low.endswith(".pdf"):
                    score += 6
                if any(k in label for k in keywords):
                    score += 3
                if "download" in href_low or "descarga" in href_low:
                    score += 2

                if score > 0:
                    candidatos.append((score, href))

            if not candidatos:
                return ""

            candidatos.sort(key=lambda x: x[0], reverse=True)
            return urljoin(self.BASE_URL, candidatos[0][1])
        except Exception:
            return ""

    def get_link_texto(self, bloque):
        """Devuelve un link 'usable' para el reporte."""
        link_detalle = self.get_link_detalle(bloque)
        if not link_detalle:
            return ""

        try:
            r = self._session.get(link_detalle, timeout=20)
            if r.status_code != 200:
                return link_detalle
            soup_det = BeautifulSoup(r.text, "html.parser")
            link_texto = self._extraer_link_texto_desde_html(soup_det)
            return link_texto or link_detalle
        except Exception:
            return link_detalle

    def get_expediente(self, soup):
        try:
            spans = soup.find_all('span')
            for s in spans:
                if "Expediente" in s.text:
                    return s.text.split(":")[-1].strip()
            return "S/D"
        except Exception:
            return "S/D"

    def get_autor_info(self, soup):
        autor = "S/D"
        bloque = "S/D"
        provincia = "S/D"
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
        except Exception:
            return autor, bloque, provincia

    def get_fechaInicio(self, soup):
        try:
            spans = soup.find_all('span')
            for s in spans:
                if "Fecha" in s.text:
                    return s.text.split(":")[-1].strip()
            return "S/D"
        except Exception:
            return "S/D"

    def get_proyecto(self, soup):
        try:
            contenedor = soup.parent
            div = contenedor.find('div', class_='dp-texto')
            return div.text.strip() if div else "S/D"
        except Exception:
            return "S/D"

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
        except Exception:
            return "S/D"

    def scrape(self, url, expedientes_completos=None):
        ya_completos = expedientes_completos or set()
        print(f"Entrando a {url}")

        try:
            self.driver.get(url)
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
            except Exception:
                print("⚠️ Alerta: Pasaron 30 segundos y no aparecieron proyectos. Puede que no haya resultados o el sitio esté muy lento.")

            soup = BeautifulSoup(self.driver.page_source, 'html.parser')
            bloques = soup.find_all('div', class_='dp-metadata')

            print(f"Procesando {len(bloques)} proyectos encontrados...")

            if len(bloques) == 0:
                print("El scraping no encontró bloques de proyectos (len=0).")
                return pd.DataFrame(self.data)

            for bloque in bloques:
                exp = self.get_expediente(bloque)
                autor, partido, provincia = self.get_autor_info(bloque)

                # Solo hacemos petición HTTP al detalle si el proyecto es nuevo o requiere reanálisis
                if exp in ya_completos:
                    link_texto = self.get_link_detalle(bloque)
                else:
                    link_texto = self.get_link_texto(bloque)

                self.data.append({
                    'Cámara de Origen': self.get_origen(bloque),
                    'Expediente': exp,
                    'Autor': autor,
                    'Fecha de inicio': self.get_fechaInicio(bloque),
                    'Proyecto': self.get_proyecto(bloque),
                    'Comisiones': self.get_comisiones(bloque),
                    'Link Texto': link_texto,
                    'Estado': '',
                    'Probabilidad': '',
                    'Partido Político': partido,
                    'Provincia': provincia,
                    'Observaciones': ''
                })

            return pd.DataFrame(self.data)

        except Exception as e:
            print(f"❌ Error interactuando con la página: {e}")
            return None
        finally:
            self._cerrar_driver()
