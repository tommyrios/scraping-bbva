import time
import re
import requests
import unicodedata
from urllib.parse import urljoin

import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from bs4 import BeautifulSoup


class ScrapearSenado:
    BASE_URL = "https://www.senado.gob.ar"

    def __init__(self):
        print("Inicializando robot Senado...")
        options = Options()
        options.add_argument("--headless=new")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
        options.add_argument("--window-size=1920,1080")
        options.add_argument(
            "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/120.0 Safari/537.36"
        )
        options.page_load_strategy = "eager"

        self.driver = webdriver.Chrome(
            service=ChromeService(ChromeDriverManager().install()),
            options=options
        )

        self.data = []
        self.mapa_datos_senadores = {}

        self._session = requests.Session()
        self._session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                          "(KHTML, like Gecko) Chrome/120.0 Safari/537.36"
        })

    def limpiar_texto(self, texto):
        if not texto:
            return "S/D"
        t = " ".join(str(texto).split()).strip()
        return t if t else "S/D"

    def _strip_accents(self, s: str) -> str:
        return "".join(c for c in unicodedata.normalize("NFKD", s) if not unicodedata.combining(c))

    def _normalizar_nombre_key(self, nombre: str) -> str:
        nombre = self.limpiar_texto(nombre)
        if nombre == "S/D":
            return ""
        n = self._strip_accents(nombre).upper()
        n = re.sub(r"\s+", " ", n).strip()
        n = n.replace(".", "")
        if "," in n:
            partes = [p.strip() for p in n.split(",", 1)]
            if len(partes) == 2 and partes[0] and partes[1]:
                n = f"{partes[0]} {partes[1]}"
        n = re.sub(r"[^A-Z0-9 Ñ]", " ", n)
        n = re.sub(r"\s+", " ", n).strip()
        return n

    def obtener_diccionario_partidos(self):
        url_lista = f"{self.BASE_URL}/senadores/listados/listaSenadoRes"
        print(f"Mapeando senadores desde {url_lista}")
        try:
            self.driver.get(url_lista)
            WebDriverWait(self.driver, 20).until(lambda d: "<table" in (d.page_source or "").lower())
            soup = BeautifulSoup(self.driver.page_source, "html.parser")
            tabla = soup.find("table", id="senadoresTabla") or soup.find("table")
            if not tabla:
                return
            tbody = tabla.find("tbody") or tabla

            for tr in tbody.find_all("tr"):
                tds = tr.find_all("td")
                if len(tds) < 4:
                    continue

                a_nombre = tds[1].find("a")
                nombre_raw = self.limpiar_texto(a_nombre.get_text(" ", strip=True) if a_nombre else tds[1].get_text(" ", strip=True))
                provincia = self.limpiar_texto(tds[2].get_text(" ", strip=True)).upper()
                partido = self.limpiar_texto(tds[3].get_text(" ", strip=True)).upper()

                key = self._normalizar_nombre_key(nombre_raw)
                if key:
                    self.mapa_datos_senadores[key] = {"provincia": provincia, "partido": partido}
        except Exception:
            return

    def _formatear_expediente(self, exp: str, tipo: str) -> str:
        exp = self.limpiar_texto(exp)
        tipo = self.limpiar_texto(tipo)
        if exp == "S/D" or tipo == "S/D":
            return exp
        try:
            if "/" in exp:
                n, anio = exp.split("/", 1)
                anio2 = anio[-2:] if len(anio) >= 2 else anio
                return f"{n}-{tipo}-{anio2}"
        except Exception:
            pass
        return exp

    def _extraer_items_listado(self, soup_listado: BeautifulSoup):
        items = []
        for a in soup_listado.find_all("a", href=True):
            href = (a.get("href") or "").strip()
            if "verExp" not in href:
                continue
            tr = a.find_parent("tr")
            if not tr:
                continue
            tds = tr.find_all("td")
            if len(tds) < 4:
                continue
            exp_raw = self.limpiar_texto(a.get_text())
            tipo = self.limpiar_texto(tds[1].get_text())
            origen_sigla = self.limpiar_texto(tds[2].get_text())
            extracto = self.limpiar_texto(tds[3].get_text())
            url_detalle = urljoin(self.BASE_URL, href)
            expediente_id = self._formatear_expediente(exp_raw, tipo)
            items.append({
                "url_detalle": url_detalle,
                "expediente_raw": exp_raw,
                "expediente_id": expediente_id,
                "tipo": tipo,
                "origen_sigla": origen_sigla,
                "extracto": extracto
            })
        uniq = {}
        for it in items:
            uniq[it["url_detalle"]] = it
        return list(uniq.values())

    def _get_autores(self, soup_det: BeautifulSoup):
        autores_div = soup_det.find("div", {"role": "tabpanel", "id": "Autores"}) or soup_det.find(id=re.compile(r"^Autores$", re.IGNORECASE))
        if not autores_div:
            return ["S/D"]
        autores = []
        for a in autores_div.find_all("a", href=True):
            title = (a.get("title") or "").strip()
            txt = self.limpiar_texto(a.get_text())
            cand = self.limpiar_texto(title or txt)
            if cand and cand != "S/D":
                autores.append(cand)
        return autores if autores else ["S/D"]

    def _get_fecha_inicio(self, soup_det: BeautifulSoup):
        h2 = soup_det.find(lambda tag: tag.name in ("h2", "h3") and "mesa de entradas" in tag.get_text(strip=True).lower())
        if not h2:
            return "S/D"
        tabla = h2.find_next("table")
        if not tabla:
            return "S/D"
        tr = (tabla.find("tbody") or tabla).find("tr")
        if not tr:
            return "S/D"
        tds = tr.find_all("td")
        if not tds:
            return "S/D"
        return self.limpiar_texto(tds[0].get_text())

    def _get_comisiones(self, soup_det: BeautifulSoup):
        h2 = soup_det.find(lambda tag: tag.name in ("h2", "h3", "h4") and "giros del expediente a comisiones" in tag.get_text(strip=True).lower())
        if not h2:
            return "S/D"
        tabla = h2.find_next("table")
        if not tabla:
            return "S/D"
        filas = (tabla.find("tbody") or tabla).find_all("tr")
        coms = []
        for f in filas:
            tds = f.find_all("td")
            if not tds:
                continue
            raw = self.limpiar_texto(tds[0].get_text(" ", strip=True))
            raw = re.split(r"\bORDEN DE GIRO\b", raw, flags=re.IGNORECASE)[0].strip()
            if raw and raw != "S/D":
                coms.append(raw)
        return ", ".join(coms) if coms else "S/D"

    def _get_proyecto(self, soup_det: BeautifulSoup, fallback_extracto="S/D"):
        tabla = soup_det.find("table", class_=re.compile(r"table-bordered"))
        if not tabla:
            return fallback_extracto
        tr = (tabla.find("tbody") or tabla).find("tr")
        if not tr:
            return fallback_extracto
        tds = tr.find_all("td")
        if len(tds) >= 4:
            return self.limpiar_texto(tds[3].get_text())
        return fallback_extracto

    def _get_link_texto_original(self, soup_det: BeautifulSoup, url_detalle: str):
        div = soup_det.find(id=re.compile(r"textoOriginal", re.IGNORECASE))
        if div:
            a = div.find("a", href=True)
            if a:
                href = (a.get("href") or "").strip()
                if href:
                    return urljoin(self.BASE_URL, href)
        for a in soup_det.find_all("a", href=True):
            href = (a.get("href") or "").strip()
            if "downloadpdf" in href.lower():
                return urljoin(self.BASE_URL, href)
        return url_detalle

    def extraer_detalle_proyecto(self, url_detalle: str, fallback_extracto="S/D"):
        try:
            r = self._session.get(url_detalle, timeout=30, allow_redirects=True)
            if r.status_code != 200:
                return None
            soup = BeautifulSoup(r.text, "html.parser")
            autores = self._get_autores(soup)
            autor_principal = autores[0] if autores else "S/D"
            if autores and len(autores) > 1 and autor_principal != "S/D":
                autor_principal = f"{autor_principal} Y OTROS"
            fecha_inicio = self._get_fecha_inicio(soup)
            proyecto = self._get_proyecto(soup, fallback_extracto=fallback_extracto)
            comisiones = self._get_comisiones(soup)
            link_texto = self._get_link_texto_original(soup, url_detalle)
            return {
                "Autor": autor_principal,
                "Fecha de inicio": fecha_inicio,
                "Proyecto": proyecto,
                "Comisiones": comisiones,
                "Link Texto": link_texto
            }
        except Exception:
            return None

    def scrape(self):
        self.obtener_diccionario_partidos()
        url_listado = "https://www.senado.gob.ar/parlamentario/parlamentaria/avanzada?cantRegistros=100"
        print(f"Entrando a {url_listado}")

        try:
            self.driver.get(url_listado)
            WebDriverWait(self.driver, 30).until(
                lambda d: ("verExp" in (d.page_source or "")) or ("<table" in (d.page_source or "").lower())
            )
            soup_listado = BeautifulSoup(self.driver.page_source, "html.parser")
            items = self._extraer_items_listado(soup_listado)
            if not items:
                self.driver.quit()
                return pd.DataFrame(self.data)
        except Exception:
            self.driver.quit()
            return pd.DataFrame(self.data)

        for it in items:
            info = self.extraer_detalle_proyecto(it["url_detalle"], fallback_extracto=it.get("extracto", "S/D"))
            if not info:
                continue

            autor_para_mostrar = info.get("Autor", "S/D") or "S/D"
            autor_para_buscar = autor_para_mostrar.replace(" Y OTROS", "").strip()
            key_autor = self._normalizar_nombre_key(autor_para_buscar)

            datos_extra = self.mapa_datos_senadores.get(key_autor, {"partido": "", "provincia": ""})

            self.data.append({
                "Cámara de Origen": "Senado",
                "Expediente": it.get("expediente_id", "S/D"),
                "Autor": autor_para_mostrar,
                "Fecha de inicio": info.get("Fecha de inicio", "S/D"),
                "Proyecto": info.get("Proyecto", "S/D"),
                "Comisiones": info.get("Comisiones", "S/D"),
                "Link Texto": info.get("Link Texto", it["url_detalle"]),
                "Estado": "",
                "Probabilidad": "",
                "Partido Político": datos_extra.get("partido", ""),
                "Provincia": datos_extra.get("provincia", ""),
                "Observaciones": ""
            })

            time.sleep(0.25)

        self.driver.quit()
        return pd.DataFrame(self.data)
