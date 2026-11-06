import os
import re
import time
import random
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import urllib3
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
import pandas as pd

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class ScrapearBoletin:
    def __init__(self):
        self.base_url = "https://www.boletinoficial.gob.ar"
        self.fecha_target_str = ""

        if self.fecha_target_str:
            self.url_seccion = f"https://www.boletinoficial.gob.ar/seccion/primera/{self.fecha_target_str}"
        else:
            self.url_seccion = "https://www.boletinoficial.gob.ar/seccion/primera"

        self.headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language": "es-AR,es;q=0.9,en-US;q=0.8,en;q=0.7",
            "Referer": "https://www.boletinoficial.gob.ar/",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
        }

        self.timeout = (12, 30)
        self.session = requests.Session()
        self.session.verify = False
        self.proxy_pool = []

        retry_strategy = Retry(
            total=2,
            connect=2,
            read=2,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS"],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

        proxy_url = (os.environ.get("PROXY_URL") or os.environ.get("BOLETIN_PROXY_URL") or "").strip()
        if proxy_url:
            print("   🔒 Usando PROXY_URL configurado para Boletín Oficial.")
            self.session.proxies.update({
                "http": proxy_url,
                "https": proxy_url,
            })
            self.proxy_pool.append(proxy_url)

    def _descubrir_proxies_argentina(self):
        """
        Obtiene candidatos de proxies en Argentina (AR) desde fuentes públicas
        cuando la IP de GitHub Actions (EE.UU.) es bloqueada por el firewall de ARSAT.
        """
        candidatos = []

        # Fuente 1: ProxyScrape (AR)
        try:
            url_ps = (
                "https://api.proxyscrape.com/v2/"
                "?request=displayproxies&protocol=http&timeout=5000&country=AR&ssl=all&anonymity=all"
            )
            r = requests.get(url_ps, timeout=10)
            if r.status_code == 200:
                for line in r.text.splitlines():
                    p = line.strip()
                    if p and ":" in p:
                        candidatos.append(f"http://{p}")
        except Exception:
            pass

        # Fuente 2: GeoNode (AR)
        try:
            url_gn = (
                "https://proxylist.geonode.com/api/proxy-list"
                "?limit=30&page=1&sort_by=lastChecked&sort_type=desc&country=AR"
            )
            r = requests.get(url_gn, timeout=10)
            if r.status_code == 200:
                data = r.json().get("data", [])
                for item in data:
                    ip = item.get("ip")
                    port = item.get("port")
                    protocols = item.get("protocols") or ["http"]
                    if ip and port and ("http" in protocols or "https" in protocols):
                        candidatos.append(f"http://{ip}:{port}")
        except Exception:
            pass

        vistos = set()
        unicos = []
        for c in candidatos:
            if c not in vistos:
                vistos.add(c)
                unicos.append(c)
        return unicos

    def _obtener_portada_con_fallback(self):
        """
        Intenta conectar directo (o con PROXY_URL si está definido).
        Si da timeout por geobloqueo en GitHub Actions, busca y valida en paralelo
        proxies de Argentina y devuelve el HTML de la portada junto con el pool de proxies activos.
        """
        try:
            response = self.session.get(self.url_seccion, headers=self.headers, timeout=self.timeout)
            if response.status_code == 200 and "detalleAviso" in response.text:
                return response.text
        except Exception as e:
            print(f"   ⚠️ Conexión directa a Boletín Oficial falló ({type(e).__name__}). Activando fallback de proxies AR...")

        candidatos = self._descubrir_proxies_argentina()
        if not candidatos:
            print("   ❌ No se encontraron proxies candidatos en Argentina.")
            return None

        print(f"   🔎 Probando {len(candidatos)} proxies de Argentina en paralelo...")

        def probar_proxy(proxy_url):
            px = {"http": proxy_url, "https": proxy_url}
            try:
                resp = requests.get(
                    self.url_seccion,
                    headers=self.headers,
                    proxies=px,
                    verify=False,
                    timeout=(8, 15),
                )
                if resp.status_code == 200 and "detalleAviso" in resp.text:
                    resp.encoding = "utf-8"
                    return proxy_url, resp.text
            except Exception:
                pass
            return None

        html_portada = None
        proxies_validos = []

        with ThreadPoolExecutor(max_workers=15) as executor:
            futures = [executor.submit(probar_proxy, p) for p in candidatos[:45]]
            for fut in as_completed(futures):
                res = fut.result()
                if res:
                    proxy_ok, html = res
                    proxies_validos.append(proxy_ok)
                    if html_portada is None:
                        html_portada = html

        if proxies_validos:
            self.proxy_pool = proxies_validos
            principal = self.proxy_pool[0]
            self.session.proxies.update({"http": principal, "https": principal})
            print(f"   ✅ Conectado a Boletín Oficial mediante pool de {len(self.proxy_pool)} proxies AR.")
            return html_portada

        return None

    def _get_con_rotacion(self, url):
        """
        Realiza GET usando la sesión actual y, si falla y hay un pool de proxies AR,
        rota entre los proxies disponibles.
        """
        try:
            resp = self.session.get(url, headers=self.headers, timeout=(10, 20))
            if resp.status_code == 200:
                return resp
        except Exception:
            pass

        for proxy_url in list(self.proxy_pool):
            try:
                px = {"http": proxy_url, "https": proxy_url}
                resp = requests.get(
                    url,
                    headers=self.headers,
                    proxies=px,
                    verify=False,
                    timeout=(10, 20),
                )
                if resp.status_code == 200:
                    self.session.proxies.update(px)
                    return resp
            except Exception:
                continue

        return None

    def procesar_detalle_completo(self, link_detalle):
        """
        Entra al aviso y extrae: Texto, Organismo y Número de Norma real.
        """
        data = {
            "texto": "",
            "organismo": "Poder Ejecutivo",  # Valor por defecto
            "norma": "S/N",
        }

        try:
            time.sleep(random.uniform(0.3, 1.0))
            response = self._get_con_rotacion(link_detalle)

            if response and response.status_code == 200:
                response.encoding = "utf-8"
                soup = BeautifulSoup(response.text, "html.parser")

                # --- 1. EXTRAER METADATA (Organismo y Norma) ---
                header = soup.find("div", {"id": "tituloDetalleAviso"})
                if header:
                    # El H1 suele ser el Organismo (ej: BANCO CENTRAL...)
                    h1 = header.find("h1")
                    if h1:
                        data["organismo"] = h1.get_text(strip=True)

                    # El H2 suele ser la Norma (ej: Comunicación "A" 1234)
                    h2 = header.find("h2")
                    if h2:
                        data["norma"] = h2.get_text(strip=True)

                if data["norma"] == "S/N" or not data["norma"]:
                    data["norma"] = "Aviso Oficial"

                # --- 2. EXTRAER TEXTO ---
                contenido = (
                    soup.find("div", {"id": "cuerpoDetalleAviso"})
                    or soup.find("div", {"id": "avisodetalle"})
                    or soup.find("div", class_="detalle-aviso")
                )

                if contenido:
                    for tag in contenido(["script", "style"]):
                        tag.decompose()

                    texto = contenido.get_text(separator="\n", strip=True)
                    texto = texto.replace("Boletín Oficial de la República Argentina", "")
                    texto = re.sub(r"e\.\s+\d{2}/\d{2}/.*", "", texto)

                    data["texto"] = texto[:6000]

            if data["norma"] == "S/N" or not data["norma"]:
                data["norma"] = "Aviso Oficial"
            return data

        except Exception:
            if data["norma"] == "S/N" or not data["norma"]:
                data["norma"] = "Aviso Oficial"
            return data  # Devuelve defaults si falla

    def scrape(self, links_completos_existentes=None):
        print(f">>> Iniciando scraping ({self.fecha_target_str or 'HOY'})...")
        links_existentes = links_completos_existentes or {}

        try:
            html_portada = self._obtener_portada_con_fallback()
            if not html_portada:
                print("❌ Error: No se pudo acceder a la portada del Boletín Oficial (conexión directa y fallback agotados).")
                return pd.DataFrame()

            soup = BeautifulSoup(html_portada, "html.parser")
            links_brutos = soup.find_all("a", href=re.compile(r"/detalleAviso/primera/"))

            unique_norms = []
            seen_ids = set()

            for a in links_brutos:
                href = a.get("href")
                match = re.search(r"/primera/(\d+)/", href)
                if match:
                    id_aviso = match.group(1)
                    if id_aviso in seen_ids:
                        continue
                    seen_ids.add(id_aviso)

                    # Título preliminar del listado
                    titulo_listado = a.get_text(" ", strip=True)
                    if len(titulo_listado) < 5:
                        parent = a.find_parent("div", class_="row")
                        if parent:
                            titulo_listado = parent.get_text(" ", strip=True)

                    unique_norms.append({"id": id_aviso, "href": href, "titulo_listado": titulo_listado})

            if not unique_norms:
                print("📭 No hay normas.")
                return pd.DataFrame()

            print(f"   ✅ Procesando {len(unique_norms)} normas...")
            datos_procesados = []

            fecha_excel = datetime.now().strftime("%d/%m/%Y")
            if self.fecha_target_str:
                fecha_excel = datetime.strptime(self.fecha_target_str, "%Y%m%d").strftime("%d/%m/%Y")

            for i, item in enumerate(unique_norms):
                id_aviso = item["id"]
                link_completo = self.base_url + item["href"]
                titulo_final = item["titulo_listado"].replace("Ver", "").strip()

                # Si el link ya existe en la planilla con análisis IA válido, evitamos descargar el detalle de nuevo
                if link_completo in links_existentes:
                    fila_guardada = links_existentes[link_completo]
                    norma_guardada = str(fila_guardada[1]).strip() if len(fila_guardada) > 1 else "Aviso Oficial"
                    autor_guardado = str(fila_guardada[2]).strip() if len(fila_guardada) > 2 else "Poder Ejecutivo"
                    datos_procesados.append({
                        "ID": f"BO{id_aviso}",
                        "Origen": "Boletin Oficial",
                        "Expediente": norma_guardada,
                        "Autor": autor_guardado,
                        "Fecha de inicio": fecha_excel,
                        "Proyecto": f"TITULO: {titulo_final}",
                        "Comisiones": link_completo,
                    })
                    continue

                info_detalle = self.procesar_detalle_completo(link_completo)

                if info_detalle["texto"]:
                    contenido_ia = (
                        f"NORMA: {info_detalle['norma']}\n"
                        f"ORGANISMO: {info_detalle['organismo']}\n"
                        f"TITULO: {titulo_final}\n\n"
                        f"--- TEXTO ---\n{info_detalle['texto']}"
                    )
                else:
                    contenido_ia = f"TITULO: {titulo_final}\n(Texto no disponible)"

                datos_procesados.append({
                    "ID": f"BO{id_aviso}",
                    "Origen": "Boletin Oficial",
                    "Expediente": info_detalle["norma"],
                    "Autor": info_detalle["organismo"],
                    "Fecha de inicio": fecha_excel,
                    "Proyecto": contenido_ia,
                    "Comisiones": link_completo,
                })

            print(f"\n   {len(datos_procesados)} normas listas.")
            return pd.DataFrame(datos_procesados)

        except Exception as e:
            print(f"❌ Error: {e}")
            return pd.DataFrame()
