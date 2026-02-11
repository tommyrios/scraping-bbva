import os
import json
import time
from google import genai
from google.genai import types

from reporte import ReporteUI


class AnalistaLegislativo:
    def __init__(self):
        api_key = os.environ.get("GEMINI_API_KEY")
        self.client = genai.Client(api_key=api_key) if api_key else None
        self.ui = ReporteUI(year_footer=2026)

    def generar_link(self, origen, expediente):
        exp = (expediente or "").strip()
        if not exp:
            return ""
        o = (origen or "").lower()
        if "diput" in o:
            return f"https://www.google.com/search?q=site:diputados.gov.ar+%22{exp}%22"
        if "senad" in o:
            return f"https://www.google.com/search?q=site:senado.gob.ar+%22{exp}%22"
        if "bolet" in o:
            return f"https://www.google.com/search?q=site:boletinoficial.gob.ar+%22{exp}%22"
        return f"https://www.google.com/search?q=%22{exp}%22"

    def _seccion_esperada(self, origen: str, id_interno: str) -> str:
        o = (origen or "").lower()
        if "boletin" in o or str(id_interno).startswith("BO"):
            return "boletin"
        if "senado" in o:
            return "senado"
        return "diputados"

    def _normalizar_impacto(self, item: dict) -> str:
        val = str(item.get("impacto_nivel") or item.get("impacto") or "BAJO").upper().strip()
        for lvl in ("ALTO", "MEDIO", "BAJO"):
            if lvl in val:
                return lvl
        if val not in ("ALTO", "MEDIO", "BAJO"):
            return "BAJO"
        return val

    def _normalizar_categorias(self, item: dict) -> list:
        cats = item.get("categorias", [])
        if isinstance(cats, str):
            raw = cats.replace("|", "/")
            parts = [p.strip() for p in raw.split("/") if p.strip()]
            cats = parts
        if not isinstance(cats, list):
            cats = []
        out = []
        for c in cats:
            if not isinstance(c, str):
                continue
            cc = c.strip()
            if not cc:
                continue
            out.append(cc[:1].upper() + cc[1:].lower())
        seen = set()
        uniq = []
        for c in out:
            key = c.lower()
            if key in seen:
                continue
            seen.add(key)
            uniq.append(c)
            if len(uniq) >= 4:
                break
        return uniq

    def analizar_proyectos(self, filas_nuevas):
        if not filas_nuevas:
            return self.ui.empty("No se han detectado nuevas normas o proyectos para analizar en este momento."), []

        items_para_modelo = []
        meta_data_por_id = {}
        seccion_esperada_por_id = {}

        for fila in filas_nuevas:
            id_interno = str(fila[0]).strip()
            origen = str(fila[1]).strip()
            expediente = str(fila[2]).strip()
            autor = str(fila[3]).strip() if len(fila) > 3 else ""
            contenido_completo = str(fila[5])

            sec = self._seccion_esperada(origen, id_interno)
            seccion_esperada_por_id[id_interno] = sec

            link_en_fila = str(fila[6]).strip() if len(fila) > 6 else ""
            link = link_en_fila or self.generar_link(origen, expediente)

            titulo_simple = contenido_completo.split("\n")[0].replace("TITULO: ", "").replace("NORMA: ", "").strip()

            meta_data_por_id[id_interno] = {
                "titulo": titulo_simple,
                "link": link,
                "referencia": expediente,
                "autor": autor,
            }

            items_para_modelo.append({
                "id_interno": id_interno,
                "referencia": expediente,
                "descripcion": contenido_completo,
                "fuente": origen,
                "autor": autor,
                "seccion_esperada": sec
            })

        if not self.client:
            data_norm = {k: {"resumen": "", "items": []} for k in ("boletin", "diputados", "senado")}
            for it in items_para_modelo:
                data_norm[it["seccion_esperada"]]["items"].append({
                    "id_interno": it["id_interno"],
                    "referencia": it["referencia"],
                    "titulo_descriptivo": meta_data_por_id[it["id_interno"]]["titulo"],
                    "impacto_nivel": "BAJO",
                    "categorias": [],
                    "justificacion": "S/D"
                })
            html = self.ui.render(
                data_norm=data_norm,
                meta_data_por_id=meta_data_por_id,
                impacto_normalizer=self._normalizar_impacto,
                categorias_normalizer=self._normalizar_categorias
            )
            return html, []

        prompt = f"""
Actúa como un analista legislativo senior para Banco BBVA (Estilo Agencia de Noticias).
Analiza los siguientes items del Boletín Oficial y Congreso.

TU OBJETIVO: Precisión absoluta. Prohibido usar frases genéricas de relleno. No inventes datos.

REGLA DE CLASIFICACIÓN (OBLIGATORIA):
- Cada item del input trae el campo "seccion_esperada" con valor: "boletin" | "diputados" | "senado".
- Debes ubicar CADA item en la sección indicada por su "seccion_esperada". Está prohibido mover items a otra sección.

SALIDA OBLIGATORIA: Devuelve SOLO un JSON válido, sin texto adicional, con esta estructura EXACTA:
{{
  "boletin": {{"resumen": "...", "items": []}},
  "diputados": {{"resumen": "...", "items": []}},
  "senado": {{"resumen": "...", "items": []}}
}}

Cada item debe tener:
- id_interno
- referencia
- titulo_descriptivo
- impacto_nivel: ALTO|MEDIO|BAJO
- categorias: lista (1 a 4)
- justificacion (máx 2 líneas)

REGLAS DE IMPACTO:
- Toda RENUNCIA/ACEPTACIÓN DE RENUNCIA/CESE de funcionarios: ALTO.
- Toda DESIGNACIÓN/NOMBRAMIENTO de funcionarios: ALTO.

Datos a analizar:
{json.dumps(items_para_modelo, ensure_ascii=False)}
"""

        modelos = ["gemini-2.5-flash", "gemini-2.5-flash-lite", "gemini-2.0-flash", "gemini-2.0-flash-lite"]

        for modelo in modelos:
            for _ in range(3):
                try:
                    response = self.client.models.generate_content(
                        model=modelo,
                        contents=prompt,
                        config=types.GenerateContentConfig(response_mime_type="application/json")
                    )
                    data = json.loads(response.text)

                    secciones_keys = ["boletin", "diputados", "senado"]
                    data_norm = {k: {"resumen": "", "items": []} for k in secciones_keys}

                    for k in secciones_keys:
                        bloque = data.get(k, {}) if isinstance(data, dict) else {}
                        if isinstance(bloque, dict):
                            data_norm[k]["resumen"] = bloque.get("resumen", "") or ""

                    items_modelo = []
                    for k in secciones_keys:
                        bloque = data.get(k, {}) if isinstance(data, dict) else {}
                        if isinstance(bloque, dict):
                            items_k = bloque.get("items", [])
                            if isinstance(items_k, list):
                                items_modelo.extend(items_k)

                    for it in items_modelo:
                        if not isinstance(it, dict):
                            continue
                        id_ref = str(it.get("id_interno", "")).strip()
                        if not id_ref:
                            continue
                        k_esp = seccion_esperada_por_id.get(id_ref)
                        if k_esp not in secciones_keys:
                            k_esp = "boletin" if id_ref.startswith("BO") else "diputados"
                        data_norm[k_esp]["items"].append(it)

                    todos_los_detalles_para_excel = []
                    vistos = set()
                    for k in secciones_keys:
                        for it in data_norm.get(k, {}).get("items", []):
                            rid = str(it.get("id_interno", "")).strip()
                            if rid and rid not in vistos:
                                todos_los_detalles_para_excel.append(it)
                                vistos.add(rid)

                    html_output = self.ui.render(
                        data_norm=data_norm,
                        meta_data_por_id=meta_data_por_id,
                        impacto_normalizer=self._normalizar_impacto,
                        categorias_normalizer=self._normalizar_categorias
                    )
                    return html_output, todos_los_detalles_para_excel

                except Exception as e:
                    txt = str(e)
                    if any(x in txt for x in ("503", "overloaded", "429", "quota", "Resource has been exhausted")):
                        time.sleep(5)
                        continue
                    break

        return self.ui.empty("Ocurrió un error al procesar el análisis con Inteligencia Artificial."), []
