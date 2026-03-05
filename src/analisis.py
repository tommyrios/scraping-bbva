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
        iid = str(id_interno or "")
        if "bolet" in o or iid.startswith("BO"):
            return "boletin"
        if "senad" in o:
            return "senado"
        return "diputados"

    def _normalizar_impacto(self, item: dict) -> str:
        val = str(item.get("impacto_nivel") or item.get("impacto") or "BAJO").upper().strip()
        for lvl in ("ALTO", "MEDIO", "BAJO"):
            if lvl in val:
                return lvl
        return "BAJO"

    def _normalizar_categorias(self, item: dict) -> list:
        cats = item.get("categorias", [])
        if isinstance(cats, str):
            raw = cats.replace("|", "/")
            cats = [p.strip() for p in raw.split("/") if p.strip()]
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

    def _ui_empty_safe(self, mensaje: str) -> str:
        if hasattr(self.ui, "empty") and callable(getattr(self.ui, "empty")):
            try:
                return self.ui.empty(mensaje)
            except Exception:
                pass
        if hasattr(self.ui, "render") and callable(getattr(self.ui, "render")):
            try:
                data_norm = {k: {"resumen": mensaje, "items": []} for k in ("boletin", "diputados", "senado")}
                return self.ui.render(
                    data_norm=data_norm,
                    meta_data_por_id={},
                    impacto_normalizer=self._normalizar_impacto,
                    categorias_normalizer=self._normalizar_categorias,
                )
            except Exception:
                pass
        return f"<p>{mensaje}</p>"

    def _coerce_text(self, v) -> str:
        if v is None:
            return ""
        if isinstance(v, (int, float, bool)):
            return str(v)
        return str(v)

    def _safe_row_get(self, fila, idx, default=""):
        try:
            return fila[idx]
        except Exception:
            return default

    def _normalize_id(self, v) -> str:
        s = self._coerce_text(v).strip()
        return s

    def _extract_title(self, contenido: str) -> str:
        txt = self._coerce_text(contenido)
        first = txt.split("\n")[0].strip() if txt else ""
        if not first:
            return ""
        for prefix in ("TITULO:", "TITULO", "NORMA:", "NORMA"):
            if first.upper().startswith(prefix):
                first = first[len(prefix):].strip(" :")
                break
        return first.strip()

    def _fallback_analysis(self, items_para_modelo, meta_data_por_id):
        secciones = ("boletin", "diputados", "senado")
        data_norm = {k: {"resumen": "", "items": []} for k in secciones}
        detalles = []
        for it in items_para_modelo:
            sec = it.get("seccion_esperada") or "diputados"
            if sec not in secciones:
                sec = "diputados"
            iid = str(it.get("id_interno") or "").strip()
            ref = str(it.get("referencia") or "").strip()
            titulo = meta_data_por_id.get(iid, {}).get("titulo") or self._extract_title(it.get("descripcion") or "")
            obj = {
                "id_interno": iid,
                "referencia": ref,
                "titulo_descriptivo": titulo or ref or iid,
                "impacto_nivel": "BAJO",
                "categorias": [],
                "justificacion": "S/D",
            }
            data_norm[sec]["items"].append(obj)
            detalles.append(obj)
        html = self.ui.render(
            data_norm=data_norm,
            meta_data_por_id=meta_data_por_id,
            impacto_normalizer=self._normalizar_impacto,
            categorias_normalizer=self._normalizar_categorias,
        )
        return html, detalles

    def _normalize_model_output(self, data, seccion_esperada_por_id):
        secciones = ("boletin", "diputados", "senado")
        data_norm = {k: {"resumen": "", "items": []} for k in secciones}
        if not isinstance(data, dict):
            return data_norm

        for k in secciones:
            bloque = data.get(k, {})
            if isinstance(bloque, dict):
                data_norm[k]["resumen"] = self._coerce_text(bloque.get("resumen") or "").strip()

        items_flat = []
        for k in secciones:
            bloque = data.get(k, {})
            if isinstance(bloque, dict):
                items_k = bloque.get("items", [])
                if isinstance(items_k, list):
                    items_flat.extend(items_k)

        for it in items_flat:
            if not isinstance(it, dict):
                continue
            iid = str(it.get("id_interno") or "").strip()
            if not iid:
                continue
            sec = seccion_esperada_por_id.get(iid)
            if sec not in secciones:
                sec = "boletin" if iid.startswith("BO") else "diputados"
            data_norm[sec]["items"].append(it)

        return data_norm

    def _unique_detalles(self, data_norm):
        secciones = ("boletin", "diputados", "senado")
        vistos = set()
        out = []
        for k in secciones:
            items = data_norm.get(k, {}).get("items", [])
            if not isinstance(items, list):
                continue
            for it in items:
                if not isinstance(it, dict):
                    continue
                iid = str(it.get("id_interno") or "").strip()
                if not iid or iid in vistos:
                    continue
                vistos.add(iid)
                out.append(it)
        return out

    def analizar_proyectos(self, filas_nuevas):
        if not filas_nuevas or not isinstance(filas_nuevas, list):
            return self._ui_empty_safe("No se han detectado nuevas normas o proyectos para analizar en este momento."), []

        items_para_modelo = []
        meta_data_por_id = {}
        seccion_esperada_por_id = {}

        for fila in filas_nuevas:
            if fila is None:
                continue
            id_interno = self._normalize_id(self._safe_row_get(fila, 0, ""))
            origen = self._coerce_text(self._safe_row_get(fila, 1, "")).strip()
            expediente = self._coerce_text(self._safe_row_get(fila, 2, "")).strip()
            autor = self._coerce_text(self._safe_row_get(fila, 3, "")).strip()
            contenido_completo = self._coerce_text(self._safe_row_get(fila, 5, ""))

            if not id_interno:
                continue

            sec = self._seccion_esperada(origen, id_interno)
            seccion_esperada_por_id[id_interno] = sec

            link_en_fila = self._coerce_text(self._safe_row_get(fila, 6, "")).strip()
            link = link_en_fila or self.generar_link(origen, expediente)

            titulo_simple = self._extract_title(contenido_completo) or expediente or id_interno

            meta_data_por_id[id_interno] = {
                "titulo": titulo_simple,
                "link": link,
                "referencia": expediente,
                "autor": autor,
            }

            items_para_modelo.append(
                {
                    "id_interno": id_interno,
                    "referencia": expediente,
                    "descripcion": contenido_completo,
                    "fuente": origen,
                    "autor": autor,
                    "seccion_esperada": sec,
                }
            )

        if not items_para_modelo:
            return self._ui_empty_safe("No se han detectado nuevas normas o proyectos para analizar en este momento."), []

        if not self.client:
            return self._fallback_analysis(items_para_modelo, meta_data_por_id)

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
""".strip()

        modelos = ["gemini-2.5-flash", "gemini-2.5-flash-lite", "gemini-2.0-flash", "gemini-2.0-flash-lite"]
        retryable_markers = ("503", "overloaded", "429", "quota", "Resource has been exhausted", "temporarily")

        last_err = None
        for modelo in modelos:
            for _ in range(3):
                try:
                    response = self.client.models.generate_content(
                        model=modelo,
                        contents=prompt,
                        config=types.GenerateContentConfig(response_mime_type="application/json"),
                    )
                    raw = getattr(response, "text", None)
                    if not raw:
                        raise ValueError("Respuesta vacía del modelo")
                    data = json.loads(raw)
                    data_norm = self._normalize_model_output(data, seccion_esperada_por_id)
                    detalles = self._unique_detalles(data_norm)

                    html_output = self.ui.render(
                        data_norm=data_norm,
                        meta_data_por_id=meta_data_por_id,
                        impacto_normalizer=self._normalizar_impacto,
                        categorias_normalizer=self._normalizar_categorias,
                    )
                    return html_output, detalles

                except Exception as e:
                    last_err = e
                    txt = str(e)
                    if any(m in txt for m in retryable_markers):
                        time.sleep(5)
                        continue
                    break

        try:
            return self._fallback_analysis(items_para_modelo, meta_data_por_id)
        except Exception:
            msg = f"Ocurrió un error al procesar el análisis con Inteligencia Artificial."
            if last_err:
                msg = f"{msg} ({str(last_err)[:160]})"
            return self._ui_empty_safe(msg), []
