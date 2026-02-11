import os
import json
import time
from typing import List, Tuple, Dict, Any
from google import genai
from google.genai import types

from reporte import ReporteUI

class AnalistaLegislativo:
    def __init__(self):
        api_key = os.environ.get("GEMINI_API_KEY")
        self.client = genai.Client(api_key=api_key) if api_key else None

        self.ui = ReporteUI(logo_cid="cid:bbva_logo")

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

    def _generar_html_vacio(self, mensaje="Sin novedades relevantes en esta ejecución."):
        return f"""
<!DOCTYPE html>
<html><head><meta charset="utf-8"></head>
<body style="font-family:Segoe UI,Roboto,Arial,sans-serif; color:#333;">
  <div style="max-width:900px;margin:0 auto;padding:24px;">
    <h2>Reporte Regulatorio Diario</h2>
    <p>{mensaje}</p>
  </div>
</body></html>
"""

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
        return val if val in ("ALTO", "MEDIO", "BAJO") else "BAJO"

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

    def analizar_proyectos(self, filas_nuevas) -> Tuple[str, List[dict]]:
        """
        Retorna:
          - html_output (email)
          - todos_los_detalles_para_excel (lista dict)
        """
        if not self.client or not filas_nuevas:
            return self._generar_html_vacio(
                "No se han detectado nuevas normas o proyectos para analizar en este momento."
            ), []

        items_para_modelo = []
        meta_data_por_id: Dict[str, Dict[str, str]] = {}
        seccion_esperada_por_id: Dict[str, str] = {}

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

            titulo_simple = (
                contenido_completo.split("\n")[0]
                .replace("TITULO: ", "")
                .replace("NORMA: ", "")
                .strip()
            )

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
                "seccion_esperada": sec,
            })

        prompt = self._build_prompt(items_para_modelo)

        modelos = ["gemini-2.5-flash", "gemini-2.5-flash-lite", "gemini-2.0-flash", "gemini-2.0-flash-lite"]

        for modelo in modelos:
            for _ in range(3):
                try:
                    print(f"Usando modelo {modelo}")
                    response = self.client.models.generate_content(
                        model=modelo,
                        contents=prompt,
                        config=types.GenerateContentConfig(response_mime_type="application/json"),
                    )
                    data = json.loads(response.text)

                    data_norm = self._normalizar_y_reubicar(data, seccion_esperada_por_id)

                    todos_los_detalles_para_excel = []
                    vistos_excel = set()
                    for k in ("boletin", "diputados", "senado"):
                        for it in data_norm.get(k, {}).get("items", []) or []:
                            id_ref = str(it.get("id_interno", "")).strip()
                            if id_ref and id_ref not in vistos_excel:
                                todos_los_detalles_para_excel.append(it)
                                vistos_excel.add(id_ref)

                    html_output = self.ui.render(
                        data_norm=data_norm,
                        meta_data_por_id=meta_data_por_id,
                        normalizar_impacto_fn=self._normalizar_impacto,
                        normalizar_categorias_fn=self._normalizar_categorias,
                        email_filtra_niveles=("ALTO", "MEDIO"),
                    )
                    return html_output, todos_los_detalles_para_excel

                except Exception as e:
                    errores_saturacion = ["503", "overloaded", "429", "quota", "Resource has been exhausted"]
                    if any(err in str(e) for err in errores_saturacion):
                        time.sleep(5)
                        continue
                    print(f"❌ Error modelo: {e}")
                    break

        return self._generar_html_vacio(
            "Ocurrió un error al procesar el análisis con Inteligencia Artificial."
        ), []

    def _build_prompt(self, items_para_modelo: List[dict]) -> str:
        return f"""
Actúa como un analista legislativo senior para Banco BBVA (Estilo Agencia de Noticias / BLapp).
Analiza los siguientes items del Boletín Oficial y Congreso.

TU OBJETIVO: Precisión absoluta. Prohibido usar frases genéricas de relleno. No inventes datos.

REGLA DE CLASIFICACIÓN (OBLIGATORIA):
- Cada item del input trae el campo "seccion_esperada" con valor: "boletin" | "diputados" | "senado".
- Debes ubicar CADA item en la sección indicada por su "seccion_esperada". Está prohibido mover items a otra sección.

SALIDA OBLIGATORIA: Devuelve SOLO un JSON válido, sin texto adicional, con esta estructura EXACTA:
{{
  "boletin": {{
    "resumen": "Resumen ejecutivo (máx 3 líneas) con lo más destacado del día, con hechos concretos.",
    "items": [{{"id_interno":"...","referencia":"...","titulo_descriptivo":"...","impacto_nivel":"ALTO|MEDIO|BAJO","categorias":["Laboral"],"justificacion":"..."}}]
  }},
  "diputados": {{
    "resumen": "Resumen ejecutivo (máx 3 líneas) de actividad parlamentaria.",
    "items": []
  }},
  "senado": {{
    "resumen": "Resumen ejecutivo (máx 3 líneas) de actividad parlamentaria.",
    "items": []
  }}
}}

INSTRUCCIONES DE REDACCIÓN (OBLIGATORIAS):
1) "titulo_descriptivo" (titular breve, periodístico)
- Si es NOMBRAMIENTO/DESIGNACIÓN: "Designación de [NOMBRE] como [CARGO] en [ORGANISMO]".
- Si es RENUNCIA/CESE: "Aceptan renuncia de [NOMBRE] como [CARGO] en [ORGANISMO]".
- Si es NORMATIVA: "Cambios en [TEMA PRINCIPAL]".
- No uses "IMPACTO ..." dentro del título.

2) "justificacion" (máx 2 líneas)
- QUÉ pasa y POR QUÉ importa. Sin relleno.
- No inventes datos. Si falta, decí "No especifica".

REGLAS DE IMPACTO (OBLIGATORIAS):
- "impacto_nivel" SOLO puede ser: "ALTO", "MEDIO" o "BAJO".
- Toda RENUNCIA/CESE y toda DESIGNACIÓN/NOMBRAMIENTO: "ALTO".

CATEGORÍAS:
- Lista 1 a 4 con inicial mayúscula.

Datos a analizar:
{json.dumps(items_para_modelo, ensure_ascii=False)}
"""

    def _normalizar_y_reubicar(self, data: Any, seccion_esperada_por_id: Dict[str, str]) -> Dict[str, Dict[str, Any]]:
        secciones_keys = ["boletin", "diputados", "senado"]
        data_norm = {k: {"resumen": "", "items": []} for k in secciones_keys}

        # resúmenes
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

        return data_norm
