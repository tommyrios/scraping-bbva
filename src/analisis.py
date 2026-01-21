import os
import json
import time
from google import genai
from google.genai import types


class AnalistaLegislativo:
    def __init__(self):
        api_key = os.environ.get("GEMINI_API_KEY")
        self.client = genai.Client(api_key=api_key) if api_key else None

    def generar_link(self, origen, expediente):
        """Fallback si no contamos con un link real en la corrida."""
        exp = (expediente or "").strip()
        if not exp:
            return ""
        if "Diputados" in (origen or ""):
            return f"https://www.google.com/search?q=site:diputados.gov.ar+%22{exp}%22"
        if "Senado" in (origen or ""):
            return f"https://www.google.com/search?q=site:senado.gob.ar+%22{exp}%22"
        return ""

    def _generar_html_header(self):
        LOGO_URL = "cid:bbva_logo"

        return f'''
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="utf-8">
            <style>
                body {{
                    font-family: 'Segoe UI', 'Roboto', Helvetica, Arial, sans-serif;
                    color: #333;
                    line-height: 1.6;
                    background-color: #ffffff;
                    margin: 0;
                    padding: 0;
                }}
                .container {{
                    width: 100%;
                    max-width: 100%;
                    margin: 0;
                    background: #fff;
                }}
                .header {{
                    background-color: #072146;
                    color: white;
                    padding: 30px 5%;
                }}
                .logo-row {{
                    text-align: left;
                    margin-bottom: 20px;
                    width: 100%;
                }}
                .logo-img {{
                    height: 30px;
                    width: auto;
                    display: block;
                    border: 0;
                    outline: none;
                    text-decoration: none;
                }}
                .title-row {{
                    text-align: center;
                    width: 100%;
                }}
                .header h1 {{
                    margin: 0;
                    font-size: 26px;
                    font-weight: 500;
                    letter-spacing: 0.5px;
                    color: #ffffff;
                }}
                .header h2 {{
                    margin: 8px 0 0;
                    font-size: 13px;
                    opacity: 0.8;
                    font-weight: 400;
                    text-transform: uppercase;
                    letter-spacing: 2px;
                    color: #a4c4e0;
                }}
                .content {{
                    padding: 40px 5%;
                    background-color: #ffffff;
                }}
                .section-title {{
                    color: #072146;
                    border-bottom: 3px solid #072146;
                    padding-bottom: 10px;
                    margin-top: 40px;
                    margin-bottom: 25px;
                    font-size: 18px;
                    font-weight: 700;
                    text-transform: uppercase;
                }}
                .resumen-block {{
                    background-color: #f4f8fb;
                    border-left: 5px solid #1973b8;
                    padding: 20px;
                    margin-bottom: 30px;
                    font-style: italic;
                    color: #444;
                    font-size: 15px;
                }}
                .item {{
                    margin-bottom: 35px;
                    padding-bottom: 25px;
                    border-bottom: 1px solid #eeeeee;
                }}
                .item:last-child {{ border-bottom: none; }}
                .badges-row {{ margin-bottom: 12px; }}
                .badge {{
                    padding: 6px 12px;
                    border-radius: 4px;
                    font-size: 11px;
                    font-weight: 700;
                    text-transform: uppercase;
                    display: inline-block;
                    vertical-align: middle;
                    margin-right: 8px;
                }}
                .bg-alto {{ background-color: #da3851; color: white; }}
                .bg-medio {{ background-color: #f8cd51; color: #121212; }}
                .bg-ref {{ background-color: #f2f2f2; color: #555; border: 1px solid #ddd; }}
                .item-title {{
                    font-size: 18px;
                    font-weight: 700;
                    color: #121212;
                    margin: 0 0 10px 0;
                    line-height: 1.4;
                }}
                .justificacion {{
                    font-size: 15px;
                    color: #444;
                    margin-bottom: 15px;
                    text-align: left;
                    line-height: 1.6;
                }}
                .btn-link {{
                    display: inline-block;
                    font-size: 12px;
                    color: #004481;
                    text-decoration: none;
                    font-weight: 700;
                    border: 2px solid #004481;
                    padding: 10px 20px;
                    border-radius: 4px;
                    transition: background 0.2s;
                    text-transform: uppercase;
                }}
                .btn-link:hover {{
                    background-color: #004481;
                    color: white;
                }}
                .empty-state {{ text-align: center; padding: 40px 0; color: #666; }}
                .empty-icon {{ font-size: 40px; margin-bottom: 15px; display: block; opacity: 0.5; }}
                .footer {{
                    background-color: #f9f9f9;
                    padding: 30px 5%;
                    text-align: center;
                    font-size: 12px;
                    color: #999;
                    border-top: 1px solid #eaeaea;
                }}
            </style>
        </head>
        <body>
            <div class="container">
                <div class="header">
                    <div class="logo-row">
                        <img src="{LOGO_URL}" alt="BBVA" class="logo-img">
                    </div>
                    <div class="title-row">
                        <h1>Reporte Regulatorio Diario</h1>
                        <h2>Sistema de Monitoreo de Asuntos Públicos</h2>
                    </div>
                </div>
                <div class="content">
        '''

    def _generar_html_footer(self):
        return """
                </div>
                <div class="footer">
                    &copy; 2026 BBVA Argentina • Generado por Inteligencia Artificial (Gemini)
                </div>
            </div>
        </body>
        </html>
        """

    def _generar_html_vacio(self, mensaje="Sin novedades relevantes en esta ejecución."):
        html = self._generar_html_header()
        html += f"""
            <div class="empty-state">
                <span class="empty-icon">✅</span>
                <h3>Sin Novedades</h3>
                <p>{mensaje}</p>
            </div>
        """
        html += self._generar_html_footer()
        return html

    def analizar_proyectos(self, filas_nuevas):
        if not self.client or not filas_nuevas:
            return self._generar_html_vacio("No se han detectado nuevas normas o proyectos para analizar en este momento."), []

        def seccion_esperada(origen: str, id_interno: str) -> str:
            o = (origen or "").lower()
            if "boletin" in o or str(id_interno).startswith("BO"):
                return "boletin"
            if "senado" in o:
                return "senado"
            return "diputados"

        items_para_modelo = []
        meta_data_por_id = {}
        seccion_esperada_por_id = {}

        for fila in filas_nuevas:
            id_interno = str(fila[0]).strip()
            origen = str(fila[1]).strip()
            expediente = str(fila[2]).strip()
            contenido_completo = str(fila[5])

            sec = seccion_esperada(origen, id_interno)
            seccion_esperada_por_id[id_interno] = sec

            link_en_fila = str(fila[6]).strip() if len(fila) > 6 else ""
            link = link_en_fila or self.generar_link(origen, expediente)

            titulo_simple = contenido_completo.split('\n')[0].replace("TITULO: ", "").replace("NORMA: ", "").strip()

            meta_data_por_id[id_interno] = {
                "titulo": titulo_simple,
                "link": link,
                "referencia": expediente
            }

            items_para_modelo.append({
                "id_interno": id_interno,
                "referencia": expediente,
                "descripcion": contenido_completo,
                "fuente": origen,
                "seccion_esperada": sec
            })

        prompt = f"""
Actúa como un analista legislativo senior para Banco BBVA (Estilo Agencia de Noticias / BLapp).
Analiza los siguientes items del Boletín Oficial y Congreso.

TU OBJETIVO: Precisión absoluta. Prohibido usar frases genéricas de relleno.

Devuelve un JSON con esta estructura exacta:
{{
  "boletin": {{
    "resumen": "...",
    "items": [{{"id_interno":"...","referencia":"...","titulo_descriptivo":"...","impacto":"...","justificacion":"..."}}]
  }},
  "diputados": {{ "resumen": "...", "items": [] }},
  "senado": {{ "resumen": "...", "items": [] }}
}}

REGLA DE CLASIFICACIÓN (OBLIGATORIA):
- Cada item del input trae el campo "seccion_esperada" con valor: "boletin" | "diputados" | "senado".
- Debes ubicar CADA item en la sección indicada por su "seccion_esperada". Está prohibido mover items a otra sección.

Datos a analizar:
{json.dumps(items_para_modelo, ensure_ascii=False)}
"""

        modelos = ["gemini-2.5-flash", "gemini-2.5-flash-lite", "gemini-2.0-flash", "gemini-2.0-flash-lite"]

        for modelo in modelos:
            for _ in range(3):
                try:
                    print(f"Usando modelo {modelo}")
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
                            data_norm[k]["resumen"] = bloque.get("resumen", "")

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

                    html_output = self._generar_html_header()

                    todos_los_detalles_para_excel = []
                    vistos_excel = set()

                    secciones = [("Boletín Oficial", "boletin"), ("Diputados", "diputados"), ("Senado", "senado")]
                    hay_contenido = False

                    for titulo_seccion, key_json in secciones:
                        bloque = data_norm.get(key_json, {})
                        items = bloque.get("items", [])
                        resumen = bloque.get("resumen", "")

                        for it in items:
                            id_ref = str(it.get("id_interno", "")).strip()
                            if id_ref and id_ref not in vistos_excel:
                                todos_los_detalles_para_excel.append(it)
                                vistos_excel.add(id_ref)

                        items_email = [p for p in items if str(p.get("impacto", "BAJO")).upper() != "BAJO"]
                        if not items_email:
                            continue

                        hay_contenido = True
                        html_output += f'<div class="section-title">{titulo_seccion}</div>'
                        if resumen:
                            html_output += f'<div class="resumen-block">{resumen}</div>'

                        orden = {"ALTO": 1, "MEDIO": 2}
                        items_ordenados = sorted(items_email, key=lambda x: orden.get(str(x.get("impacto", "MEDIO")).upper(), 99))

                        for p in items_ordenados:
                            id_ref = str(p.get("id_interno", "")).strip()
                            meta = meta_data_por_id.get(id_ref, {})

                            titulo_mostrar = p.get("titulo_descriptivo") or meta.get("titulo") or "Sin título"
                            ref = p.get("referencia") or meta.get("referencia") or ""
                            link_web = meta.get("link") or "#"
                            justificacion = p.get("justificacion", "")
                            impacto = str(p.get("impacto", "MEDIO")).upper()

                            clase_badge = "bg-alto" if impacto == "ALTO" else "bg-medio"

                            html_output += f"""
                            <div class="item">
                                <div class="badges-row">
                                    <span class="badge bg-ref">{ref}</span>
                                    <span class="badge {clase_badge}">IMPACTO {impacto}</span>
                                </div>
                                <div class="item-title">{titulo_mostrar}</div>
                                <div class="justificacion">{justificacion}</div>
                                <a href="{link_web}" target="_blank" class="btn-link">Ver Texto Oficial &rarr;</a>
                            </div>
                            """

                    if not hay_contenido:
                        html_output += """
                        <div class="empty-state">
                            <span class="empty-icon">✅</span>
                            <h3>Sin Novedades de Impacto</h3>
                            <p>No hubo items con impacto <b>Alto</b> o <b>Medio</b>.</p>
                        </div>
                        """

                    html_output += self._generar_html_footer()
                    return html_output, todos_los_detalles_para_excel

                except Exception as e:
                    errores_saturacion = ["503", "overloaded", "429", "quota", "Resource has been exhausted"]
                    if any(err in str(e) for err in errores_saturacion):
                        time.sleep(5)
                        continue
                    print(f"❌ Error modelo: {e}")
                    break

        return self._generar_html_vacio("Ocurrió un error al procesar el análisis con Inteligencia Artificial."), []