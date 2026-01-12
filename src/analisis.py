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
        exp = expediente.strip()
        if "Diputados" in origen:
            return f"https://www.google.com/search?q=site:diputados.gov.ar+%22{exp}%22"
        elif "Senado" in origen:
            return f"https://www.senado.gob.ar/parlamentario/comisiones/verExp/{exp}"
        return ""

    def _generar_html_header(self):
        """Genera el encabezado HTML con estilos CSS en línea (Email-safe)."""
        return """
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="utf-8">
            <style>
                body { font-family: 'Segoe UI', Arial, sans-serif; color: #333; line-height: 1.6; background-color: #f4f4f4; margin: 0; padding: 0; }
                .container { max-width: 800px; margin: 20px auto; background: #fff; border-radius: 8px; overflow: hidden; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }
                .header { background-color: #004481; color: white; padding: 25px; text-align: center; }
                .header h1 { margin: 0; font-size: 24px; font-weight: normal; letter-spacing: 0.5px; }
                .header h2 { margin: 5px 0 0; font-size: 14px; opacity: 0.9; font-weight: normal; }
                .content { padding: 30px; }
                
                /* Secciones */
                .section-title { color: #004481; border-bottom: 2px solid #004481; padding-bottom: 5px; margin-top: 30px; margin-bottom: 20px; font-size: 18px; font-weight: bold; text-transform: uppercase; letter-spacing: 1px; }
                .resumen-block { background-color: #f0f7fc; border-left: 4px solid #2dcccd; padding: 15px; margin-bottom: 25px; font-style: italic; color: #444; font-size: 14px; }
                
                /* Items / Tarjetas */
                .item { margin-bottom: 25px; padding-bottom: 20px; border-bottom: 1px solid #eee; }
                .item:last-child { border-bottom: none; }
                
                /* Badges */
                .badges { margin-bottom: 8px; }
                .badge { padding: 4px 8px; border-radius: 4px; font-size: 10px; font-weight: bold; text-transform: uppercase; display: inline-block; vertical-align: middle; margin-right: 5px;}
                .bg-alto { background-color: #da3851; color: white; } /* Rojo */
                .bg-medio { background-color: #f8cd51; color: #121212; } /* Amarillo */
                .bg-bajo { background-color: #028484; color: white; } /* Turquesa */
                .bg-ref { background-color: #e9e9e9; color: #555; border: 1px solid #ccc; }
                
                /* Contenido Item */
                .item-title { font-size: 16px; font-weight: bold; color: #121212; margin: 0 0 5px 0; }
                .justificacion { font-size: 14px; color: #444; margin-bottom: 12px; text-align: justify; }
                
                /* Botón */
                .btn-link { display: inline-block; font-size: 11px; color: #004481; text-decoration: none; font-weight: bold; border: 1px solid #004481; padding: 6px 12px; border-radius: 4px; transition: all 0.2s; }
                .btn-link:hover { background-color: #004481; color: white; }
                
                /* Footer */
                .footer { background-color: #f4f4f4; padding: 15px; text-align: center; font-size: 11px; color: #888; border-top: 1px solid #ddd; }
            </style>
        </head>
        <body>
            <div class="container">
                <div class="header">
                    <h1>Reporte Regulatorio Diario</h1>
                    <h2>Sistema de Monitoreo Legislativo</h2>
                </div>
                <div class="content">
        """

    def _generar_html_footer(self):
        return """
                </div>
                <div class="footer">
                    Generado automáticamente por Inteligencia Artificial (Gemini) • BBVA Regulatorio
                </div>
            </div>
        </body>
        </html>
        """

    def analizar_proyectos(self, filas_nuevas):
        if not self.client or not filas_nuevas:
            return "Sin novedades relevantes.", []

        lista_proy_texto = []
        meta_data_por_id = {} 

        for fila in filas_nuevas:
            id_interno = fila[0]
            origen = fila[1]
            expediente = fila[2]
            contenido_completo = fila[5] 
            
            if "Boletin" in origen:
                link = fila[6]
            else:
                link = self.generar_link(origen, expediente)
            
            # Limpieza básica para el título si viene con etiquetas del scraper anterior
            titulo_simple = contenido_completo.split('\n')[0].replace("TITULO: ", "").replace("NORMA: ", "")
            
            meta_data_por_id[id_interno] = {"titulo": titulo_simple, "link": link, "origen": origen}
            
            item = {
                "id_interno": id_interno, 
                "referencia": expediente,
                "descripcion": contenido_completo, 
                "fuente": origen
            }
            lista_proy_texto.append(str(item))

        # --- TU PROMPT ORIGINAL ---
        prompt = f"""
        Actúa como un analista legislativo senior para Banco BBVA (Estilo Agencia de Noticias / BLapp).
        Analiza los siguientes items del Boletín Oficial y Congreso.

        TU OBJETIVO: Precisión absoluta. Prohibido usar frases genéricas de relleno.

        Instrucciones para la redacción de campos:
        1. "titulo_descriptivo":
           - Titular periodístico breve.
           - Si es DESIGNACIÓN: "Designación de [APELLIDO] en [ORGANISMO]".
           - Si es NORMATIVA: "Cambios en [TEMA PRINCIPAL] (ej: Tarifas, Impuestos)".
           - Elimina códigos burocráticos (ej: 'RESOL-2026...').

        2. "justificacion" (El análisis):
           - ESTILO: Sintético pero rico en datos (2 líneas máximo).
           - PARA DESIGNACIONES: DEBES mencionar explícitamente el NOMBRE COMPLETO y el CARGO EXACTO. (Ej: "Designa a Luis Fontana como titular de ANMAT en reemplazo de Nélida Bisio").
           - PARA NORMATIVAS: Explica QUÉ se establece (montos, plazos, tasas, leyes que se modifican). NO digas "tiene impacto sectorial", di POR QUÉ (ej: "Fija precio de energía en 28 USD/MWh" o "Modifica alícuota de impuesto PAIS").

        Devuelve un JSON con esta estructura exacta:
        {{
            "boletin": {{
                "resumen": "Resumen ejecutivo de 3 líneas con lo más destacado del día.",
                "items": [ 
                    {{ 
                        "id_interno": "...", 
                        "referencia": "...", 
                        "titulo_descriptivo": "...",
                        "impacto": "...", 
                        "justificacion": "..." 
                    }} 
                ]
            }},
            "diputados": {{
                "resumen": "Resumen ejecutivo de actividad parlamentaria.",
                "items": []
            }},
            "senado": {{
                "resumen": "Resumen ejecutivo de actividad parlamentaria.",
                "items": []
            }}
        }}

        CRITERIOS DE IMPACTO:
        - ALTO: Normas vigentes o Proyectos clave (Financiero, Cambiario, Impositivo, Laboral). Todo lo emanado por: BCRA, CNV, UIF, AFIP (ARCA), Secretaría de Comercio, Ministerio de Economía. 
        Normas sobre: Tasas de interés, Deuda Pública (Letras, Bonos), Tipo de Cambio, Impuestos, Lavado de Dinero (PLA/FT), Seguridad Informática. Designaciones CLAVE: Directorio BCRA, Ministro de Economía, Jefatura de Gabinete. 
        
        - MEDIO: Designaciones de funcionarios (Secretarios, Directores, Embajadores) y normas sectoriales específicas. 
       
        - BAJO: Temas de interés general, declaraciones de interés o efemérides. Homologaciones de convenios colectivos de industrias ajenas (Ej: Pasteleros, Vidrio, Madera), salvo que marquen una pauta salarial general muy relevante.
        Premios, becas, declaraciones de interés cultural. Multas a particulares desconocidos (contrabando menor).

        Datos a analizar:
        {json.dumps(lista_proy_texto, ensure_ascii=False)}
        """

        modelos = ["gemini-2.5-flash", "gemini-2.5-flash-lite", "gemini-2.0-flash", "gemini-2.0-flash-lite"]
        
        for modelo in modelos:
            for intento in range(3): 
                try:
                    print(f"Usando modelo {modelo}")
                    response = self.client.models.generate_content(
                        model=modelo, contents=prompt,
                        config=types.GenerateContentConfig(response_mime_type="application/json")
                    )
                    data = json.loads(response.text)
                    
                    # --- GENERACIÓN DE REPORTE HTML ---
                    html_output = self._generar_html_header()
                    todos_los_detalles_para_excel = []

                    secciones = [
                        ("Boletín Oficial", "boletin"),
                        ("Diputados", "diputados"),
                        ("Senado", "senado")
                    ]

                    hay_contenido = False

                    for titulo_seccion, key_json in secciones:
                        bloque = data.get(key_json, {})
                        items = bloque.get("items", [])
                        resumen = bloque.get("resumen", "")
                        
                        todos_los_detalles_para_excel.extend(items)

                        if not items and "Sin movimientos" in resumen:
                            continue
                        
                        hay_contenido = True

                        # 1. Título de la Sección
                        html_output += f'<div class="section-title">{titulo_seccion}</div>'
                        
                        # 2. Resumen Ejecutivo
                        if resumen:
                            html_output += f'<div class="resumen-block">{resumen}</div>'

                        # 3. Ordenar items por impacto (Alto -> Medio -> Bajo)
                        orden_impacto = {"ALTO": 1, "MEDIO": 2, "BAJO": 3}
                        items_ordenados = sorted(items, key=lambda x: orden_impacto.get(x.get("impacto", "BAJO"), 99))

                        # 4. Renderizar cada item
                        for p in items_ordenados:
                            id_ref = p.get('id_interno')
                            meta = meta_data_por_id.get(id_ref, {})
                            
                            titulo_mostrar = p.get("titulo_descriptivo", meta.get("titulo", "Sin título"))
                            link_web = meta.get("link", "#")
                            ref = p.get('referencia', '')
                            justificacion = p.get('justificacion', '')
                            impacto = p.get('impacto', 'BAJO').upper()

                            # Seleccionar color del badge según impacto
                            clase_badge = "bg-bajo"
                            if impacto == "ALTO": clase_badge = "bg-alto"
                            elif impacto == "MEDIO": clase_badge = "bg-medio"

                            html_output += f"""
                            <div class="item">
                                <div class="badges">
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
                        <div style="text-align: center; padding: 40px; color: #666;">
                            <i>No se encontraron novedades legislativas relevantes para el sector en el día de la fecha.</i>
                        </div>
                        """

                    html_output += self._generar_html_footer()

                    return html_output, todos_los_detalles_para_excel

                except Exception as e:
                    errores_saturacion = ["503", "overloaded", "429", "quota", "Resource has been exhausted"]
                    if any(err in str(e) for err in errores_saturacion):
                        tiempo_espera = 5 * (intento + 1) 
                        print(f"⚠️ Modelo {modelo} saturado. Reintentando en {tiempo_espera}s... ({intento+1}/3)")
                        time.sleep(tiempo_espera)
                        continue 
                    else:
                        print(f"❌ Error no recuperable con {modelo}: {e}")
                        break 
        
        return "Error en análisis IA", []