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
        """Genera el encabezado HTML con Logo BBVA propio (GitHub Raw)."""
        
        # Enlace directo a tu imagen en RAW
        LOGO_URL = "https://raw.githubusercontent.com/tommyrios/scraping-bbva/main/src/assets/BBVA_WHITE.png"
        
        return f'''
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="utf-8">
            <style>
                /* Reset y Fuente */
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
                
                /* --- HEADER CONFIG --- */
                .header {{ 
                    background-color: #072146; /* Azul BBVA */
                    color: white; 
                    padding: 30px 5%; 
                }}
                
                /* Fila del Logo: Alineada a la Izquierda */
                .logo-row {{
                    text-align: left;
                    margin-bottom: 20px;
                    width: 100%;
                }}
                
                /* Imagen del Logo */
                .logo-img {{ 
                    height: 30px; /* Tamaño ajustado */
                    width: auto; 
                    display: block; 
                    border: 0;
                    outline: none;
                    text-decoration: none;
                }}

                /* Fila del Título: Centrada */
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
                
                /* --- CONTENIDO --- */
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
                
                /* Items */
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
        """Helper para generar reporte vacío pero con Branding."""
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
            
            titulo_simple = contenido_completo.split('\n')[0].replace("TITULO: ", "").replace("NORMA: ", "")
            meta_data_por_id[id_interno] = {"titulo": titulo_simple, "link": link, "origen": origen}
            
            item = {
                "id_interno": id_interno, 
                "referencia": expediente,
                "descripcion": contenido_completo, 
                "fuente": origen
            }
            lista_proy_texto.append(str(item))

        # --- AQUÍ ESTÁ TU PROMPT EXACTO ---
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
                    
                    html_output = self._generar_html_header()
                    todos_los_detalles_para_excel = [] 

                    secciones = [
                        ("Boletín Oficial", "boletin"),
                        ("Diputados", "diputados"),
                        ("Senado", "senado")
                    ]

                    hay_contenido_relevante_total = False

                    for titulo_seccion, key_json in secciones:
                        bloque = data.get(key_json, {})
                        items = bloque.get("items", [])
                        resumen = bloque.get("resumen", "")
                        
                        todos_los_detalles_para_excel.extend(items)

                        # FILTRO PARA EL EMAIL: Solo ALTO y MEDIO
                        items_email = [p for p in items if p.get('impacto', 'BAJO').upper() != 'BAJO']

                        if not items_email and "Sin movimientos" in resumen:
                            continue
                        
                        if items_email:
                            hay_contenido_relevante_total = True
                            html_output += f'<div class="section-title">{titulo_seccion}</div>'
                            
                            if resumen:
                                html_output += f'<div class="resumen-block">{resumen}</div>'

                            orden_impacto = {"ALTO": 1, "MEDIO": 2}
                            items_ordenados = sorted(items_email, key=lambda x: orden_impacto.get(x.get("impacto", "MEDIO"), 99))

                            for p in items_ordenados:
                                id_ref = p.get('id_interno')
                                meta = meta_data_por_id.get(id_ref, {})
                                
                                titulo_mostrar = p.get("titulo_descriptivo", meta.get("titulo", "Sin título"))
                                link_web = meta.get("link", "#")
                                ref = p.get('referencia', '')
                                justificacion = p.get('justificacion', '')
                                impacto = p.get('impacto', 'MEDIO').upper()

                                clase_badge = "bg-medio"
                                if impacto == "ALTO": clase_badge = "bg-alto"

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

                    if not hay_contenido_relevante_total:
                        html_output += """
                        <div class="empty-state">
                            <span class="empty-icon">✅</span>
                            <h3>Sin Novedades de Impacto</h3>
                            <p>Se han analizado las normas del día, pero ninguna alcanza el nivel de impacto <b>Alto</b> o <b>Medio</b> para el sector.</p>
                        </div>
                        """

                    html_output += self._generar_html_footer()

                    return html_output, todos_los_detalles_para_excel

                except Exception as e:
                    errores_saturacion = ["503", "overloaded", "429", "quota", "Resource has been exhausted"]
                    if any(err in str(e) for err in errores_saturacion):
                        time.sleep(5)
                        continue 
                    else:
                        print(f"❌ Error modelo: {e}")
                        break 
        
        return self._generar_html_vacio("Ocurrió un error al procesar el análisis con Inteligencia Artificial."), []
