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

    def analizar_proyectos(self, filas_nuevas):
        if not self.client or not filas_nuevas:
            return "Sin novedades relevantes.", []

        lista_proy_texto = []
        meta_data_por_id = {} 

        for fila in filas_nuevas:
            id_interno = fila[0]
            origen = fila[1]
            expediente = fila[2]
            # En fila[5] viene el texto completo desde el scraper
            contenido_completo = fila[5] 
            
            if "Boletin" in origen:
                link = fila[6]
            else:
                link = self.generar_link(origen, expediente)
            
            # Guardamos metadata básica
            titulo_simple = contenido_completo.split('\n')[0].replace("TITULO: ", "")
            meta_data_por_id[id_interno] = {"titulo": titulo_simple, "link": link, "origen": origen}
            
            item = {
                "id_interno": id_interno, 
                "referencia": expediente,
                "descripcion": contenido_completo, # Pasa el texto full a la IA
                "fuente": origen
            }
            lista_proy_texto.append(str(item))

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
                    
                    mensaje_final = ""
                    todos_los_detalles_para_excel = []

                    secciones = [
                        ("Reporte Boletín Oficial", "boletin"),
                        ("Reporte Diputados", "diputados"),
                        ("Reporte Senado", "senado")
                    ]

                    def formatear_item(p):
                        id_ref = p.get('id_interno')
                        meta = meta_data_por_id.get(id_ref, {})
                        
                        # Usamos el título generado por IA que es más limpio
                        titulo_mostrar = p.get("titulo_descriptivo", meta.get("titulo", "Sin título"))
                        link_web = meta.get("link", "")
                        ref = p.get('referencia', '')
                        
                        texto = f"• *[{ref}]:* {titulo_mostrar}\n"
                        texto += f"{p.get('justificacion')}\n"
                        texto += f"Link: {link_web}\n"
                        return texto

                    for titulo_seccion, key_json in secciones:
                        bloque = data.get(key_json, {})
                        items = bloque.get("items", [])
                        resumen = bloque.get("resumen", "Sin movimientos.")
                        
                        todos_los_detalles_para_excel.extend(items)

                        if not items and "Sin movimientos" in resumen: continue

                        mensaje_final += f"📢 *{titulo_seccion}*\n{resumen}\n\n"

                        altos = [x for x in items if x.get('impacto') == 'ALTO']
                        medios = [x for x in items if x.get('impacto') == 'MEDIO']
                        
                        if altos:
                            mensaje_final += "🚨 *Impacto ALTO*\n"
                            for p in altos: mensaje_final += formatear_item(p) + "\n"
                        
                        if medios:
                            mensaje_final += "⚠️ *Impacto MEDIO*\n"
                            for p in medios: mensaje_final += formatear_item(p) + "\n"
                        
                        mensaje_final += "----------------------------------------\n\n"

                    if not mensaje_final: mensaje_final = "✅ *Sin novedades relevantes.*"

                    return mensaje_final, todos_los_detalles_para_excel

                except Exception as e:
                    errores_saturacion = ["503", "overloaded", "429", "quota"]
                    if any(err in str(e) for err in errores_saturacion):
                        time.sleep(5 * (intento + 1))
                        continue
                    else:
                        print(f"❌ Error {modelo}: {e}")
                        break
        
        return "Error en análisis IA", []
