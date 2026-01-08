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
            titulo = fila[5]
            
            if "Boletin" in origen:
                link = fila[6]
            else:
                link = self.generar_link(origen, expediente)
            
            meta_data_por_id[id_interno] = {"titulo": titulo, "link": link, "origen": origen}
            
            item = {
                "id_interno": id_interno, 
                "referencia": expediente,
                "descripcion": titulo,
                "fuente": origen
            }
            lista_proy_texto.append(str(item))

        prompt = f"""
        Eres un analista de riesgos para Banco BBVA.
        Clasifica y analiza los siguientes items en 3 categorías: Boletín Oficial, Cámara de Diputados y Senado.

        Devuelve un JSON con esta estructura exacta:
        {{
            "boletin": {{
                "resumen": "Resumen ejecutivo de 3 líneas sobre las normas publicadas hoy (sin formato markdown cursiva).",
                "items": [ 
                    {{ 
                        "id_interno": "...", 
                        "referencia": "...", 
                        "titulo_descriptivo": "Titular periodístico breve y limpio. ELIMINA códigos técnicos como 'DECTO-2024-APN'.",
                        "impacto": "...", 
                        "justificacion": "..." 
                    }} 
                ]
            }},
            "diputados": {{
                "resumen": "Resumen ejecutivo de 3 líneas sobre la actividad en Diputados (sin formato markdown cursiva).",
                "items": []
            }},
            "senado": {{
                "resumen": "Resumen ejecutivo de 3 líneas sobre la actividad en Senado (sin formato markdown cursiva).",
                "items": []
            }}
        }}

        CRITERIOS DE IMPACTO:
        - ALTO: Normas vigentes (Boletín) o Proyectos con alto riesgo regulatorio/financiero/impositivo (Cámaras Legislativas).
        - MEDIO: Nombramiento de funcionarios (Boletín), Impacto indirecto o sectorial (Boletín y Cámaras Legislativas).
        - BAJO: Temas de interés general o irrelevantes (Boletín y Cámaras Legislativas).

        Datos a analizar:
        {json.dumps(lista_proy_texto, ensure_ascii=False)}
        """

        modelos = ["gemini-2.5-flash", "gemini-2.5-flash-lite", "gemini-2.0-flash", "gemini-2.0-flash-lite"]
        for modelo in modelos:
            try:
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

                    if not items and "Sin movimientos" in resumen:
                        continue

                    mensaje_final += f"📢 *{titulo_seccion}*\n"
                    mensaje_final += f"{resumen}\n\n"

                    altos = [x for x in items if x.get('impacto') == 'ALTO']
                    medios = [x for x in items if x.get('impacto') == 'MEDIO']
                    
                    if altos:
                        mensaje_final += "🚨 *Impacto ALTO*\n"
                        for p in altos:
                            mensaje_final += formatear_item(p) + "\n"
                    
                    if medios:
                        mensaje_final += "⚠️ *Impacto MEDIO*\n"
                        for p in medios:
                            mensaje_final += formatear_item(p) + "\n"
                    
                    mensaje_final += "----------------------------------------\n\n"

                if not mensaje_final:
                    mensaje_final = "✅ *Sin novedades legislativas ni normativas relevantes hoy.*"

                return mensaje_final, todos_los_detalles_para_excel

            except Exception as e:
                    errores_saturacion = ["503", "overloaded", "429", "quota"]
                    es_saturacion = any(err in str(e) for err in errores_saturacion)
                    
                    if es_saturacion:
                        tiempo_espera = 5 * (intento + 1) 
                        print(f"⚠️ Modelo {modelo} saturado. Reintentando en {tiempo_espera}s... ({intento+1}/3)")
                        time.sleep(tiempo_espera)
                        continue 
                    else:
                        print(f"❌ Error no recuperable con {modelo}: {e}")
                        break
        
        return "Error en análisis IA", []
