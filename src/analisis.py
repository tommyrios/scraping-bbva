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
        Analiza estos proyectos legislativos y normas del Boletín Oficial para Banco BBVA.
        
        Devuelve un JSON exacto:
        {{
            "resumen_general": "Párrafo de 4-5 líneas. DEBES comenzar mencionando las normas aprobadas en el Boletín Oficial si las hay, y luego resumir la tendencia legislativa del Congreso.",
            "analisis_individual": [
                {{
                    "id_interno": "ID",
                    "referencia": "Expediente o Norma",
                    "impacto": "ALTO", 
                    "justificacion": "Explicación directa para ejecutivos."
                }}
            ]
        }}

        Criterios:
        - ALTO: Normas vigentes (Boletín) siempre son prioritarias. Leyes de bancos, impuestos, datos, laboral.
        - MEDIO: Impacto indirecto.
        - BAJO: Temas irrelevantes.

        Datos:
        {json.dumps(lista_proy_texto, ensure_ascii=False)}
        """

        modelos = ["gemini-1.5-flash", "gemini-2.0-flash-exp"]
        for modelo in modelos:
            try:
                response = self.client.models.generate_content(
                    model=modelo, contents=prompt,
                    config=types.GenerateContentConfig(response_mime_type="application/json")
                )
                data = json.loads(response.text)
                detalles = data.get("analisis_individual", [])
                resumen = data.get("resumen_general", "")

                msg = f"📢 *Resumen Ejecutivo:*\n{resumen}\n\n"
                
                niveles = {'ALTO': [], 'MEDIO': [], 'BAJO': []}
                for d in detalles:
                    imp = d.get('impacto', 'BAJO')
                    if imp in niveles: niveles[imp].append(d)

                def formatear_item(p):
                    id_ref = p.get('id_interno')
                    meta = meta_data_por_id.get(id_ref, {})
                    titulo_real = meta.get("titulo", "Sin título")
                    link_web = meta.get("link", "")
                    origen = meta.get("origen", "")
                    ref = p.get('referencia', '')
                    
                    texto = f"• *{titulo_real}*\n"
                    
                    if "Boletin" in origen:
                        texto += f"  🏛️ {origen} - {ref}\n"
                    else:
                        texto += f"  🏛️ {origen} (Exp: {ref})\n"
                    
                    texto += f"  👉 _{p.get('justificacion')}_\n"
                    texto += f"  🔗 Link: {link_web}\n"
                    return texto

                if niveles['ALTO']:
                    msg += "🚨 *ALERTA: IMPACTO ALTO*\n"
                    for p in niveles['ALTO']:
                        msg += formatear_item(p) + "\n"

                if niveles['MEDIO']:
                    msg += "⚠️ *Impacto Medio / Monitorear*\n"
                    for p in niveles['MEDIO']:
                        msg += formatear_item(p) + "\n"
                
                bajos = len(niveles['BAJO'])
                if bajos > 0:
                    msg += f"📉 *Normas/Proyectos de Impacto Bajo:* {bajos}\n"

                return msg, detalles

            except Exception:
                continue
        
        return "Error en análisis IA", []