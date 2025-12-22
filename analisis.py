import os
import json
import time
from google import genai
from google.genai import types

class AnalistaLegislativo:
    def __init__(self):
        api_key = os.environ.get("GEMINI_API_KEY")
        if not api_key:
            print("Falta GEMINI_API_KEY")
            self.client = None
        else:
            self.client = genai.Client(api_key=api_key)

    def analizar_proyectos(self, filas_nuevas):
        if not self.client or not filas_nuevas:
            return "Sin análisis disponible.", []

        lista_proy_texto = []
        titulos_por_id = {} 

        for fila in filas_nuevas:
            id_interno = fila[0]
            titulo = fila[5]
            titulos_por_id[id_interno] = titulo
            
            item = {
                "id_interno": id_interno, 
                "expediente": fila[2],
                "titulo": titulo
            }
            lista_proy_texto.append(str(item))

        prompt = f"""
        Eres un analista de riesgos legislativos para Banco BBVA.
        Analiza estos proyectos de ley y devuelve un objeto JSON con este formato exacto:
        
        {{
            "resumen_general": "Un párrafo de 3 líneas resumiendo la tendencia del día (ej: 'Se presentaron normas sobre tarjetas de crédito y regulaciones laborales...').",
            "analisis_individual": [
                {{
                    "id_interno": "PLxxx",
                    "expediente": "123-D-2024",
                    "impacto": "ALTO", 
                    "justificacion": "Explicación técnica de 1 oración sobre el riesgo financiero."
                }}
            ]
        }}

        IMPORTANTE:
        - Impacto ALTO: Regula tasas, comisiones, impuestos bancarios, datos personales, defensa consumidor.
        - Impacto MEDIO: Impacto indirecto económico, pymes o laboral.
        - Impacto BAJO: Declaraciones de interés, efemérides, temas ajenos al sector.
        - La 'justificacion' debe ser útil para un gerente de banco.

        Proyectos:
        {json.dumps(lista_proy_texto, ensure_ascii=False)}
        """

        intentos_maximos = 3
        espera = 10

        for intento in range(intentos_maximos):
            try:
                response = self.client.models.generate_content(
                    model="gemini-1.5-flash",
                    contents=prompt,
                    config=types.GenerateContentConfig(
                        response_mime_type="application/json"
                    )
                )
                
                data = json.loads(response.text)
                detalles = data.get("analisis_individual", [])
                resumen = data.get("resumen_general", "Sin resumen general.")

                mensaje_whatsapp = f"📢 *Resumen Ejecutivo:*\n{resumen}\n\n"
                
                altos = [d for d in detalles if d.get('impacto') == 'ALTO']
                medios = [d for d in detalles if d.get('impacto') == 'MEDIO']
                bajos = [d for d in detalles if d.get('impacto') == 'BAJO']

                if altos:
                    mensaje_whatsapp += "🚨 *ALERTA: IMPACTO ALTO*\n"
                    for p in altos:
                        id_ref = p.get('id_interno')
                        titulo_real = titulos_por_id.get(id_ref, "Proyecto")
                        titulo_corto = (titulo_real[:80] + '...') if len(titulo_real) > 80 else titulo_real
                        expediente = p.get('expediente', '')
                        
                        mensaje_whatsapp += f"• *{expediente}*: {titulo_corto}\n"
                        mensaje_whatsapp += f"  _👉 {p.get('justificacion')}_\n\n"

                if medios:
                    mensaje_whatsapp += "⚠️ *Impacto Medio / Monitorear*\n"
                    for p in medios:
                        id_ref = p.get('id_interno')
                        titulo_real = titulos_por_id.get(id_ref, "Proyecto")
                        titulo_corto = (titulo_real[:80] + '...') if len(titulo_real) > 80 else titulo_real
                        expediente = p.get('expediente', '')
                        
                        mensaje_whatsapp += f"• *{expediente}*: {titulo_corto}\n"
                        mensaje_whatsapp += f"  _{p.get('justificacion')}_\n"
                    mensaje_whatsapp += "\n"

                if bajos:
                    mensaje_whatsapp += f"📉 *Proyectos de Impacto Bajo/Nulo:* {len(bajos)}\n"

                if not altos and not medios:
                    mensaje_whatsapp += "\n✅ *Sin riesgos regulatorios directos detectados hoy.*"

                return mensaje_whatsapp, detalles

            except Exception as e:
                error_str = str(e)
                if "429" in error_str or "RESOURCE_EXHAUSTED" in error_str:
                    if intento < intentos_maximos - 1:
                        time.sleep(espera)
                        espera *= 2 
                        continue
                
                print(f"Error en Gemini: {e}")
                return "Error al generar análisis con IA.", []
        
        return "Error: Gemini no respondió tras varios intentos.", []