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
        Analiza estos proyectos de ley y devuelve un objeto JSON.

        Criterios:
        - ALTO: Regula tasas, comisiones, impuestos bancarios, datos personales, tarjetas.
        - MEDIO: Impacto indirecto económico, pymes o laboral.
        - BAJO: Declaraciones, efemérides, temas ajenos al sector.

        Formato JSON requerido:
        {{
            "resumen_general": "Párrafo de 3 lineas con la conclusión general del día.",
            "analisis_individual": [
                {{
                    "id_interno": "PLxxx",
                    "impacto": "ALTO, MEDIO o BAJO",
                    "justificacion": "Explicación técnica de 1 oración."
                }}
            ]
        }}

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
                resumen = data.get("resumen_general", "Sin resumen.")

                mensaje_whatsapp = f"📢 *Resumen Ejecutivo:*\n{resumen}\n\n"
                
                hay_alertas = False
                
                proyectos_alto = [d for d in detalles if d.get('impacto') == 'ALTO']
                proyectos_medio = [d for d in detalles if d.get('impacto') == 'MEDIO']
                
                if proyectos_alto:
                    mensaje_whatsapp += "🚨 *ALERTA: IMPACTO ALTO*\n"
                    for p in proyectos_alto:
                        id_ref = p.get('id_interno')
                        titulo_real = titulos_por_id.get(id_ref, "Proyecto")
                        titulo_corto = (titulo_real[:75] + '...') if len(titulo_real) > 75 else titulo_real
                        
                        mensaje_whatsapp += f"• *{p.get('expediente')}*: {titulo_corto}\n"
                        mensaje_whatsapp += f"  _👉 {p.get('justificacion')}_\n\n"
                    hay_alertas = True

                if proyectos_medio:
                    mensaje_whatsapp += "⚠️ *Impacto Medio / Monitorear*\n"
                    for p in proyectos_medio:
                        id_ref = p.get('id_interno')
                        titulo_real = titulos_por_id.get(id_ref, "Proyecto")
                        titulo_corto = (titulo_real[:75] + '...') if len(titulo_real) > 75 else titulo_real
                        
                        mensaje_whatsapp += f"• {titulo_corto}\n"
                        mensaje_whatsapp += f"  _{p.get('justificacion')}_\n"
                    hay_alertas = True

                if not hay_alertas:
                    mensaje_whatsapp += "✅ *Sin riesgos regulatorios directos detectados hoy.*"

                return mensaje_whatsapp, detalles

            except Exception as e:
                error_str = str(e)
                if "429" in error_str or "RESOURCE_EXHAUSTED" in error_str:
                    if intento < intentos_maximos - 1:
                        print(f"Cuota Gemini excedida. Reintentando en {espera}s...")
                        time.sleep(espera)
                        espera *= 2 
                        continue
                
                print(f"Error en Gemini: {e}")
                return "Error al generar análisis con IA.", []
        
        return "Error: Gemini no respondió tras varios intentos.", []