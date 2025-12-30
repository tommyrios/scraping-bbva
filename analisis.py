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

    def generar_link(self, origen, expediente):
        exp_limpio = expediente.strip()
        if "Diputados" in origen:
            return f"https://www.google.com/search?q=site:diputados.gov.ar+%22{exp_limpio}%22"
        elif "Senado" in origen:
            return f"https://www.senado.gob.ar/parlamentario/comisiones/verExp/{exp_limpio}"
        return ""

    def analizar_proyectos(self, filas_nuevas):
        if not self.client or not filas_nuevas:
            return "Sin proyectos relevantes para analizar hoy.", []

        lista_proy_texto = []
        meta_data_por_id = {} 

        for fila in filas_nuevas:
            id_interno = fila[0]
            origen = fila[1]
            expediente = fila[2]
            titulo = fila[5]
            
            link = self.generar_link(origen, expediente)
            
            meta_data_por_id[id_interno] = {
                "titulo": titulo,
                "link": link
            }
            
            item = {
                "id_interno": id_interno, 
                "expediente": expediente,
                "titulo": titulo
            }
            lista_proy_texto.append(str(item))

        prompt = f"""
        Eres un analista de riesgos legislativos para Banco BBVA.
        Analiza estos proyectos de ley y devuelve un objeto JSON con este formato exacto:
        
        {{
            "resumen_general": "Un párrafo de 3 líneas resumiendo la tendencia del día.",
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

        modelos_a_probar = [
            "gemini-2.5-flash",
            "gemini-2.0-flash-exp"
        ]

        intentos_maximos_por_modelo = 2
        espera_base = 5

        for modelo in modelos_a_probar:
            for intento in range(intentos_maximos_por_modelo):
                try:
                    response = self.client.models.generate_content(
                        model=modelo,
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
                            meta = meta_data_por_id.get(id_ref, {})
                            titulo_real = meta.get("titulo", "Proyecto")
                            link_web = meta.get("link", "")
                            expediente = p.get('expediente', '')
                            
                            mensaje_whatsapp += f"• *[{expediente}]({link_web})*: {titulo_real}\n"
                            mensaje_whatsapp += f"  _👉 {p.get('justificacion')}_\n\n"

                    if medios:
                        mensaje_whatsapp += "⚠️ *Impacto Medio / Monitorear*\n"
                        for p in medios:
                            id_ref = p.get('id_interno')
                            meta = meta_data_por_id.get(id_ref, {})
                            titulo_real = meta.get("titulo", "Proyecto")
                            link_web = meta.get("link", "")
                            expediente = p.get('expediente', '')
                            
                            mensaje_whatsapp += f"• *[{expediente}]({link_web})*: {titulo_real}\n"
                            mensaje_whatsapp += f"  _{p.get('justificacion')}_\n"
                        mensaje_whatsapp += "\n"

                    if bajos:
                        mensaje_whatsapp += f"📉 *Proyectos de Impacto Bajo/Nulo:* {len(bajos)}\n"

                    if not altos and not medios:
                        mensaje_whatsapp += "\n✅ *Sin riesgos regulatorios directos detectados hoy.*"

                    return mensaje_whatsapp, detalles

                except Exception as e:
                    error_str = str(e)
                    if "404" in error_str or "NOT_FOUND" in error_str:
                        break 
                    
                    if "429" in error_str or "RESOURCE_EXHAUSTED" in error_str:
                        if intento < intentos_maximos_por_modelo - 1:
                            tiempo_espera = espera_base * (intento + 1)
                            time.sleep(tiempo_espera)
                            continue
                    
                    break 
        
        return "Error IA: No se pudo generar el análisis.", []