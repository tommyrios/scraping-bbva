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
        for fila in filas_nuevas:
            item = {
                "id_interno": fila[0], 
                "expediente": fila[2],
                "titulo": fila[5]
            }
            lista_proy_texto.append(str(item))

        prompt = f"""
        Eres un analista de riesgos legislativos para Banco BBVA.
        Analiza estos proyectos de ley y devuelve un objeto JSON con este formato:
        
        {{
            "resumen_whatsapp": "Texto breve para enviar por chat.",
            "analisis_individual": [
                {{
                    "id_interno": "PLxxx",
                    "impacto": "ALTO, MEDIO, BAJO o NULO",
                    "justificacion": "Breve frase de por qué afecta o no"
                }}
            ]
        }}

        Criterios:
        - ALTO: Regula tasas, comisiones, impuestos bancarios, datos personales.
        - MEDIO: Impacto indirecto económico o laboral.
        - BAJO/NULO: Declaraciones, efemérides, ajenos al sector.

        Proyectos:
        {json.dumps(lista_proy_texto, ensure_ascii=False)}
        """

        intentos_maximos = 5
        espera = 15

        for intento in range(intentos_maximos):
            try:
                response = self.client.models.generate_content(
                    model="gemini-1.5.-flash",
                    contents=prompt,
                    config=types.GenerateContentConfig(
                        response_mime_type="application/json"
                    )
                )
                
                data = json.loads(response.text)
                texto_wa = data.get("resumen_whatsapp", "Análisis completado.")
                detalles = data.get("analisis_individual", [])
                
                return texto_wa, detalles

            except Exception as e:
                error_str = str(e)
                if "429" in error_str or "RESOURCE_EXHAUSTED" in error_str:
                    if intento < intentos_maximos - 1:
                        print(f"Cuota Gemini excedida. Reintentando en {espera} segundos...")
                        time.sleep(espera)
                        espera *= 2 
                        continue
                
                print(f"Error en Gemini: {e}")
                return "Error al generar análisis con IA.", []
        
        return "Error: Gemini no respondió tras varios intentos.", []
