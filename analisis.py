import os
from google import genai

class AnalistaLegislativo:
    def __init__(self):
        api_key = os.environ.get("GEMINI_API_KEY")
        if not api_key:
            print("Falta GEMINI_API_KEY")
            self.client = None
        else:
            self.client = genai.Client(api_key=api_key)

    def analizar_proyectos(self, filas_nuevas):
        if not self.client:
            return "Análisis no disponible (Falta API Key)."

        if not filas_nuevas:
            return "No hay proyectos nuevos para analizar hoy."

        texto_proyectos = ""
        for fila in filas_nuevas:
            origen = fila[1] if len(fila) > 1 else "Congreso"
            expediente = fila[2] if len(fila) > 2 else "?"
            autor = fila[3] if len(fila) > 3 else "S/D"
            titulo = fila[5] if len(fila) > 5 else "Sin título"
            
            texto_proyectos += f"- [{origen}] {expediente} ({autor}): {titulo}\n"

        prompt = f"""
        Eres un analista de riesgos legislativos para el sector bancario y financiero (Banco BBVA).
        Tu tarea es leer los siguientes proyectos de ley ingresados hoy en el Congreso Argentino y detectar si alguno es relevante.

        Criterios de relevancia:
        - Regulaciones bancarias, tasas de interés, tarjetas de crédito, comisiones.
        - Impuestos que afecten al sector financiero.
        - Normativas sobre datos personales, ciberseguridad o fintech.
        - Modificaciones al código civil/comercial sobre deudas o ejecuciones.
        - Normativas laborales que impacten grandes empleadores.

        Lista de Proyectos:
        {texto_proyectos}

        Instrucciones:
        1. Si NINGÚN proyecto es relevante, responde SOLO: "Los proyectos presentados no presentan riesgos directos para el negocio bancario."
        2. Si hay proyectos relevantes, lístalos indicando POR QUÉ son importantes y el nivel de impacto (ALTO/MEDIO/BAJO).
        3. Sé conciso y usa formato WhatsApp.
        """

        try:
            response = self.client.models.generate_content(
                model="gemini-2.5-flash",
                contents=prompt
            )
            return response.text

        except Exception as e:
            print(f"Error en Gemini: {e}")
            return "Error al generar análisis con IA."
