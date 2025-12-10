import os
import google.generativeai as genai

class AnalistaLegislativo:
    def __init__(self):
        self.api_key = os.environ.get('GEMINI_API_KEY')
        if self.api_key:
            genai.configure(api_key=self.api_key)
            self.model = genai.GenerativeModel('gemini-pro')
        else:
            self.model = None

    def analizar_proyectos(self, lista_proyectos):
        """
        Recibe una lista de filas con los datos de los proyectos.
        Retorna un string con el análisis hecho por Gemini.
        """
        if not self.model:
            return "⚠️ No hay API Key de Gemini configurada."

        if not lista_proyectos:
            return "No hay proyectos nuevos para analizar."

        texto_proyectos = ""

        for p in lista_proyectos:
            texto_proyectos += f"- Exp: {p[2]} | Autor: {p[3]} | Título: {p[5]}\n"

        prompt = f"""
        Eres un Analista de Riesgo Regulatorio para el Banco BBVA Argentina.
        Analiza la siguiente lista de nuevos proyectos de ley ingresados en Diputados:

        {texto_proyectos}

        Instrucciones:
        1. Identifica temas recurrentes (ej: "Seguridad", "Impuestos", "Homenajes").
        2. Busca palabras clave de riesgo bancario: Tasas, BCRA, Tarjetas, Créditos, Fintech, Deudores, Impuestos, Comisiones.
        3. Si un proyecto impacta al banco, márcalo con 🚨 y explica brevemente por qué.
        4. Si son solo declaraciones de interés o temas irrelevantes, pon: "🟢 Sin impacto regulatorio relevante."

        Formato de respuesta (para WhatsApp):
        - Usa emojis.
        - Sé extremadamente conciso (máximo 200 palabras en total).
        - No saludes, ve directo al grano.
        """

        try:
            response = self.model.generate_content(prompt)
            return f"\n🧠 *Análisis Gemini:*\n{response.text}"

        except Exception as e:
            return f"\n⚠️ Error en análisis IA: {str(e)}"
