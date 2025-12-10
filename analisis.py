import os
import google.generativeai as genai

class AnalistaLegislativo:
    def __init__(self):
        self.api_key = os.environ.get('GEMINI_API_KEY')
        self.model = None
        
        if self.api_key:
            try:
                genai.configure(api_key=self.api_key)
                
                print(f"Versión de librería genai: {genai.__version__}")
                
                modelos_disponibles = []
                for m in genai.list_models():
                    if 'generateContent' in m.supported_generation_methods:
                        modelos_disponibles.append(m.name)
                
                modelo_a_usar = ""
                if 'models/gemini-1.5-flash' in modelos_disponibles:
                    modelo_a_usar = 'models/gemini-1.5-flash'
                elif 'models/gemini-pro' in modelos_disponibles:
                    modelo_a_usar = 'models/gemini-pro'
                elif modelos_disponibles:
                    modelo_a_usar = modelos_disponibles[0]
                else:
                    print("⚠️ ALERTA: La API no devolvió ningún modelo disponible.")
                
                if modelo_a_usar:
                    print(f"✅ Usando modelo: {modelo_a_usar}")
                    self.model = genai.GenerativeModel(modelo_a_usar)
                
            except Exception as e:
                print(f"Error configurando Gemini: {e}")
                self.model = None
        else:
            print("Falta GEMINI_API_KEY")

    def analizar_proyectos(self, lista_proyectos):
        """
        Recibe una lista de filas con los datos de los proyectos.
        Retorna un string con el análisis hecho por Gemini.
        """
        if not self.model:
            return "⚠️ No se pudo configurar el modelo de IA."

        if not lista_proyectos:
            return "No hay proyectos nuevos para analizar."

        texto_proyectos = ""
        for p in lista_proyectos:
            texto_proyectos += f"- Exp: {p[2]} | Autor: {p[3]} | Título: {p[5]}\n"

        prompt = f"""
        Eres un Analista de Riesgo Regulatorio para el Banco BBVA Argentina, en el area de Asuntos Publicos dentro de la direccion de Relaciones Institucionales.
        Tu misión es filtrar los siguientes proyectos de ley nuevos y generar un resumen EJECUTIVO y MUY BREVE.
        
        Lista de proyectos:
        {texto_proyectos}

        Instrucciones:
        1. SOLO menciona proyectos que representen un riesgo u oportunidad real para el negocio bancario (Créditos, BCRA, Tarjetas, Impuestos, Datos, Hipotecas).
        2. Si un proyecto es crítico, usa 🚨 y resume el impacto.
        3. Agrupa el resto de proyectos irrelevantes (homenajes, declaraciones, educación) en una sola frase final genérica.
        4. No uses introducciones o saludos. El analisis en total debe tener una longitud de máximo 500 caracteres, debes sintetizar la imformación relevante y ser conciso.
        5. No uses asteriscos para marcar texto en negrita. 
        
        """

        try:
            response = self.model.generate_content(prompt)
            
            return f"\n{response.text}"

        except Exception as e:
            print(f"DEBUG IA: {e}") 
            return f"\n⚠️ Error generando análisis: {str(e)}"
