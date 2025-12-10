import os
import google.generativeai as genai

class AnalistaLegislativo:
    def __init__(self):
        self.api_key = os.environ.get('GEMINI_API_KEY')
        self.model = None
        
        if self.api_key:
            try:
                genai.configure(api_key=self.api_key)
                
                # --- PASO 1: DIAGNÓSTICO Y SELECCIÓN AUTOMÁTICA ---
                print(f"Versión de librería genai: {genai.__version__}")
                
                # Listamos los modelos que soportan generar texto
                modelos_disponibles = []
                for m in genai.list_models():
                    if 'generateContent' in m.supported_generation_methods:
                        modelos_disponibles.append(m.name)
                
                print(f"Modelos encontrados: {modelos_disponibles}")
                
                # Lógica de selección de modelo (Prioridad: Flash > Pro > Cualquiera)
                modelo_a_usar = ""
                
                # Buscamos preferidos
                if 'models/gemini-1.5-flash' in modelos_disponibles:
                    modelo_a_usar = 'models/gemini-1.5-flash'
                elif 'models/gemini-pro' in modelos_disponibles:
                    modelo_a_usar = 'models/gemini-pro'
                # Si no están los famosos, agarramos el primero de la lista que sirva
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
            return "⚠️ No se pudo configurar el modelo de IA (Ver logs)."

        if not lista_proyectos:
            return "No hay proyectos nuevos para analizar."

        # 1. Preparamos el texto
        texto_proyectos = ""
        for p in lista_proyectos:
            # Indices: 2=Expediente, 3=Autor, 5=Título/Proyecto
            texto_proyectos += f"- Exp: {p[2]} | Autor: {p[3]} | Título: {p[5]}\n"

        # 2. El Prompt
        prompt = f"""
        Eres un Analista de Riesgo Regulatorio para el Banco BBVA Argentina.
        Analiza la siguiente lista de nuevos proyectos de ley ingresados en Diputados:

        {texto_proyectos}

        Instrucciones:
        1. Identifica temas recurrentes.
        2. Busca palabras clave de riesgo bancario: Tasas, BCRA, Tarjetas, Créditos, Fintech, Deudores, Impuestos.
        3. Si un proyecto impacta al banco, márcalo con 🚨.
        4. Si son irrelevantes, pon: "🟢 Sin impacto regulatorio relevante."

        Formato de respuesta (para WhatsApp):
        - Usa emojis.
        - Máximo 200 palabras.
        - Directo al grano.
        """

        try:
            # Generamos la respuesta
            response = self.model.generate_content(prompt)
            return f"\n🧠 *Análisis IA ({self.model.model_name}):*\n{response.text}"

        except Exception as e:
            print(f"DEBUG IA: {e}") 
            return f"\n⚠️ Error generando análisis: {str(e)}"
