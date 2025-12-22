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

        # LISTA DE MODELOS DE RESPALDO (Si falla uno, prueba el otro)
        modelos_a_probar = [
            "gemini-1.5-flash",
            "gemini-1.5-flash-002",
            "gemini-1.5-flash-001",
            "gemini-2.0-flash-exp"
        ]

        intentos_maximos_por_modelo = 2
        espera_base = 5

        for modelo in modelos_a_probar:
            print(f"Probando modelo IA: {modelo}...")
            
            for intento in range(intentos_maximos_por_modelo):
                try:
                    response = self.client.models.generate_content(
                        model=modelo,
                        contents=prompt,
                        config=types.GenerateContentConfig(
                            response_mime_type="application/json"
                        )
                    )
                    
                    # Si llegamos aca, funcionó
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
                    
                    # Si es error 404 (Modelo no encontrado), rompemos el loop de reintentos 
                    # y pasamos al SIGUIENTE MODELO de la lista principal
                    if "404" in error_str or "NOT_FOUND" in error_str:
                        print(f"Modelo {modelo} no encontrado (404). Pasando al siguiente...")
                        break 
                    
                    # Si es error 429 (Cuota), esperamos y reintentamos EL MISMO modelo
                    if "429" in error_str or "RESOURCE_EXHAUSTED" in error_str:
                        if intento < intentos_maximos_por_modelo - 1:
                            tiempo_espera = espera_base * (intento + 1)
                            print(f"Cuota excedida ({modelo}). Reintentando en {tiempo_espera}s...")
                            time.sleep(tiempo_espera)
                            continue
                    
                    print(f"Error desconocido en Gemini ({modelo}): {e}")
                    # Si es otro error, tambien probamos el siguiente modelo por las dudas
                    break 
        
        return "Error: Ningún modelo de IA respondió correctamente.", []
