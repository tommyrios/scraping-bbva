import os
import json
import time
import requests
from bs4 import BeautifulSoup
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

    def traer_texto_boletin(self, url):
        """
        Entra a la URL del Boletín Oficial y extrae el texto de la norma
        para que la IA pueda encontrar los nombres de los funcionarios.
        """
        if not url or "boletinoficial.gob.ar" not in url:
            return ""
        
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
            response = requests.get(url, headers=headers, timeout=10)
            
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')

                contenido = soup.find('div', {'id': 'avisodetalle'})
                if not contenido:
                    contenido = soup.find('div', class_='detalle-aviso')
                
                if contenido:
                    texto_limpio = contenido.get_text(separator=' ', strip=True)

                    return texto_limpio[:8000] 
            
            return ""
        except Exception as e:
            print(f"⚠️ No se pudo leer contenido de {url}: {e}")
            return ""

    def analizar_proyectos(self, filas_nuevas):
        if not self.client or not filas_nuevas:
            return "Sin novedades relevantes.", []

        lista_proy_texto = []
        meta_data_por_id = {} 

        print(f">>> Recopilando texto completo de {len(filas_nuevas)} items (esto puede tardar unos segundos)...")

        for fila in filas_nuevas:
            id_interno = fila[0]
            origen = fila[1]
            expediente = fila[2]
            titulo = fila[5]
            
            texto_extra = ""
            
            if "Boletin" in origen:
                link = fila[6]
                # AQUÍ ESTÁ LA MAGIA: Traemos el texto real
                texto_extra = self.traer_texto_boletin(link)
            else:
                link = self.generar_link(origen, expediente)
            
            meta_data_por_id[id_interno] = {"titulo": titulo, "link": link, "origen": origen}
            
            # Combinamos el título corto con el texto completo extraído
            descripcion_completa = titulo
            if texto_extra:
                descripcion_completa += f" || CONTENIDO DETALLADO: {texto_extra}"

            item = {
                "id_interno": id_interno, 
                "referencia": expediente,
                "descripcion": descripcion_completa, # Enviamos TODO a la IA
                "fuente": origen
            }
            lista_proy_texto.append(str(item))

        prompt = f"""
        Actúa como un analista legislativo senior para Banco BBVA (Estilo Agencia de Noticias / BLapp).
        Analiza los siguientes items del Boletín Oficial y Congreso.
        
        IMPORTANTE: Se te ha provisto el "CONTENIDO DETALLADO" de las normas. ÚSALO para encontrar nombres propios.

        TU OBJETIVO: Precisión absoluta.

        Instrucciones para la redacción de campos:
        1. "titulo_descriptivo":
           - Titular periodístico breve.
           - Si es DESIGNACIÓN: "Designación de [NOMBRE Y APELLIDO ENCONTRADO EN EL TEXTO] en [ORGANISMO]".
           - Si es NORMATIVA: "Cambios en [TEMA PRINCIPAL]".
           - Elimina códigos burocráticos.

        2. "justificacion" (Observación Técnica):
           - ESTILO: Descriptivo y preciso.
           - PARA DESIGNACIONES: Busca en el texto provisto el nombre de la persona designada. Ejemplo: "El decreto designa a Fernando Iglesias...".
           - PARA NORMATIVAS: Detalla montos, tasas y plazos extraídos del texto.

        Devuelve un JSON con esta estructura exacta:
        {{
            "boletin": {{
                "resumen": "Resumen ejecutivo de 3 líneas. OBLIGATORIO: Menciona explícitamente los APELLIDOS de los funcionarios designados que encuentres en el texto.",
                "items": [ 
                    {{ 
                        "id_interno": "...", 
                        "referencia": "...", 
                        "titulo_descriptivo": "...",
                        "impacto": "...", 
                        "justificacion": "..." 
                    }} 
                ]
            }},
            "diputados": {{
                "resumen": "Resumen ejecutivo de actividad parlamentaria.",
                "items": []
            }},
            "senado": {{
                "resumen": "Resumen ejecutivo de actividad parlamentaria.",
                "items": []
            }}
        }}

        CRITERIOS DE IMPACTO:
        - ALTO: Normas vigentes o Proyectos clave (Financiero, Cambiario, Impositivo, Laboral).
        - MEDIO: Designaciones de funcionarios y normas sectoriales específicas.
        - BAJO: Temas de interés general.

        Datos a analizar:
        {json.dumps(lista_proy_texto, ensure_ascii=False)}
        """

        modelos = ["gemini-2.5-flash", "gemini-2.5-flash-lite", "gemini-2.0-flash", "gemini-2.0-flash-lite"]
        
        for modelo in modelos:
            for intento in range(3): 
                try:
                    print(f"Usando modelo {modelo}")
                    
                    response = self.client.models.generate_content(
                        model=modelo, contents=prompt,
                        config=types.GenerateContentConfig(response_mime_type="application/json")
                    )
                    data = json.loads(response.text)
                    
                    mensaje_final = ""
                    todos_los_detalles_para_excel = []

                    secciones = [
                        ("Reporte Boletín Oficial", "boletin"),
                        ("Reporte Diputados", "diputados"),
                        ("Reporte Senado", "senado")
                    ]

                    def formatear_item(p):
                        id_ref = p.get('id_interno')
                        meta = meta_data_por_id.get(id_ref, {})
                        
                        titulo_mostrar = p.get("titulo_descriptivo", meta.get("titulo", "Sin título"))
                        link_web = meta.get("link", "")
                        ref = p.get('referencia', '')
                        
                        texto = f"• *[{ref}]:* {titulo_mostrar}\n"
                        texto += f"{p.get('justificacion')}\n"
                        texto += f"Link: {link_web}\n"
                        return texto

                    for titulo_seccion, key_json in secciones:
                        bloque = data.get(key_json, {})
                        items = bloque.get("items", [])
                        resumen = bloque.get("resumen", "Sin movimientos.")
                        
                        todos_los_detalles_para_excel.extend(items)

                        if not items and "Sin movimientos" in resumen:
                            continue

                        mensaje_final += f"📢 *{titulo_seccion}*\n"
                        mensaje_final += f"{resumen}\n\n"

                        altos = [x for x in items if x.get('impacto') == 'ALTO']
                        medios = [x for x in items if x.get('impacto') == 'MEDIO']
                        
                        if altos:
                            mensaje_final += "🚨 *Impacto ALTO*\n"
                            for p in altos:
                                mensaje_final += formatear_item(p) + "\n"
                        
                        if medios:
                            mensaje_final += "⚠️ *Impacto MEDIO*\n"
                            for p in medios:
                                mensaje_final += formatear_item(p) + "\n"
                        
                        mensaje_final += "----------------------------------------\n\n"

                    if not mensaje_final:
                        mensaje_final = "✅ *Sin novedades legislativas ni normativas relevantes hoy.*"

                    return mensaje_final, todos_los_detalles_para_excel

                except Exception as e:
                    errores_saturacion = ["503", "overloaded", "429", "quota", "Resource has been exhausted"]
                    es_saturacion = any(err in str(e) for err in errores_saturacion)
                    
                    if es_saturacion:
                        tiempo_espera = 5 * (intento + 1) 
                        print(f"⚠️ Modelo {modelo} saturado. Reintentando en {tiempo_espera}s... ({intento+1}/3)")
                        time.sleep(tiempo_espera)
                        continue 
                    else:
                        print(f"❌ Error no recuperable con {modelo}: {e}")
                        break 
        
        return "Error en análisis IA", []
