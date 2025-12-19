import os
import json
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from notifications import MensajeSender
from senadores import ScrapearSenado
from analisis import AnalistaLegislativo

if __name__ == "__main__":
    
    sender = MensajeSender()
    print("--- Iniciando Workflow Senado (Modo Coordenadas Manuales) ---")
    
    try:
        bot = ScrapearSenado()
        df_resultado = bot.scrape()

        if df_resultado is not None and not df_resultado.empty:
            df_resultado = df_resultado.iloc[::-1]
            print(f"✅ Scraping finalizado. {len(df_resultado)} proyectos recolectados.")
        else:
            print("⚠️ El scraping no devolvió resultados.")
            sender.enviar_difusion("⚠️ *Alerta Senado*: No se obtuvieron datos de la web.")
            exit()

        print("-" * 50)
        print("Conectando con Google Sheets...")

        if 'GCP_CREDENTIALS' in os.environ:
            json_creds = json.loads(os.environ['GCP_CREDENTIALS'])
            scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
            creds = Credentials.from_service_account_info(json_creds, scopes=scopes)
            gc = gspread.authorize(creds)
        else:
            raise Exception("No se encontró GCP_CREDENTIALS")

        URL_PLANILLA = "https://docs.google.com/spreadsheets/d/16aksCoBrIFB6Vy8JpiuVBEpfGNHdUNJcsCKb2k33tsQ/edit?gid=0#gid=0"
        NOMBRE_HOJA = "Proyectos"

        wb = gc.open_by_url(URL_PLANILLA)
        sheet = wb.worksheet(NOMBRE_HOJA)

        print("Leyendo estructura actual de la hoja...")
        todos_los_datos = sheet.get_all_values()
        
        cantidad_filas_actuales = len(todos_los_datos)
        print(f"📊 La hoja tiene actualmente {cantidad_filas_actuales} filas ocupadas.")

        mapa_expedientes = {}
        ids_existentes_numeros = []

        for i, fila in enumerate(todos_los_datos):
            if i == 0: continue 
            
            if len(fila) < 3: continue

            id_actual = str(fila[0]).strip()
            exp_actual = str(fila[2]).strip() 
            
            if exp_actual:
                mapa_expedientes[exp_actual] = {
                    'indice_lista': i, 
                    'datos': fila
                }
            
            if id_actual.startswith("PL"):
                try:
                    num = int(id_actual.replace("PL", ""))
                    ids_existentes_numeros.append(num)
                except: pass

        proximo_id = 1
        if ids_existentes_numeros:
            proximo_id = max(ids_existentes_numeros) + 1
        
        print(f"ℹ️ Próximo ID a asignar: PL{proximo_id:03d}")

        operaciones_batch = [] 
        filas_nuevas_analisis = [] 

        contador_actualizados = 0
        contador_nuevos = 0
        contador_omitidos = 0
        
        puntero_fila_nueva = cantidad_filas_actuales + 1

        for index, row in df_resultado.iterrows():
            exp_web = str(row['Expediente']).strip()
            fecha_web = str(row['Fecha de inicio']).strip()

            if exp_web in mapa_expedientes:
                info_sheet = mapa_expedientes[exp_web]
                fila_idx = info_sheet['indice_lista']
                datos_viejos = info_sheet['datos']
                
                fecha_sheet = str(datos_viejos[4]).strip() if len(datos_viejos) > 4 else ""

                if fecha_web != fecha_sheet:
                    id_orig = datos_viejos[0]
                    estado_orig = datos_viejos[7] if len(datos_viejos) > 7 else ""
                    prob_orig = datos_viejos[8] if len(datos_viejos) > 8 else ""
                    obs_orig = datos_viejos[11] if len(datos_viejos) > 11 else ""

                    fila_update = [
                        id_orig,            
                        'Senado',           
                        row['Expediente'],  
                        row['Autor'],       
                        row['Fecha de inicio'], 
                        row['Proyecto'],    
                        row['Comisiones'],  
                        estado_orig,        
                        prob_orig,          
                        row['Partido Político'], 
                        row['Provincia'],   
                        obs_orig            
                    ]
                    
                    fila_update = [str(x) if pd.notna(x) else "" for x in fila_update]
                    
                    numero_fila_real = fila_idx + 1 
                    rango = f"A{numero_fila_real}:L{numero_fila_real}"
                    
                    operaciones_batch.append({
                        'range': rango,
                        'values': [fila_update]
                    })
                    contador_actualizados += 1
                else:
                    contador_omitidos += 1
            else:
                id_str = f"PL{proximo_id:03d}"
                fila_new = [
                    id_str,
                    'Senado',
                    row['Expediente'],
                    row['Autor'],
                    row['Fecha de inicio'],
                    row['Proyecto'],
                    row['Comisiones'],
                    '', '',
                    row['Partido Político'],
                    row['Provincia'],
                    ''
                ]
                fila_new = [str(x) if pd.notna(x) else "" for x in fila_new]
                
                rango = f"A{puntero_fila_nueva}:L{puntero_fila_nueva}"
                
                operaciones_batch.append({
                    'range': rango,
                    'values': [fila_new]
                })
                
                filas_nuevas_analisis.append(fila_new)
                
                proximo_id += 1
                puntero_fila_nueva += 1
                contador_nuevos += 1

        if operaciones_batch:
            print(f"💾 Guardando {len(operaciones_batch)} cambios en la planilla...")
            sheet.batch_update(operaciones_batch)
        else:
            print("💤 No hubo cambios para guardar.")

        print("🤖 Solicitando análisis a Gemini...")
        analista = AnalistaLegislativo()
        texto_analisis = analista.analizar_proyectos(filas_nuevas_analisis)

        msg_final = (
            f"*Reporte Senado Diario*\n\n"
            f"✅ *Nuevos:* {contador_nuevos}\n"
            f"🔄 *Actualizados:* {contador_actualizados}\n"
            f"⏭️ *Sin Cambios:* {contador_omitidos}\n\n"
            f"📝 *Análisis IA:*\n{texto_analisis}"
        )

        sender.enviar_difusion(msg_final)
        print("✅ Fin del proceso.")

    except Exception as e:
        err_msg = f"❌ *Error Crítico Senado*: {str(e)}"
        print(err_msg)
        sender.enviar_difusion(err_msg)
        exit(1)
