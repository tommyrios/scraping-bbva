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
    print("--- Iniciando Workflow Senado ---")
    
    try:
        bot = ScrapearSenado()
        df_resultado = bot.scrape()

        if df_resultado is not None and not df_resultado.empty:
            df_resultado = df_resultado.iloc[::-1]
            print(f"✅ Scraping finalizado. {len(df_resultado)} proyectos.")
        else:
            print("⚠️ El scraping no devolvió resultados.")
            sender.enviar_difusion("⚠️ *Alerta Senado*: Sin datos.")
            exit()

        print("-" * 50)
        
        if 'GCP_CREDENTIALS' in os.environ:
            json_creds = json.loads(os.environ['GCP_CREDENTIALS'])
            scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
            creds = Credentials.from_service_account_info(json_creds, scopes=scopes)
            gc = gspread.authorize(creds)
        else:
            raise Exception("Falta GCP_CREDENTIALS")

        URL_PLANILLA = "https://docs.google.com/spreadsheets/d/16aksCoBrIFB6Vy8JpiuVBEpfGNHdUNJcsCKb2k33tsQ/edit?gid=0#gid=0"
        NOMBRE_HOJA = "Proyectos"

        wb = gc.open_by_url(URL_PLANILLA)
        sheet = wb.worksheet(NOMBRE_HOJA)

        todos_los_datos = sheet.get_all_values()
        cantidad_filas_ocupadas = len(todos_los_datos)
        
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
                    'fila_excel': i + 1,
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

        operaciones_batch = [] 
        filas_nuevas_analisis = [] 

        contador_actualizados = 0
        contador_nuevos = 0
        contador_omitidos = 0

        nuevos_reales = []
        for index, row in df_resultado.iterrows():
            if str(row['Expediente']).strip() not in mapa_expedientes:
                nuevos_reales.append(row)
        
        if nuevos_reales:
            print(f"🧱 Agregando {len(nuevos_reales)} filas vacías...")
            sheet.add_rows(len(nuevos_reales))
        
        puntero_fila = cantidad_filas_ocupadas + 1

        for index, row in df_resultado.iterrows():
            exp_web = str(row['Expediente']).strip()
            fecha_web = str(row['Fecha de inicio']).strip()

            if exp_web in mapa_expedientes:
                info = mapa_expedientes[exp_web]
                datos_viejos = info['datos']
                fecha_sheet = str(datos_viejos[4]).strip() if len(datos_viejos) > 4 else ""

                if fecha_web != fecha_sheet:
                    id_orig = datos_viejos[0]
                    estado = datos_viejos[7] if len(datos_viejos) > 7 else ""
                    prob = datos_viejos[8] if len(datos_viejos) > 8 else ""
                    obs = datos_viejos[11] if len(datos_viejos) > 11 else ""

                    fila_update = [
                        id_orig, 'Senado', row['Expediente'], row['Autor'],
                        row['Fecha de inicio'], row['Proyecto'], row['Comisiones'],
                        estado, prob, row['Partido Político'], row['Provincia'], obs
                    ]
                    fila_update = [str(x) if pd.notna(x) else "" for x in fila_update]
                    
                    rango = f"A{info['fila_excel']}:L{info['fila_excel']}"
                    operaciones_batch.append({'range': rango, 'values': [fila_update]})
                    contador_actualizados += 1
                else:
                    contador_omitidos += 1
            else:
                id_str = f"PL{proximo_id:03d}"
                fila_new = [
                    id_str, 'Senado', row['Expediente'], row['Autor'],
                    row['Fecha de inicio'], row['Proyecto'], row['Comisiones'],
                    '', '', row['Partido Político'], row['Provincia'], ''
                ]
                fila_new = [str(x) if pd.notna(x) else "" for x in fila_new]
                
                rango = f"A{puntero_fila}:L{puntero_fila}"
                operaciones_batch.append({'range': rango, 'values': [fila_new]})
                
                fila_meta = list(fila_new)
                fila_meta.append(puntero_fila)
                filas_nuevas_analisis.append(fila_meta)
                
                proximo_id += 1
                puntero_fila += 1
                contador_nuevos += 1

        if operaciones_batch:
            print(f"💾 Guardando {len(operaciones_batch)} cambios base...")
            sheet.batch_update(operaciones_batch, value_input_option='USER_ENTERED')

        print("🤖 Solicitando análisis a Gemini...")
        analista = AnalistaLegislativo()
        datos_para_ia = [f[:-1] for f in filas_nuevas_analisis]
        texto_whatsapp, detalles_ia = analista.analizar_proyectos(datos_para_ia)

        updates_ia = []
        if detalles_ia:
            print("🧠 Escribiendo análisis de IA en la planilla...")
            mapa_id_fila = { f[0]: f[12] for f in filas_nuevas_analisis }

            for item in detalles_ia:
                id_interno = item.get('id_interno')
                impacto = item.get('impacto', '')
                justificacion = item.get('justificacion', '')
                
                if id_interno in mapa_id_fila:
                    num_fila = mapa_id_fila[id_interno]
                    updates_ia.append({'range': f"I{num_fila}", 'values': [[impacto]]})
                    updates_ia.append({'range': f"L{num_fila}", 'values': [[f"IA: {justificacion}"]]})

            if updates_ia:
                sheet.batch_update(updates_ia, value_input_option='USER_ENTERED')

        msg_final = (
            f"*Reporte Senado Diario*\n\n"
            f"✅ *Nuevos:* {contador_nuevos}\n"
            f"🔄 *Actualizados:* {contador_actualizados}\n"
            f"⏭️ *Sin Cambios:* {contador_omitidos}\n\n"
            f"📝 *Resumen IA:*\n{texto_whatsapp}"
        )

        sender.enviar_difusion(msg_final)
        print("✅ Fin del proceso.")

    except Exception as e:
        err_msg = f"❌ *Error Crítico Senado*: {str(e)}"
        print(err_msg)
        sender.enviar_difusion(err_msg)
        exit(1)