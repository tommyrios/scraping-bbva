import os
import json
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from notifications import MensajeSender
from diputados import ScrapearDiputados
from senadores import ScrapearSenado
from analisis import AnalistaLegislativo

def procesar_datos(df_nuevos, hoja_sheet, nombre_origen):
    if df_nuevos is None or df_nuevos.empty:
        return f"⚠️ *{nombre_origen}:* No se obtuvieron datos o la web estaba caída.\n"

    todos_los_datos = hoja_sheet.get_all_values()
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
    filas_para_analizar = [] 

    contador_actualizados = 0
    contador_nuevos = 0
    contador_recuperados_ia = 0
    contador_omitidos = 0

    nuevos_reales = []
    for index, row in df_nuevos.iterrows():
        if str(row['Expediente']).strip() not in mapa_expedientes:
            nuevos_reales.append(row)
    
    if nuevos_reales:
        hoja_sheet.add_rows(len(nuevos_reales))
    
    puntero_fila = cantidad_filas_ocupadas + 1

    for index, row in df_nuevos.iterrows():
        exp_web = str(row['Expediente']).strip()
        fecha_web = str(row['Fecha de inicio']).strip()

        if exp_web in mapa_expedientes:
            info = mapa_expedientes[exp_web]
            datos_viejos = info['datos']
            fecha_sheet = str(datos_viejos[4]).strip() if len(datos_viejos) > 4 else ""
            obs_actual = str(datos_viejos[11]).strip() if len(datos_viejos) > 11 else ""
            
            if not obs_actual or "Error" in obs_actual:
                contador_recuperados_ia += 1
                fila_reconstruida = [
                    datos_viejos[0], nombre_origen, row['Expediente'], row['Autor'],
                    row['Fecha de inicio'], row['Proyecto'], row['Comisiones'],
                    '','','','',''
                ]
                fila_reconstruida.append(info['fila_excel'])
                filas_para_analizar.append(fila_reconstruida)

            if fecha_web != fecha_sheet:
                id_orig = datos_viejos[0]
                estado = datos_viejos[7] if len(datos_viejos) > 7 else ""
                prob = datos_viejos[8] if len(datos_viejos) > 8 else ""
                obs = datos_viejos[11] if len(datos_viejos) > 11 else ""

                fila_update = [
                    id_orig, nombre_origen, row['Expediente'], row['Autor'],
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
                id_str, nombre_origen, row['Expediente'], row['Autor'],
                row['Fecha de inicio'], row['Proyecto'], row['Comisiones'],
                '', '', row['Partido Político'], row['Provincia'], ''
            ]
            fila_new = [str(x) if pd.notna(x) else "" for x in fila_new]
            
            rango = f"A{puntero_fila}:L{puntero_fila}"
            operaciones_batch.append({'range': rango, 'values': [fila_new]})
            
            fila_meta = list(fila_new)
            fila_meta.append(puntero_fila)
            filas_para_analizar.append(fila_meta)
            
            proximo_id += 1
            puntero_fila += 1
            contador_nuevos += 1

    if operaciones_batch:
        hoja_sheet.batch_update(operaciones_batch, value_input_option='USER_ENTERED')

    analista = AnalistaLegislativo()
    datos_para_ia = [f[:-1] for f in filas_para_analizar]
    texto_analisis, detalles_ia = analista.analizar_proyectos(datos_para_ia)

    updates_ia = []
    if detalles_ia:
        mapa_id_fila = { f[0]: f[12] for f in filas_para_analizar }
        for item in detalles_ia:
            id_interno = item.get('id_interno')
            impacto = item.get('impacto', '')
            justificacion = item.get('justificacion', '')
            
            if id_interno in mapa_id_fila:
                num_fila = mapa_id_fila[id_interno]
                updates_ia.append({'range': f"I{num_fila}", 'values': [[impacto]]})
                updates_ia.append({'range': f"L{num_fila}", 'values': [[justificacion]]})

        if updates_ia:
            hoja_sheet.batch_update(updates_ia, value_input_option='USER_ENTERED')

    reporte = (
        f"🏛️ *Cámara de {nombre_origen}*\n"
        f"✅ Nuevos: {contador_nuevos} | ♻️ Re-analizados: {contador_recuperados_ia}\n\n"
        f"{texto_analisis}\n"
        f"----------------------------------------\n\n"
    )
    return reporte

if __name__ == "__main__":
    print("--- Iniciando Proceso Unificado Congreso ---")
    sender = MensajeSender()
    
    try:
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

        reporte_final = "*📢 Reporte Diario del Congreso Argentino*\n\n"

        print(">>> Iniciando Diputados...")
        try:
            bot_dip = ScrapearDiputados()
            df_dip = bot_dip.scrape("https://www.diputados.gov.ar/proyectos/")
            if df_dip is not None and not df_dip.empty:
                df_dip = df_dip.iloc[::-1]
            reporte_final += procesar_datos(df_dip, sheet, "Diputados")
        except Exception as e:
            print(f"Error Diputados: {e}")
            reporte_final += f"⚠️ *Diputados:* Error al procesar: {str(e)}\n\n"

        print(">>> Iniciando Senado...")
        try:
            bot_sen = ScrapearSenado()
            df_sen = bot_sen.scrape()
            if df_sen is not None and not df_sen.empty:
                df_sen = df_sen.iloc[::-1]
            reporte_final += procesar_datos(df_sen, sheet, "Senado")
        except Exception as e:
            print(f"Error Senado: {e}")
            reporte_final += f"⚠️ *Senado:* Error al procesar: {str(e)}\n\n"

        print(">>> Enviando Email Unificado...")
        sender.enviar_difusion(reporte_final)
        print("✅ Proceso finalizado.")

    except Exception as e:
        err_msg = f"❌ *Error Crítico General*: {str(e)}"
        print(err_msg)
        sender.enviar_difusion(err_msg)
        exit(1)