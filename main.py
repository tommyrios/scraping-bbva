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
    """
    Carga datos en Sheet y devuelve estadísticas y filas para analizar.
    NO genera texto de reporte, solo devuelve datos crudos.
    """
    stats = {
        "origen": nombre_origen,
        "nuevos": 0,
        "reanalizados": 0,
        "omitidos": 0,
        "error": None
    }
    
    if df_nuevos is None or df_nuevos.empty:
        stats["error"] = "Sin datos web"
        return stats, []

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
                stats["reanalizados"] += 1
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
            else:
                stats["omitidos"] += 1
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
            stats["nuevos"] += 1

    if operaciones_batch:
        hoja_sheet.batch_update(operaciones_batch, value_input_option='USER_ENTERED')
        
    return stats, filas_para_analizar

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

        todas_filas_para_ia = []
        texto_estadisticas = "*📊 Reporte de Actividad Diaria*\n\n"

        print(">>> Procesando Diputados...")
        try:
            bot_dip = ScrapearDiputados()
            df_dip = bot_dip.scrape("https://www.diputados.gov.ar/proyectos/")
            if df_dip is not None and not df_dip.empty:
                df_dip = df_dip.iloc[::-1]
            
            stats_dip, filas_dip = procesar_datos(df_dip, sheet, "Diputados")
            todas_filas_para_ia.extend(filas_dip)
            
            if stats_dip['error']:
                texto_estadisticas += f"🏛️ *Diputados:* ⚠️ {stats_dip['error']}\n"
            else:
                texto_estadisticas += (
                    f"🏛️ *Diputados:*\n"
                    f"   ✅ Nuevos: {stats_dip['nuevos']} | ♻️ Re-analizados: {stats_dip['reanalizados']}\n"
                )

        except Exception as e:
            print(f"Error Diputados: {e}")
            texto_estadisticas += f"🏛️ *Diputados:* ❌ Error crítico ({str(e)})\n"

        print(">>> Procesando Senado...")
        try:
            bot_sen = ScrapearSenado()
            df_sen = bot_sen.scrape()
            if df_sen is not None and not df_sen.empty:
                df_sen = df_sen.iloc[::-1]
            
            stats_sen, filas_sen = procesar_datos(df_sen, sheet, "Senado")
            todas_filas_para_ia.extend(filas_sen)

            if stats_sen['error']:
                texto_estadisticas += f"🏛️ *Senado:* ⚠️ {stats_sen['error']}\n"
            else:
                texto_estadisticas += (
                    f"🏛️ *Senado:*\n"
                    f"   ✅ Nuevos: {stats_sen['nuevos']} | ♻️ Re-analizados: {stats_sen['reanalizados']}\n"
                )

        except Exception as e:
            print(f"Error Senado: {e}")
            texto_estadisticas += f"🏛️ *Senado:* ❌ Error crítico ({str(e)})\n"

        texto_estadisticas += "\n----------------------------------------\n"

        print(">>> Analizando con IA...")
        analista = AnalistaLegislativo()
        datos_para_ia = [f[:-1] for f in todas_filas_para_ia]
        texto_analisis, detalles_ia = analista.analizar_proyectos(datos_para_ia)

        updates_ia = []
        if detalles_ia:
            mapa_id_fila = { f[0]: f[12] for f in todas_filas_para_ia }
            for item in detalles_ia:
                id_interno = item.get('id_interno')
                impacto = item.get('impacto', '')
                justificacion = item.get('justificacion', '')
                
                if id_interno in mapa_id_fila:
                    num_fila = mapa_id_fila[id_interno]
                    updates_ia.append({'range': f"I{num_fila}", 'values': [[impacto]]})
                    updates_ia.append({'range': f"L{num_fila}", 'values': [[justificacion]]})

            if updates_ia:
                sheet.batch_update(updates_ia, value_input_option='USER_ENTERED')

        reporte_final = (
            f"{texto_estadisticas}\n"
            f"{texto_analisis}"
        )

        print(">>> Enviando Email...")
        sender.enviar_difusion(reporte_final)
        print("✅ Proceso finalizado.")

    except Exception as e:
        err_msg = f"❌ *Error Crítico General*: {str(e)}"
        print(err_msg)
        sender.enviar_difusion(err_msg)
        exit(1)