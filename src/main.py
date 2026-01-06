import os
import json
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from notifications import MensajeSender
from diputados import ScrapearDiputados
from senadores import ScrapearSenado
from boletin import ScrapearBoletin
from analisis import AnalistaLegislativo

def procesar_datos(df_nuevos, hoja_sheet, nombre_origen):
    stats = {
        "origen": nombre_origen,
        "nuevos": 0,
        "reanalizados": 0,
        "omitidos": 0,
        "error": None
    }
    
    if df_nuevos is None or df_nuevos.empty:
        stats["error"] = "Sin datos nuevos hoy"
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
        
        prefijo = "BO" if "Boletin" in nombre_origen else "PL"
        if id_actual.startswith(prefijo):
            try:
                num = int(id_actual.replace(prefijo, ""))
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
    prefijo_id = "BO" if "Boletin" in nombre_origen else "PL"

    for index, row in df_nuevos.iterrows():
        exp_web = str(row['Expediente']).strip()
        
        autor = str(row['Autor'])
        fecha = str(row['Fecha de inicio'])
        sintesis_o_titulo = str(row['Proyecto'])
        link = str(row['Comisiones']) 

        partido = str(row.get('Partido Político', ''))
        provincia = str(row.get('Provincia', ''))

        if exp_web in mapa_expedientes:
            info = mapa_expedientes[exp_web]
            datos_viejos = info['datos']
            
            idx_obs = 7 if "Boletin" in nombre_origen else 11
            obs_actual = str(datos_viejos[idx_obs]).strip() if len(datos_viejos) > idx_obs else ""
            
            if not obs_actual or "Error" in obs_actual:
                stats["reanalizados"] += 1
                
                fila_virtual = [
                    datos_viejos[0], nombre_origen, exp_web, autor,
                    fecha, sintesis_o_titulo, link, 
                    '','','','','' 
                ]
                fila_virtual.append(info['fila_excel'])
                filas_para_analizar.append(fila_virtual)
            else:
                stats["omitidos"] += 1
        else:
            id_str = f"{prefijo_id}{proximo_id:03d}"
            
            if "Boletin" in nombre_origen:
                fila_sheet = [
                    id_str, nombre_origen, exp_web, autor,
                    fecha, link 
                ]
                letra_final = "F"
            else:
                fila_sheet = [
                    id_str, nombre_origen, exp_web, autor,
                    fecha, sintesis_o_titulo, link,
                    '', '', partido, provincia, ''
                ]
                letra_final = "L"

            fila_sheet = [str(x) if pd.notna(x) else "" for x in fila_sheet]
            
            rango = f"A{puntero_fila}:{letra_final}{puntero_fila}"
            operaciones_batch.append({'range': rango, 'values': [fila_sheet]})
            
            fila_ia = [
                id_str, nombre_origen, exp_web, autor,
                fecha, sintesis_o_titulo, link,
                '', '', '', '', ''
            ]
            fila_ia.append(puntero_fila)
            filas_para_analizar.append(fila_ia)
            
            proximo_id += 1
            puntero_fila += 1
            stats["nuevos"] += 1

    if operaciones_batch:
        hoja_sheet.batch_update(operaciones_batch, value_input_option='USER_ENTERED')
        
    return stats, filas_para_analizar

if __name__ == "__main__":
    print("--- Iniciando Proceso Unificado ---")
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
        wb = gc.open_by_url(URL_PLANILLA)
        
        sheet_proyectos = wb.worksheet("Proyectos")
        sheet_boletin = wb.worksheet("Boletin")

        todas_filas_para_ia = []
        texto_estadisticas = "*📊 Reporte Diario*\n\n"

        print(">>> Procesando Diputados...")
        try:
            bot_dip = ScrapearDiputados()
            df_dip = bot_dip.scrape("https://www.diputados.gov.ar/proyectos/")
            if df_dip is not None and not df_dip.empty: df_dip = df_dip.iloc[::-1]
            stats, filas = procesar_datos(df_dip, sheet_proyectos, "Diputados")
            todas_filas_para_ia.extend(filas)
            texto_estadisticas += f"🏛️ *Diputados:* {stats['nuevos']} nuevos\n" if not stats['error'] else f"🏛️ *Diputados:* ⚠️ {stats['error']}\n"
        except Exception as e: print(f"Err Dip: {e}")

        print(">>> Procesando Senado...")
        try:
            bot_sen = ScrapearSenado()
            df_sen = bot_sen.scrape()
            if df_sen is not None and not df_sen.empty: df_sen = df_sen.iloc[::-1]
            stats, filas = procesar_datos(df_sen, sheet_proyectos, "Senado")
            todas_filas_para_ia.extend(filas)
            texto_estadisticas += f"🏛️ *Senado:* {stats['nuevos']} nuevos\n" if not stats['error'] else f"🏛️ *Senado:* ⚠️ {stats['error']}\n"
        except Exception as e: print(f"Err Sen: {e}")

        print(">>> Procesando Boletín...")
        try:
            bot_bo = ScrapearBoletin()
            df_bo = bot_bo.scrape()
            stats, filas = procesar_datos(df_bo, sheet_boletin, "Boletin Oficial")
            todas_filas_para_ia.extend(filas)
            texto_estadisticas += f"📜 *Boletín:* {stats['nuevos']} normas\n" if not stats['error'] else f"📜 *Boletín:* ⚠️ {stats['error']}\n"
        except Exception as e: print(f"Err BO: {e}")

        texto_estadisticas += "\n----------------------------------------\n"

        print(">>> Analizando con IA...")
        analista = AnalistaLegislativo()
        datos_ia_clean = [f[:-1] for f in todas_filas_para_ia]
        texto_analisis, detalles_ia = analista.analizar_proyectos(datos_ia_clean)

        if detalles_ia:
            print(">>> Guardando resultados...")
            mapa_id_fila = { f[0]: f[-1] for f in todas_filas_para_ia }
            
            upd_proy = []
            upd_boletin = []

            for item in detalles_ia:
                id_int = item.get('id_interno')
                imp = item.get('impacto', '')
                just = item.get('justificacion', '')
                
                if id_int in mapa_id_fila:
                    fila = mapa_id_fila[id_int]
                    if id_int.startswith("BO"):
                        upd_boletin.append({'range': f"G{fila}", 'values': [[imp]]})
                        upd_boletin.append({'range': f"H{fila}", 'values': [[just]]})
                    else:
                        upd_proy.append({'range': f"I{fila}", 'values': [[imp]]})
                        upd_proy.append({'range': f"L{fila}", 'values': [[just]]})

            if upd_proy: sheet_proyectos.batch_update(upd_proy, value_input_option='USER_ENTERED')
            if upd_boletin: sheet_boletin.batch_update(upd_boletin, value_input_option='USER_ENTERED')

        print(">>> Enviando Email...")
        sender.enviar_difusion(f"{texto_estadisticas}\n{texto_analisis}")
        print("✅ Fin.")

    except Exception as e:
        print(f"❌ Error: {e}")
        sender.enviar_difusion(f"Error Crítico: {e}")
        exit(1)