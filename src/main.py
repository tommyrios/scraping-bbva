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

class GestorEstado:
    def __init__(self, sheet_obj, prefijo_id, columna_id=0):
        self.sheet = sheet_obj
        self.prefijo = prefijo_id
        
        raw_data = self.sheet.get_all_values()
        self.fila_actual = len(raw_data) + 1
        
        ids_existentes = []
        self.mapa_existentes = {} 
        
        idx_expediente = 1 if prefijo_id == "BO" else 2

        for i, row in enumerate(raw_data):
            if i == 0: continue 
            if len(row) < 2: continue
            
            id_val = str(row[0]).strip()
            exp_val = str(row[idx_expediente]).strip() if len(row) > idx_expediente else ""
            
            if exp_val:
                self.mapa_existentes[exp_val] = {
                    'fila': i + 1,
                    'datos': row
                }

            if id_val.startswith(self.prefijo):
                try:
                    num = int(id_val.replace(self.prefijo, ""))
                    ids_existentes.append(num)
                except: pass
        
        self.proximo_id_num = (max(ids_existentes) + 1) if ids_existentes else 1

    def obtener_datos_existentes(self, expediente):
        return self.mapa_existentes.get(str(expediente).strip())

    def registrar_nuevo(self):
        id_str = f"{self.prefijo}{self.proximo_id_num:03d}"
        fila = self.fila_actual
        
        self.proximo_id_num += 1
        self.fila_actual += 1
        
        return id_str, fila

def procesar_lote(df_nuevos, gestor, nombre_origen, operaciones_globales, filas_ia_globales):
    stats = {"nuevos": 0, "reanalizados": 0, "omitidos": 0}
    
    if df_nuevos is None or df_nuevos.empty:
        return stats

    nuevos_reales_count = 0
    for _, row in df_nuevos.iterrows():
        if not gestor.obtener_datos_existentes(row['Expediente']):
            nuevos_reales_count += 1
            
    if nuevos_reales_count > 0:
        gestor.sheet.add_rows(nuevos_reales_count)

    for _, row in df_nuevos.iterrows():
        exp_web = str(row['Expediente']).strip()
        autor = str(row['Autor'])
        fecha = str(row['Fecha de inicio'])
        proyecto = str(row['Proyecto'])

        link = str(row['Comisiones'])

        link_para_reporte = str(row.get('Link Texto', '')).strip() if gestor.prefijo != "BO" else link
        
        partido = str(row.get('Partido Político', ''))
        provincia = str(row.get('Provincia', ''))

        info_existente = gestor.obtener_datos_existentes(exp_web)

        if info_existente:
            datos_viejos = info_existente['datos']
            fila_excel = info_existente['fila']
            
            idx_obs = 5 if gestor.prefijo == "BO" else 11
            obs_actual = str(datos_viejos[idx_obs]).strip() if len(datos_viejos) > idx_obs else ""

            if not obs_actual or "Error" in obs_actual:
                stats["reanalizados"] += 1

                if gestor.prefijo == "BO":
                    origen_final = nombre_origen
                    link_reporte_final = link  
                else:
                    origen_final = str(datos_viejos[1]).strip() if len(datos_viejos) > 1 else nombre_origen
                    link_reporte_final = ""

                fila_ia = [
                    datos_viejos[0], origen_final, exp_web, autor,
                    fecha, proyecto, link_reporte_final,
                    '','','','','' 
                ]
                fila_ia.append(fila_excel) 
                filas_ia_globales.append(fila_ia)
            else:
                stats["omitidos"] += 1
        else:
            id_nuevo, fila_excel = gestor.registrar_nuevo()
            
            if gestor.prefijo == "BO":
                valores = [
                    id_nuevo, exp_web, autor, fecha,
                    '', '', link 
                ]
                rango = f"A{fila_excel}:G{fila_excel}"
            else:
                origen_final = str(row.get('Cámara de Origen') or nombre_origen).strip() or nombre_origen

                valores = [
                    id_nuevo, origen_final, exp_web, autor,
                    fecha, proyecto, link,
                    '', '', partido, provincia, ''
                ]
                rango = f"A{fila_excel}:L{fila_excel}"

            valores = [str(x) if pd.notna(x) else "" for x in valores]
            operaciones_globales.append({'range': rango, 'values': [valores]})
            
            fila_ia = [
                id_nuevo, origen_final if gestor.prefijo != "BO" else nombre_origen, exp_web, autor,
                fecha, proyecto, link_para_reporte if gestor.prefijo != "BO" else link,
                '', '', '', '', ''
            ]
            fila_ia.append(fila_excel)
            filas_ia_globales.append(fila_ia)
            
            stats["nuevos"] += 1
            
    return stats

if __name__ == "__main__":
    print("--- Sistema Unificado v2.0 (COMPLETO) ---")
    sender = MensajeSender()
    
    try:
        if 'GCP_CREDENTIALS' not in os.environ: raise Exception("Falta GCP_CREDENTIALS")
        json_creds = json.loads(os.environ['GCP_CREDENTIALS'])
        creds = Credentials.from_service_account_info(json_creds, scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"])
        gc = gspread.authorize(creds)
        
        wb = gc.open_by_url("https://docs.google.com/spreadsheets/d/16aksCoBrIFB6Vy8JpiuVBEpfGNHdUNJcsCKb2k33tsQ/edit?gid=0#gid=0")
        sheet_proy = wb.worksheet("Proyectos")
        sheet_bo = wb.worksheet("Boletin")

        gestor_proy = GestorEstado(sheet_proy, "PL")
        gestor_bo = GestorEstado(sheet_bo, "BO")

        filas_ia_globales = []
        batch_proy = []
        batch_bo = []
        
        print(">>> Diputados...")
        try:
            df = ScrapearDiputados().scrape("https://www.diputados.gov.ar/proyectos/")
            if df is not None and not df.empty: df = df.iloc[::-1]
            st = procesar_lote(df, gestor_proy, "Diputados", batch_proy, filas_ia_globales)
            print(f"   Resultados: {st['nuevos']} nuevos")
        except Exception as e: 
            print(f"❌ Error Diputados: {e}")

        print(">>> Senado...")
        try:
            df = ScrapearSenado().scrape()
            if df is not None and not df.empty: df = df.iloc[::-1]
            st = procesar_lote(df, gestor_proy, "Senado", batch_proy, filas_ia_globales)
            print(f"   Resultados: {st['nuevos']} nuevos")
        except Exception as e: 
            print(f"❌ Error Senado: {e}")
        
        print(">>> Boletín...")
        try:
            df = ScrapearBoletin().scrape()
            st = procesar_lote(df, gestor_bo, "Boletin Oficial", batch_bo, filas_ia_globales)
            print(f"   Resultados: {st['nuevos']} normas")
        except Exception as e: 
            print(f"❌ Error Boletín: {e}")

        if batch_proy: sheet_proy.batch_update(batch_proy, value_input_option='USER_ENTERED')
        if batch_bo: sheet_bo.batch_update(batch_bo, value_input_option='USER_ENTERED')

        print(">>> Analizando IA...")
        analista = AnalistaLegislativo()
        datos_limpios = [f[:-1] for f in filas_ia_globales]
        
        texto_analisis, resultados_ia = analista.analizar_proyectos(datos_limpios)

        if resultados_ia:
            print(">>> Guardando resultados IA en Excel...")
            updates_proy_ia = []
            updates_bo_ia = []
            
            mapa_filas = {f[0]: f[-1] for f in filas_ia_globales}

            for item in resultados_ia:
                id_ref = item.get('id_interno')
                imp = item.get('impacto', '')
                just = item.get('justificacion', '')
                
                if id_ref in mapa_filas:
                    fila = mapa_filas[id_ref]
                    if id_ref.startswith("BO"):
                        updates_bo_ia.append({'range': f"E{fila}", 'values': [[imp]]})
                        updates_bo_ia.append({'range': f"F{fila}", 'values': [[just]]})
                    else:
                        updates_proy_ia.append({'range': f"I{fila}", 'values': [[imp]]})
                        updates_proy_ia.append({'range': f"L{fila}", 'values': [[just]]})

            if updates_proy_ia: sheet_proy.batch_update(updates_proy_ia, value_input_option='USER_ENTERED')
            if updates_bo_ia: sheet_bo.batch_update(updates_bo_ia, value_input_option='USER_ENTERED')

        print(">>> Enviando mail...")
        sender.enviar_difusion(texto_analisis)
        print("✅ Éxito total.")

    except Exception as e:
        print(f"❌ FATAL: {e}")
        sender.enviar_difusion(f"<html><body><h1>Error Crítico en Ejecución</h1><p>{e}</p></body></html>")
        exit(1)