import os
import json
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from notifications import MensajeSender
from diputados import ScrapearDiputados
from analisis import AnalistaLegislativo

if __name__ == "__main__":
    
    sender = MensajeSender()
    
    try:
        url_objetivo = "https://www.diputados.gov.ar/proyectos/"
        bot = Scrapear()
        df_resultado = bot.scrape(url_objetivo)

        if df_resultado is not None and not df_resultado.empty:
            df_resultado = df_resultado.iloc[::-1]
            print("Orden invertido para carga cronológica.")

        print("-" * 50)
        print("Iniciando proceso de sincronización con Google Sheets...")

        if 'GCP_CREDENTIALS' in os.environ:
            json_creds = json.loads(os.environ['GCP_CREDENTIALS'])
            scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
            creds = Credentials.from_service_account_info(json_creds, scopes=scopes)
            gc = gspread.authorize(creds)
        else:
            raise Exception("No se encontró la variable de entorno GCP_CREDENTIALS")

        URL_PLANILLA = "https://docs.google.com/spreadsheets/d/16aksCoBrIFB6Vy8JpiuVBEpfGNHdUNJcsCKb2k33tsQ/edit?gid=0#gid=0"
        NOMBRE_HOJA = "Proyectos"

        print(f"Abriendo planilla...")
        wb = gc.open_by_url(URL_PLANILLA)
        sheet = wb.worksheet(NOMBRE_HOJA)

        datos_existentes = sheet.get_all_records()
        df_sheet = pd.DataFrame(datos_existentes)

        filas_nuevas = []
        contador_actualizados = 0
        contador_omitidos = 0

        proximo_id = 1
        if not df_sheet.empty and 'ID' in df_sheet.columns:
            max_id_numeric = 0
            max_id_pl_format = 0

            pl_ids_series = df_sheet['ID'][df_sheet['ID'].astype(str).str.startswith('PL', na=False)]
            if not pl_ids_series.empty:
                numeric_parts = pl_ids_series.str.replace('PL', '', regex=False).astype(str).str.extract(r'^(\d+)$', expand=False)
                max_pl_id_val = pd.to_numeric(numeric_parts, errors='coerce').max()
                if pd.notna(max_pl_id_val): max_id_pl_format = int(max_pl_id_val)

            numeric_ids_series = df_sheet['ID'][~df_sheet['ID'].astype(str).str.startswith('PL', na=False)]
            if not numeric_ids_series.empty:
                max_numeric_id_val = pd.to_numeric(numeric_ids_series, errors='coerce').max()
                if pd.notna(max_numeric_id_val): max_id_numeric = int(max_numeric_id_val)

            proximo_id = max(max_id_numeric, max_id_pl_format) + 1

        if df_resultado is not None and not df_resultado.empty:

            print(f"Analizando {len(df_resultado)} proyectos scrapeados...")

            for index, row in df_resultado.iterrows():
                expediente_nuevo = str(row['Expediente']).strip()
                fecha_nueva = str(row['Fecha de inicio']).strip()

                match = pd.DataFrame()
                if not df_sheet.empty:
                    match = df_sheet[df_sheet['Expediente'].astype(str) == expediente_nuevo]

                if match.empty:
                    formatted_id = f"PL{proximo_id:03d}"
                    fila_ordenada = [
                        formatted_id, row['Cámara de Origen'], row['Expediente'], row['Autor'],
                        row['Fecha de inicio'], row['Proyecto'], row['Comisiones'],
                        '', '', row['Partido Político'], row['Provincia'], ''
                    ]
                    fila_limpia = [str(x) if pd.notna(x) else "" for x in fila_ordenada]
                    filas_nuevas.append(fila_limpia)
                    proximo_id += 1
                else:
                    idx_existente = match.index[0]
                    fecha_existente = str(match.iloc[0]['Fecha de inicio']).strip()
                    id_existente = str(match.iloc[0]['ID']).strip() 

                    if fecha_nueva != fecha_existente:
                        fila_sheet_num = idx_existente + 2
                        fila_actualizada = [
                            id_existente, row['Cámara de Origen'], row['Expediente'], row['Autor'],
                            row['Fecha de inicio'], row['Proyecto'], row['Comisiones'],
                            match.iloc[0]['Estado'], match.iloc[0]['Probabilidad'],
                            row['Partido Político'], row['Provincia'], match.iloc[0]['Observaciones']
                        ]
                        fila_limpia = [str(x) if pd.notna(x) else "" for x in fila_actualizada]
                        rango = f"A{fila_sheet_num}:L{fila_sheet_num}"
                        sheet.update(range_name=rango, values=[fila_limpia])
                        contador_actualizados += 1
                    else:
                        contador_omitidos += 1

            if filas_nuevas:
                print(f"Cargando {len(filas_nuevas)} proyectos NUEVOS...")
                sheet.append_rows(filas_nuevas)

            print("Solicitando análisis a Gemini...")
            analista = AnalistaLegislativo()
            texto_analisis = analista.analizar_proyectos(filas_nuevas)

            msg_final = (
                f"*Reporte Diputados Diario*\n\n"
                f"✅ *Nuevos:* {len(filas_nuevas)}\n"
                f"🔄 *Actualizados:* {contador_actualizados}\n"
                f"⏭️ *Omitidos:* {contador_omitidos}\n\n"
                f"📝 *Análisis de Gemini*\n {texto_analisis}"
            )
        else:
            msg_final = "⚠️ *Alerta *\n\nEl scraping no trajo datos."

        sender.enviar_difusion(msg_final)

    except Exception as e:
        err_msg = f"❌ *Error Crítico *\n\n{str(e)}"
        print(err_msg)
        sender.enviar_difusion(err_msg)
        exit(1)
