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
    def __init__(self, sheet_obj, prefijo_id):
        self.sheet = sheet_obj
        self.prefijo = prefijo_id

        raw_data = self.sheet.get_all_values()
        self.fila_actual = len(raw_data) + 1

        ids_existentes = []
        self.mapa_existentes = {}

        # En BO el expediente está en columna B (index 1). En Proyectos en columna C (index 2).
        idx_expediente = 1 if prefijo_id == "BO" else 2

        for i, row in enumerate(raw_data):
            if i == 0:
                continue

            id_val = str(row[0]).strip() if len(row) > 0 else ""
            exp_val = str(row[idx_expediente]).strip() if len(row) > idx_expediente else ""

            if exp_val:
                self.mapa_existentes[exp_val] = {"fila": i + 1, "datos": row}

            if id_val.startswith(self.prefijo):
                try:
                    num = int(id_val.replace(self.prefijo, ""))
                    ids_existentes.append(num)
                except:
                    pass

        self.proximo_id_num = (max(ids_existentes) + 1) if ids_existentes else 1

    def obtener_datos_existentes(self, expediente):
        return self.mapa_existentes.get(str(expediente).strip())

    def registrar_nuevo(self):
        id_str = f"{self.prefijo}{self.proximo_id_num:03d}"
        fila = self.fila_actual

        self.proximo_id_num += 1
        self.fila_actual += 1

        return id_str, fila


def procesar_lote(df_nuevos, gestor, nombre_origen_default, operaciones_globales, filas_ia_globales):
    """
    - Escribe a Sheets SOLO las columnas definidas (no guarda Link Texto).
    - A IA/reporte le pasa el link “Link Texto” (si existe) para que aparezca en el mail.
    """
    stats = {"nuevos": 0, "reanalizados": 0, "omitidos": 0}

    if df_nuevos is None or df_nuevos.empty:
        return stats

    # Precontar cuántos realmente son nuevos para add_rows
    nuevos_reales_count = 0
    for _, row in df_nuevos.iterrows():
        if not gestor.obtener_datos_existentes(row["Expediente"]):
            nuevos_reales_count += 1

    if nuevos_reales_count > 0:
        gestor.sheet.add_rows(nuevos_reales_count)

    for _, row in df_nuevos.iterrows():
        exp_web = str(row.get("Expediente", "")).strip()
        autor = str(row.get("Autor", ""))
        fecha = str(row.get("Fecha de inicio", ""))
        proyecto = str(row.get("Proyecto", ""))

        # En Sheet: en BO se usa Comisiones como link; en Proyectos Comisiones son comisiones.
        comisiones_o_link_bo = str(row.get("Comisiones", ""))

        # Para el reporte: PL usa Link Texto (PDF/detalle) si está; BO usa Comisiones (link BO)
        link_texto = str(row.get("Link Texto", "")).strip()
        link_para_reporte = link_texto if gestor.prefijo != "BO" else comisiones_o_link_bo

        partido = str(row.get("Partido Político", ""))
        provincia = str(row.get("Provincia", ""))

        info_existente = gestor.obtener_datos_existentes(exp_web)

        if info_existente:
            datos_viejos = info_existente["datos"]
            fila_excel = info_existente["fila"]

            idx_obs = 5 if gestor.prefijo == "BO" else 11
            obs_actual = str(datos_viejos[idx_obs]).strip() if len(datos_viejos) > idx_obs else ""

            # Reanaliza si no hay observación o tiene error
            if (not obs_actual) or ("Error" in obs_actual):
                stats["reanalizados"] += 1

                if gestor.prefijo == "BO":
                    origen_final = nombre_origen_default
                    link_reporte_final = comisiones_o_link_bo  # en BO sí viene de la fila/scraper
                else:
                    # En Proyectos, origen está guardado en columna B
                    origen_final = str(datos_viejos[1]).strip() if len(datos_viejos) > 1 else nombre_origen_default
                    # No guardamos link en Sheet: para reanalizados sin corrida nueva, dejamos vacío y analisis cae a fallback
                    link_reporte_final = ""

                fila_ia = [
                    datos_viejos[0],  # id interno
                    origen_final,
                    exp_web,
                    autor,
                    fecha,
                    proyecto,
                    link_reporte_final,
                    "", "", "", "", ""
                ]
                fila_ia.append(fila_excel)
                filas_ia_globales.append(fila_ia)
            else:
                stats["omitidos"] += 1

        else:
            # Nuevo
            id_nuevo, fila_excel = gestor.registrar_nuevo()

            if gestor.prefijo == "BO":
                # Sheet Boletín: A..G (G=link)
                valores = [
                    id_nuevo,          # A
                    exp_web,           # B
                    autor,             # C
                    fecha,             # D
                    "",                # E impacto (IA)
                    "",                # F justificación (IA)
                    comisiones_o_link_bo  # G link BO
                ]
                rango = f"A{fila_excel}:G{fila_excel}"
                origen_final = nombre_origen_default
            else:
                # Sheet Proyectos: A..L (G=comisiones)
                origen_final = str(row.get("Cámara de Origen") or nombre_origen_default).strip() or nombre_origen_default
                valores = [
                    id_nuevo,      # A
                    origen_final,  # B
                    exp_web,       # C
                    autor,         # D
                    fecha,         # E
                    proyecto,      # F
                    comisiones_o_link_bo,  # G comisiones
                    "",            # H estado
                    "",            # I probabilidad/impacto (IA en I)
                    partido,       # J
                    provincia,     # K
                    ""             # L observaciones / justificación IA en L
                ]
                rango = f"A{fila_excel}:L{fila_excel}"

            valores = [str(x) if pd.notna(x) else "" for x in valores]
            operaciones_globales.append({"range": rango, "values": [valores]})

            # Para IA/reporte: link real solo acá (no se guarda en Sheet)
            fila_ia = [
                id_nuevo,
                origen_final,
                exp_web,
                autor,
                fecha,
                proyecto,
                link_para_reporte,
                "", "", "", "", ""
            ]
            fila_ia.append(fila_excel)
            filas_ia_globales.append(fila_ia)

            stats["nuevos"] += 1

    return stats


def normalizar_impacto(item: dict) -> str:
    """
    Lee el nuevo campo impacto_nivel (preferido) y cae al viejo impacto si hiciera falta.
    Devuelve exactamente: ALTO | MEDIO | BAJO (o vacío si no hay).
    """
    val = str(item.get("impacto_nivel") or item.get("impacto") or "").upper().strip()
    if not val:
        return ""
    for lvl in ("ALTO", "MEDIO", "BAJO"):
        if lvl in val:
            return lvl
    return val if val in ("ALTO", "MEDIO", "BAJO") else ""


if __name__ == "__main__":
    print("--- Sistema Unificado v2.0 (COMPLETO) ---")
    sender = MensajeSender()

    try:
        if "GCP_CREDENTIALS" not in os.environ:
            raise Exception("Falta GCP_CREDENTIALS")

        json_creds = json.loads(os.environ["GCP_CREDENTIALS"])
        creds = Credentials.from_service_account_info(
            json_creds,
            scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
        )
        gc = gspread.authorize(creds)

        wb = gc.open_by_url(
            "https://docs.google.com/spreadsheets/d/16aksCoBrIFB6Vy8JpiuVBEpfGNHdUNJcsCKb2k33tsQ/edit?gid=0#gid=0"
        )
        sheet_proy = wb.worksheet("Proyectos")
        sheet_bo = wb.worksheet("Boletin")

        gestor_proy = GestorEstado(sheet_proy, "PL")
        gestor_bo = GestorEstado(sheet_bo, "BO")

        filas_ia_globales = []
        batch_proy = []
        batch_bo = []

        # --- DIPUTADOS ---
        print(">>> Diputados...")
        try:
            df = ScrapearDiputados().scrape("https://www.diputados.gov.ar/proyectos/")
            if df is not None and not df.empty:
                df = df.iloc[::-1]
            st = procesar_lote(df, gestor_proy, "Diputados", batch_proy, filas_ia_globales)
            print(f"   Resultados: {st['nuevos']} nuevos | {st['reanalizados']} reanalizados | {st['omitidos']} omitidos")
        except Exception as e:
            print(f"❌ Error Diputados: {e}")

        # --- SENADO ---
        print(">>> Senado...")
        try:
            df = ScrapearSenado().scrape()
            if df is not None and not df.empty:
                df = df.iloc[::-1]
            st = procesar_lote(df, gestor_proy, "Senado", batch_proy, filas_ia_globales)
            print(f"   Resultados: {st['nuevos']} nuevos | {st['reanalizados']} reanalizados | {st['omitidos']} omitidos")
        except Exception as e:
            print(f"❌ Error Senado: {e}")

        # --- BOLETIN ---
        print(">>> Boletín...")
        try:
            df = ScrapearBoletin().scrape()
            st = procesar_lote(df, gestor_bo, "Boletin Oficial", batch_bo, filas_ia_globales)
            print(f"   Resultados: {st['nuevos']} nuevos | {st['reanalizados']} reanalizados | {st['omitidos']} omitidos")
        except Exception as e:
            print(f"❌ Error Boletín: {e}")

        # Escribir batchs a Sheets
        if batch_proy:
            sheet_proy.batch_update(batch_proy, value_input_option="USER_ENTERED")
        if batch_bo:
            sheet_bo.batch_update(batch_bo, value_input_option="USER_ENTERED")

        # --- IA ---
        print(">>> Analizando IA...")
        analista = AnalistaLegislativo()
        datos_limpios = [f[:-1] for f in filas_ia_globales]  # sin fila excel
        texto_analisis, resultados_ia = analista.analizar_proyectos(datos_limpios)

        # --- Guardar resultados IA en Excel (Impacto + Justificación) ---
        if resultados_ia:
            print(">>> Guardando resultados IA en Excel...")
            updates_proy_ia = []
            updates_bo_ia = []

            mapa_filas = {f[0]: f[-1] for f in filas_ia_globales}

            for item in resultados_ia:
                id_ref = str(item.get("id_interno", "")).strip()
                if not id_ref:
                    continue

                impacto_nivel = normalizar_impacto(item)  # ✅ ALTO/MEDIO/BAJO
                just = str(item.get("justificacion", "")).strip()

                if id_ref in mapa_filas:
                    fila = mapa_filas[id_ref]

                    if id_ref.startswith("BO"):
                        # Boletín: Impacto en E, Justificación en F
                        if impacto_nivel:
                            updates_bo_ia.append({"range": f"E{fila}", "values": [[impacto_nivel]]})
                        updates_bo_ia.append({"range": f"F{fila}", "values": [[just]]})
                    else:
                        # Proyectos: Impacto en I, Justificación en L
                        if impacto_nivel:
                            updates_proy_ia.append({"range": f"I{fila}", "values": [[impacto_nivel]]})
                        updates_proy_ia.append({"range": f"L{fila}", "values": [[just]]})

            if updates_proy_ia:
                sheet_proy.batch_update(updates_proy_ia, value_input_option="USER_ENTERED")
            if updates_bo_ia:
                sheet_bo.batch_update(updates_bo_ia, value_input_option="USER_ENTERED")

        # --- Enviar mail ---
        print(">>> Enviando mail...")
        sender.enviar_difusion(texto_analisis)
        print("✅ Éxito total.")

    except Exception as e:
        print(f"❌ FATAL: {e}")
        sender.enviar_difusion(
            f"<html><body><h1>Error Crítico en Ejecución</h1><p>{e}</p></body></html>"
        )
        exit(1)
