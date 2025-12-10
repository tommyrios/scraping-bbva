import pandas as pd
from senadores import ScrapearSenado

if __name__ == "__main__":
    print("🧪 MODO DE PRUEBA: Iniciando scraping EXCLUSIVO de Senado...")
    
    try:
        # Inicializamos solo el bot de Senado
        bot = ScrapearSenado()
        
        # Ejecutamos el scrape (recuerda que ahora entra link por link)
        df_resultado = bot.scrape()

        print("-" * 50)
        
        if df_resultado is not None and not df_resultado.empty:
            print(f"✅ ¡Éxito! Se extrajeron {len(df_resultado)} proyectos.")
            
            # Mostramos una muestra en consola
            print("\n--- Muestra de las primeras 3 filas ---")
            print(df_resultado.head(3))
            
            # Mostramos un ejemplo completo de la primera fila para ver detalles
            print("\n--- Detalle del primer proyecto ---")
            primer_proyecto = df_resultado.iloc[0]
            print(f"📁 Expediente: {primer_proyecto['Expediente']}")
            print(f"📅 Fecha:      {primer_proyecto['Fecha de inicio']}")
            print(f"👤 Autor:      {primer_proyecto['Autor']}")
            print(f"🏛️ Comisiones: {primer_proyecto['Comisiones']}")
            print(f"📄 Título:     {primer_proyecto['Proyecto'][:100]}...") # Cortamos para que no llene la pantalla

            # Guardamos a CSV para que lo puedas revisar tranquilo
            nombre_archivo = "test_senado.csv"
            df_resultado.to_csv(nombre_archivo, index=False, encoding='utf-8-sig')
            print(f"\n💾 Resultados guardados en '{nombre_archivo}' para revisión manual.")

        else:
            print("⚠️ El dataframe volvió vacío. Revisa si la página del Senado responde o si cambió el HTML.")

    except Exception as e:
        print(f"❌ Error durante la prueba: {e}")
