import pandas as pd
import sys
import os

# 1. Definición de la Función Principal

def process_data(input_file_path):
    """
    Procesa un archivo Parquet de viajes de NYC Taxis y genera dos reportes CSV.
    
    El Reporte 1 (Ultima Ubicación) ha sido adaptado para usar Zone/Borough 
    en lugar de Latitud/Longitud, debido a que los archivos Parquet modernos 
    no contienen estas coordenadas.
    """
    
    # Verificación de que se proporcionó el archivo Parquet
    if not os.path.exists(input_file_path):
        print(f"Error: El archivo de entrada '{input_file_path}' no se encontró.")
        return

    print(f"Iniciando el procesamiento del archivo: {input_file_path}")


    COLUMNAS_CLAVE = {
        'tpep_pickup_datetime': 'timestamp',
        'PULocationID': 'vehicle_id',
    }
    
    # 1. Carga de Datos de Referencia (taxi_zone_lookup.csv) 

    LOOKUP_FILE = 'taxi_zone_lookup.csv'
    if not os.path.exists(LOOKUP_FILE):
        print(f"Error: El archivo de referencia '{LOOKUP_FILE}' no se encontró. Necesario para Reporte 1.")
        return
        
    try:
        df_lookup = pd.read_csv(LOOKUP_FILE)
        df_lookup.rename(columns={'LocationID': 'vehicle_id'}, inplace=True)
        df_lookup['vehicle_id'] = df_lookup['vehicle_id'].astype(str)
        df_lookup = df_lookup[['vehicle_id', 'Borough', 'Zone']]
        print(f"Datos de referencia de ubicación cargados con {len(df_lookup)} zonas.")
        
    except Exception as e:
        print(f"Error al leer el archivo de referencia {LOOKUP_FILE}: {e}")
        return
        
    # 1b. Carga de Datos de Eventos (Parquet) 

    try:
        # Usamos list() para asegurar que Pandas reciba una lista de nombres de columnas
        df = pd.read_parquet(
            input_file_path,
            columns=list(COLUMNAS_CLAVE.keys()) 
        )
        
        # Renombrar las columnas para usar nombres consistentes
        df.rename(columns=COLUMNAS_CLAVE, inplace=True)
        
        # Preparación de datos
        df['timestamp'] = pd.to_datetime(df['timestamp']) 
        df['vehicle_id'] = df['vehicle_id'].astype(str)
        
        print(f"Datos de viajes cargados con {len(df)} registros.")

    except Exception as e:
        print(f"Error al leer el archivo Parquet: {e}")
        return

    # 2. Generar Salida 1: "Última Ubicación Conocida"
    
    print("Generando Reporte 1: Última Ubicación Conocida (usando Zona/Borough)...")

    # Encontrar el último registro por cada vehicle_id basado en el timestamp.
    df_ultima_ubicacion = df.sort_values(
        by=['vehicle_id', 'timestamp'],
        ascending=False
    ).drop_duplicates(
        subset=['vehicle_id'],
        keep='first'
    )

    # 3. Hacemos un JOIN con la tabla de referencia
    df_ultima_ubicacion = pd.merge(
        df_ultima_ubicacion[['vehicle_id', 'timestamp']],
        df_lookup,
        on='vehicle_id',
        how='left' 
    )
    
    # 4. Seleccionar y renombrar las columnas
    df_ultima_ubicacion = df_ultima_ubicacion[[
        'vehicle_id', 'timestamp', 'Borough', 'Zone'
    ]].rename(columns={
        'timestamp': 'ultimo_timestamp',
        'Borough': 'ultimo_distrito', 
        'Zone': 'ultima_zona'       
    })

    # Guardar el archivo CSV
    output_file_1 = 'ultima_ubicacion.csv'
    df_ultima_ubicacion.to_csv(output_file_1, index=False)
    print(f"Reporte 1 guardado en: {output_file_1} ({len(df_ultima_ubicacion)} filas)")

    # 3. Generar Salida 2: "Reporte de Viajes por Hora" 
    
    print("Generando Reporte 2: Reporte de Viajes por Hora...")

    # 1. Extraer la hora (0-23)
    df['hora_del_dia'] = df['timestamp'].dt.hour
    
    # 2. Agrupar por hora y contar los registros
    df_viajes_por_hora = df.groupby('hora_del_dia').size().reset_index(name='total_viajes')

    # 3. Ordenar por hora del día
    df_viajes_por_hora = df_viajes_por_hora.sort_values(by='hora_del_dia')
    
    # Guardar el archivo CSV
    output_file_2 = 'viajes_por_hora.csv'
    df_viajes_por_hora.to_csv(output_file_2, index=False)
    print(f"Reporte 2 guardado en: {output_file_2} ({len(df_viajes_por_hora)} filas)")
    
    print("\nProcesamiento completado con éxito.")


# 4. Punto de Entrada del Script

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Uso: python process_data.py <ruta_al_archivo_parquet>")
        print("Ejemplo: python process_data.py yellow_tripdata_2025-01.parquet")
    else:
        parquet_file_path = sys.argv[1]
        process_data(parquet_file_path)
