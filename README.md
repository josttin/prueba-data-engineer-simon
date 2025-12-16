Prueba Técnica: Ingeniero de Datos Junior - Procesamiento de Flota NYC
Este proyecto aborda el desafío de la prueba técnica para el rol de Ingeniero de Datos Junior, centrado en el procesamiento de un dataset de viajes de taxi de la ciudad de Nueva York. El objetivo principal es generar dos datasets de salida clave, optimizando la ejecución y manejo de la memoria.

1. Visión General de la Solución
El script process_data.py se encarga de:

Cargar y optimizar los datos de viajes Parquet y el archivo de referencia CSV.

Enriquecer los datos de viaje con información de zona para suplir la falta de coordenadas. (Ver la sección 3).

Generar el Reporte 1: Última ubicación conocida por cada vehículo (ultima_ubicacion.csv).

Generar el Reporte 2: Conteo de viajes por hora (viajes_por_hora.csv).

2. Herramientas y Stack Tecnológico
Se ha elegido un stack estándar en la industria de Data Engineering que prioriza la eficiencia en la manipulación tabular y la lectura optimizada de formatos de big data.

Lenguaje: Python

Librerías Clave:
Pandas: Es la herramienta de facto para la manipulación, limpieza y transformación de DataFrames (datos tabulares).
PyArrow: Necesario para leer archivos en formato Parquet de manera eficiente, lo cual es crítico para optimizar el uso de la memoria al trabajar con archivos grandes.

3. Adaptación Crítica de los Datos (El Punto Clave de la Solución)
Durante el análisis del dataset (archivos Parquet de años recientes, como el ejemplo yellow_tripdata_2025-01.parquet), identifiqué un cambio en la estructura de la fuente que afecta directamente el Reporte 1 (Última Ubicación).

El Problema
Los archivos Parquet modernos ya no incluyen las coordenadas de latitud/longitud (pickup_latitude / pickup_longitude) que históricamente se usaban para la ubicación.

En su lugar, solo contienen la ID de la Zona (PULocationID).

Mi Solución: Enriquecimiento de Datos (Data Enrichment)
En lugar de descartar el Reporte 1, adapté la solución mediante un proceso de enriquecimiento de datos:

Se cargó el archivo de referencia taxi_zone_lookup.csv (el cual mapea la ID a un nombre de distrito y zona).

Se realizó una operación de JOIN (una unión de tablas) entre los datos de viaje y este archivo de referencia usando la columna PULocationID.

Resultado del Reporte 1
El reporte final ultima_ubicacion.csv cumple con la lógica de encontrar el último evento temporal por ID de vehículo, pero utiliza las columnas ultimo_distrito y ultima_zona en lugar de latitud/longitud, lo cual es una solución práctica y fiel a los datos disponibles.

4. Instalación y Ejecución
Requisitos
Necesitas tener Python instalado en tu sistema. Se recomienda usar un entorno virtual.

4.1. Instalación de Dependencias
Abre tu Terminal y navega hasta la carpeta raíz de este proyecto.

Instala las librerías necesarias especificadas en requirements.txt:

Bash
pip install -r requirements.txt

4.2. Preparación para la Ejecución
Asegúrate de lo siguiente:

El archivo de datos de viajes (ej. yellow_tripdata_2025-01.parquet).

El archivo de referencia (taxi_zone_lookup.csv).

El script (process_data.py).

... deben estar ubicados en la misma carpeta.

4.3. El Comando de Ejecución
Ejecuta el script pasándole el nombre del archivo Parquet como argumento:

Bash
python process_data.py yellow_tripdata_2025-01.parquet

4.4. Archivos de Salida
La ejecución exitosa del script generará dos archivos CSV en la misma carpeta:

ultima_ubicacion.csv

viajes_por_hora.csv

(Los archivos CSV que están subidos a este repositorio son el resultado directo de una ejecución de prueba y sirven como muestra.)


