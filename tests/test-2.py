import pandas as pd
import h3
import os
import json
import time

inicio = time.time()

def clean_df(df):
    return df[(df["quality_flag"] <= 1) & (df["value"].notnull())].copy()

def limpiar_carpeta(ruta_carpeta):
    print('Limpiando carpeta')
    # Listar todos los archivos en la carpeta
    for archivo in os.listdir(ruta_carpeta):
        ruta_archivo = os.path.join(ruta_carpeta, archivo)
        # Verificar que sea un archivo (no carpeta)
        if os.path.isfile(ruta_archivo):
            try:
                os.remove(ruta_archivo)
                # print(f"Eliminado: {ruta_archivo}")
            except Exception as e:
                print(f"No se pudo eliminar {ruta_archivo}: {e}")


def save_jsons_grouped(data_grouped):
    print('Guardando jsons')

    folder = "jsons"
    os.makedirs(folder, exist_ok=True)

    for filename, list_objs in data_grouped.items():
        filepath = os.path.join(folder, filename)

        # Si el archivo existe, cargar contenido existente y agregar nuevos objetos
        if os.path.exists(filepath):
            with open(filepath, "r", encoding="utf-8") as f:
                try:
                    existing_data = json.load(f)
                    if not isinstance(existing_data, list):
                        existing_data = [existing_data]
                except json.JSONDecodeError:
                    existing_data = []
            combined_data = existing_data + list_objs
        else:
            combined_data = list_objs

        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(combined_data, f, ensure_ascii=False, indent=4)
        print(f"JSON guardado en {filename}")


def generate_jsons_with_h3(pre, df, lat_col="lat", lon_col="lon", resolution=1):
    print('Generando data')
    data_grouped = {}

    for i, row in df.iterrows():
        row_dict = row.drop(labels=["quality_flag"]).to_dict()

        h3_index = h3.latlng_to_cell(row[lat_col], row[lon_col], resolution)
        filename = f"{pre}_{resolution}_{h3_index}.json"

        if filename not in data_grouped:
            data_grouped[filename] = []
        data_grouped[filename].append(row_dict)

        # print(filename)

    save_jsons_grouped(data_grouped)


limpiar_carpeta("./jsons")

df_no2_raw = pd.read_csv("no2.csv")
df_no2 = clean_df(df_no2_raw.head(50000))
generate_jsons_with_h3("no2", df_no2)

df_hcho_raw = pd.read_csv("hcho.csv")
df_hcho = clean_df(df_hcho_raw.head(50000))
generate_jsons_with_h3("hcho", df_hcho)


def generar_html_mapa_calor_leaflet_embebido(
    carpeta_json, archivo_salida="mapa_calor_leaflet.html"
):
    datos_completos = []

    for archivo in os.listdir(carpeta_json):
        if archivo.endswith(".json"):
            ruta = os.path.join(carpeta_json, archivo)
            with open(ruta, "r", encoding="utf-8") as f:
                datos = json.load(f)
                # Agregar lat, lon, value como lista [lat, lon, peso]
                for punto in datos:
                    if "lat" in punto and "lon" in punto and "value" in punto:
                        datos_completos.append(
                            [punto["lat"], punto["lon"], punto["value"]]
                        )

    # Convertir a JSON para embebido
    datos_json_str = json.dumps(datos_completos)

    plantilla_html = f"""
<!DOCTYPE html>
<html lang="es">
<head>
    <meta charset="UTF-8">
    <title>Mapa de Calor con Leaflet y Heatmap</title>
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <!-- Leaflet CSS -->
    <link rel="stylesheet" href="https://unpkg.com/leaflet/dist/leaflet.css"/>
    <style>
        #map {{
            height: 90vh;
            width: 100%;
        }}
    </style>
</head>
<body>
    <h2>Mapa de Calor con Leaflet</h2>
    <div id="map"></div>

    <!-- Leaflet JS -->
    <script src="https://unpkg.com/leaflet/dist/leaflet.js"></script>
    <!-- Leaflet.heat plugin -->
    <script src="https://unpkg.com/leaflet.heat/dist/leaflet-heat.js"></script>

    <script>
        // Datos embebidos: [lat, lon, peso]
        const heatData = {datos_json_str};

        // Inicializar mapa centrado en primer punto, o en coordenadas fijas si no hay datos
        const center = heatData.length > 0 ? [heatData[0][0], heatData[0][1]] : [26.335546, -95.566635];
        const map = L.map('map').setView(center, 6);

        // Capa base de OpenStreetMap
        L.tileLayer('https://{{s}}.tile.openstreetmap.org/{{z}}/{{x}}/{{y}}.png', {{
            maxZoom: 18,
            attribution: '© OpenStreetMap'
        }}).addTo(map);

        // Crear capa heatmap con datos y último parámetro como intensidad (peso)
        const heat = L.heatLayer(heatData, {{
            radius: 25,
            blur: 15,
            maxZoom: 17,
            // Opcional: max intensity 
            max: Math.max(...heatData.map(p => p[2]))
        }}).addTo(map);
    </script>
</body>
</html>
"""

    with open(archivo_salida, "w", encoding="utf-8") as f:
        f.write(plantilla_html)

    print(
        f"Archivo '{archivo_salida}' generado con {len(datos_completos)} puntos embebidos."
    )


# Uso:
generar_html_mapa_calor_leaflet_embebido("./jsons")

fin = time.time()

print(f"Tiempo de ejecución: {fin - inicio} segundos")
