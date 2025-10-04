import pandas as pd
import h3
import os
import json
import time


inicio = time.time()


def clean_df(df):
    return df[(df["quality_flag"] <= 1) & (df["value"].notnull())].copy()


def limpiar_carpeta(ruta_carpeta):
    print("Limpiando carpeta " + ruta_carpeta)
    for archivo in os.listdir(ruta_carpeta):
        ruta_archivo = os.path.join(ruta_carpeta, archivo)
        if os.path.isfile(ruta_archivo):
            try:
                os.remove(ruta_archivo)
            except Exception as e:
                print(f"No se pudo eliminar {ruta_archivo}: {e}")


def save_jsons_grouped(data_grouped, folder):
    print("Guardando jsons")
    os.makedirs(folder, exist_ok=True)

    for filename, obj in data_grouped.items():
        filepath = os.path.join(folder, filename)

        # Guardar un solo objeto (no lista)
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(obj, f, ensure_ascii=False, indent=4)
        print(f"JSON guardado en {filename}")


def generate_jsons_with_h3_avg(
    pre, df, folder, lat_col="lat", lon_col="lon", resolution=1
):
    print("Generando data con promedio")
    data_grouped = {}

    # Crear columna con índice h3
    df["h3_index"] = df.apply(
        lambda x: h3.latlng_to_cell(x[lat_col], x[lon_col], resolution), axis=1
    )

    # Agrupar por h3
    grouped = df.groupby("h3_index")

    for h3_index, group in grouped:
        # Calcular promedio de 'value'
        promedio_value = group["value"].mean()

        # Obtener centroide (lat, lon) del hexágono
        lat_centroide, lon_centroide = h3.cell_to_latlng(h3_index)

        # Crear dict con promedio y coordenadas
        obj = {
            # "h3_index": h3_index,
            "lat": lat_centroide,
            "lon": lon_centroide,
            "value": promedio_value,
        }
        print(group["value"].count())

        filename = f"{pre}_{resolution}_{h3_index}.json"
        data_grouped[filename] = obj

    save_jsons_grouped(data_grouped, folder)


def generar_html_mapa_calor_leaflet_embebido(
    folder, output="mapa_calor_con_promedios.html"
):
    datos_completos = []

    for archivo in os.listdir(folder):
        if archivo.endswith(".json"):
            ruta = os.path.join(folder, archivo)
            with open(ruta, "r", encoding="utf-8") as f:
                punto = json.load(f)
                if (
                    isinstance(punto, dict)
                    and "lat" in punto
                    and "lon" in punto
                    and "value" in punto
                ):
                    datos_completos.append(
                        {
                            "lat": punto["lat"],
                            "lon": punto["lon"],
                            "value": punto["value"],
                        }
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
        /* Estilos de la leyenda */
        .legend {{
            position: absolute;
            bottom: 30px;
            left: 10px;
            padding: 10px;
            background: white;
            box-shadow: 0 0 15px rgba(0,0,0,0.2);
            border-radius: 5px;
            font-family: Arial, sans-serif;
            font-size: 14px;
            color: #555;
            z-index: 1000;
        }}
        .legend .gradient {{
            height: 15px;
            background: linear-gradient(to right, blue, cyan, lime, yellow, red);
            margin-bottom: 5px;
            border: 1px solid #999;
        }}
        .legend .labels {{
            display: flex;
            gap: 10px;
            justify-content: space-between;
        }}
    </style>
</head>
<body>
    <h2>Mapa Calor {output.split('.')[0].replace('map_', '').replace('_', ' ').title()}</h2>
    <div id="map"></div>

    <!-- Leaflet JS -->
    <script src="https://unpkg.com/leaflet/dist/leaflet.js"></script>
    <!-- Leaflet.heat plugin -->
    <script src="https://unpkg.com/leaflet.heat/dist/leaflet-heat.js"></script>

    <script>
        // Datos embebidos: [lat, lon, peso]
        const heatData = {datos_json_str};

        const heatArray = heatData.map(p => [p.lat, p.lon, p.value]);

        // Inicializar mapa centrado en primer punto, o en coordenadas fijas si no hay datos
        const center = heatData.length > 0 ? [heatData[0].lat, heatData[0].lon] : [26.335546, -95.566635];
        const map = L.map('map').setView(center, 6);

        // Capa base de OpenStreetMap
        L.tileLayer('https://{{s}}.tile.openstreetmap.org/{{z}}/{{x}}/{{y}}.png', {{
            maxZoom: 18,
            attribution: '© OpenStreetMap'
        }}).addTo(map);

        // Crear capa heatmap con datos y último parámetro como intensidad (peso)
        const heat = L.heatLayer(heatArray, {{
            radius: 30,          // radio mayor para suavizar
            blur: 20,            // desenfoque más suave
            maxZoom: 16,         // nivel máximo donde se ve el heatmap
            max: Math.max(...heatArray.map(p => p[2])), // normalización intensidad
            minOpacity: 0.5,     // opacidad mínima más alta
            gradient: {{
                0.4: 'blue',
                0.6: 'cyan',
                0.7: 'lime',
                0.8: 'yellow',
                1.0: 'red'
            }}
        }}).addTo(map);

        const minValue = Math.min(...heatArray.map(p => p[2]));
        const maxValue = Math.max(...heatArray.map(p => p[2]));

        // Crear control de leyenda
        const legend = L.control({{position: 'bottomleft'}});

        legend.onAdd = function(map) {{
            const div = L.DomUtil.create('div', 'legend');
            div.innerHTML = `
                <div class="gradient"></div>
                <div class="labels">
                <span>${{minValue.toExponential(2)}}</span>
                <span>${{maxValue.toExponential(2)}}</span>
                </div>
            `;
            return div;
        }};

        legend.addTo(map);
    </script>
</body>
</html>
"""

    with open(output, "w", encoding="utf-8") as f:
        f.write(plantilla_html)

    print(f"Archivo '{output}' generado con {len(datos_completos)} puntos embebidos.")


limpiar_carpeta("./jsons_no2")
limpiar_carpeta("./jsons_hcho")

processTotal = True
n_items = 100000
resolution = 1


df_no2_raw = pd.read_csv("no2.csv")
df_no2 = clean_df(df_no2_raw if processTotal else df_no2_raw.head(n_items))
print(f"Número de items: {len(df_no2)}")
generate_jsons_with_h3_avg("no2", df_no2, resolution=resolution, folder="jsons_no2")
generar_html_mapa_calor_leaflet_embebido("./jsons_no2", f"map_avg_no2_{resolution}.html")

df_hcho_raw = pd.read_csv("hcho.csv")
df_hcho = clean_df(df_hcho_raw if processTotal else df_hcho_raw.head(n_items))
print(f"Número de items: {len(df_hcho)}")
generate_jsons_with_h3_avg("hcho", df_hcho, resolution=resolution, folder="jsons_hcho")
generar_html_mapa_calor_leaflet_embebido("./jsons_hcho", f"map_avg_hcho_{resolution}.html")

fin = time.time()

print(f"Tiempo de ejecución: {fin - inicio} segundos")
