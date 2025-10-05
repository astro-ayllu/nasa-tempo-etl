import numpy as np
import pandas as pd
from collections import defaultdict
from src.utils.logger import logger

def detect(
    df: pd.DataFrame,
    umbral: float = 5e15,
    eps_deg: float = 0.12,       # ~ tamaño del píxel en grados (ajusta)
    min_samples: int = 4,        # mínimo de puntos para formar un cluster
):
    logger.info(f"Detectando zonas de alerta con umbral {umbral}, eps {eps_deg}, min_samples {min_samples}")
    # --- 1) Nos quedamos solo con puntos sobre el umbral (sparse)
    alertas = df.loc[df["value"] > umbral, ["lat", "lon", "value"]].copy()
    if alertas.empty:
        return []

    # --- 2) DBSCAN (evita construir grillas grandes)
    from sklearn.cluster import DBSCAN
    X = alertas[["lat", "lon"]].to_numpy(dtype=np.float32)
    db = DBSCAN(eps=eps_deg, min_samples=min_samples,
                metric="euclidean", algorithm="ball_tree").fit(X)
    labels = db.labels_
    alertas = alertas.assign(cluster=labels)
    alertas = alertas[alertas["cluster"] >= 0].copy()  # descartar ruido

    if alertas.empty:
        return []

    # --- 3) Agrupar puntos por cluster
    groups = defaultdict(list)
    for (lat, lon, val, cid) in alertas[["lat","lon","value","cluster"]].itertuples(index=False, name=None):
        groups[cid].append((float(lat), float(lon), float(val)))

    # Intentar convex hull con shapely; si no está, usar bounding box
    try:
        from shapely.geometry import MultiPoint
        USE_SHAPELY = True
    except Exception:
        USE_SHAPELY = False

    def build_boundary(coords_latlon):
        # coords_latlon: [(lat,lon), ...]
        if USE_SHAPELY and len(coords_latlon) >= 3:
            mp = MultiPoint([(lon, lat) for lat,lon in coords_latlon])  # (x,y)=(lon,lat)
            hull = mp.convex_hull
            if hull.geom_type == "Polygon":
                return [{"lat": float(lat), "lon": float(lon)} for lon,lat in hull.exterior.coords]
        # Fallback: bounding box
        lats = [p[0] for p in coords_latlon]
        lons = [p[1] for p in coords_latlon]
        return [
            {"lat": min(lats), "lon": min(lons)},
            {"lat": min(lats), "lon": max(lons)},
            {"lat": max(lats), "lon": max(lons)},
            {"lat": max(lats), "lon": min(lons)},
            {"lat": min(lats), "lon": min(lons)},
        ]

    alert_zones = []
    for cid, pts in groups.items():
        lats = [p[0] for p in pts]
        lons = [p[1] for p in pts]
        vals = [p[2] for p in pts]

        centroid = {"lat": float(np.mean(lats)), "lon": float(np.mean(lons))}
        boundary = build_boundary(list(zip(lats, lons)))
        #cluster_df = pd.DataFrame({"lat": lats, "lon": lons, "value": vals})

        alert_zones.append({
            "centroid": centroid,
            "boundary": boundary,
            "min_value": float(np.min(vals)),
            "max_value": float(np.max(vals)),
            "mean_value": float(np.mean(vals)),
            #"data": cluster_df.to_json()
        })

    return alert_zones
