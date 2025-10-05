
from src.utils import earthdata, alert_zones, storage

def process_data():
    no2_data = earthdata.fetch_no2_data()
    alert_zones_no2 = alert_zones.detect(no2_data)
    hcho_data = earthdata.fetch_hcho_data()
    alert_zones_hcho = alert_zones.detect(hcho_data,umbral=4e16)

    data = {
      'no2': len(no2_data),
      'hcho': len(hcho_data),
      'alert_zones_no2': len(alert_zones_no2),
      'alert_zones_hcho': len(alert_zones_hcho)
    }
    storage.save_files([{
        'filename': f"no2_alert_zones.json",
        'content': alert_zones_no2
    }, {
        'filename': f"hcho_alert_zones.json",
        'content': alert_zones_hcho
    }])
    return data
