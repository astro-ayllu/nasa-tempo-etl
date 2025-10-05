from src.utils import alert_zones, earthdata, group_data, storage, time_manager, mongo


def process_data():
    datetime = time_manager.get_current_time()

    no2_data = earthdata.fetch_no2_data()
    alert_zones_no2 = alert_zones.detect(no2_data)
    no2_grouped_info = group_data.group_data("NO2", no2_data, datetime)
    storage.save_files(no2_grouped_info)

    hcho_data = earthdata.fetch_hcho_data()
    alert_zones_hcho = alert_zones.detect(hcho_data, umbral=4e16)
    hcho_grouped_info = group_data.group_data("HCHO", hcho_data, datetime)
    storage.save_files(hcho_grouped_info)

    mongo.save_processing(datetime)

    data = {
        "no2": len(no2_data),
        "hcho": len(hcho_data),
        "alert_zones_no2": len(alert_zones_no2),
        "alert_zones_hcho": len(alert_zones_hcho),
    }
    return data
