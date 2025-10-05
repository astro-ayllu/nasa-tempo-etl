from src.utils import (
    alert_zones,
    earthdata,
    group_data,
    storage,
    time_manager,
    db,
    fire_data
)
import requests
import datetime as dt


def process_data():
    datetime = time_manager.get_current_time()

    no2_data = earthdata.fetch_no2_data()
    alert_zones_no2 = alert_zones.detect(no2_data)
    grouped_alert_zones_no2 = group_data.group_all_alert_zones("NO2", alert_zones_no2, datetime)
    no2_grouped_info = group_data.group_data("NO2", no2_data, datetime)
    storage.save_files(no2_grouped_info)
    storage.save_files(grouped_alert_zones_no2)

    hcho_data = earthdata.fetch_hcho_data()
    alert_zones_hcho = alert_zones.detect(hcho_data, umbral=4e16)
    grouped_alert_zones_hcho = group_data.group_all_alert_zones("HCHO", alert_zones_hcho, datetime)
    hcho_grouped_info = group_data.group_data("HCHO", hcho_data, datetime)
    storage.save_files(hcho_grouped_info)
    storage.save_files(grouped_alert_zones_hcho)

    db.save_processing(datetime)

    data = {
        "no2": len(no2_data),
        "hcho": len(hcho_data),
        "alert_zones_no2": len(alert_zones_no2),
        "alert_zones_hcho": len(alert_zones_hcho),
    }
    return data


def save_fire_data():
    fire_data.save_fire_data(
        "https://firms.modaps.eosdis.nasa.gov/data/active_fire/modis-c6.1/csv/MODIS_C6_1_USA_contiguous_and_Hawaii_24h.csv",
        "MODIS_C6_1_USA_contiguous_and_Hawaii_24h.csv",
    )
    fire_data.save_fire_data(
        "https://firms.modaps.eosdis.nasa.gov/data/active_fire/suomi-npp-viirs-c2/csv/SUOMI_VIIRS_C2_USA_contiguous_and_Hawaii_24h.csv",
        "SUOMI_VIIRS_C2_USA_contiguous_and_Hawaii_24h.csv",
    )
    fire_data.save_fire_data(
        "https://firms.modaps.eosdis.nasa.gov/data/active_fire/noaa-20-viirs-c2/csv/J1_VIIRS_C2_USA_contiguous_and_Hawaii_24h.csv",
        "J1_VIIRS_C2_USA_contiguous_and_Hawaii_24h.csv",
    )
    fire_data.save_fire_data(
        "https://firms.modaps.eosdis.nasa.gov/data/active_fire/noaa-21-viirs-c2/csv/J2_VIIRS_C2_USA_contiguous_and_Hawaii_24h.csv",
        "J2_VIIRS_C2_USA_contiguous_and_Hawaii_24h.csv",
    )
    fire_data.save_fire_data(
        "https://firms.modaps.eosdis.nasa.gov/data/active_fire/landsat/csv/LANDSAT_USA_contiguous_and_Hawaii_24h.csv",
        "LANDSAT_USA_contiguous_and_Hawaii_24h.csv",
    )


def historical_data():
    date = (dt.datetime.utcnow() - dt.timedelta(days=1)).strftime("%Y-%m-%d")
    
    no2_data = earthdata.fetch_no2_historical_data_warnings(date)
    historical_no2 = group_data.group_historical_data(no2_data, "NO2", date)
    storage.save_files(historical_no2)

    hcho_data = earthdata.fetch_hcho_historical_data_warnings(date)
    historical_hcho = group_data.group_historical_data(hcho_data, "HCHO", date)
    storage.save_files(historical_hcho)

    db.save_processing(date, process_key="historical_day_warning_points")

    return {
        "no2": len(no2_data),
        "hcho": len(hcho_data),
    }