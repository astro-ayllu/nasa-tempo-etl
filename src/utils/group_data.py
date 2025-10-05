import time

import h3
import pandas as pd
from src.utils.logger import logger


def group_data_by_resolution(
    param: str, df: pd.DataFrame, resolutionA: int, resolucionB: int, datetime
):
    start = time.time()

    logger.info(
        f"Grouping data for {param} [{datetime}] res[{resolutionA}, {resolucionB}]: {len(df)} items"
    )

    df["h3_resA"] = df.apply(
        lambda row: h3.latlng_to_cell(row["lat"], row["lon"], resolutionA), axis=1
    )

    groups_resA = df.groupby("h3_resA")

    grouped_data = []

    for groupA, dataA in groups_resA:
        dataA["h3_resB"] = dataA.apply(
            lambda row: h3.latlng_to_cell(row["lat"], row["lon"], resolucionB), axis=1
        )

        groupB = dataA.groupby("h3_resB")["value"].agg(["mean", "count"]).reset_index()

        output_list = groupB.apply(
            lambda row: [row["h3_resB"], row["mean"], int(row["count"])], axis=1
        ).tolist()

        filename = f"{param}_{datetime}_{resolutionA}_{groupA}.json"

        grouped_data.append({"filename": filename, "content": output_list})

    end = time.time()
    logger.info(f"Data processed on {end - start} seconds")

    return grouped_data


def group_data(param: str, df: pd.DataFrame, datetime):
    info_0_4 = group_data_by_resolution(param, df, 0, 4, datetime)
    info_1_5 = group_data_by_resolution(param, df, 1, 5, datetime)
    info_2_6 = group_data_by_resolution(param, df, 2, 6, datetime)

    return info_0_4 + info_1_5 + info_2_6


def group_historical_points(param: str, df: pd.DataFrame, resolution: int, date: str):
    logger.info(f"Grouping historical data for {param} [{date}]: {len(df)} items")

    grouped_data = []

    if len(df) == 0:
        return grouped_data

    df["h3_res"] = df.apply(
        lambda row: h3.latlng_to_cell(row["lat"], row["lon"], resolution), axis=1
    )

    grouped = df.groupby("h3_res")
    for h3_res, group in grouped:
        content = group.apply(lambda row: [row["lat"], row["lon"], row["value"]], axis=1).tolist()
        filename = f"{param}_HISTORICAL_{date}_{resolution}_{h3_res}.json"
        grouped_data.append({"filename": filename, "content": content})
    return grouped_data

def group_historical_data(df:pd.DataFrame,param: str,date:str):
    historical_0=group_historical_points(param, df, 0, date)
    historical_1=group_historical_points(param, df, 1, date)
    historical_2=group_historical_points(param, df, 2, date)

    return historical_0 + historical_1 + historical_2

def group_alert_zones(param: str, alert_zones: list, resolution: int,date: str):
    logger.info(f"Grouping alert zones by resolution {resolution}: {len(alert_zones)} items")
    groups = {}
    if len(alert_zones) == 0:
        return []   

    for i, zone in enumerate(alert_zones):
        lat = zone["centroid"]["lat"]
        lon = zone["centroid"]["lon"]
        h3_res = h3.latlng_to_cell(lat, lon, resolution)
        groups[h3_res]=groups[h3_res]+[zone]

    grouped_data = []
    for h3_res, zones in groups.items():
        filename = f"{param}_ALERT_ZONES_{date}_{resolution}_{h3_res}.json"
        grouped_data.append({"filename": filename, "content": zones})   

    return grouped_data

def group_all_alert_zones(param: str, alert_zones: list,date: str):
    alert_zones_0=group_alert_zones(param, alert_zones, 0, date)
    alert_zones_1=group_alert_zones(param, alert_zones, 1, date)
    alert_zones_2=group_alert_zones(param, alert_zones, 2, date)

    return alert_zones_0 + alert_zones_1 + alert_zones_2