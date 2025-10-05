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

    for groupA, dataA in groups_resA:
        dataA["h3_resB"] = dataA.apply(
            lambda row: h3.latlng_to_cell(row["lat"], row["lon"], resolucionB), axis=1
        )

        groupB = dataA.groupby("h3_resB")["value"].agg(["mean", "count"]).reset_index()

        output_list = groupB.apply(
            lambda row: [row["h3_resB"], row["mean"], int(row["count"])], axis=1
        ).tolist()

        filename = f"{param}_{datetime}_{resolutionA}_{groupA}.json"

        end = time.time()
        print(f"Data processed on {end - start} seconds")

        return {"filename": filename, "content": output_list}


def group_data(param: str, df: pd.DataFrame, datetime):
    info_0_4 = group_data_by_resolution(param, df, 0, 4, datetime)
    info_1_5 = group_data_by_resolution(param, df, 1, 5, datetime)
    info_2_6 = group_data_by_resolution(param, df, 2, 6, datetime)

    return [info_0_4, info_1_5, info_2_6]
