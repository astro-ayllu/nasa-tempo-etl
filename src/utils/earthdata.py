import earthaccess, xarray as xr, pandas as pd, numpy as np, datetime as dt, netCDF4 as nc
from scipy.interpolate import griddata
from typing import Collection
from src.utils.logger import logger

NO2_SHORT_NAME="TEMPO_NO2_L2_NRT"
HCHO_SHORT_NAME="TEMPO_HCHO_L2_NRT"

PATH_FILES="./data_files/"



def _get_last_time_interval():
  now_utc = dt.datetime.utcnow()
  # UTC-3
  now_utc_minus3 = now_utc - dt.timedelta(hours=3)
  start_time = now_utc_minus3 - dt.timedelta(hours=2)
  end_time = now_utc_minus3 + dt.timedelta(hours=2)
  start_time_str = start_time.strftime("%Y-%m-%d %H:%M:%S")
  end_time_str = end_time.strftime("%Y-%m-%d %H:%M:%S")
  logger.info(f"Searching data from {start_time_str} to {end_time_str}")
  return [start_time_str, end_time_str]

def _get_last_granulate(short_name,count=9):
  earthaccess.login()
  [start_time,end_time]=_get_last_time_interval()
  results=earthaccess.search_data(short_name=short_name,temporal=(start_time,end_time))
  return results[-count:]

def _get_granulates_by_date(short_name,date):
  earthaccess.login()
  start_time = f"{date} 00:00:00"
  end_time = f"{date} 23:59:59"
  logger.info(f"Searching data from {start_time} to {end_time}")
  results=earthaccess.search_data(short_name=short_name,temporal=(start_time,end_time))
  return results

def _download_files(granulates):
  earthaccess.download(granulates, local_path=PATH_FILES)

def _get_filenames_of_granulates(granulates):
  filenames=[]
  for g in granulates:
    filenames.append(g.data_links()[0].split("/")[-1])
  return filenames


def _get_lat_lon_from_tempo_dataset(ds):
  geolocation=ds.groups["geolocation"]
  latitude=geolocation.variables["latitude"]
  lat=latitude[:]
  longitude=geolocation.variables["longitude"]
  lon=longitude[:]
  return [lat,lon]

def _get_dimension_from_tempo_product(product,dimension_name):
  dimension=product.variables[dimension_name]
  return dimension[:]

def _get_dataframe_of_file(filename,value_key):
  import os
  full_path = os.path.join(PATH_FILES, filename)  
  with nc.Dataset(full_path) as ds:
    product=ds.groups["product"]

    # flag_meanings: normal suspicious bad || flag_values: [0 1 2]
    flags_qa=_get_dimension_from_tempo_product(product,"main_data_quality_flag")
    value=_get_dimension_from_tempo_product(product,value_key)

    [lat,lon]=_get_lat_lon_from_tempo_dataset(ds)

    df = pd.DataFrame({
      "quality_flag":flags_qa.ravel(),
      "lat": lat.ravel(),
      "lon": lon.ravel(),
      "value": value.ravel()
    })

    return df

def _get_dataframe_of_files(filenames,value_key):
  df_dim=pd.DataFrame()

  for filename in filenames:
      logger.info(f'processing granulate:{filename}')
      df=_get_dataframe_of_file(filename,value_key)
      df_dim=pd.concat([df_dim,df])

  logger.info('Dataframe generated')
  return df_dim



def clean_df(df_old):
  df = df_old[df_old["quality_flag"].isin([0, 1])].copy()

  df = df[df['value'] > 0].copy()

  # Revisión inicial de NaN
  logger.info(f"Total NaN en 'value': {df['value'].isna().sum()}")
  logger.info(f"Porcentaje NaN: {df['value'].isna().mean()*100} %")

  
  # Crear malla (grid) de coordenadas
  lat = df["lat"].values
  lon = df["lon"].values
  values = df["value"].values

  # Máscara de puntos válidos y NaN
  mask_valid = ~np.isnan(values)
  mask_nan = np.isnan(values)

  # Interpolación usando vecinos cercanos (puedes probar 'linear' o 'cubic')
  df.loc[mask_nan, "value"] = griddata(
      points = np.column_stack((lat[mask_valid], lon[mask_valid])),
      values = values[mask_valid],
      xi = np.column_stack((lat[mask_nan], lon[mask_nan])),
      method = "nearest"   # opciones: 'nearest', 'linear', 'cubic'
  )
  return df


def fetch_no2_data():
  logger.info("Fetching NO2 data...")
  granulates=_get_last_granulate(NO2_SHORT_NAME)
  logger.info(f"Found {len(granulates)} granulates.")
  logger.info("Downloading files...")
  _download_files(granulates)
  filenames=_get_filenames_of_granulates(granulates)
  logger.info("Generating dataframe...")
  df_no2=_get_dataframe_of_files(filenames,"vertical_column_troposphere")
  logger.info("Cleaning dataframe...")
  df_no2=clean_df(df_no2)
  return df_no2

def fetch_hcho_data():
  logger.info("Fetching HCHO data...")
  granulates=_get_last_granulate(HCHO_SHORT_NAME)
  logger.info(f"Found {len(granulates)} granulates.")
  logger.info("Downloading files...")
  _download_files(granulates)
  filenames=_get_filenames_of_granulates(granulates)
  logger.info("Generating dataframe...")  
  df_hcho=_get_dataframe_of_files(filenames,"vertical_column")
  logger.info("Cleaning dataframe...")
  df_hcho=clean_df(df_hcho)
  return df_hcho

def _get_warning_points_of_granulate(filename,dimension_name,umbral=5e15):
  df=_get_dataframe_of_file(filename,dimension_name)
  df=clean_df(df)
  df_warnings=df.loc[df["value"]>umbral,["lat","lon","value"]].copy()
  return df_warnings

def fetch_no2_historical_data_warnings(date):
  logger.info(f"Fetching historical data for: {date}")
  granulates = _get_granulates_by_date(NO2_SHORT_NAME, date)
  logger.info(f"Found {len(granulates)} granulates.")
  logger.info("Downloading files...")
  _download_files(granulates)
  filenames = _get_filenames_of_granulates(granulates)
  df=pd.DataFrame()
  for filename in filenames:
      logger.info(f'Processing granulate: {filename}')
      df_warnings = _get_warning_points_of_granulate(filename,dimension_name="vertical_column_troposphere")
      df=pd.concat([df,df_warnings])
  return df

def fetch_hcho_historical_data_warnings(date):
  logger.info(f"Fetching historical data for: {date}")
  granulates = _get_granulates_by_date(HCHO_SHORT_NAME, date)
  logger.info(f"Found {len(granulates)} granulates.")
  logger.info("Downloading files...")
  _download_files(granulates)
  filenames = _get_filenames_of_granulates(granulates)
  df=pd.DataFrame()
  for filename in filenames:
      logger.info(f'Processing granulate: {filename}')
      df_warnings = _get_warning_points_of_granulate(filename,dimension_name="vertical_column", umbral=4e16)
      df=pd.concat([df,df_warnings])
  return df


