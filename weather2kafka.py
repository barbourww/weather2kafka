import threading
import traceback
import json
import time
import os
import sys
import datetime as dt
from zoneinfo import ZoneInfo

import requests
import gzip
import shutil
import rasterio
from rasterio.transform import xy
from pyproj import Transformer
import matplotlib.pyplot as plt
from datetime import datetime
from bs4 import BeautifulSoup
import numpy as np
from pyproj import CRS
from pyproj import Transformer

import kafka_confluent as kc
import psycopg as pg

import logging
logger = logging.getLogger(__name__)
from dotenv import load_dotenv
load_dotenv()

nashville_tz = ZoneInfo('US/Central')


def now_dtz():
    return dt.datetime.now(tz=nashville_tz)


# Helper function to wrap thread targets for fatal error handling
def thread_wrapper(target_func, args=(), name=""):
    def wrapped():
        try:
            target_func(*args)
        except Exception:
            logger.critical(f"Unhandled exception in thread '{name}', exiting entire process.")
            traceback.print_exc(file=sys.stderr)
            sys.exit(1)
    return wrapped


def _connect_weather_database() -> pg.Connection:
    host = os.environ['SQL_HOSTNAME']
    port = os.environ['SQL_PORT']
    user = os.environ['SQL_USERNAME']
    password = os.environ['SQL_PASSWORD']
    database = 'NDOT'
    retry_counter = 5
    while retry_counter > 0:
        try:
            db_conn = pg.connect(
                host=host,
                port=port,
                user=user,
                password=password,
                dbname=database,
                autocommit=True)
            return db_conn
        except pg.OperationalError as e:
            connection_error_context = e
            logger.warning("Could not connect database for weather writing. Trying again....")
            retry_counter -= 1
            time.sleep(2)
    else:
        logger.error(f"Weather destination database parameters used were: "
                      f"host={host}, port={port}, dbname={database}, user={user}")
        raise pg.OperationalError(f"Could not connect database after all attempts.")


def _check_weather_database_connections(db_conn: pg.Connection) -> bool:
    try:
        with db_conn.cursor() as cur:
            cur.execute("SELECT 1=1;")
            cur.fetchall()
            return True
    except Exception:
        logger.warning("Weather database connection check failed. Attempting reconnect.")
        return False



class WeatherForecastProducer:
    def __init__(self, url, poll_interval_minutes, kafka_config):
        self.url = url
        self.poll_interval_seconds = poll_interval_minutes * 60
        self.kc = kc.KafkaConfluentHelper(kafka_config)

        self.topic_name = "weather_forecast"
        self.partition_key = "0"
        # Persistent DB connection for inserts
        self.db_conn = _connect_weather_database()
    def insert_weather_batch(self, current_dict: dict, forecast_dicts: list[dict], write_time: dt.datetime):
        """
        Insert the current observation and forecast periods into laddms.weather using a single write_time.
        Ensures the class has a valid persistent DB connection before inserting.
        """
        # Ensure DB connection is valid; reconnect if needed
        if not _check_weather_database_connections(self.db_conn):
            try:
                self.db_conn.close()
            except Exception:
                pass
            self.db_conn = _connect_weather_database()

        insert_sql = """
            INSERT INTO laddms.weather_conditions (
                write_time,
                start_time,
                end_time,
                generate_time,
                is_daytime,
                temperature,
                feels_like,
                humidity,
                short_forecast,
                precip_chance,
                precip_last3hours
            )
            VALUES (
                %(write_time)s, %(start_time)s, %(end_time)s, %(generate_time)s, %(is_daytime)s, 
                %(temperature)s, %(feels_like)s, %(humidity)s, %(short_forecast)s, 
                %(precip_chance)s, %(precip_last3hours)s
            );
        """

        # Use executemany for efficiency
        with self.db_conn.cursor() as cur:
            cur.executemany(insert_sql, [{'write_time': write_time, **d} for d in [current_dict] + forecast_dicts])
        logger.info(f"Inserted {len(forecast_dicts) + 1} rows into laddms.weather.")


    def wait(self):
        time.sleep(self.poll_interval_seconds)

    def pull_weather_forecast(self, latitude, longitude, num_forecast_hours):
        # Step 1: Get metadata from /points
        point_resp = requests.get(f"{self.url}/points/{latitude},{longitude}").json()
        stations_url = point_resp['properties']['observationStations']
        forecast_hourly_url = point_resp['properties']['forecastHourly']

        # Step 2: Get observation station and latest observation
        stations = requests.get(stations_url).json()
        station_id = stations['observationStations'][0].split('/')[-1]
        obs = requests.get(f"{self.url}/stations/{station_id}/observations/latest").json()['properties']

        # Extract current weather data
        if obs.get('temperature', {}).get('unitCode', '').upper() == 'WMOUNIT:DEGC':
            # convert to degF
            temperature = (float(obs['temperature']['value'])  * 9 / 5) + 32
        else:
            temperature = None
        humidity = obs['relativeHumidity']['value']
        # If can't find the value, use None
        if obs.get('precipitationLast3Hours', {}).get('value', -1) == -1:
            precip_last = None
        # If value is present but None, assume 0.
        elif obs.get('precipitationLast3Hours', {}).get('value') is None:
            precip_last = 0
        elif obs.get('precipitationLast3Hours', {}).get('value', '').upper() == 'NONE':
            precip_last = 0
        elif len(obs.get('precipitationLast3Hours', {}).get('value', '')) > 0:
            if obs.get('precipitationLast3Hours', {}).get('unitCode', '').upper() == 'WMOUNIT:MM':
                # convert to inches
                precip_last = float(obs.get('precipitationLast3Hours', {}).get('value')) / 25.4
            else:
                precip_last = None
        else:
            precip_last = None
        if obs.get('heatIndex', {}).get('value', None) is not None:
            if obs.get('heatIndex', {}).get('unitCode', '').upper() == 'WMOUNIT:DEGC':
                # convert to degF
                feels_like = (float(obs.get('heatIndex').get('value')) * 9 / 5) + 32
            else:
                feels_like = None
        elif obs.get('windChill', {}).get('value', None) is not None:
            if obs.get('windChill', {}).get('unitCode', '').upper() == 'WMOUNIT:DEGC':
                # convert to degF
                feels_like = (float(obs.get('windChill', {}).get('value')) * 9 / 5) + 32
            else:
                feels_like = None
        else:
            feels_like = None

        # Output Current Conditions
        current_dict = {
            'start_time': obs['timestamp'],
            'end_time': None,
            'generate_time': obs['timestamp'],
            'is_daytime': None,
            'temperature': temperature,
            'feels_like': feels_like,
            'humidity': humidity,
            'short_forecast': obs.get('textDescription', None),
            'precip_chance': None,
            'precip_last3hours': precip_last,
        }

        # Current UTC time (aware, not naive)
        utc_now = datetime.now(tz=ZoneInfo("UTC"))
        central_now = utc_now.astimezone(ZoneInfo("US/Central"))

        forecast = requests.get(forecast_hourly_url).json()
        forecast_periods = forecast['properties']['periods']
        # forecast_gen_time = dt.datetime.fromisoformat(forecast['properties']['generatedAt'])
        print(forecast)

        forecast_dicts = []
        for period in forecast_periods:
            start_time = dt.datetime.fromisoformat(period['startTime'])
            if start_time < central_now:
                continue
            if period['temperatureUnit'].upper() == 'F':
                temp = float(period['temperature'])
            elif period['temperatureUnit'].upper() == 'C':
                temp = (float(period['temperature']) * 9 / 5) + 32
            else:
                temp = None

            try:
                humidity = float(period['relativeHumidity']['value'])
            except (ValueError, KeyError):
                humidity = None
            try:
                precip_chance = float(period['probabilityOfPrecipitation']['value'])
            except (ValueError, KeyError, TypeError):
                precip_chance = None

            forecast_dict = {
                'start_time': period['startTime'],
                'end_time': period['endTime'],
                'generate_time': forecast['properties']['generatedAt'],
                'is_daytime': period.get('isDaytime', None),
                'temperature': temp,
                'feels_like': None,
                'humidity': humidity,
                'short_forecast': period.get('shortForecast', None),
                'precip_chance': precip_chance,
                'precip_last3hours': None,
            }
            forecast_dicts.append(forecast_dict)
            if len(forecast_dicts) >= num_forecast_hours:
                break

        return current_dict, forecast_dicts

    def produce_current_and_forecast_to_kafka(self, current_dict: dict, forecast_dicts: list[dict]):
        # Produce to Kafka
        self.kc.send(topic=self.topic_name, key=self.partition_key,
                     json_data=json.dumps(current_dict),
                     headers=[('service', b'weather'), ('datatype', b'current')])
        for fd in forecast_dicts:
            self.kc.send(topic=self.topic_name, key=self.partition_key,
                         json_data=json.dumps(fd),
                         headers=[('service', b'weather'), ('datatype', b'forecast')])
        logger.info(f"Produced {len(forecast_dicts) + 1} weather data points to Kafka.")

        # Now write to the database
        # Use a single write_time for all rows in this batch
        write_time = now_dtz()
        try:
            self.insert_weather_batch(current_dict=current_dict, forecast_dicts=forecast_dicts, write_time=write_time)
        except Exception as e:
            logger.error("Failed to insert weather data into the database.")
            logger.exception(e, exc_info=True)


class WeatherRadarProducer:
    def __init__(self, url, poll_interval_minutes, kafka_config):
        self.url = url
        self.poll_interval_seconds = poll_interval_minutes * 60
        self.kc = kc.KafkaConfluentHelper(kafka_config)

        self.topic_name = "weather_radar"
        self.partition_key = "0"


    def wait(self):
        time.sleep(self.poll_interval_seconds)

    def pull_weather_radar(self):
        # Step 1: Download GeoTIFF .gz
        # Fetch directory listing page
        response = requests.get(self.url)
        if response.status_code != 200:
            raise RuntimeError(f"Failed to fetch directory listing: {self.url}")

        # Parse the page to extract available filenames
        soup = BeautifulSoup(response.text, 'html.parser')
        files = [a['href'] for a in soup.find_all('a', href=True) if a['href'].endswith('.tif.gz')]
        if not files:
            raise RuntimeError("No radar files found in directory listing.")

        # Get the latest file based on timestamp
        latest_file = sorted(files)[-1]
        radar_url = self.url + latest_file

        print(f"Fetching latest Radar File: {radar_url}")
        response = requests.get(radar_url)
        if response.status_code != 200:
            raise RuntimeError(f"Failed to download radar file: {radar_url}")

        with open("radar.tif.gz", "wb") as f:
            f.write(response.content)

        # Step 2: Unzip to GeoTIFF
        with gzip.open("radar.tif.gz", 'rb') as f_in:
            with open("radar.tif", 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)

        # Step 3: Clip to 100 miles radius around Nashville
        with rasterio.open("radar.tif") as src:
            # Read RGB bands and Alpha band
            r = src.read(1)
            g = src.read(2)
            b = src.read(3)
            alpha = src.read(4)

            # Stack into RGBA image array
            rgba = np.dstack((r, g, b, alpha))

            # Generate per-pixel coordinate arrays
            rows, cols = np.meshgrid(np.arange(src.height), np.arange(src.width), indexing='ij')
            lon_flat, lat_flat = xy(src.transform, rows.flatten(), cols.flatten(), offset='center')
            lon = np.array(lon_flat).reshape(rows.shape)
            lat = np.array(lat_flat).reshape(rows.shape)

        # ---- Convert to UTM zone 16N and plot focused Tennessee subplot ----

        # Constants
        center_lat = 36.16
        center_lon = -86.78

        # Define UTM Zone 16N CRS
        utm_crs = CRS.from_epsg(26916)
        transformer_to_utm = Transformer.from_crs(src.crs, utm_crs, always_xy=True)

        # Define bounding box in Lat/Lon around Nashville (approx 100 miles buffer)
        buffer_m = 1609.34 * 50  # miles to meters
        buffer_deg = buffer_m / 111000  # Approx degrees per km
        buffer_deg = 0.2
        min_lon_box = center_lon - buffer_deg
        max_lon_box = center_lon + buffer_deg
        min_lat_box = center_lat - buffer_deg
        max_lat_box = center_lat + buffer_deg

        # Find indices that fall within bounding box
        lat_mask = (lat >= min_lat_box) & (lat <= max_lat_box)
        lon_mask = (lon >= min_lon_box) & (lon <= max_lon_box)
        combined_mask = lat_mask & lon_mask

        # Get bounding indices for slicing
        valid_rows, valid_cols = np.where(combined_mask)
        row_min, row_max = valid_rows.min(), valid_rows.max()
        col_min, col_max = valid_cols.min(), valid_cols.max()

        # Slice the data arrays to Tennessee area
        rgba_slice = rgba[row_min:row_max + 1, col_min:col_max + 1, :]
        lon_slice = lon[row_min:row_max + 1, col_min:col_max + 1]
        lat_slice = lat[row_min:row_max + 1, col_min:col_max + 1]

        # Convert sliced coordinates to UTM
        utm_x_slice, utm_y_slice = transformer_to_utm.transform(lon_slice, lat_slice)

    def produce_radar_to_kafka(self, radar_dict):
        for stop in self.stops_dict.values():
            payload = {
                "stop_id": stop.stop_id,
                "name": stop.name,
                "lat": stop.lat,
                "lon": stop.lon,
                "routes": list(stop.routes)
            }
            self.kc.send(topic=self.topic_name, key=self.partition_key,
                         json_data=json.dumps(payload),
                         headers=[('service', b'gtfs'), ('datatype', b'stops')])
        logger.info(f"Produced {len(self.stops_dict)} stops to kafka from GTFS static.")


def update_weather_forecast(url, poll_interval, num_forecast_hours, locations: list[tuple], receiver_kafka_config):
    forecast_receiver = WeatherForecastProducer(url, poll_interval, kafka_config=receiver_kafka_config)
    logger.info("Created new instance of weather forecast receiver.")
    while True:
        for location in locations:
            lat, lon = location
            # 1) get the latest forecast
            try:
                current_dict, forecast_dicts = forecast_receiver.pull_weather_forecast(latitude=lat, longitude=lon,
                                                                                      num_forecast_hours=num_forecast_hours)
            except Exception as e:
                logger.error("Failed to pull updated weather forecast.")
                logger.exception(e, exc_info=True)
                forecast_receiver.wait()
                continue
            # 2) produce forecast to Kafka
            try:
                forecast_receiver.produce_current_and_forecast_to_kafka(current_dict=current_dict,
                                                                        forecast_dicts=forecast_dicts)
            except Exception as e:
                logger.error("Failed to assemble and send weather forecast to Kafka.")
                logger.exception(e, exc_info=True)
            # 3) invoke WAIT on the receiver object
        forecast_receiver.wait()


def update_weather_radar(url, poll_interval, receiver_kafka_config):
    radar_receiver = WeatherRadarProducer(url, poll_interval, kafka_config=receiver_kafka_config)
    logger.info("Created new instance of weather radar receiver.")
    while True:
        # 1) get the latest radar data
        try:
            rcv_data = radar_receiver.pull_weather_radar()
        except Exception as e:
            logger.error("Failed to pull updated weather radar data.")
            logger.exception(e, exc_info=True)
            radar_receiver.wait()
            continue
        # 2) produce radar data to Kafka
        try:
            radar_receiver.produce_radar_to_kafka(radar_dict=rcv_data)
        except Exception as e:
            logger.error("Failed to assemble and send radar data to Kafka.")
            logger.exception(e, exc_info=True)
        # 3) invoke WAIT on the receiver object
        radar_receiver.wait()


if __name__ == "__main__":
    common_kafka_config = {
        'KAFKA_BOOTSTRAP': os.environ.get('KAFKA_BOOTSTRAP'),
        'KAFKA_USER':  os.environ.get('KAFKA_USER'),
        'KAFKA_PASSWORD': os.environ.get('KAFKA_PASSWORD'),
    }

    log_path = str(os.environ.get('LOG_PATH')) if os.environ.get('LOG_PATH') else "."
    loggerFile = log_path + '/weather2kafka.log'
    loggerFile = './weather2kafka.log'
    print('Saving logs to: ' + loggerFile)
    FORMAT = '%(asctime)s %(message)s'

    debug = True  # set to False to disable console logging

    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.setLevel(logging.DEBUG if debug else logging.INFO)

    file_handler = logging.FileHandler(loggerFile)
    file_handler.setLevel(logging.DEBUG if debug else logging.INFO)
    file_handler.setFormatter(logging.Formatter(FORMAT))
    root_logger.addHandler(file_handler)

    if debug:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.DEBUG)
        console_handler.setFormatter(logging.Formatter(FORMAT))
        root_logger.addHandler(console_handler)

    logger.info("Starting 2x weather to Kafka producer threads.")
    if True:
        locations = [
            (float(os.environ.get('WEATHER_FORECAST_LAT')), float(os.environ.get('WEATHER_FORECAST_LON')))
        ]
        threading.Thread(target=thread_wrapper(update_weather_forecast, args=(
            os.environ.get('WEATHER_FORECAST_URL'),
            int(os.environ.get('WEATHER_FORECAST_UPDATE_SECS')),
            int(os.environ.get('WEATHER_NUM_FORECAST_HOURS')),
            locations,
            common_kafka_config), name="weather_forecast")).start()
    if False:
        threading.Thread(target=thread_wrapper(update_weather_radar, args=(
            os.environ.get('WEATHER_RADAR_URL'),
            int(os.environ.get('WEATHER_RADAR_UPDATE_SECS')),
            common_kafka_config), name="weather_radar")).start()
