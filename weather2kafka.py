import threading
import traceback
import json
import time
import os
import sys

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

import logging
logger = logging.getLogger(__name__)
from dotenv import load_dotenv
load_dotenv()


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


class WeatherForecastProducer:
    def __init__(self, url, poll_interval_minutes, kafka_config):
        self.url = url
        self.poll_interval_seconds = poll_interval_minutes * 60
        self.kc = kc.KafkaConfluentHelper(kafka_config)

        self.topic_name = ""
        self.partition_key = ""


    def wait(self):
        time.sleep(self.poll_interval_seconds)

    def pull_weather_forecast(self):
        # Step 1: Get metadata from /points
        point_resp = requests.get(f"{BASE_URL}/points/{LAT},{LON}").json()
        stations_url = point_resp['properties']['observationStations']
        forecast_hourly_url = point_resp['properties']['forecastHourly']

        # Step 2: Get observation station and latest observation
        stations = requests.get(stations_url).json()
        station_id = stations['observationStations'][0].split('/')[-1]
        obs = requests.get(f"{BASE_URL}/stations/{station_id}/observations/latest").json()['properties']

        # Extract current weather data
        temperature = obs['temperature']['value']
        humidity = obs['relativeHumidity']['value']
        precip_last_hour = obs.get('precipitationLastHour', {}).get('value')
        feels_like = obs.get('heatIndex', {}).get('value') or obs.get('windChill', {}).get('value')

        # Step 3: Get next 6-hour forecast
        forecast = requests.get(forecast_hourly_url).json()
        next6 = forecast['properties']['periods'][:6]

        # Output Current Conditions
        print(f"\nCurrent Conditions in Nashville, TN:")
        print(f"Temperature: {temperature} °C")
        print(f"Feels Like: {feels_like} °C")
        print(f"Humidity: {humidity}%")
        print(f"Precipitation (last hour): {precip_last_hour} mm")

        # Output Forecast
        print("\nForecast for the Next 6 Hours:")
        for period in next6:
            start_time = period['startTime']
            temp = period['temperature']
            short_forecast = period['shortForecast']
            precip_chance = period['probabilityOfPrecipitation']['value']
            print(f"{start_time}: {temp}°C, {short_forecast}, Precip: {precip_chance}%")

    def produce_forecast_to_kafka(self, forecast_dict):
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


class WeatherRadarProducer:
    def __init__(self, url, poll_interval_minutes, kafka_config):
        self.url = url
        self.poll_interval_seconds = poll_interval_minutes * 60
        self.kc = kc.KafkaConfluentHelper(kafka_config)

        self.topic_name = ""
        self.partition_key = ""


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
        radar_url = base_url + latest_file

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


def update_weather_forecast(url, poll_interval, receiver_kafka_config):
    forecast_receiver = WeatherForecastProducer(url, poll_interval, kafka_config=receiver_kafka_config)
    logger.info("Created new instance of weather forecast receiver.")
    while True:
        # 1) get the latest forecast
        try:
            rcv_data = forecast_receiver.pull_weather_forecast()
        except Exception as e:
            logger.error("Failed to pull updated weather forecast.")
            logger.exception(e, exc_info=True)
            forecast_receiver.wait()
            continue
        # 2) produce forecast to Kafka
        try:
            forecast_receiver.produce_forecast_to_kafka(forecast_dict=rcv_data)
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
        threading.Thread(target=thread_wrapper(update_weather_forecast, args=(
            os.environ.get('WEATHER_FORECAST_URL'),
            int(os.environ.get('WEATHER_FORECAST_UPDATE_SECS')),
            float(os.environ.get('WEATHER_FORECAST_LAT')),
            float(os.environ.get('WEATHER_FORECAST_LON')),
            common_kafka_config), name="weather_forecast")).start()
    if True:
        threading.Thread(target=thread_wrapper(update_weather_radar, args=(
            os.environ.get('WEATHER_RADAR_URL'),
            int(os.environ.get('WEATHER_RADAR_UPDATE_SECS')),
            common_kafka_config), name="weather_radar")).start()
