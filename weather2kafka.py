import threading
import traceback

import requests
import warnings, urllib3
import argparse
from google.transit import gtfs_realtime_pb2
import geopandas as gpd
import pandas as pd
from shapely.geometry import LineString, Point
import zipfile
import io
import json
import time
import os

import sys

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
        pass

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
        pass

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
            common_kafka_config), name="weather_forecast")).start()
    if True:
        threading.Thread(target=thread_wrapper(update_weather_radar, args=(
            os.environ.get('WEATHER_RADAR_URL'),
            int(os.environ.get('WEATHER_RADAR_UPDATE_SECS')),
            common_kafka_config), name="weather_radar")).start()
