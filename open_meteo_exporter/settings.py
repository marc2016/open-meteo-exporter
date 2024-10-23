import json
import logging
import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.DEBUG)

TIMEZONE = os.getenv("TIMEZONE", "Europe/Berlin")
PROJECT_DIR = str(Path(__file__).parent.parent)

# General
SYSTEM_NAME = os.getenv("SYSTEM_NAME", "no_system")
DIRECTORY = os.getenv("DIRECTORY")
VERBOSE =os.getenv("VERBOSE", 'False').lower() in ('true', '1')

# open meteo
#https://archive-api.open-meteo.com/v1/archive?latitude=51.538&longitude=7.6897&start_date=2024-10-07&end_date=2024-10-09&hourly=cloud_cover,direct_radiation&timezone=Europe%2FBerlin
OPEN_METEO_BASE_URL_FORECAST = os.getenv("OPEN_METEO_BASE_URL_FORECAST")
OPEN_METEO_BASE_URL_ARCHIVE = os.getenv("OPEN_METEO_BASE_URL_ARCHIVE")
OPEN_METEO_LATITUDE = os.getenv("OPEN_METEO_LATITUDE")
OPEN_METEO_LONGITUDE = os.getenv("OPEN_METEO_LONGITUDE")
OPEN_METEO_HOURLY_PARAMETERS = json.loads(os.getenv("OPEN_METEO_HOURLY_PARAMETERS"))
OPEN_METEO_TIMEZONE = os.getenv("OPEN_METEO_TIMEZONE")
OPEN_METEO_INFLUXDB_MEASUREMENT = os.getenv("OPEN_METEO_INFLUXDB_MEASUREMENT")

# INFLUX
INFLUXDB_HOST = os.getenv("INFLUXDB_HOST")
INFLUXDB_PORT = os.getenv("INFLUXDB_PORT", '8086')
INFLUXDB_BUCKET = os.getenv("INFLUXDB_BUCKET")
INFLUXDB_ORG = os.getenv("INFLUXDB_ORG")
INFLUXDB_TOKEN = os.getenv("INFLUXDB_TOKEN")
