from datetime import datetime, timedelta
import logging
import signal
import sys
import threading

from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS
import requests

from open_meteo_exporter import settings

def doImport():
    """
    Run main application with can interface
    """
    # Verbose output
    if settings.VERBOSE is True:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    if not settings.INFLUXDB_HOST or not settings.INFLUXDB_ORG or not settings.INFLUXDB_BUCKET:
        error_message = 'INFLUX_HOST or INFLUX_ORG or INFLUX_BUCKET not defined!'
        logging.error(error_message)
        raise Exception(error_message)

    url = settings.OPEN_METEO_BASE_URL_FORECAST
    params = {
        "latitude": settings.OPEN_METEO_LATITUDE,
        "longitude": settings.OPEN_METEO_LONGITUDE,
        "past_days": 1,
        "forecast_days": 1,
        "hourly": ['cloud_cover', 'direct_radiation', 'diffuse_radiation'],
        "timezone": settings.OPEN_METEO_TIMEZONE
    }

    # Make the GET request
    response = requests.get(url, params=params)

    # Check if the request was successful
    if response.status_code == 200:
        # Parse the JSON response
        data = response.json()
        logging.debug(data)
    else:
        logging.error(f"Error: {response.status_code}")

    client = InfluxDBClient(
        url=settings.INFLUXDB_HOST+":"+settings.INFLUXDB_PORT,
        token=settings.INFLUXDB_TOKEN,
        org=settings.INFLUXDB_ORG)
    write_api = client.write_api(write_options=SYNCHRONOUS)

    current_time = datetime.now() 
    # Prepare records for InfluxDB
    influx_records = []
    for time, cloud, direct_radiation, diffuse_radiation in zip(data['hourly']['time'], data['hourly']['cloud_cover'], data['hourly']['direct_radiation'], data['hourly']['diffuse_radiation']):
      logging.debug(f"Time: {time}, Cloud Cover: {cloud}%, Direct Radiation: {direct_radiation} W/m², Direct Radiation: {diffuse_radiation} W/m²")
      # Convert the time string to a datetime object
      record_time = datetime.fromisoformat(time)
      if record_time > current_time - timedelta(hours=2):
         continue
      record={
         "measurement": settings.OPEN_METEO_INFLUXDB_MEASUREMENT,
          "tags": {
              "system": settings.SYSTEM_NAME,
          },
          "time": f"{time}:00Z",
          "fields": {
              "cloud_cover": cloud,
              "direct_radiation": direct_radiation,
              "diffuse_radiation": diffuse_radiation
          },
      }
      # Add the record to the list
      influx_records.append(record)

    write_api.write(org=settings.INFLUXDB_ORG, bucket=settings.INFLUXDB_BUCKET, record=influx_records)
    write_api.close()
    logging.info('Succeddfull added data to influx.')

e = threading.Event()

class GracefulKiller:
  kill_now = False
  def __init__(self):
    signal.signal(signal.SIGINT, self.exit_gracefully)
    signal.signal(signal.SIGTERM, self.exit_gracefully)

  def exit_gracefully(self,signum, frame):
    self.kill_now = True
    e.set()
    

if __name__ == '__main__':
  logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
  killer = GracefulKiller()
  while True:
    if killer.kill_now:
      break
    doImport()
    e.wait(timeout=3600)
    if killer.kill_now:
      break

  logging.info("End of the program. I was killed gracefully :)")
