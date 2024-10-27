from datetime import datetime, timedelta, timezone
import logging
import signal
import sys
import threading

from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS
import requests

from open_meteo_exporter import settings

def chunks(input_list, n):
    n = max(1, n)
    return (input_list[i : i + n] for i in range(0, len(input_list), n))

def checkOldestDate():
  query = f'''
    from(bucket: "{settings.INFLUXDB_BUCKET}")
    |> range(start: 0)  // Start from the beginning
    |> filter(fn: (r) => r._measurement == "{settings.OPEN_METEO_INFLUXDB_MEASUREMENT}" // Filter by measurement
              and r.system == "{settings.SYSTEM_NAME}")  // Filter by system
    |> sort(columns: ["_time"], desc: false)  // Sort by time in ascending order
    |> limit(n: 1)  // Limit to 1 record
    '''
  client = InfluxDBClient(
        url=settings.INFLUXDB_HOST+":"+settings.INFLUXDB_PORT,
        token=settings.INFLUXDB_TOKEN,
        org=settings.INFLUXDB_ORG)
  result_last_point_query = list(client.query_api().query(query))
  client.close()
  if not result_last_point_query:
     return
  time = result_last_point_query[0].records[0].values["_time"]
  logging.debug("Last record %s", time)
  
  api_oldest_date = datetime.fromisoformat(settings.OPEN_METEO_CHECK_OLDEST_DATE)
  api_oldest_date = api_oldest_date.replace(tzinfo=timezone.utc)
  if time <= api_oldest_date :
    return


  url = settings.OPEN_METEO_BASE_URL_ARCHIVE
  params = {
      "latitude": settings.OPEN_METEO_LATITUDE,
      "longitude": settings.OPEN_METEO_LONGITUDE,
      "start_date": settings.OPEN_METEO_CHECK_OLDEST_DATE,
      "end_date": time.strftime('%Y-%m-%d'),
      "hourly": ['cloud_cover', 'direct_radiation', 'diffuse_radiation']
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

  # Prepare records for InfluxDB
  influx_records = []
  for time, cloud, direct_radiation, diffuse_radiation in zip(data['hourly']['time'], data['hourly']['cloud_cover'], data['hourly']['direct_radiation'], data['hourly']['diffuse_radiation']):
      logging.debug(f"Time: {time}, Cloud Cover: {cloud}%, Direct Radiation: {direct_radiation} W/m², Diffuse Radiation: {diffuse_radiation} W/m²")
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
  influx_records_chunked = chunks(influx_records, 10000)
  write_api = client.write_api(write_options=SYNCHRONOUS)
  for chunk in influx_records_chunked:
        write_api.write(org=settings.INFLUXDB_ORG, bucket=settings.INFLUXDB_BUCKET, record=chunk)
        logging.debug("Datapoints in influxdb saved")
  
  write_api.close()
  logging.info('Succeddfull added data to influx.')

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
        "hourly": ['cloud_cover', 'direct_radiation', 'diffuse_radiation']
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
      logging.debug(f"Time: {time}, Cloud Cover: {cloud}%, Direct Radiation: {direct_radiation} W/m², Diffuse Radiation: {diffuse_radiation} W/m²")
      # Convert the time string to a datetime object
      record_time = datetime.fromisoformat(time)
      if record_time > current_time - timedelta(hours=1):
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

  if settings.OPEN_METEO_CHECK_OLD_DATA:
    checkOldestDate()

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
