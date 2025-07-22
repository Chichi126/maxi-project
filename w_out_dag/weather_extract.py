
# import logging
# from datetime import datetime
# import pandas as pd
# import openmeteo_requests
# import requests_cache
# from retry_requests import retry
# from google.cloud import storage
# from io import BytesIO 
# import os


# # Configuration
# GCS_BUCKET_NAME = 'maxi-sales-bucket001'

# logger = logging.getLogger(__name__)

# # Set the Google Cloud credentials file path
# os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/opt/airflow/include/servkey.json"

# def weather_data():
#     """Fetch weather data from Open-Meteo API for multiple locations"""
#     try:
#         # Initialize clients ONCE outside the loop
#         cache_session = requests_cache.CachedSession('.cache', expire_after=-1)
#         retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
#         openmeteo = openmeteo_requests.Client(session=retry_session)
        
#         # Initialize storage client ONCE
#         storage_client = storage.Client()
#         bucket = storage_client.bucket(GCS_BUCKET_NAME)
        
#         url = "https://archive-api.open-meteo.com/v1/archive"
        
#         LOCATIONS = [
#             {'latitude': 52.52, 'longitude': 13.41, 'name': 'berlin'},
#             {'latitude': 40.71, 'longitude': -74.01, 'name': 'new_york'},
#             {'latitude': 34.05, 'longitude': -118.25, 'name': 'los_angeles'},
#             {'latitude': 48.85, 'longitude': 2.35, 'name': 'paris'},
#             {'latitude': 51.51, 'longitude': -0.13, 'name': 'london'}
#         ]
        
#         results = []
        
#         # Generate timestamp ONCE for all files
#         timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        
#         for location in LOCATIONS:
#             logger.info(f"Starting weather data fetch for {location['name']} from Open-Meteo API")
            
#             params = {
#                 "latitude": location['latitude'],
#                 "longitude": location['longitude'],
#                 "start_date": "2023-01-01",
#                 "end_date": "2024-12-01",
#                 "daily": [
#                 "temperature_2m_mean", "weather_code", "sunshine_duration", 
#                 "temperature_2m_max", "temperature_2m_min", "apparent_temperature_mean", 
#                 "apparent_temperature_max", "apparent_temperature_min", "sunrise", 
#                 "sunset", "rain_sum", "snowfall_sum"
#                 ],
#                 "timezone": "Europe/London"
#             }
            
#             # Make API request
#             responses = openmeteo.weather_api(url, params=params)
#             response = responses[0]
            
#             # Log the response coordinates
#             logger.info(f"Fetched weather data for {location['name']} at coordinates {response.Latitude()}°N {response.Longitude()}°E")
            
#             # Process daily data
#             daily = response.Daily()
        
#             # Create date range
#             date_range = pd.date_range(
#                 start=pd.to_datetime(daily.Time(), unit="s", utc=True),
#                 end=pd.to_datetime(daily.TimeEnd(), unit="s", utc=True),
#                 freq=pd.Timedelta(seconds=daily.Interval()),
#                 inclusive="left"
#             )
        
#             # Extract variables
#             daily_temperature_2m_mean = daily.Variables(0).ValuesAsNumpy()
#             daily_weather_code = daily.Variables(1).ValuesAsNumpy()
#             daily_sunshine_duration = daily.Variables(2).ValuesAsNumpy()
#             daily_temperature_2m_max = daily.Variables(3).ValuesAsNumpy()
#             daily_temperature_2m_min = daily.Variables(4).ValuesAsNumpy()
#             daily_sunrise = daily.Variables(8).ValuesInt64AsNumpy()
#             daily_sunset = daily.Variables(9).ValuesInt64AsNumpy()
            
#             # Create DataFrame for each location
#             daily_data = {
#                 "date": date_range,
#                 "location": location['name'],
#                 "latitude": location['latitude'],
#                 "longitude": location['longitude'],
#                 "temperature_2m_mean": daily_temperature_2m_mean,
#                 "weather_code": daily_weather_code,
#                 "sunshine_duration": daily_sunshine_duration,
#                 "temperature_2m_max": daily_temperature_2m_max,
#                 "temperature_2m_min": daily_temperature_2m_min,
#                 "sunrise": daily_sunrise,
#                 "sunset": daily_sunset,
#             }
            
#             df = pd.DataFrame(data=daily_data)
        
#             logger.info(f"Successfully created DataFrame with {len(df)} rows for {location['name']}")
            
#             # Upload DataFrame to Google Cloud Storage
#             buffer = StringIO()
#             df.to_csv(buffer, index=False)
#             buffer.seek(0)
            
#             # Use the SAME timestamp for all files in this DAG run
#             file_name = f"{location['name']}_weather_data_{timestamp}.csv"
#             blob = bucket.blob(f'weather_data/{file_name}') 
#             blob.upload_from_file(buffer, content_type='text/csv')
#             results.append(file_name)
#             logger.info(f"Weather data for {location['name']} saved to GCS as {file_name}")
        
#         return results
        
#     except Exception as e:
#         logger.error(f"Error fetching weather data: {str(e)}")
#         raise

# print("WEATHER DATA FETCHED")
# weather_data()

import logging
from datetime import datetime
import pandas as pd
import openmeteo_requests
import requests_cache
from retry_requests import retry
from google.cloud import storage
from io import BytesIO
import os


# Configuration
GCS_BUCKET_NAME = 'maxi-sales-bucket002'

logger = logging.getLogger(__name__)

# Set the Google Cloud credentials file path
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/Users/apple/Desktop/maxi-project/cloud_infra/credentials.json"

def weather_data():
    """Fetch weather data from Open-Meteo API for multiple locations"""
    try:
        # Initialize clients ONCE outside the loop
        cache_session = requests_cache.CachedSession('.cache', expire_after=-1)
        retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
        openmeteo = openmeteo_requests.Client(session=retry_session)
        
        # Initialize storage client ONCE
        storage_client = storage.Client()
        bucket = storage_client.bucket(GCS_BUCKET_NAME)
        
        url = "https://archive-api.open-meteo.com/v1/archive"
        
        LOCATIONS = [
            {'latitude': 52.52, 'longitude': 13.41, 'name': 'berlin'},
            {'latitude': 40.71, 'longitude': -74.01, 'name': 'new_york'},
            {'latitude': 34.05, 'longitude': -118.25, 'name': 'los_angeles'},
            {'latitude': 48.85, 'longitude': 2.35, 'name': 'paris'},
            {'latitude': 51.51, 'longitude': -0.13, 'name': 'london'}
        ]
        
        results = []
        
        # Generate timestamp ONCE for all files
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        
        for location in LOCATIONS:
            logger.info(f"Starting weather data fetch for {location['name']} from Open-Meteo API")
            
            params = {
                "latitude": location['latitude'],
                "longitude": location['longitude'],
                "start_date": "2023-01-01",
                "end_date": "2024-12-01",
                "daily": [
                "temperature_2m_mean", "weather_code", "sunshine_duration", 
                "temperature_2m_max", "temperature_2m_min", "apparent_temperature_mean", 
                "apparent_temperature_max", "apparent_temperature_min", "sunrise", 
                "sunset", "rain_sum", "snowfall_sum"
                ],
                "timezone": "Europe/London"
            }
            
            # Make API request
            responses = openmeteo.weather_api(url, params=params)
            response = responses[0]
            
            # Log the response coordinates
            logger.info(f"Fetched weather data for {location['name']} at coordinates {response.Latitude()}°N {response.Longitude()}°E")
            
            # Process daily data
            daily = response.Daily()
        
            # Create date range
            date_range = pd.date_range(
                start=pd.to_datetime(daily.Time(), unit="s", utc=True),
                end=pd.to_datetime(daily.TimeEnd(), unit="s", utc=True),
                freq=pd.Timedelta(seconds=daily.Interval()),
                inclusive="left"
            ).tz_convert(None) 
        
            # Extract variables
            daily_temperature_2m_mean = daily.Variables(0).ValuesAsNumpy()
            daily_weather_code = daily.Variables(1).ValuesAsNumpy()
            daily_sunshine_duration = daily.Variables(2).ValuesAsNumpy()
            daily_temperature_2m_max = daily.Variables(3).ValuesAsNumpy()
            daily_temperature_2m_min = daily.Variables(4).ValuesAsNumpy()
            daily_sunrise = daily.Variables(8).ValuesInt64AsNumpy()
            daily_sunset = daily.Variables(9).ValuesInt64AsNumpy()
            
            # Create DataFrame for each location
            daily_data = {
                "date": date_range,
                "location": location['name'],
                "latitude": location['latitude'],
                "longitude": location['longitude'],
                "temperature_2m_mean": daily_temperature_2m_mean,
                "weather_code": daily_weather_code,
                "sunshine_duration": daily_sunshine_duration,
                "temperature_2m_max": daily_temperature_2m_max,
                "temperature_2m_min": daily_temperature_2m_min,
                "sunrise": daily_sunrise,
                "sunset": daily_sunset,
            }
            
            df = pd.DataFrame(data=daily_data)
        
            logger.info(f"Successfully created DataFrame with {len(df)} rows for {location['name']}")
            
            # Upload DataFrame to Google Cloud Storage
            buffer = BytesIO()
            df.to_parquet(buffer, index=False)
            buffer.seek(0)
            
            # Use the SAME timestamp for all files in this DAG run
            file_name = f"{location['name']}_weather_data_{timestamp}.parquet"
            blob = bucket.blob(f'weather_data/{file_name}') 
            blob.upload_from_string(buffer.getvalue(), content_type='application/octet-stream')
            results.append(file_name)
            logger.info(f"Weather data for {location['name']} saved to GCS as {file_name}")
        
        return results
        
    except Exception as e:
        logger.error(f"Error fetching weather data: {str(e)}")
        raise

print("WEATHER DATA FETCHED")
weather_data()