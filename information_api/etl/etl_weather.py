''' ETL for collecting weather data '''
from prefect import task, flow
import requests
import pandas as pd
import yaml

DEFAULT_TIMEOUT = 10

API_KEY = yaml.safe_load(open(file='etl/apikeys.yml', encoding='utf-8').read())['openweathermap']

def _get_position_by_name(name: str):
    url = 'http://api.openweathermap.org/geo/1.0/direct?'
    params = {
        'q': name,
        'limit': 1,
        'appid': API_KEY
    }

    res = requests.get(url, params=params, timeout=DEFAULT_TIMEOUT)
    data = res.json()[0]
    return data['lat'], data['lon']


def _get_name_by_position(position):
    url = 'http://api.openweathermap.org/geo/1.0/reverse?'
    params = {
        'lat': position[0],
        'lon': position[1],
        'limit': 1,
        'appid': API_KEY
    }

    res = requests.get(url, params=params, timeout=DEFAULT_TIMEOUT)
    data = res.json()[0]
    return f"{data['name']}, {data['country']}"


@task(name='weather_extract')
def extract(position, city):
    ''' E of ETL to extract weather data from openweathermap.org '''
    if not position:
        position = _get_position_by_name(city)
    else:
        city = _get_name_by_position(position)

    url_current = 'https://api.openweathermap.org/data/2.5/weather?'
    url_forecast = 'https://api.openweathermap.org/data/2.5/forecast?'
    params = {
        'lat': position[0],
        'lon': position[1],
        'units': 'metric',
        'appid': API_KEY
    }
    current = requests.get(url_current, params=params, timeout=DEFAULT_TIMEOUT)
    forecast = requests.get(url_forecast, params=params, timeout=DEFAULT_TIMEOUT)
    return {'city': city, 'position': position, 'current': current.json(), 'forecast': forecast.json()}


@task(name='weather_transform')
def transform(info: dict):
    ''' T of ETL to transform weather data extracted from openweathermap.org '''
    city = {
        'city': info['city']
    }
    position = {
        'lat': info['position'][0],
        'lon': info['position'][1]
    }
    current = {
        'temp': info['current']['main']['temp'],
        'description': info['current']['weather'][0]['description'],
        'wind': info['current']['wind']['speed']
    }
    forecast = {}
    for date in info['forecast']['list']:
        forecast[date['dt_txt']] = {
            'temp': date['main']['temp'],
            'description': date['weather'][0]['description'],
            'wind': date['wind']['speed']
        }
    forecast_dataframe = pd.DataFrame().from_dict(forecast,
                                                  orient='index',
                                                  columns=forecast[list(forecast.keys())[0]].keys())
    return {'city': city, 'position': position, 'current': current, 'forecast': forecast_dataframe.to_dict(orient='index')}


@task(name='weather_load')
def load(data: dict) -> dict:
    ''' L of ETL to load weather data from openweathermap.org '''
    # save to DB/file
    return data


@flow(name='weather_etl_pipeline')
def weather_flow(position=None, city: str='Budapest'):
    ''' function to implement the ETL flow of weather data '''
    info = extract(position, city)
    data = transform(info)
    result = load(data)
    return result
