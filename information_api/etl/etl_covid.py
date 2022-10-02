''' ETL for collecting covid data '''
import json
from prefect import task, flow
import requests
from bs4 import BeautifulSoup

DEFAULT_TIMEOUT = 10

@task(name='covid_extract')
def extract(iso2: str) -> str:
    ''' E of ETL to extract covid data from worldometers.info '''
    def _get_country_link(iso2):
        country_links = json.load(open(file="worldometer_corona_countries.json", encoding='utf-8'))
        return country_links[iso2.upper()]

    url = "https://www.worldometers.info/coronavirus/" + ("country/" + _get_country_link(iso2) if iso2 != 'GLOBAL' else '')

    res = requests.get(url=url, timeout=DEFAULT_TIMEOUT)
    assert res, 'No data fetched!'
    return res.content


@task(name='covid_transform')
def transform(html: str) -> dict:
    ''' T of ETL to transform extracted covid data from worldometers.info '''
    soup = BeautifulSoup(html, features="html.parser")
    numbers = soup.find_all('div', {'class':'maincounter-number'})
    country = soup.find_all('title')[0].string.split(' ')[0]
    data = {
        'country': country if country != 'COVID' else 'GLOBAL',
        'cases': numbers[0].span.text,
        'deaths': numbers[1].span.text,
        'recovered': numbers[2].span.text
    }
    return data


@task(name='covid_load')
def load(data: dict) -> dict:
    ''' L of ETL to load covid data from worldometers.info '''
    # save to DB/file
    return data


@flow(name='covid_etl_pipeline')
def covid_flow(url: str):
    ''' function to implement the ETL flow of weather data '''
    html = extract(url)
    data = transform(html)
    result = load(data)
    return result
