''' ETL for collecting baserate data '''
from datetime import date, timedelta, datetime
from prefect import task, flow
import requests
import pandas as pd

DEFAULT_TIMEOUT = 10

@task(name='baserate_hu_extract')
def extract(from_date: date, to_date: date) -> str:
    ''' E of ETL to extract baserate data from mnb.hu '''
    from_day = from_date.strftime('%d')
    from_month = from_date.strftime('%m')
    to_day = to_date.strftime('%d')
    to_month = to_date.strftime('%m')
    url = f'https://www.mnb.hu/en/jegybanki_alapkamat_alakulasa?datefrom={from_day}%2F{from_month}%2F{from_date.year}&datetill={to_day}%2F{to_month}%2F{to_date.year}&order=0'
    res = requests.get(url, timeout=DEFAULT_TIMEOUT)
    assert res, 'No data fetched!'
    return res.content


@task(name='baserate_hu_transform')
def transform(html: str) -> dict:
    ''' T of ETL to transform extracted baserate data from mnb.hu '''
    table = pd.read_html(html)[0].set_index('Date')
    table.index = [datetime.strptime(i, '%d %B %Y').strftime('%Y-%m-%d') for i in table.index]
    return {'info': table.head(1).to_dict(orient='index'), 'hist': table.to_dict(orient='index')}


@task(name='baserate_hu_load')
def load(data: dict) -> dict:
    ''' L of ETL to load baserate data from mnb.hu '''
    # save to DB/file
    return data


@flow(name='baserate_hu_etl_pipeline')
def baserate_hu_flow(from_date: date=date.today()-timedelta(days=365), to_date: date=date.today()):
    ''' function to implement the ETL flow of baserate data '''
    info = extract(from_date, to_date)
    data = transform(info)
    result = load(data)
    return result
