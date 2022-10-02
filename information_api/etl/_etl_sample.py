''' ETL for collecting <type> data '''
from prefect import task, flow

@task(name='weather_extract')
def extract():
    ''' E of ETL to extract <type> data from <url> '''
    return 0


@task(name='weather_transform')
def transform():
    ''' T of ETL to transform <type> data extracted from <url> '''
    return 0


@task(name='weather_load')
def load():
    ''' L of ETL to load <type> data from <url> '''
    return 0


@flow(name='weather_etl_pipeline')
def weather_flow():
    ''' function to implement the ETL flow of <type> data '''
    info = extract()
    data = transform()
    result = load()
    return result