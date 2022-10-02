''' Main module of API '''
from flask import Flask, request

from etl.etl_covid import covid_flow
from etl.etl_price import price_flow
from etl.etl_weather import weather_flow
from etl.etl_baserate_hu import baserate_hu_flow


app = Flask(__name__)

@app.route('/', methods=['GET'])
def index() -> dict[str, str]:
    ''' default API response: Hello World '''
    info_dict = {
        'info': 'Hello World',
    }
    return info_dict


@app.route('/price', methods=['GET'])
def get_price():
    ''' price API response '''
    ticker = request.args.get('ticker', default='EURHUF=X')
    info_dict = {
        ticker: price_flow(ticker)
    }
    return info_dict


@app.route('/covid', methods=['GET'])
def get_covid():
    ''' covid API response '''
    country = request.args.get('country', default='GLOBAL')
    info_dict = {
        'covid_' + country.lower() : covid_flow(country)
    }
    return info_dict


@app.route('/baserate', methods=['GET'])
def get_baserate_hu():
    ''' baserate API response '''
    info_dict = {
        'baserate_hu': baserate_hu_flow()
    }
    return info_dict


@app.route('/weather', methods=['GET'])
def get_weather():
    ''' weather API response '''
    lat = request.args.get('lat', default=None)
    lon = request.args.get('lon', default=None)
    position = (lat, lon) if lat and lon else None
    city = request.args.get('city', default='Budapest')
    info_dict = {
        'weather_' + city.lower() : weather_flow(position=position, city=city)
    }
    return info_dict


# main loop to run app
if __name__ == '__main__':
    app.run()
