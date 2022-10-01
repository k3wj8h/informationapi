''' ETL for collecting price data '''
from prefect import task, flow
import yfinance as yf


@task(name='price_extract')
def extract(ticker_symbol: str) -> yf.ticker.Ticker:
    ''' E of ETL to extract price data from yahoo.com '''
    ticker = yf.Ticker(ticker_symbol)
    assert 'shortName' in ticker.info.keys() and ticker.info['shortName'], f'Ticker symbol {ticker_symbol} is not exist'
    return ticker


@task(name='price_transform')
def transform(ticker: yf.ticker.Ticker, period: str, columns: list) -> dict:
    ''' T of ETL to transform extracted price data from yahoo.com '''
    assert set(columns).issubset([None, 'Open','High','Low','Close','Volume','Dividents','Stock Splits','Close%']), 'Bad column value. Valid column names: Open,High,Low,Close,Volume,Dividents,Stock,Splits,Close%'
    assert period in [None,'1d','1wk','1mo','6mo','1y','5y','max'], 'Bad period value. Valid periods: 1d,1wk,1mo,6mo,1y,5y,max'

    info_needed = ['bid', 'ask', 'currentPrice', 'regularMarketPrice', 'previousClose', 'shortName', 'exchange']
    info = dict((k, ticker.info[k]) for k in info_needed if k in ticker.info.keys())
    info['change'] = round(100 * (info['regularMarketPrice'] / info['previousClose'] -1), 2)

    hist = ticker.history(period=period)
    hist['Close'] = hist['Close'].round(2)
    hist['Close%'] = round(100 * (hist.Close / hist.Close.shift() - 1), 2)
    hist.fillna('N/A', inplace=True)
    hist.index = hist.index.strftime("%Y-%m-%d")
    return {'info': info, 'hist': hist[columns].to_dict(orient='index')}


@task(name='price_load')
def load(data: dict) -> dict:
    ''' L of ETL to load price data from yahoo.com '''
    # save to DB/file
    return data


@flow(name='price_etl_pipeline')
def price_flow(ticker_symbol, period='1wk', columns=None) -> dict:
    ''' function to implement the ETL flow of price data '''
    if columns is None:
        columns = ['Close','Volume','Close%']
    ticker = extract(ticker_symbol)
    data = transform(ticker, period, columns)
    result = load(data)
    return result
