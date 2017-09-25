import os
import json
import requests
import pandas as pd
from pandas import DataFrame
import datetime as dt
import settings as st


def get_tick_info (tick, info):
    df = pd.read_csv(os.path.join(st.master, 'us_public_companies.csv'))
    df.set_index('Ticker', inplace=True)
    return df.loc[tick, info]


def get_tick_info2 (tick, info):
    df = pd.read_csv(os.path.join(st.master, 's&p_500.csv'))
    df.set_index('Ticker', inplace=True)
    return df.loc[tick, info]


def print_summary(ticker, count):
    #ticker = ticker.replace('_', '')
    company = get_tick_info2(ticker, 'Name')
    print "Company {0}: {1} [{2}]".format(count, company, ticker)


def get_ticker_url (ticker):
    return 'https://www.quandl.com/api/v3/datasets/EOD/{0}.json'.format(ticker)


# nrow is not compatible with either start or end
# start and end are ignored if nrow is not None
def get_data (ticker, nrow=None, start=None, end=None, trans=None):
    if ticker == 'BRKB':
        ticker = 'BRK_B'
    data_url = get_ticker_url(ticker)
    query = {'api_key':st.api_key, 'rows':nrow, 'start_date':start, 'end_date':end, 
             'transform':trans}
    response = requests.get(data_url, params=query)

    if response.status_code != 200:
        raise ValueError("Request failed: %d" % response.status_code)
    else:
        data = response.json()['dataset']['data']
        cols = response.json()['dataset']['column_names']
        df = DataFrame(data, columns=cols)
        df.set_index('Date', inplace=True)
        return df


def get_close (ticker, date=None, adj=False):
    if ticker == 'BRKB':
        ticker = 'BRK_B'
    if date is None:
        df = get_data(ticker, nrow=1)
    else:
        df = get_data(ticker, start=date, end=date)

    if adj:
        return df['Adj_Close'][0]
    else:
        return df['Close'][0]


def get_components (index_name, start_ticker=None):
    source = os.path.join(st.master, index_name+'.csv')
    df = pd.read_csv(source)
    df.set_index('Ticker', inplace=True)

    if start_ticker is not None:
        df = df.loc[start_ticker:, :]

    return df.index.tolist()



def main (start='2001-01-01', tickers=None, index_name='s&p_500', start_ticker=None):
    if tickers is None:
        tickers = get_components(index_name, start_ticker)
    elif type(tickers) is str:
        tickers = [tickers]
    
    count = 1
    for tick in tickers:
        print_summary(tick, count)
        df = get_data(tick, start=start)
        tick = tick.replace('_', '')
        df.to_csv(os.path.join(st.eod, tick+'.csv'))
        count += 1



if __name__ == '__main__':
    pd.set_option('expand_frame_repr', False)
