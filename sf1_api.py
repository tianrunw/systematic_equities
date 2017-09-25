import os
import json
import requests
import pandas as pd
from pandas import DataFrame
import datetime as dt
import settings as st

sf1_url = 'https://www.quandl.com/api/v3/datatables/SHARADAR/SF1.json'
indicators = ['TICKER','DIMENSION','DATEKEY',
              'REVENUE','NETINC','NCF','CASHNEQ',
              'DE','NETMARGIN',
              'EPS','PE','PS','PB','BVPS','PRICE']
indicators = ','.join(indicators)


def get_tickers ():
    url = 'http://www.sharadar.com/meta/tickers.json'
    response = requests.get(url)
    content = response.json()
    data = map(lambda d: map(str, d.values()), content)
    cols = content[0].keys()
    df = DataFrame(data, columns=cols)
    df.set_index('Ticker', inplace=True)
    df = df[df['Exchange'] != 'DELISTED']
    df = df[df['Is Foreign'] == 'N']
    df.to_csv(os.path.join(st.master, 'us_public_companies.csv'))


def get_indicators ():
    url = 'http://www.sharadar.com/meta/indicators.json'
    response = requests.get(url)
    content = response.json()
    data = map(lambda d: map(str, d.values()), content)
    cols = content[0].keys()
    df = DataFrame(data, columns=cols)
    df.set_index('Indicator', inplace=True)
    df.to_csv(os.path.join(st.master, 'indicators.csv'))


def get_indic_info (indic, info='Description'):
    df = pd.read_csv(os.path.join(st.master, 'indicators.csv'))
    df.set_index('Indicator', inplace=True)
    return df.loc[indic, info]


def get_tick_info (tick, info):
    df = pd.read_csv(os.path.join(st.master, 'us_public_companies.csv'))
    df.set_index('Ticker', inplace=True)
    return df.loc[tick, info]


# content: json object
def print_summary(content):
    #print json.dumps(content, indent=4)
    tick = content['datatable']['data'][0][0]
    print "-" * len(indicators)
    print "Name:    {0}".format(get_tick_info(tick, 'Name'))
    print "Ticker:  {0}".format(tick)
    print "Sector:  {0}".format(get_tick_info(tick, 'Sector'))
    print "Reports: {0}".format(len(content['datatable']['data']))
    print indicators
    print "-" * len(indicators)


def adjust_numbers(df, unit=10**9):
    df['revenue'] = df['revenue']/unit
    df['netinc'] = df['netinc']/unit
    df['ncf'] = df['ncf']/unit
    df['cashneq'] = df['cashneq']/unit
    return df


def get_quarterly (ticker, dim='ARQ', start=None, end=None):
    ticker = ticker.replace('_', '')
    ticker = ticker.replace('.', '')
    query = {'ticker':ticker, 'qopts.columns':indicators, 'dimension':dim, 
             'calendardate.gte':start, 'calendardate.lte':end, 
             'api_key':st.api_key}
    response = requests.get(sf1_url, params=query)

    if response.status_code != 200:
        raise ValueError("Request failed: %d" % response.status_code)
    else:
        #print_summary(response.json())
        data = response.json()['datatable']['data']
        cols = response.json()['datatable']['columns']
        cols = [d['name'] for d in cols]
        df = DataFrame(data, columns=cols)
        df.set_index('datekey', inplace=True)
        df = adjust_numbers(df)
        return df


def write_data (dfs):
    for (ticker, df) in dfs.items():
        df.to_csv(os.path.join(st.sf1, ticker+'.csv'))


def main ():
    tickers = ['AMZN','BRKB','SBUX','AAPL','GOOGL','LOXO','COST','PCLN','JNJ','FB','DFS','TSLA']
    tickers.sort()
    dfs = [get_quarterly(tick) for tick in tickers]
    dfs = dict(zip(tickers, dfs))
    write_data(dfs)
    return dfs

if __name__ == '__main__':
    pd.set_option('expand_frame_repr', False)
