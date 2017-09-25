import os
import pandas as pd
import datetime as dt
import settings as st
from sf1_api import get_quarterly
from eod_api import get_close, get_components, print_summary


# returns the adjusted close of date from local repository
def get_local_close (ticker, strd):
    df = pd.read_csv(os.path.join(st.eod, ticker+'.csv'))
    df.set_index('Date', inplace=True)
    date1 = dt.datetime.strptime(strd, '%Y-%m-%d')
    date0 = dt.datetime.strptime(df.index[-1], '%Y-%m-%d')
    if date1 < date0:
        return df.loc[df.index[-1], 'Adj_Close']
    else:
        return df.loc[strd, 'Adj_Close']


# get immediate preceding or succeding close price of strd
# preceding close does not consider current date; succeding does
def get_proxima (ticker, strd, offset):
    date = dt.datetime.strptime(strd, '%Y-%m-%d')
    if offset < 0:
        date = date + dt.timedelta(days=offset)
    while True:
        try:
            close = get_local_close(ticker, date.strftime('%Y-%m-%d'))
            break
        except Exception as e:
            if e.__class__.__name__ == 'KeyError':
                date = date + dt.timedelta(days=offset)
            else:
                raise
    return close


def get_combined (ticker):
    quarter = get_quarterly(ticker)
    on_dates = quarter.index
    off_dates = quarter.index[1:]
    closes0 = map(lambda d: get_proxima(ticker, d, 1), on_dates)
    closes1 = map(lambda d: get_proxima(ticker, d, -1), off_dates)
    closes1.append(get_close(ticker, adj=True))
    assert(len(closes0) == len(closes1))

    quarter['P0'] = closes0
    quarter['P1'] = closes1
    quarter.drop(['dimension','price'], axis=1, inplace=True)

    for col in list(quarter):
        if pd.isnull(quarter[col]).all():
            quarter.drop(col, axis=1, inplace=True)

    return quarter


def main (tickers=None, index_name='s&p_500', start_ticker=None):
    if tickers is None:
        tickers = get_components(index_name, start_ticker)
    elif type(tickers) is str:
        tickers = [tickers]
    
    count = 1
    for tick in tickers:
        print_summary(tick, count)
        df = get_combined(tick)
        df.to_csv(os.path.join(st.ptf, tick+'.csv'))
        count += 1



if __name__ == '__main__':
    pd.set_option('expand_frame_repr', False)
