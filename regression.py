import os
import numpy as np
import pandas as pd
import datetime as dt
import settings as st
import statsmodels.formula.api as sfa
from eod_api import get_close, get_tick_info2


# out-of-sample prediction
def predict (rg, last):
    result = rg.params['Intercept']
    assert(rg.params.index[0] == 'Intercept')

    for index_name in rg.params.index[1:]:
        result += rg.params[index_name] * last[index_name]

    return result


def regress (source, signif):
    df = pd.read_csv(source)
    df.set_index('datekey', inplace=True)
    df.dropna(axis=0, inplace=True)
    ticker = df['ticker'][0]

    last = df.iloc[-1, :]
    df.drop(df.index[-1], axis=0, inplace=True)

    if len(df) < 30:
        print "Ticker: {0} has {1} observations. Regression halted.".format(ticker, len(df))
        return None, None
    else:
        print "Ticker: {0}".format(ticker)

    variables = list(df)

    for x in ['ticker', 'P0', 'P1']:
        variables.remove(x)

    formula = "np.log(P1) ~ {0}".format('+'.join(variables))
    rg0 = sfa.ols(formula=formula, data=df).fit()

    while (rg0.pvalues[1:] > signif).any():
        # remove variable with max t-statistic p-value
        variables.remove(rg0.pvalues[1:].idxmax())
        formula = "np.log(P1) ~ {0}".format('+'.join(variables))
        rg1 = sfa.ols(formula=formula, data=df).fit()

        # check if new model superior to previous
        if (rg1.rsquared > rg0.rsquared) or (rg1.fvalue > rg0.fvalue):
            rg0 = rg1
        else:
            break

    return rg0, predict(rg0, last)


# name: file name with .csv extension
def parallel_wrapper (name, signif):
    source = os.path.join(st.ptf, name)
    df = pd.read_csv(source)
    last_date = df['datekey'].iloc[-1]
    ticker = name[:-4]
    rg, prd = regress(source, signif)

    if rg is None:
        return None

    current = get_close(ticker)
    expected = np.exp(prd)
    exp_return = (expected - current)/current
    
    # number of parameters
    nop = len(rg.params) - 1
    # number of price-related parameters
    pnop = sum([1 for i in rg.params.index if i in ['pe','ps','pb']])
    # residual standard error
    resid_se = np.sqrt(rg.mse_resid)

    sector = get_tick_info2(ticker, 'Sector')
    spy = get_tick_info2(ticker, 'Weight')

    return [ticker, last_date, current, expected, exp_return, rg.nobs, nop, 
            pnop, rg.rsquared_adj, rg.fvalue, resid_se, spy, sector]


def main (signif=0.001):
    filenames = os.listdir(st.ptf)
    filenames = [m for m in filenames if m.endswith('.csv')]
    filenames.sort()

    df = [parallel_wrapper(name, signif) for name in filenames]

    while None in df:
        df.remove(None)

    cols = ['Ticker', 'Last', 'Current', 'Expected', 'Return', 'Obs', 'Nop', 
            'Pnop', 'Adj_R2', 'F_stat', 'RSE', 'Weight', 'Sector']
    df = pd.DataFrame(df, columns=cols)
    df.set_index('Ticker', inplace=True)

    date_name = dt.datetime.today().date().strftime('%Y%m%d')
    df.to_csv(os.path.join(st.wtr_vm, date_name+'.csv'))

    print "-"*80
    print "Number of securities: {0}".format(len(df))
    print "Saved as {0}".format(date_name+'.csv')
    print "-"*80


def quick_read (offset=0):
    filenames = os.listdir(st.wtr_vm)
    filenames.sort()
    last_file = filenames[-1+offset]
    assert(last_file.endswith('.csv'))
    source = os.path.join(st.wtr_vm, last_file)

    df = pd.read_csv(source)
    df.set_index('Ticker', inplace=True)

    return df


def screen_main (target=0.05, ar2=0.8, nrow=None, sort='Adj_R2', sector=[]):
    df = quick_read()
    
    # expected return and adjusted R2 criteria
    df = df[df['Return'] > target]
    df = df[df['Adj_R2'] > ar2]

    # sector-specific criteria
    if len(sector) > 0:
        crit = map(lambda s: s in sector, df['Sector'])
        df = df[crit]

    df.sort_values(sort, ascending=False, inplace=True)

    return df.head(nrow)


def ranking_helper (i0, df0, df1, weights):
    tick0 = df0.index[i0]
    i1 = df1.index.get_loc(tick0)
    score = i0 * weights[0] + i1 * weights[1]
    return score


# weights: (Return, Adj_R2)
def ranking (target=0.05, ar2=0.8, sector=[], weights=(0.5, 0.5)):
    assert(sum(weights) == 1)
    df0 = screen_main(target=target, ar2=ar2, sort='Return', sector=sector)
    df1 = screen_main(target=target, ar2=ar2, sort='Adj_R2', sector=sector)
    assert(len(df0) == len(df1))

    ranks = [ranking_helper(i, df0, df1, weights) for i in xrange(len(df0))]

    df0['Rank'] = ranks
    df0.sort_values('Rank', ascending=True, inplace=True)

    return df0




if __name__ == '__main__':
    pd.set_option('expand_frame_repr', False)
