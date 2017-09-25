import os

#master = '/Volumes/wtr_passport/'
master = '/Users/tianrunw/github/portfolio_analysis/data'
eod = os.path.join(master, 'eod')
ptf = os.path.join(master, 'ptf')
wtr_vm = os.path.join(master, 'wtr_value_momentum')

assert(os.path.exists(master))
assert(os.path.exists(eod))
assert(os.path.exists(ptf))
assert(os.path.exists(wtr_vm))

# Quandl API key
api_key = ''