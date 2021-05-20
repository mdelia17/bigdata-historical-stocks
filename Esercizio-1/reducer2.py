#!/usr/bin/env python3
"""reducer.py"""

import sys
from datetime import datetime

TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"

def percentuale(a, b):
    return float("{:.5f}".format(float((b-a)/a*100)))

ticker_2_set = {}

for line in sys.stdin:
    
    line = line.strip()
    ticker, open, close, low, high, date = line.split("\t")
    try:
        open = float("{:.5f}".format(float(open)))
        close = float("{:.5f}".format(float(close)))
        low = float("{:.5f}".format(float(low)))
        high = float("{:.5f}".format(float(high)))
        date = datetime.strptime(date, TIMESTAMP_FORMAT)
    except ValueError:
        continue

    if ticker not in ticker_2_set:
        ticker_2_set[ticker] = [date,close,date,close,low,high,date,0,0,0] 
        if (open < close):
            ticker_2_set[ticker][7] += 1
            ticker_2_set[ticker][9] = date.year
            ticker_2_set[ticker][8] = 1

    else:
        if (date < ticker_2_set[ticker][0]):
            ticker_2_set[ticker][0] = date
            ticker_2_set[ticker][1] = close
        if (date > ticker_2_set[ticker][2]):
            ticker_2_set[ticker][2] = date
            ticker_2_set[ticker][3] = close
        if (low < ticker_2_set[ticker][4]):
            ticker_2_set[ticker][4] = low
        if (high > ticker_2_set[ticker][5]):
            ticker_2_set[ticker][5] = high

        if (open < close):
            if (date-ticker_2_set[ticker][6]).days == 1:
                ticker_2_set[ticker][7] += 1
            else: 
                ticker_2_set[ticker][7] = 1
            if ticker_2_set[ticker][7] >= ticker_2_set[ticker][8]:
                    ticker_2_set[ticker][8] = ticker_2_set[ticker][7]
                    ticker_2_set[ticker][9] = date.year
        else: 
            ticker_2_set[ticker][7] = 0
        ticker_2_set[ticker][6] = date 

sorted = sorted(ticker_2_set.items(), key=lambda x: x[1][2], reverse=True)

for t in sorted:
    
    print("%s\t%s\t%s\t%f\t%f\t%f\t%i\t%s" % (t[0], t[1][0], t[1][2], percentuale(t[1][1],t[1][3]), t[1][5], t[1][4], t[1][8], t[1][9]))