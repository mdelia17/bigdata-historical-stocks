#!/usr/bin/env python3
"""reducer.py"""

import sys
from datetime import datetime

TIMESTAMP_FORMAT = "%Y-%m-%d"
ticker_2_set = {}

for line in sys.stdin:
    
    line = line.strip()
    ticker, open, close, low, high, date = line.split("\t")
    try:
        open = float(open)
        close = float(close)
        low = float(low)
        high = float(high)
        date = datetime.strptime(date, TIMESTAMP_FORMAT)
    except ValueError:
        continue

    if ticker not in ticker_2_set:
        ticker_2_set[ticker] = [date,close,date,close,low,high] 
        # # cont strike
        # ticker_2_set[ticker].append(0) 
        # # anno fine strike
        # ticker_2_set[ticker].append(date.year) 
        # # strike max
        # ticker_2_set[ticker].append(0) 
        # # anno strike max
        # ticker_2_set[ticker].append(date.year) 
        # if (open < close):
        #     ticker_2_set[ticker][6] += 1

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
        # if (open < close):
        #     ticker_2_set[ticker][6] += 1
        #     ticker_2_set[ticker][7] = date.year
        # else: 
        #     if (ticker_2_set[ticker][6] > ticker_2_set[ticker][8]):
        #         ticker_2_set[ticker][8] = ticker_2_set[ticker][6]
        #         ticker_2_set[ticker][6] = 0
        #         ticker_2_set[ticker][9] = ticker_2_set[ticker][7]

sorted = sorted(ticker_2_set.items(), key=lambda x: x[1][2], reverse=True)

for t in sorted:
#     ticker_2_set_ordered[i[0]] = i[1]

# # ciclo per ogni chiave
# for ticker in ticker_2_set:

    percentuale = (t[1][3] - t[1][1])/t[1][1] * 100

    # if (ticker_2_set[ticker][6] > ticker_2_set[ticker][8]): 
    #     ticker_2_set[ticker][8] = ticker_2_set[ticker][6]
    #     ticker_2_set[ticker][9] = ticker_2_set[ticker][7]

    print("%s\t%s\t%s\t%f\t%f\t%f" % (t[0], t[1][0], t[1][2], percentuale, t[1][5], t[1][4]))
    # , t[1][8], t[1][9]