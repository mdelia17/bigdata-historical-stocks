#!/usr/bin/env python3
"""reducer.py"""

import sys
from datetime import datetime

TIMESTAMP_FORMAT = "%Y-%m-%d"
ticker_date_2_set = {}

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

    if ticker not in ticker_date_2_set:
        ticker_date_2_set[ticker,date] = [open, close, low, high] 

sorted = sorted(ticker_date_2_set.items(), key=lambda x: x[0], reverse=False)

for t in sorted:

    print("%s\t%f\t%f\t%f\t%f\t%s" % (t[0][0], t[1][0], t[1][1], t[1][2], t[1][3], t[0][1]))