#!/usr/bin/env python3
"""reducer.py"""

import sys
from datetime import datetime

TIMESTAMP_FORMAT = "%Y-%m-%d"
prices_2_set = {}
stocks_2_set = {}

for line in sys.stdin:
    line = line.strip()
    line = line.split("\t")
    if len(line)==3:
        ticker = line[0]
        close = line[1]
        date = line[2]
        try:
            close = float(close)
            date = datetime.strptime(date, TIMESTAMP_FORMAT)
        except ValueError:
            continue
        prices_2_set[(ticker,date)] = [close]
    elif len(line)==2:
        ticker = line[0]
        name = line[1]
        stocks_2_set[ticker] = name

keys = list (prices_2_set.keys())
for key in keys:
    if key[0] in stocks_2_set:
        name = stocks_2_set[key[0]]
        prices_2_set[key].append(name)
    else:
        prices_2_set.pop(key)

for i in prices_2_set:
    print("%s\t%s\t%f\t%s" % (i[0], i[1], prices_2_set[i][0], prices_2_set[i][1]))