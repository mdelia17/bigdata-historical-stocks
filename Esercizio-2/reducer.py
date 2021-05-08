#!/usr/bin/env python3
"""reducer.py"""

import sys
from datetime import datetime

TIMESTAMP_FORMAT = "%Y-%m-%d"
prices_2_set = {}
stocks_2_set = {}

for line in sys.stdin:
    # print(str(line))
    line = line.strip()
    line = line.split("\t")
    if len(line)==4:
        # print(str(line))
        ticker = line[0]
        close = line[1]
        volume = line[2]
        date = line[3]
        try:
            close = float(close)
            volume = int(volume)
            date = datetime.strptime(date, TIMESTAMP_FORMAT)
        except ValueError:
            continue
        prices_2_set[(ticker,date)] = [close, volume]
    elif len(line)==2:
        # print(str(line))
        ticker = line[0]
        sector = line[1]
        stocks_2_set[ticker] = sector

keys = list (prices_2_set.keys())
for key in keys:
    if key[0] in stocks_2_set and key[1].year<=2018 and key[1].year>=2009 : 
        sector = stocks_2_set[key[0]] # dobbiamo fare forse qualche controllo
        prices_2_set[key].append(sector)
    else:
        prices_2_set.pop(key)

for i in prices_2_set:
    print("%s\t%s\t%i\t%s\t%s" % (i[0], i[1], prices_2_set[i][0], prices_2_set[i][1], prices_2_set[i][2]))