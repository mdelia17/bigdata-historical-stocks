#!/usr/bin/env python3
"""reducer.py"""

import sys
from datetime import datetime

TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"
sector_2_set ={}
for line in sys.stdin:
    # print(line)
    line = line.strip()
    ticker, date, close, name = line.split("\t")
    try:
        date = datetime.strptime(date, TIMESTAMP_FORMAT)
        close = float(close)
    except ValueError:
        continue
    if (ticker, date.month) not in sector_2_set:
        sector_2_set[(ticker, date.month)] = [date, close, date, close, name]
    else: 
        if sector_2_set[(ticker, date.month)][0] == date: 
            sector_2_set[(ticker, date.month)][1] += close
        if sector_2_set[(ticker, date.month)][2] == date: 
            sector_2_set[(ticker, date.month)][3] += (close)
        if sector_2_set[(ticker, date.month)][0] > date: 
            sector_2_set[(ticker, date.month)][0] = date
            sector_2_set[(ticker, date.month)][1] = close
        if sector_2_set[(ticker, date.month)][2] < date: 
            sector_2_set[(ticker, date.month)][2] = date
            sector_2_set[(ticker, date.month)][3] = close

for elem in sector_2_set:
    # print(str(key))
    percentuale = (sector_2_set[elem][3]-sector_2_set[elem][1])/sector_2_set[elem][1] * 100

    print("%s\t%i\t%s" % (sector_2_set[elem][4], elem[1], percentuale))
