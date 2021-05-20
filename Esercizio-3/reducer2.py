#!/usr/bin/env python3
"""reducer.py"""

import sys
from datetime import datetime

TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"

def perc(a, b):
    return float("{:.5f}".format(float((b-a)/a*100)))

ticker_month_2_set ={}

for line in sys.stdin:
    line = line.strip()
    ticker, date, close, name = line.split("\t")
    try:
        date = datetime.strptime(date, TIMESTAMP_FORMAT)
        close = float("{:.5f}".format(float(close)))
    except ValueError:
        continue
    if (ticker, date.month) not in ticker_month_2_set:
        ticker_month_2_set[(ticker, date.month)] = [date, close, date, close, name]
    else: 
        if ticker_month_2_set[(ticker, date.month)][0] == date: 
            ticker_month_2_set[(ticker, date.month)][1] += close
        if ticker_month_2_set[(ticker, date.month)][2] == date: 
            ticker_month_2_set[(ticker, date.month)][3] += close
        if ticker_month_2_set[(ticker, date.month)][0] > date: 
            ticker_month_2_set[(ticker, date.month)][0] = date
            ticker_month_2_set[(ticker, date.month)][1] = close
        if ticker_month_2_set[(ticker, date.month)][2] < date: 
            ticker_month_2_set[(ticker, date.month)][2] = date
            ticker_month_2_set[(ticker, date.month)][3] = close

for elem in ticker_month_2_set:
    print("%s\t%i\t%f" % (ticker_month_2_set[elem][4], elem[1], perc(ticker_month_2_set[elem][1],ticker_month_2_set[elem][3])))
