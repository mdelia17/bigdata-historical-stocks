#!/usr/bin/env python3
"""reducer.py"""

import sys
from datetime import datetime

TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"
sector_2_set ={}

for line in sys.stdin:
    line = line.strip()
    sector, ticker, date, close, volume = line.split("\t")
    try:
        date = datetime.strptime(date, TIMESTAMP_FORMAT)
        close = float(close)
        volume = int(volume)
    except ValueError as e:
        print(e)
        continue
    if (sector, date.year) not in sector_2_set:
        sector_2_set[(sector, date.year)] = [date, date, close, close]
        sector_2_set[(sector, date.year)].append({ticker: [date, date, close, close, volume]})
    else: 
        if sector_2_set[(sector, date.year)][0] == date: 
            sector_2_set[(sector, date.year)][2] += close
        if sector_2_set[(sector, date.year)][1] == date: 
            sector_2_set[(sector, date.year)][3] += close
        if sector_2_set[(sector, date.year)][0] > date: 
            sector_2_set[(sector, date.year)][0] = date
            sector_2_set[(sector, date.year)][2] = close
        if sector_2_set[(sector, date.year)][1] < date: 
            sector_2_set[(sector, date.year)][1] = date
            sector_2_set[(sector, date.year)][3] = close

        if ticker not in sector_2_set[(sector, date.year)][4]:
            sector_2_set[(sector, date.year)][4][ticker] = [date, date, close, close, volume] 
        else:
            if sector_2_set[(sector, date.year)][4][ticker][0] > date:
                sector_2_set[(sector, date.year)][4][ticker][0] = date
                sector_2_set[(sector, date.year)][4][ticker][2] = close
            if sector_2_set[(sector, date.year)][4][ticker][1] < date:
                sector_2_set[(sector, date.year)][4][ticker][1] = date
                sector_2_set[(sector, date.year)][4][ticker][3] = close
            sector_2_set[(sector, date.year)][4][ticker][4] += volume    
        
sorted_sector_2_set = sorted(sector_2_set.items(), key=lambda x: x[0][0], reverse=False)
for elem in sorted_sector_2_set:
    percentuale = (elem[1][3]-elem[1][2])/elem[1][2] * 100
    ticker_percent_max = ""
    percentuale_max = -1000000000000
    ticker_volume_max = ""
    volume_max = 0.0
    ticker_2_set = elem[1][4]
    
    for ticker in ticker_2_set:
        percentuale2 = (ticker_2_set[ticker][3] - ticker_2_set[ticker][2])/ticker_2_set[ticker][2] * 100
        if percentuale2 > percentuale_max :
            ticker_percent_max = ticker
            percentuale_max = percentuale2
        if ticker_2_set[ticker][4] > volume_max :
            ticker_volume_max = ticker
            volume_max = ticker_2_set[ticker][4]

    print("%s\t%s\t%f\t%s\t%f\t%s\t%i" % (elem[0][0], elem[0][1], percentuale, ticker_percent_max, percentuale_max, ticker_volume_max, volume_max))