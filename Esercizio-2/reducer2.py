#!/usr/bin/env python3
"""reducer.py"""

import sys
from datetime import datetime

TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"
sector_2_set ={}
for line in sys.stdin:
    # print(line)
    line = line.strip()
    # al posto di ticker ci dovrebbe essere sector
    ticker, date, close, volume, sector = line.split("\t")
    try:
        date = datetime.strptime(date, TIMESTAMP_FORMAT)
        close = float(close)
        volume = int(volume)
    except ValueError:
        continue
    if (sector, date.year) not in sector_2_set:
        sector_2_set[(sector, date.year)] = list()
        sector_2_set[(sector, date.year)].append(date)
        sector_2_set[(sector, date.year)].append(date)
        sector_2_set[(sector, date.year)].append([close])
        sector_2_set[(sector, date.year)].append([close])
        sector_2_set[(sector, date.year)].append({ticker: [date, date, close, close, volume]})
    else: 
        if sector_2_set[(sector, date.year)][0] == date: 
            sector_2_set[(sector, date.year)][2].append(close)
        if sector_2_set[(sector, date.year)][1] == date: 
            sector_2_set[(sector, date.year)][3].append(close)
        if sector_2_set[(sector, date.year)][0] > date: 
            sector_2_set[(sector, date.year)][0] = date
            sector_2_set[(sector, date.year)][2] = [close]
        if sector_2_set[(sector, date.year)][1] < date: 
            sector_2_set[(sector, date.year)][1] = date
            sector_2_set[(sector, date.year)][3] = [close]
        # per il dict di ticker
        if ticker not in sector_2_set[(sector, date.year)][4]:
            sector_2_set[(sector, date.year)][4] = {ticker: list()} # non avevamo creato il dizionario con poi dentro la lista
            sector_2_set[(sector, date.year)][4][ticker].append(date)
            sector_2_set[(sector, date.year)][4][ticker].append(date)
            sector_2_set[(sector, date.year)][4][ticker].append(close)
            sector_2_set[(sector, date.year)][4][ticker].append(close)
            sector_2_set[(sector, date.year)][4][ticker].append(volume)
        else:
            if sector_2_set[(sector, date.year)][4][ticker][0] > date:
                sector_2_set[(sector, date.year)][4][ticker][0] = date
                sector_2_set[(sector, date.year)][4][ticker][2] = close
            if sector_2_set[(sector, date.year)][4][ticker][1] < date:
                sector_2_set[(sector, date.year)][4][ticker][1] = date
                sector_2_set[(sector, date.year)][4][ticker][3] = close
            sector_2_set[(sector, date.year)][4][ticker][4] += volume    
        
            
# keys = list (prices_2_set.keys())
for key in sector_2_set:
    # print(str(key))
    percentuale = (sum(sector_2_set[key][3])-sum(sector_2_set[key][2]))/sum(sector_2_set[key][2]) * 100
    # ticker_percent_max = ""
    # percentuale_max = 0.0
    ticker_volume_max = ""
    volume_max = 0.0

    # Troviamo l'azione con maggiore percentuale e volume per un settore in un certo anno
    for key_int in sector_2_set[key][4]:
        # print(str(key_int), sector_2_set[key][4][key_int][3], sector_2_set[key][4][key_int][2])
        # il problema è che l'azione compare solo una volta in un certo settore in un anno e quindi la differenza percentuale è 0
        # percentuale2 = (sector_2_set[key][4][key_int][3] - sector_2_set[key][4][key_int][2])/sector_2_set[key][4][key_int][2] * 100
        # if percentuale2 > percentuale_max :
        #     ticker_percent_max = key_int
        #     percentuale_max = percentuale2
        if sector_2_set[key][4][key_int][4] > volume_max :
            ticker_volume_max = key_int
            volume_max = sector_2_set[key][4][key_int][4]
    # Quando usciamo da questo ciclo interno abbiamo, per il settore in un certo anno l'azione con percentuale e volume maggiore e la stampiamo
    print("%s\t%s\t%f\t%s\t%i" % (key[0], key[1], percentuale, ticker_volume_max, volume_max ))
    # da aggiungere \t%s\t%f ticker_percent_max, percentuale_max
