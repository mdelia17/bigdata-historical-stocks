#!/usr/bin/env python3
"""reducer.py"""

import sys

sector_2_set ={}
keyList = [1,2,3,4,5,6,7,8,9,10,11,12]

for line in sys.stdin:
    # print(line)
    line = line.strip()
    name, month, percentuale = line.split("\t")
    try:
        month = int(month)
        percentuale = float(percentuale)
    except ValueError:
        continue
    if name not in sector_2_set:
        sector_2_set[name] = {key: [] for key in keyList}
        sector_2_set[name][month].append(percentuale)
    else:  
        sector_2_set[name][month].append(percentuale)
        
for elem in sector_2_set:
    month_2_percent = sector_2_set[elem]
    tutti_mesi = ""
    for elem2 in month_2_percent:
        if len(month_2_percent[elem2]) > 0:
            average_perc = sum(month_2_percent[elem2])/len(month_2_percent[elem2]) 
            tutti_mesi = tutti_mesi+str(average_perc)+"\t"
        else:
            tutti_mesi = tutti_mesi + "-\t"
            
    print("%s\t%s" % (elem, tutti_mesi))