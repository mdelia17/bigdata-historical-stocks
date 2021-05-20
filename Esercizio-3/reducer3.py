#!/usr/bin/env python3
"""reducer.py"""

import sys

name_2_set ={}
KEYLIST = [1,2,3,4,5,6,7,8,9,10,11,12]

for line in sys.stdin:
    line = line.strip()
    name, month, percentuale = line.split("\t")
    try:
        month = int(month)
        percentuale = float(percentuale)
    except ValueError:
        continue
    if name not in name_2_set:
        name_2_set[name] = {key: [] for key in KEYLIST}
        name_2_set[name][month].append(percentuale)
    else:  
        name_2_set[name][month].append(percentuale)
        
for elem_name in name_2_set:
    month_2_percent = name_2_set[elem_name]
    tutti_mesi = ""
    for elem_month in month_2_percent:
        if len(month_2_percent[elem_month]) > 0:
            average_perc = float("{:.5f}".format(float(sum(month_2_percent[elem_month])/len(month_2_percent[elem_month]) )))
            tutti_mesi = tutti_mesi + str(average_perc) + "\t"
        else:
            tutti_mesi = tutti_mesi + "-\t"
            
    print("%s\t%s" % (elem_name, tutti_mesi))