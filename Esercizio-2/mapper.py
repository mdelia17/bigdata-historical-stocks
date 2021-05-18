#!/usr/bin/env python3
"""mapper.py"""

import sys
import csv
from datetime import datetime

TIMESTAMP_FORMAT = "%Y-%m-%d"

reader = csv.reader(sys.stdin)

for line in reader:
    if len(line)==8:
        if datetime.strptime(line[7], TIMESTAMP_FORMAT).year <=2018 and  datetime.strptime(line[7], TIMESTAMP_FORMAT).year >=2009:
            print('%s\t%s\t%s\t%s' % (line[0], line[2], line[6], line[7]))
    else: 
        print('%s\t%s' % (line[0], line[3]))


    
