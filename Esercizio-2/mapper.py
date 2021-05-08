#!/usr/bin/env python3
"""mapper.py"""

import sys
import csv

reader = csv.reader(sys.stdin)

for line in reader:
    if len(line)==8:
        print('%s\t%s\t%s\t%s' % (line[0], line[2], line[6], line[7]))
    else: 
        print('%s\t%s' % (line[0], line[3]))


    
