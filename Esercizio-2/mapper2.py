#!/usr/bin/env python3
"""mapper.py"""

import sys

for line in sys.stdin:
    line = line.strip()
    ticker, date, close, volume, sector = line.split("\t")
    print('%s\t%s\t%s\t%s\t%s' % (sector, ticker, date, close, volume))

    
