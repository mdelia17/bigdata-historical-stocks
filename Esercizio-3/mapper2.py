#!/usr/bin/env python3
"""mapper.py"""

import sys

for line in sys.stdin:
    line = line.strip()
    #print(line)
    # ad ogni reducer devono arrivare tutte le righe di uno stesso settore
    ticker, date, close, name = line.split("\t")
    print('%s\t%s\t%s\t%s' % (ticker, date, close, name))

    
