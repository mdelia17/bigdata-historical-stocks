#!/usr/bin/env python3
"""mapper.py"""

import sys

for line in sys.stdin:
    line = line.strip()
    #print(line)
    # ad ogni reducer devono arrivare tutte le righe di uno stesso settore
    name, month, percentuale = line.split("\t")
    print('%s\t%s\t%s' % (name, month, percentuale))

    
