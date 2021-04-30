#!/usr/bin/env python3
"""mapper.py"""

import sys
import csv

reader = csv.reader(sys.stdin)

for line in reader:
    print('%s\t%s\t%s\t%s\t%s\t%s' % (line[0], line[1], line[2], line[4], line[5], line[7]))
