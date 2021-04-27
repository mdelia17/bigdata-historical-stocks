#!/usr/bin/env python3
"""mapper.py"""

import sys
import csv

reader = csv.reader(sys.stdin)
# read lines from STDIN (standard input)
for line in reader:

    for word in line:
        # write in standard output the mappings word -> 1
        # in the form of tab-separated pairs
        print('%s\t%i' % (word, 1))
