#!/usr/bin/env python3
"""mapper.py"""

import sys

# read lines from STDIN
for line in sys.stdin:
	line = line.strip()

	# gestire le linee con elementi mancanti
	ticker, open_a, close_a, adj_close, low, high, volume, date, exchange, name, sector, industry = line.split(',')

	print(f"{ticker}\t{close_a}\t{volume}\t{date}\t{sector}")