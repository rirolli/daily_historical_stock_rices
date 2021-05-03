#!/usr/bin/env python3
"""mapper.py"""

import sys

# read lines from STDIN
for line in sys.stdin:
	line = line.strip()

	ticker, open_a, close_a, _, low, high, _, date = line.split(',')

	print(f"{ticker}\t{open_a}\t{close_a}\t{low}\t{high}\t{date}")