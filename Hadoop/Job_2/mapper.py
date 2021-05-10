#!/usr/bin/env python3
"""mapper.py"""

import sys

# read lines from STDIN
for line in sys.stdin:
	line = line.strip()

	# gestire le linee con elementi mancanti
	line = line.replace(', ', ' ')
	line = line.split(',')

	ticker = line[0]
	open_a = line[1]
	close_a = line[2]
	adj_close = line[3]
	low = line[4]
	high = line[5]
	volume = line[6]
	date = line[7]
	exchange = line[8]
	name = line[9]
	sector = line[10]
	industry = line[11]

	print(f"{ticker}\t{close_a}\t{volume}\t{date}\t{sector}")