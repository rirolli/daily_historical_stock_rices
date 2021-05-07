#!/usr/bin/env python3
"""mapper_join.py"""

import sys

_id = -1

# read lines from STDIN
for line in sys.stdin:
	line = line.strip()

	line = line.split(",")

	ticker = "-1"

	open_a = "-1"
	close_a = "-1"
	adj_close = "-1"
	low = "-1"
	high = "-1"
	volume = "-1"
	date = "-1"

	exchange = "-1"
	name = "-1"
	sector = "-1"
	industry = "-1"

	# se la lunghezza di line è 8 allora i dati vengono dal file historical_stock_prices.csv
	if len(line) == 8:
		_id += 1	# identificatore di riga
		ticker = line[0]
		if ticker=="ticker":
			continue
		open_a = line[1]
		close_a = line[2]
		adj_close = line[3]
		low = line[4]
		high = line[5]
		volume = line[6]
		date = line[7]
		print(f"{_id}\t{ticker}\t{open_a}\t{close_a}\t{adj_close}\t{low}\t{high}\t{volume}\t{date}\t{exchange}\t{name}\t{sector}\t{industry}")
	else:
		ticker = line[0]
		if ticker=="ticker":
			continue
		exchange = line[1]
		name = line[2]
		sector = line[3]
		industry = line[4]
		print(f"{'-1'}\t{ticker}\t{open_a}\t{close_a}\t{adj_close}\t{low}\t{high}\t{volume}\t{date}\t{exchange}\t{name}\t{sector}\t{industry}")
