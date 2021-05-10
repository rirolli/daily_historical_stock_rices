#!/usr/bin/env python3
"""mapper_join.py"""

import sys

# read lines from STDIN
for line in sys.stdin:
	line = line.strip()

	line = line.replace(', ', ' ')
	line = line.split(",")

	ticker = "nan"

	open_a = "nan"
	close_a = "nan"
	adj_close = "nan"
	low = "nan"
	high = "nan"
	volume = "nan"
	date = "nan"

	exchange = "nan"
	name = "nan"
	sector = "nan"
	industry = "nan"

	# se la lunghezza di line è 8 allora i dati vengono dal file historical_stock_prices.csv
	if len(line) == 8:
		ticker = line[0]
		open_a = line[1]
		close_a = line[2]
		adj_close = line[3]
		low = line[4]
		high = line[5]
		volume = line[6]
		date = line[7]
		print(f"{ticker}\t{open_a}\t{close_a}\t{adj_close}\t{low}\t{high}\t{volume}\t{date}\t{exchange}\t{name}\t{sector}\t{industry}")
	else:
		ticker = line[0]
		exchange = line[1]
		name = line[2]
		sector = line[3]
		industry = line[4]
		print(f"{ticker}\t{open_a}\t{close_a}\t{adj_close}\t{low}\t{high}\t{volume}\t{date}\t{exchange}\t{name}\t{sector}\t{industry}")
