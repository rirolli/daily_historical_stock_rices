#!/usr/bin/env python3
"""mapper.py"""

import sys
import csv

from datetime import datetime

# dizionari
ticker_sector_map = {}

with open('historical_stocks.csv') as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=',')

    firstLine = True    # True se ci troviamo nella prima riga

    for row in csv_reader:
        if not firstLine:
            ticker, _, _, sector, _ = row  # ticker, exchange, name, sector, industry
            ticker_sector_map[ticker] = sector
        else:
            firstLine = False

# read lines from STDIN
for line in sys.stdin:
    line = line.strip()

    # gestire le linee con elementi mancanti
    line = line.replace(', ', ' ')

    # ticker, open, close, adj_close, low, high, volume, date
    ticker, _, close_a, _, _, _, volume, date = line.split(',')

    try:
        date = datetime.strptime(date, '%Y-%m-%d')
    except Exception:
        continue

    if date.year >= 2009 and date.year <= 2018:  # scrematura in base all'intervallo annuale
        # mappatura secondo il ticker
        print(
            f"{ticker}\t{close_a}\t{volume}\t{date.date()}\t{ticker_sector_map[ticker]}")
