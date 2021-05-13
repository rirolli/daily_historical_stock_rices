#!/usr/bin/env python3
"""mapper.py"""

import sys
import csv
from datetime import datetime

# In questo mapper è importante aggiungere ad ogni
# record il nome dell'azienda per ogni ticker.

# dizionari
ticker_name_map = {}

with open('historical_stocks.csv') as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=',')

    firstLine = True    # True se ci troviamo nella prima riga

    for row in csv_reader:
        if not firstLine:
            ticker, _, name, _, _ = row
            if name != 'N/A':
                ticker_name_map[ticker] = name
            else:
                ticker_name_map[ticker] = ticker
        else:
            firstLine = False


for line in sys.stdin:
    line = line.strip()

    # gestire le linee con elementi mancanti
    line = line.replace(', ', ' ')
    ticker, _, close_a, _, _, _, _, date = line.split(',')

    try:
        date = datetime.strptime(date, '%Y-%m-%d')
    except Exception:
        continue

    if date.year == 2017:   # scrematura in base all'anno
        # mappatura secondo il ticker
        print(f"{ticker}\t{ticker_name_map[ticker]}\t{close_a}\t{date.date()}")
