#!/usr/bin/env python3
"""reducer_join.py"""

import sys

hist_dict = {}
hist_price_dict = {}

for line in sys.stdin:

    line = line.strip()

    ticker, open_a, close_a, adj_close, low, high, volume, date, exchange, name, sector, industry  = line.split("\t")

    # generazione dei dizionari
    if open_a == 'nan':  # la riga viene dal file historical_stocks.csv
        hist_dict[ticker] = [exchange, name, sector, industry]
    else:
        if ticker not in hist_price_dict:
            hist_price_dict[ticker] = []
        curr_list = [open_a, close_a, adj_close, low, high, volume, date]
        hist_price_dict[ticker].append(curr_list)

for k, v in hist_price_dict.items():
    exchange = hist_dict[k][0]
    name = hist_dict[k][1]
    sector = hist_dict[k][2]
    industry = hist_dict[k][3]
    for elem in v:
        print(f"{k},{elem[0]},{elem[1]},{elem[2]},{elem[3]},{elem[4]},{elem[5]},{elem[6]},{exchange},{name},{sector},{industry}")