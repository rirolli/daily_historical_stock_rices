#!/usr/bin/env python3
"""reducer_join.py"""

import sys

hist_dict = {}
hist_price_dict = {}

for line in sys.stdin:

    line = line.strip()

    _id, ticker, open_a, close_a, adj_close, low, high, volume, date, exchange, name, sector, industry  = line.split("\t")

    # generazione dei dizionari
    if _id=="-1":  # la riga viene dal file historical_stocks.csv
        hist_dict[ticker] = [exchange, name, sector, industry]
    else:
        hist_price_dict[_id] = [ticker, open_a, close_a, adj_close, low, high, volume, date]


for e in hist_price_dict.keys():
    # elementi da hist_price_dict
    ticker = hist_price_dict[e][0]
    open_a = hist_price_dict[e][1]
    close_a = hist_price_dict[e][2]
    adj_close = hist_price_dict[e][3]
    low = hist_price_dict[e][4]
    high = hist_price_dict[e][5]
    volume = hist_price_dict[e][6]
    date = hist_price_dict[e][7]
    # elementi da hist_dict
    exchange = hist_dict[hist_price_dict[_id][0]][0]
    name = hist_dict[hist_price_dict[_id][0]][1]
    sector = hist_dict[hist_price_dict[_id][0]][2]
    industry = hist_dict[hist_price_dict[_id][0]][3]
    print(f"{ticker},{open_a},{close_a},{adj_close},{low},{high},{volume},{date},{exchange},{name},{sector},{industry}")
