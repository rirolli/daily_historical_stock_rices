#!/usr/bin/env bash

cat historical_stock_prices.csv historical_stocks.csv | python3 mapper_join.py | sort -k 1,1 | python3 reducer_join.py > output_file
