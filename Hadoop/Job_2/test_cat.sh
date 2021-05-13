#!/usr/bin/env bash

cat historical_stock_prices.csv | python3 mapper.py | sort -k 1,1 | python3 reducer.py > output_file
