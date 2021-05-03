#!/usr/bin/env bash

cat data.csv | python3 mapper.py | sort -k 1,1 | python3 reducer.py > output_file
