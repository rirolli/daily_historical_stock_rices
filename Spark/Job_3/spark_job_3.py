#!/usr/bin/env python3
"""spark application"""

# Un job in grado di generare le coppie di aziende che si somigliano
# (sulla base di una soglia scelta a piacere) in termini di variazione
# percentuale mensile nell’anno 2017 mostrando l’andamento mensile delle due aziende.
# 
# Esempio:
# Soglia=1%, coppie:
# 1:{Apple, Intel}: GEN: Apple +2%, Intel +2,5%, FEB: Apple +3%, Intel +2,7%, MAR: Apple +0,5%, Intel +1,2%, ...;
# 2:{Amazon, IBM}: GEN: Amazon +1%, IBM +0,5%, FEB: Amazon +0,7%, IBM +0,5%, MAR: Amazon +1,4%, IBM +0,7%, ..

import argparse
from pyspark import StorageLevel
from pyspark.sql import SparkSession

# parser
parser = argparse.ArgumentParser()
parser.add_argument("--input_path", type=str, nargs='+', help="Input file path")
parser.add_argument("--output_path", type=str, help="Output folder path")

args = parser.parse_args()
input_file = args.input_path
output_file = args.output_path

if 'historical_stock_prices' in input_file[0]:
    hsp_file = input_file[0]
    hs_file = input_file[1]
else:
    hsp_file = input_file[1]
    hs_file = input_file[0]

# spark session
spark = SparkSession \
    .builder \
        .appName("Primo esempio") \
            .getOrCreate()

# funzioni di supporto
def get_max(x, y):
    if x < y:
        return y
    return x

def get_min(x, y):
    if x > y:
        return y
    return x

def percentage_change(i,f):
    r = 100*((f-i)/i)
    return r

def get_year(d):
    return d[:4]

def get_month(d):
    return d[5:7]

def get_day(d):
    return d[8:]

# import del testo
# ticker, open, close, adj_close, low, high, volume, date
hsp_RDD = spark.sparkContext.textFile(hsp_file) \
    .map(lambda line: line.split(',')) \
        .filter(lambda line: len(line)>1) \
            .filter(lambda line: line[0]!='ticker') \
                .filter(lambda line: get_year(line[7]) == "2017") \
                    .map(lambda line: (line[0], (line[2],line[7])))

# ticker, exchange, name, sector, industry
hs_RDD = spark.sparkContext.textFile(hs_file) \
    .map(lambda line: line.split(',')) \
        .filter(lambda line: len(line)>1) \
            .filter(lambda line: line[0]!='ticker') \
                .map(lambda line: (line[0], line[2]))

# join => ('DSKE', (('10.1999998092651', '2017-01-17'), '"DASEKE))
# map  => ('DSKE', '"DASEKE', '10.1999998092651', '2017-01-17')
input_RDD = hsp_RDD.join(hs_RDD) \
    .map(lambda line: (line[0], line[1][1], line[1][0][0], line[1][0][1])) \

# persist input_RDD
input_RDD.persist(StorageLevel.MEMORY_AND_DISK)

# calcolo della variazione percentuale mensile
# ((TICKER, NOME, MESE), (GIORNO, CLOSE))
fisrt_month_date_RDD = input_RDD.map(lambda line: ((line[0],line[1],int(get_month(line[3]))),(int(get_day(line[3])),float(line[2])))) \
    .reduceByKey(lambda x, y: (x[0], x[1]) if x[0]<y[0] else (y[0], y[1])) \

last_month_date_RDD = input_RDD.map(lambda line: ((line[0],line[1],int(get_month(line[3]))),(int(get_day(line[3])),float(line[2])))) \
    .reduceByKey(lambda x, y: (x[0], x[1]) if x[0]>y[0] else (y[0], y[1])) \

# (('DSKE', '"DASEKE', 12), ((1, 12.8400001525879), (29, 14.289999961853)))
percentage_month_RDD = fisrt_month_date_RDD.join(last_month_date_RDD) \
    .map(lambda line: (line[0], percentage_change(line[1][0][1], line[1][1][1]))) \
        .sortByKey() \
    .collect()

spark.sparkContext.parallelize(percentage_month_RDD) \
    .coalesce(1) \
        .saveAsTextFile(output_file)