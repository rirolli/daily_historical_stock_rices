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

input_file = ['file:////home/riccardo/Scrivania/Progetto/02_Spark/job_3/historical_stock_prices.csv', 'file:////home/riccardo/Scrivania/Progetto/02_Spark/job_3/historical_stocks.csv']
output_file = 'file:////home/riccardo/Scrivania/Progetto/02_Spark/job_3/output'

if 'historical_stock_prices' in input_file[0]:
    hsp_file = input_file[0]
    hs_file = input_file[1]
else:
    hsp_file = input_file[1]
    hs_file = input_file[0]

THRESHOLD=1

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

def similar_percentage_change(a, b, threshold=THRESHOLD):
    return abs(a-b) <= threshold

# import del testo
# ticker, open, close, adj_close, low, high, volume, date
hsp_RDD = spark.sparkContext.textFile(hsp_file) \
    .map(lambda line: line.split(',')) \
        .filter(lambda line: len(line)>1 and line[0]!='ticker' and get_year(line[7]) == "2017") \
            .map(lambda line: (line[0], (line[2],line[7])))

# ticker, exchange, name, sector, industry
hs_RDD = spark.sparkContext.textFile(hs_file) \
    .map(lambda line: line.split(',')) \
        .filter(lambda line: len(line)>1 and line[0]!='ticker') \
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
# => (('A', '"AGILENT TECHNOLOGIES', 2), 4.141293108074551)
percentage_month_RDD = fisrt_month_date_RDD.join(last_month_date_RDD) \
    .map(lambda line: (line[0], percentage_change(line[1][0][1], line[1][1][1]))) \
        .sortByKey()

# persist percentage_month_RDD
percentage_month_RDD.persist(StorageLevel.MEMORY_AND_DISK)

################################################################################################ fin qui tutto ok
# percentage_month_RDD = percentage_month_RDD.collect()   # diventa una lista!

# simil_map = {}
# for i in range(len(percentage_month_RDD)):
#     for j in range(i, len(percentage_month_RDD)):
#         i_line = percentage_month_RDD[i]
#         j_line = percentage_month_RDD[j]
#         couple = (i_line[0][0],j_line[0][0])
#         i_perc_change = i_line[1]
#         j_perc_change = j_line[1]
#         simil = similar_percentage_change(i_perc_change, j_perc_change)

#         if i_line==j_line:
#             continue
#         elif i_line[0][2]==j_line[0][2]:
#             month = i_line[0][2]
#             if couple not in simil_map and simil is True:
#                 simil_map[couple] = [(month, (i_perc_change, j_perc_change))]
#             elif couple in simil_map and simil is True:
#                 simil_map[couple].append((month, (i_perc_change, j_perc_change)))
#             else:
#                 continue

# simil_map = {}
# for i_line in percentage_month_RDD:
#     for j_line in percentage_month_RDD:
#         couple = (i_line[0][0],j_line[0][0])
#         i_perc_change = i_line[1]
#         j_perc_change = j_line[1]
#         simil = similar_percentage_change(i_perc_change, j_perc_change)

#         if i_line==j_line:
#             continue
#         elif i_line[0][2]==j_line[0][2]:
#             month = i_line[0][2]
#             if couple not in simil_map and simil is True:
#                 simil_map[couple] = [(month, (i_perc_change, j_perc_change))]
#             elif couple in simil_map and simil is True:
#                 simil_map[couple].append((month, (i_perc_change, j_perc_change)))
#             else:
#                 continue

# similar_RDD = spark.sparkContext.parallelize(simil_map.items()) \
#     .filter(lambda line: len(line[1])==12) \
#         .map(lambda line: (line[0], sorted(line[1], key=lambda tup: tup[0]))) \
#             .take(25)
# (('"AGILENT TECHNOLOGIES', 'AMERISOURCEBERGEN CORPORATION (HOLDING CO)'), [(1, (5.33447935620313, 5.6530663774866445)), (2, (4.141293108074551, 3.272770010047949))])


# # (('A', '"AGILENT TECHNOLOGIES', 2), 4.141293108074551)
# # => (2, (4.141293108074551, '"AGILENT TECHNOLOGIES'))
similar_RDD = percentage_month_RDD.map(lambda line: ((line[0][2]), (line[1], line[0][1], line[0][0])))

# # ((1, (5.33447935620313, '"AGILENT TECHNOLOGIES', 'A')), (1, (5.33447935620313, '"AGILENT TECHNOLOGIES', 'A')))
# # => (('A', '"AGILENT TECHNOLOGIES', 'A', 'ALCOA CORPORATION', 1), 5.33447935620313, 26.430804229616694)
similar_RDD = similar_RDD.cartesian(similar_RDD) \
    .filter(lambda line: line[0][0] == line[1][0] and line[0][1][2] != line[1][1][2]) \
        .map(lambda line: ((line[0][1][2], line[0][1][1], line[1][1][2], line[1][1][1], line[0][0]), line[0][1][0], line[1][1][0])) \
            .filter(lambda line: similar_percentage_change(line[1], line[2]))

similar_RDD.persist(StorageLevel.MEMORY_AND_DISK)

# # => (('A', '"AGILENT TECHNOLOGIES', 'A', 'ALCOA CORPORATION', 1), 5.33447935620313, 26.430804229616694)
# # (('DTE ENERGY COMPANY', '"CAVCO INDUSTRIES'), [(4, (2.882148898519137, 2.3265806783631113)), (7, (1.4402090485896346, 2.2745050168502003)), (4, (1.842754961235011, 2.3265806783631113)), (7, (1.4566962393317195, 2.2745050168502003)), (8, (0.07350202005133324, 0.2608093230069242)), (12, (-0.7404541511591695, -0.39163894039025243)), (12, (0.07974664204323223, -0.39163894039025243)), (1, (0.2646991704747534, -0.6572310574826654)), (4, (1.8907166034033258, 2.3265806783631113)), (1, (0.0, -0.6572310574826654)), (11, (-1.5797149473704621, -2.3589493548314553)), (12, (-0.03734215319414561, -0.39163894039025243))])
grouped_similar_RDD = similar_RDD.map(lambda line: ((line[0][0], line[0][1], line[0][2], line[0][3]),[(line[0][4], (line[1], line[2]))])) \
    .reduceByKey(lambda x, y: x+y) \
        .filter(lambda line: len(line[1])==12) \
            .map(lambda line: ((line[0][1], line[0][3]), sorted(line[1], key=lambda tup: tup[0]))) \
                .sortByKey() \
                    .collect()

# # in caso togliere:
# # .map(lambda line: (line[0], sorted(line[1], key=lambda tup: tup[0]))) \
# #         .sortByKey() \

spark.sparkContext.parallelize(grouped_similar_RDD) \
    .coalesce(1) \
        .saveAsTextFile(output_file)