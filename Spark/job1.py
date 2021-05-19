#!/usr/bin/env python3

"""spark application"""
from pyspark.sql import SparkSession
from pyspark import StorageLevel

def data_min(x, y):
	if x[1] > y[1]:
		return y
	else:
		return x


def data_max(x, y):
	if x[1] < y[1]:
		return y
	else:
		return x

input_filepath  = "file:///Users/seb/Desktop/BigData/daily-historical-stock-prices-1970-2018/historical_stock_prices.csv"
output_filepath = "file:///Users/seb/Desktop/BigData/daily-historical-stock-prices-1970-2018/output" 

# initialize SparkSession with the proper configuration
spark = SparkSession.builder.appName("job1").getOrCreate()

# read the input file and obtain an RDD with a record for each line
# ticker,open,close,adj_close,lowThe,highThe,volume,date
input_RDD = spark.sparkContext.textFile(input_filepath).map(lambda linea: linea.split(","))

input_RDD = input_RDD.filter(lambda linea: linea[0] != "ticker")

# input_RDD = input.filter(lambda linea: linea[7][0:4] >= "2009" and linea[7][0:4] <= "2018")

# ticker azione e data della prima quotazione
# first_date = input_RDD.map(lambda linea : (linea[0], linea[7])).reduceByKey(lambda x, y: min(x, y))

# ticker azione relativa data dell'ultima quotazione
# last_date = input_RDD.map(lambda linea : (linea[0],linea[7])).reduceByKey(lambda x, y: max(x, y))

# ticker azione, prezzo di chiusura e relativa data della prima quotazione
#(ticker,(close,first_date))
first_date = input_RDD.map(lambda linea : (linea[0],(linea[2],linea[7]))).reduceByKey(lambda x, y: data_min(x, y))

# ticker azione,prezzo di chiusura e relativa data dell'ultima quotazione
#(ticker,(close,last_date))
last_date = input_RDD.map(lambda linea : (linea[0],(linea[2],linea[7]))).reduceByKey(lambda x, y: data_max(x,y))

# ticker azione e prezzo minimo 
min_price = input_RDD.map(lambda linea : (linea[0],float(linea[4]))).reduceByKey(lambda x, y: min(x, y))
# ticker azione e prezzo massimo
max_price = input_RDD.map(lambda linea : (linea[0],float(linea[5]))).reduceByKey(lambda x, y: max(x, y))

# (ticker, (min_price,max_price))
min_max_price=min_price.join(max_price)


# calcolo variazione percentuale della quotazione 
# ticker,(prezzo di chiusura e relativa data della prima quotazione),(prezzo di chiusura e relativa data dell'ultima quotazione)
join_variazione_percentuale = first_date.join(last_date) 

#(ticker,((min_price,max_price),((first_data),(last_data))))
#join_output=min_max_price.join(join_variazione_percentuale).map(lambda linea:(linea[0],linea[1][0][0],linea[1][0][1],linea[1][1][0][1],linea[1][1][1][1]))

# (ticker ( variazione percentuale della quotazione))
variazione_percentuale = join_variazione_percentuale.map(lambda linea: (linea[0],((float(linea[1][1][0]) - float(linea[1][0][0]))/float(linea[1][0][0]))*100))

#output=min_max_price.join(join_variazione_percentuale).join(variazione_percentuale)
join_output=min_max_price.join(join_variazione_percentuale).map(lambda linea:(linea[0],linea[1][0][0],linea[1][0][1],linea[1][1][0][1],linea[1][1][1][1])).join(variazione_percentuale)
join_output.saveAsTextFile(output_filepath)


# this final action saves the RDD of strings as a new folder in the FS
# spark.sparkContext.parallelize(output_RDD).saveAsTextFile(output_filepath)

# output_RDD.saveAsTextFile(output_filepath)
