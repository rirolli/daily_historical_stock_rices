#!/usr/bin/env python3

"""spark application"""
from pyspark.sql import SparkSession
from pyspark import StorageLevel
from operator import add

def data_min(x, y):
	if x[0] > y[0]:
		return y
	else:
		return x


def data_max(x, y):
	if x[0] < y[0]:
		return y
	else:
		return x

def max(x,y):
	if x[1]>y[1]:
		return x
	else: 
		return y

input_filepath_prices  = "file:///Users/seb/Desktop/BigData/daily-historical-stock-prices-1970-2018/historical_stock_prices.csv"
input_filepath_sector = "file:///Users/seb/Desktop/BigData/daily-historical-stock-prices-1970-2018/historical_stocks.csv"

output_filepath = "file:///Users/seb/Desktop/BigData/daily-historical-stock-prices-1970-2018/output" 

# initialize SparkSession with the proper configuration
spark = SparkSession.builder.appName("job2").getOrCreate()

# read the input file and obtain an RDD with a record for each line
# ticker,open,close,adj_close,lowThe,highThe,volume,date
input_RDD_prices = spark.sparkContext.textFile(input_filepath_prices).map(lambda linea: linea.split(","))
input_RDD_prices = input_RDD_prices.filter(lambda linea: linea[0] != "ticker") \
	.filter(lambda linea: linea[7][0:4] >= "2009" and linea[7][0:4] <= "2018")


# ticker,exchange,name,INC.,sector,industry
input_RDD_sector = spark.sparkContext.textFile(input_filepath_sector).map(lambda linea: linea.split(","))
input_RDD_sector = input_RDD_sector.filter(lambda linea: linea[0] != "ticker") \
	.filter(lambda linea: linea[4] != "N/A")

# ticker,((close,volume,date),(name,sector))
input_RDD= input_RDD_prices.map(lambda linea:(linea[0],(linea[2],linea[6],linea[7])))\
	.join(input_RDD_sector.map(lambda linea:(linea[0],(linea[2],linea[4]))))

# ((sector, anno)(min_data nell'anno,somma close per settore nella data min)
sector_close_min=input_RDD.map(lambda linea:((linea[1][1][1],linea[1][0][2]),(float(linea[1][0][0])))).reduceByKey(lambda a, b: a + b)\
		.map(lambda linea:((linea[0][0],linea[0][1][0:4]),(linea[0][1],linea[1]))).reduceByKey(lambda x, y: data_min(x, y))				
# ((sector, anno)(max_data nell'anno,somma close per settore nella data max)
sector_close_max=input_RDD.map(lambda linea:((linea[1][1][1],linea[1][0][2]),(float(linea[1][0][0])))).reduceByKey(lambda a, b: a + b)\
		.map(lambda linea:((linea[0][0],linea[0][1][0:4]),(linea[0][1],linea[1]))).reduceByKey(lambda x, y: data_max(x, y))			
#((sector,anno)variazione_settore_nell'anno)
variazione_sector=sector_close_min.join(sector_close_max)\
	.map(lambda linea:((linea[0][0],linea[0][1]),(((linea[1][1][1] - linea[1][0][1])/linea[1][0][1])*100)))

# ((sector, anno)(ticker,min_data nell'anno,close nella data min)
sector_azione_min=input_RDD.map(lambda linea:((linea[1][1][1],linea[1][0][2][0:4]),(linea[1][0][2],linea[0],float(linea[1][0][0])))).reduceByKey(lambda x, y: data_min(x, y))
# ((sector, anno)(ticker,max_data nell'anno,close nella data max)
sector_azione_max=input_RDD.map(lambda linea:((linea[1][1][1],linea[1][0][2][0:4]),(linea[1][0][2],linea[0],float(linea[1][0][0])))).reduceByKey(lambda x, y: data_max(x, y))
#((sector,anno)variazione_settore_nell'anno)
variazione_azione=sector_azione_min.join(sector_azione_max)\
	.map(lambda linea:((linea[0][0],linea[0][1]),(linea[1][0][1],(((linea[1][1][2] - linea[1][0][2])/linea[1][0][2])*100)))).reduceByKey(lambda x,y: max(x,y))

# ((sector, anno),(ticker,somma_volume))
azione_max_volume = input_RDD.map(lambda linea: ((linea[1][1][1],linea[1][0][2][0:4],linea[0]),(float(linea[1][0][1])))).reduceByKey(lambda a, b: a + b) \
 .map(lambda line : ((line[0][0],line[0][1]),(line[0][2],line[1]))).reduceByKey(lambda x, y: max(x, y))
	 

output=variazione_sector.join(variazione_azione).join(azione_max_volume)
output.saveAsTextFile(output_filepath)
#time=