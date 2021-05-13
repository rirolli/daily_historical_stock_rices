#!/usr/bin/env bash

# Comando di esecuzione delle operazione necessarie per lo svolgimento del Job2

USER_FOLDER=riccardo
DATA_INPUT=historical_stock_prices.csv
DATA_INPUT_1=historical_stocks.csv

$HADOOP_HOME/bin/hdfs dfs -rm -r /user/$USER_FOLDER/input
$HADOOP_HOME/bin/hdfs dfs -mkdir /user/$USER_FOLDER/input
$HADOOP_HOME/bin/hdfs dfs -rm -r /user/$USER_FOLDER/output
# load dei dataset
$HADOOP_HOME/bin/hdfs dfs -put $DATA_INPUT input
$HADOOP_HOME/bin/hdfs dfs -put $DATA_INPUT_1 input
# mapreduce per calcolare i risultati voluti
$HADOOP_HOME/bin/hadoop jar $HADOOP_HOME/streaming/hadoop-streaming-3.2.2.jar -mapper mapper.py -reducer reducer.py -input /user/$USER_FOLDER/input/$DATA_INPUT -output /user/$USER_FOLDER/output/data_output
