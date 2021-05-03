#!/usr/bin/env bash

DATA_INPUT=historical_stock_prices.csv

$HADOOP_HOME/bin/hdfs dfs -rm -r /user/riccardo/input
$HADOOP_HOME/bin/hdfs dfs -mkdir /user/riccardo/input
$HADOOP_HOME/bin/hdfs dfs -rm -r /user/riccardo/output
$HADOOP_HOME/bin/hdfs dfs -put $DATA_INPUT input
$HADOOP_HOME/bin/hadoop jar $HADOOP_HOME/streaming/hadoop-streaming-3.2.2.jar -mapper mapper.py -reducer reducer.py -input /user/riccardo/input/$DATA_INPUT -output /user/riccardo/output/data_output
