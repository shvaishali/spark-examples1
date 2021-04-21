#!/bin/bash
source ../../env.sh
PROJECT_NAME="nba_analysis"
INPUT_PATH="/${PROJECT_NAME}/input/"
OUTPUT_PATH="/${PROJECT_NAME}/output/"
TEST_DATA="data_fraction.csv"
SRC="./analyse_pz.py"
/usr/local/hadoop/bin/hdfs dfs -rm -r $INPUT_PATH
/usr/local/hadoop/bin/hdfs dfs -rm -r $OUTPUT_PATH
/usr/local/hadoop/bin/hdfs dfs -mkdir -p $INPUT_PATH
/usr/local/hadoop/bin/hdfs dfs -copyFromLocal $TEST_DATA $INPUT_PATH
/usr/local/hadoop/bin/hdfs dfs -ls $INPUT_PATH
/usr/local/spark/bin/spark-submit --master=spark://$SPARK_MASTER:7077 $SRC --data_file hdfs://$SPARK_MASTER:9000$INPUT_PATH
