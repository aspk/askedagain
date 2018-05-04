#!/bin/bash
# $SPARK_HOME/bin/spark-submit --master spark://ip-10-0-0-9:7077 --executor-memory 6G --driver-memory 6G preprocess.py

$SPARK_HOME/bin/spark-submit --master spark://ip-10-0-0-9:7077 preprocess.py