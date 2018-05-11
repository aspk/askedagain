#!/bin/bash
$SPARK_HOME/bin/spark-submit --master spark://ip-10-0-0-5:7077 --conf spark.yarn.executor.memoryOverhead=600 --executor-memory 6G --driver-memory 6G preprocess.py