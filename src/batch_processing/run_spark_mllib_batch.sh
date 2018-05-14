#!/bin/bash
# $SPARK_HOME/bin/spark-submit --master spark://ip-10-0-0-5:7077 spark_mllib_batch.py

$SPARK_HOME/bin/spark-submit --packages com.amazonaws:aws-java-sdk-pom:1.10.34,org.apache.hadoop:hadoop-aws:2.7.3 spark_mllib_batch.py