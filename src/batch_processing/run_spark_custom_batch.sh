#!/bin/bash
# $SPARK_HOME/bin/spark-submit --master spark://ip-10-0-0-9:7077 --conf "spark.yarn.appMasterEnv.PYTHONHASHSEED=0" spark_custom_batch.py
$SPARK_HOME/bin/spark-submit --conf "spark.yarn.appMasterEnv.PYTHONHASHSEED=0" --packages com.amazonaws:aws-java-sdk-pom:1.10.34,org.apache.hadoop:hadoop-aws:2.7.3 spark_custom_batch.py
