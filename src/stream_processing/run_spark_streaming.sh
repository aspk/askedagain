#!/bin/bash

/usr/local/spark/bin/spark-submit --master spark://ec2-54-190-60-245.us-west-2.compute.amazonaws.com:7077 --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2 --py-files spark_stream.py