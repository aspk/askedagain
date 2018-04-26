#!/bin/bash

# create topic
/usr/local/kafka/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 3 --partitions 2 --topic soq

# see if topic exists
/usr/local/kafka/bin/kafka-topics.sh --describe --zookeeper localhost:2181 --topic soq