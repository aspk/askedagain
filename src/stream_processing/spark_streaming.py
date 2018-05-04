import os
import pickle

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json
import datetime

from cassandra.cluster import Cluster
from cassandra.query import BatchStatement
from cassandra import ConsistencyLevel

import redis

import numpy as np

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/config")
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/lib")
import debug as config

os.environ["PYSPARK_SUBMIT_ARGS"] = "--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.2.0 pyspark-shell"


# def send_cassandra(data):
#     cluster = Cluster(config.CASSANDRA_SERVER)
#     session = cluster.connect()

#     insert_stmt = session.prepare("INSERT INTO questions(question, q_body, q_timestamp) VALUES (?,?,?)");
#     count = 0
#     batch = BatchStatement()

#     for record in iter:
#         batch.add(insert_stmt, record[0], record[1], record[2])
#         count += 1

#         if(count % 500 == 0):
#             session.execute(batch)
#             batch = BatchStatement()

#     session.execute(batch)
#     session.shutdown()



''' DATA FUNCTIONS '''
def extract_data(json_whole):
    return {
        'question': json_whole['title'],
        'question_post': json_whole['body'],
        'time': datetime.datetime.now().strftime("%I:%M%p on %B %d, %Y")
    }


def process_question(record):
    # Preprocess 

    # Compute MinHash

    # Compute LSH Signature

    # Fetch relevant bucket from Redis and compare within bucket


def process_mini_batch(iter):
    r = redis.StrictRedis(host=config.REDIS_SERVER, port=6379, db=0)

    for record in iter:
        print("Sending partition - pushing record to redis")
        existing_list = [] if r.get("mvp") is None or "None" else pickle.loads(r.get("mvp"))
        existing_list.append(record)
        r.set("mvp", pickle.dumps(existing_list))

    for record in iter:
        if len(record) > 0:
            process_question(record)



def main():

	spark_conf = SparkConf().setAppName("Spark Streaming MinHash")

	global sc
    sc = SparkContext(conf=spark_conf)

    global ssc
    ssc = StreamingContext(sc, config.SPARK_STREAMING_MINI_BATCH_WINDOW)
    ssc.checkpoint("_spark_streaming_checkpoint")

    kafka_stream = KafkaUtils.createDirectStream(
        ssc, 
        [config.KAFKA_TOPIC], 
        {"metadata.broker.list": config.KAFKA_SERVER}
    )

    kafka_stream.map(lambda kafka_response: json.loads(kafka_response[1]))\
        .map(lambda json_body: extract_data(json_body))\
        .foreachRDD(lambda rdd: rdd.foreachPartition(process_mini_batch))

    ssc.start()
    ssc.awaitTermination()


if __name__ == "__main__":
    main()
