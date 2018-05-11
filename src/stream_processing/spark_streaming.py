
import os
import sys
import redis
import json
import datetime

from pyspark import SparkContext
from pyspark.conf import SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils


os.environ["PYSPARK_SUBMIT_ARGS"] = "--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.2.0 pyspark-shell"
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/config")
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/lib")
import config
import util
from min_hash import MinHash
from locality_sensitive_hash import LSH

# Converts incoming question, Adds timestamp to incoming question
def extract_data(data):
    data["time"] = datetime.datetime.now().strftime("%I:%M%p on %B %d, %Y")
    return data


def process_question(question):
    rdb = redis.StrictRedis(host=config.REDIS_SERVER, port=6379, db=0)
    tags = question.tags.split("|")
    for tag in tags:
        # Fetch all questions from that tag
        tq_table = rdb.hgetall("lsh:{0}".format(tag))
        tq = tq_table.values()
        tq_df = sql_context.read.json(sc.parallelize(tq))
        # Perform comparison and upload to Redis
        # If comparison above certain threshold, compare MinHash and upoad to Redis
        print("Almost there!")


# Compute MinHash
def process_mini_batch(rdd, mh, lsh):

    for question in rdd:
        if len(question) > 0:
            print("Record: {0}".format(question))
            process_question(question)


def main():

    spark_conf = SparkConf().setAppName("Spark Streaming MinHash")

    global sc
    sc = SparkContext(conf=spark_conf)
    sc.addFile(os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/lib/min_hash.py")
    sc.addFile(os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/lib/locality_sensitive_hash.py")
    sc.addFile(os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/lib/util.py")
    sc.addFile(os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/config/config.py")

    global ssc
    ssc = StreamingContext(sc, config.SPARK_STREAMING_MINI_BATCH_WINDOW)
    ssc.checkpoint("_spark_streaming_checkpoint")
    ssc.addFile(os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/lib/min_hash.py")
    ssc.addFile(os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/lib/locality_sensitive_hash.py")
    ssc.addFile(os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/lib/util.py")
    ssc.addFile(os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/config/config.py")

    kafka_stream = KafkaUtils.createDirectStream(
        ssc,
        [config.KAFKA_TOPIC],
        {"metadata.broker.list": config.KAFKA_SERVER}
    )

    # Create and save MinHash and LSH or load them from file
    if (not os.path.isfile(config.MIN_HASH_PICKLE) or not os.path.isfile(config.LSH_PICKLE)):
        mh = MinHash(config.MIN_HASH_K_VALUE)
        lsh = LSH(config.LSH_NUM_BANDS, config.LSH_BAND_WIDTH, config.LSH_NUM_BUCKETS)

        util.save_pickle_file(mh, config.MIN_HASH_PICKLE)
        util.save_pickle_file(lsh, config.LSH_PICKLE)
    else:
        mh = util.load_pickle_file(config.MIN_HASH_PICKLE)
        lsh = util.load_pickle_file(config.LSH_PICKLE)

    # Process stream
    kafka_stream.map(lambda kafka_response: json.loads(kafka_response[1]))\
        .map(lambda json_body: extract_data(json_body))\
        .foreachRDD(lambda rdd: rdd.foreachPartition(lambda y: process_mini_batch(y, mh, lsh)))

    ssc.start()
    ssc.awaitTermination()


if __name__ == "__main__":
    main()
