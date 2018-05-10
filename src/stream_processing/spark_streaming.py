
import os
import sys
import pickle
os.environ["PYSPARK_SUBMIT_ARGS"] = "--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.2.0 pyspark-shell"
from pyspark import SparkContext
from pyspark.conf import SparkConf
from pyspark.sql import SQLContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json
import datetime

import redis

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/config")
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/lib")
import debug as config
from min_hash import MinHash
from lsh import LSH
import util




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
    r = redis.StrictRedis(host=config.REDIS_SERVER, port=6379, db=config.REDIS_QUESTIONS_DB)
    # Preprocess
    # Create and save MinHash and LSH or load them from file
    # if (os.path.isfile(config.MIN_HASH_PICKLE) == False and os.path.isfile(config.LSH_PICKLE) == False):
    #     mh = MinHash(config.MIN_HASH_K_VALUE)
    #     lsh = LSH(config.LSH_NUM_BANDS, config.LSH_BAND_WIDTH, config.LSH_NUM_BUCKETS)
    #
    #     util.save_pickle_file(mh, config.MIN_HASH_PICKLE)
    #     util.save_pickle_file(lsh, config.LSH_PICKLE)
    # else:
    #     mh = util.load_pickle_file(config.MIN_HASH_PICKLE)
    #     mlsh = util.load_pickle_file(config.LSH_PICKLE)
    # Compute MinHash

    mh = MinHash(config.MIN_HASH_K_VALUE)
    lsh = LSH(config.LSH_NUM_BANDS, config.LSH_BAND_WIDTH, config.LSH_NUM_BUCKETS)

    tokens = record["question"].split(" ")
    tokens_mh = mh.calc_min_hash_signature(tokens)
    # Compute LSH Signature
    tokens_lsh = lsh.find_lsh_buckets(tokens_mh)
    # Upload to redis the incoming hashes

    r.set(record["question"],pickle.dumps(tokens_lsh))



def process_mini_batch(iter):
    # for record in iter:
    #     print("Sending partition - pushing record to redis")
    #     existing_list = [] if r.get("mvp") is None or "None" else pickle.loads(r.get("mvp"))
    #     existing_list.append(record)
    #     r.set("mvp", pickle.dumps(existing_list))

    for record in iter:
        if len(record) > 0:
            print("Record: {0}".format(record))
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
