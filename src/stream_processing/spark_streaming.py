
import os
import sys
import redis
import json
import datetime

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql.functions import udf


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


def store_dup_cand_redis(tag, rdd):
    rdb = redis.StrictRedis(config.REDIS_SERVER, port=6379, db=0)
    for cand in rdd:
        cand_reformatted = (tag, cand.q1_id, cand.q1_title, cand.q2_id, cand.q2_title)
        rdb.zadd("dup_cand", cand.mh_js, cand_reformatted)


def process_question(question, mh, lsh):
    rdb = redis.StrictRedis(config.REDIS_SERVER, port=6379, db=0)

    q_mh = mh.calc_min_hash_signature(question)
    q_lsh = lsh.find_lsh_buckets(q_mh)

    tags = question["tags"].split("|")
    for tag in tags:
        # Fetch all questions from that tag
        tq_table_size = rdb.zcard("lsh:{0}".format(tag))

        if(tq_table_size >= config.DUP_QUESTION_MIN_TAG_SIZE):
            tq_table = rdb.zrangebyscore("lsh:{0}".format(tag), config.QUESTION_POPULARITY_THRESHOLD, "+inf", withscores=False)
            tq = tq_table
            print(tag, tq_table_size)
            # tq_df = ssc.read.json(ssc.parallelize(tq))
            # Perform comparison of LSH
            # find_lsh_sim = udf(lambda tq_lsh: lsh.command_bands_count(tq_lsh, q_lsh))
            # lsh_sim_tq_df = tq_df.withColumn("lsh_sim", find_lsh_sim(tq_df.q1_min_hash))
            # lsh_cand_tq_df = lsh_sim_tq_df.filter(lsh_sim_tq_df.lsh_sim >= config.LSH_SIMILARITY_BAND_COUNT)

            # If comparison above certain threshold, compare MinHash and upoad to Redis
            # find_mh_js = udf(lambda x, y: mh.jaccard_sim_score(x, y))
            # mh_cand_df = lsh_cand_tq_df.withColumn("mh_js", find_mh_js(lsh_cand_tq_df.q1_min_hash, lsh_cand_tq_df.q2_min_hash))
            # cand_df = mh_cand_df.filter(mh_cand_df.mh_js >= config.DUP_QUESTION_MIN_HASH_THRESHOLD)
            # print(tag, tq_table_size)
        #     cand_df.foreachPartition(lambda rdd: store_dup_cand_redis(tag, rdd))
        #     cand_reformatted = (tag, cand.q1_id, cand.q1_title, cand.q2_id, cand.q2_title)
        #     rdb.zadd("dup_cand", mh_js, cand_reformatted)


# Compute MinHash
def process_mini_batch(rdd, mh, lsh):
    for question in rdd:
        if len(question) > 0:
            process_question(question, mh, lsh)


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

    kafka_stream = KafkaUtils.createDirectStream(
        ssc,
        [config.KAFKA_TOPIC],
        {"metadata.broker.list": config.KAFKA_SERVERS}
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
        .foreachRDD(lambda rdd: rdd.foreachPartition(lambda question: process_mini_batch(question, mh, lsh)))

    ssc.start()
    ssc.awaitTermination()


if __name__ == "__main__":
    main()
