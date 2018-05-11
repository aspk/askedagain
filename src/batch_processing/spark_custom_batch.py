import sys
import os
import time
import json
from termcolor import colored

from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import udf, col
import redis

from pyspark.sql.types import IntegerType
from pyspark.sql.types import ArrayType

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/config")
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/lib")
import config
import util
import min_hash
import locality_sensitive_hash


# Reads all JSON files from S3 bucket and returns as a dataframe
def read_all_from_bucket():
    bucket_name = config.S3_BUCKET_BATCH_PREPROCESSED
    if(config.LOG_DEBUG): print(colored("[BATCH]: Reading S3 files to master dataframe...", "green"))

    df = sql_context.read.json("s3a://{0}/*.json*".format(bucket_name))
    if(config.LOG_DEBUG): print(colored("[BATCH]: Created master dataframe for S3 files...", "green"))
    return df


# Store question data
def store_lsh_redis(rdd):
    rdb = redis.StrictRedis(config.REDIS_SERVER, port=6379, db=0)
    for q in rdd:
        tags = q.tags.split("|")
        for tag in tags:
            q_json = json.dumps({"id": q.id, "title": q.title, "min_hash": q.min_hash, "lsh_hash": q.lsh_hash})
            rdb.hset("lsh:{0}".format(tag), q.id, q_json)
            rdb.sadd("lsh_keys", "lsh:{0}".format(tag))


# Computes MinHashes, LSHes for all in DataFrame
def compute_minhash_lsh(df, mh, lsh):
    calc_min_hash = udf(lambda x: list(map(lambda x: int(x), mh.calc_min_hash_signature(x))), ArrayType(IntegerType()))
    calc_lsh_hash = udf(lambda x: list(map(lambda x: int(x), lsh.find_lsh_buckets(x))), ArrayType(IntegerType()))

    df = df.withColumn("min_hash", calc_min_hash("text_body_stemmed"))
    df = df.withColumn("lsh_hash", calc_lsh_hash("min_hash"))

    df.foreachPartition(store_lsh_redis)


# Store LSH similarity data
def store_lsh_sim_redis(tag, rdd):
    rdb = redis.StrictRedis(config.REDIS_SERVER, port=6379, db=0)
    for sim in rdd:
        q_pair = (tag, sim.q1_id, sim.q1_title, sim.q1_min_hash, sim.q2_id, sim.q2_title, sim.q2_min_hash)
        rdb.zadd("lsh_sim:{0}".format(tag), sim.lsh_sim, q_pair)
        rdb.sadd("lsh_sim_keys", "lsh_sim:{0}".format(tag))


# Compares LSH signatures within tags and uploads to redis
def compare_lsh_within_tags(lsh):
    rdb = redis.StrictRedis(config.REDIS_SERVER, port=6379, db=0)

    # Fetch all tags from lsh_keys set
    tags = []
    for lsh_key in rdb.sscan_iter("lsh_keys", match="*", count=500):
        tags.append(lsh_key.replace("lsh:", ""))

    for tag in tags:
        tq_table = rdb.hgetall("lsh:{0}".format(tag))
        tq = tq_table.values()
        if config.LOG_DEBUG: print(colored("{0}: {1} question(s)".format(tag, len(tq)), "yellow"))
        tq_df = sql_context.read.json(sc.parallelize(tq))

        find_lsh_sim = udf(lambda x, y: lsh.common_bands_count(x, y), IntegerType())
        lsh_sim_df = tq_df.alias("q1").join(tq_df.alias("q2"), col("q1.id") < col("q2.id")).select(
            col("q1.id").alias("q1_id"),
            col("q2.id").alias("q2_id"),
            col("q1.min_hash").alias("q1_min_hash"),
            col("q2.min_hash").alias("q2_min_hash"),
            col("q1.title").alias("q1_title"),
            col("q2.title").alias("q2_title"),
            find_lsh_sim("q1.lsh_hash", "q2.lsh_hash").alias("lsh_sim")
        ).sort("q1_id", "q2_id")
        lsh_sim_df.foreachPartition(lambda rdd: store_lsh_sim_redis(tag, rdd))


# Compares MinHash signature of questions with > 2 LSH element overlaps, uploads to redis
def compare_mh_for_cands(mh):
    rdb = redis.StrictRedis(config.REDIS_SERVER, port=6379, db=0)

    # Fetch all tags from lsh_sim_keys set
    tags = []
    for lsh_sim_key in rdb.sscan_iter("lsh_sim_keys", match="*", count=500):
        tags.append(lsh_sim_key.replace("lsh_sim:", ""))

    for tag in tags:
        tq_candidates = rdb.zrangebyscore("lsh_sim:{0}".format(tag), config.LSH_SIMILARITY_BAND_COUNT, "+inf")
        for tq_cand_string in tq_candidates:
            tq_cand = eval(tq_cand_string)
            tag, q1_id, q1_title, q1_min_hash, q2_id, q2_title, q2_min_hash = tq_cand

            tq_jss = mh.jaccard_sim_score(q1_min_hash, q2_min_hash)

            tq_reformatted_cand = (tag, q1_id, q1_title, q2_id, q2_title)
            rdb.zadd("dup_cand:{0}".format(tag), tq_jss, tq_reformatted_cand)  # Add to tag specific table
            rdb.zadd("dup_cand", tq_jss, tq_reformatted_cand)  # Add to general duplicate table


def run_minhash_lsh():
    df = read_all_from_bucket()
    #  Create and save MinHash and LSH if not exist or load them from file
    if(not os.path.isfile(config.MIN_HASH_PICKLE) or not os.path.isfile(config.LSH_PICKLE)):
        mh = min_hash.MinHash(config.MIN_HASH_K_VALUE)
        lsh = locality_sensitive_hash.LSH(config.LSH_NUM_BANDS, config.LSH_BAND_WIDTH, config.LSH_NUM_BUCKETS)
        util.save_pickle_file(mh, config.MIN_HASH_PICKLE)
        util.save_pickle_file(lsh, config.LSH_PICKLE)
    else:
        mh = util.load_pickle_file(config.MIN_HASH_PICKLE)
        lsh = util.load_pickle_file(config.LSH_PICKLE)

    # Compute MinHash/LSH hashes for every question
    if (config.LOG_DEBUG): print(colored("[BATCH]: Calculating MinHash hashes and LSH hashes...", "green"))
    compute_minhash_lsh(df, mh, lsh)

    # Compute pairwise LSH similarities for questions within tags
    if (config.LOG_DEBUG): print(colored("[BATCH]: Fetching questions in same namespace from Redis, calculating LSH hash similarities, uploading similarities back to Redis...", "cyan"))
    compare_lsh_within_tags(lsh)

    # Compare MinHashes for two question if LSH has overlap > 2
    if (config.LOG_DEBUG): print(colored("[BATCH]: Comparing MinHashes for candidates, uploading MinHash similarities for candidates back to Redis...", "cyan"))
    compare_mh_for_cands(mh)


def main():
    spark_conf = SparkConf().setAppName("Spark Custom MinHashLSH").set("spark.cores.max", "30")

    global sc
    sc = SparkContext(conf=spark_conf)
    sc.setLogLevel("ERROR")
    sc.addFile(os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/lib/min_hash.py")
    sc.addFile(os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/lib/locality_sensitive_hash.py")
    sc.addFile(os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/lib/util.py")
    sc.addFile(os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/config/config.py")

    global sql_context
    sql_context = SQLContext(sc)

    start_time = time.time()
    run_minhash_lsh()
    end_time = time.time()
    print(colored("Spark Custom MinHashLSH run time (seconds): {0} seconds".format(end_time - start_time), "magenta"))


if(__name__ == "__main__"):
    main()
