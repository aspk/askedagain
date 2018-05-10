import sys
import os
import time
import json
import pickle
from termcolor import colored

from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import udf,col
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
    bucket = util.get_bucket(bucket_name)
    fields = []
    if(config.LOG_DEBUG): print(colored("[BATCH]: Reading S3 files to master dataframe...", "green"))
    # specify schema to speed up read from s3 when there's time - currently infers schema
    # schema = StructType(fields)
    df = sql_context.read.json("s3a://{0}/*.json*".format(bucket_name))
    if(config.LOG_DEBUG): print(colored("[BATCH]: Created master dataframe for S3 files...", "green"))

    return df

# Store question data
def store_lsh_redis(rdd):
    rdb = redis.StrictRedis(config.REDIS_SERVER, port=6379, db=0)
    for q in rdd:
        tags = q.tags.split("|")
        for tag in tags:
            q_json = json.dumps({"title":q.title,"min_hash":q.min_hash,"lsh_hash":q.lsh_hash})
            rdb.hset("lsh:{0}".format(tag), q.id, q_json)
            print("{0} --- {1}".format("lsh:{0}".format(tag), q.title))


def pull_top_lsh_sim_redis(tag):
    rdb = redis.StrictRedis(config.REDIS_SERVER, port=6379, db=0)
    # Pull tags from redis
    # Check if in tags
    # If not in tags, but is everything, pull everything


def store_dup_cands_redis(rdd):
    rdb = redis.StrictRedis(config.REDIS_SERVER, port=6379, db=0)
    # for dup_cand in rdd:
    #     tags = dup_cand.tags
    #     for tag in tags:
    #         rdb.set("dup_cand:{0}:{1}+{2}".format(tag,dup_cand[0],dup_cand[1]))


def run_minhash_lsh():
    df = read_all_from_bucket()
    # Consider broadcasting these variables
    #  Create and save MinHash and LSH if not exist or load them from file
    if(os.path.isfile(config.MIN_HASH_PICKLE) == False and os.path.isfile(config.LSH_PICKLE) == False):
        mh = min_hash.MinHash(config.MIN_HASH_K_VALUE)
        lsh = locality_sensitive_hash.LSH(config.LSH_NUM_BANDS, config.LSH_BAND_WIDTH, config.LSH_NUM_BUCKETS)
        util.save_pickle_file(mh, config.MIN_HASH_PICKLE)
        util.save_pickle_file(lsh, config.LSH_PICKLE)
    else:
        mh = util.load_pickle_file(config.MIN_HASH_PICKLE)
        lsh = util.load_pickle_file(config.LSH_PICKLE)

    calc_min_hash = udf(lambda x: list(map(lambda x:int(x), mh.calc_min_hash_signature(x))), ArrayType(IntegerType()))
    calc_lsh_hash = udf(lambda x: list(map(lambda x:int(x), lsh.find_lsh_buckets(x))),ArrayType(IntegerType()))

    # Create min hashes for all titles in df
    if (config.LOG_DEBUG): print(colored("[BATCH]: Calculating MinHash hashes...", "green"))
    df = df.withColumn("min_hash", calc_min_hash("text_body_stemmed"))

    # Create locality sensitive hashes with document ID and store in redis
    if (config.LOG_DEBUG): print(colored("[BATCH]: Calculating LSH hashes...", "green"))
    df = df.withColumn("lsh_hash",calc_lsh_hash("min_hash"))

    # Update and store hashes to redis with lsh namespace
    if (config.LOG_DEBUG): print(colored("[BATCH]: Uploading LSH hashes to Redis...", "cyan"))
    df.foreachPartition(store_lsh_redis)

    # Check pairs similarity by only comparing with those in same namespace from redis
    if (config.LOG_DEBUG): print(colored("[BATCH]: Fetching questions in same namespace from Redis, calculating LSH hash similarities, uploading similarities back to Redis...", "cyan"))
    # use redis.hget <lsh_hash:java> to get everything associated with key quickly

    # df.crossJoin(df.select("lsh_hash"))
    find_lsh_sim = udf(lambda x, y: lsh.common_buckets_count(x,y), IntegerType())
    df.alias("i").join(df.alias("j"), col("i.ID") < col("j.ID")) \
        .select(
        col("i.ID").alias("i"),
        col("j.ID").alias("j"),
        find_lsh_sim("i.norm", "j.norm").alias("lsh_sim")) \
        .sort("i", "j") \
        .show()

    # Query redis for similarity above threshold
    if (config.LOG_DEBUG): print(colored("[BATCH]: Fetching candidate duplicate pairs in same namespace from Redis, comparing MinHash hashes, uploading similarities back to Redis...", "cyan"))
    # use redis hscan to get all similarities, hget <lsh_sim:java> to get candidate duplicate pairs


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
    print(colored("Spark Custom MinHashLSH run time (seconds): {0} seconds".format(end_time - start_time),"magenta"))


if(__name__ == "__main__"):
    main()