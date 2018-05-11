import sys
import os
import redis
import time
from termcolor import colored

from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import SQLContext

from pyspark.ml.feature import MinHashLSH
from pyspark.sql.functions import col
from pyspark.ml.feature import CountVectorizer

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/config")
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/lib")
import debug as config

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.amazonaws:aws-java-sdk:1.10.34,org.apache.hadoop:hadoop-aws:2.6.0 pyspark-shell'


# Reads all JSON files from S3 bucket and returns as a dataframe
def read_all_from_bucket():
    bucket_name = config.S3_BUCKET_BATCH_PREPROCESSED
    if(config.LOG_DEBUG): print(colored("[BATCH]: Reading S3 files to master dataframe...", "green"))

    df = sql_context.read.json("s3a://{0}/*.json*".format(bucket_name))
    if(config.LOG_DEBUG): print(colored("[BATCH]: Created master dataframe for S3 files...", "green"))
    return df

# Store LSH similarity data
def store_spark_mllib_sim_redis(rdd):
    rdb = redis.StrictRedis(config.REDIS_SERVER, port=6379, db=0)
    for sim in rdd:
        q_pair = (sim.q1_id, sim.q1_title, sim.q2_id, sim.q2_title)
        rdb.zadd("spark_mllib_sim", sim.jaccard_sim, q_pair)


def run_minhash_lsh():
    df = read_all_from_bucket()
    mh = MinHashLSH(inputCol="text_body_vectorized", outputCol="min_hash", numHashTables=config.LSH_NUM_BANDS)

    # Vectorize so we can fit to MinHashLSH model
    vectorizer = CountVectorizer(inputCol="text_body_stemmed", outputCol="text_body_vectorized")
    vectorizer_transformer = vectorizer.fit(df)
    vdf = vectorizer_transformer.transform(df)

    if(config.LOG_DEBUG): print(colored("[MLLIB BATCH]: Fitting MinHashLSH model...", "green"))
    model = mh.fit(vdf)
    model.transform(vdf).show()

    # Approximate similarity join between pairwise elements
    if(config.LOG_DEBUG): print(colored("[MLLIB BATCH]: Computing approximate similarity join...", "green"))
    sim_join = model.approxSimilarityJoin(vdf, vdf, 0.6, distCol="jaccard_sim").select(
        col("datasetA.id").alias("q1_id"),
        col("datasetB.id").alias("q2_id"),
        col("datasetA.title").alias("q1_title"),
        col("datasetB.title").alias("q2_title"),
        col("datasetA.text_body_vectorized").alias("q1_text_body"),
        col("datasetB.text_body_vectorized").alias("q2_text_body"),
        col("jaccard_sim")
    )

    # Upload LSH similarities to Redis
    sim_join.foreachPartition(store_spark_mllib_sim_redis)


def main():
    spark_conf = SparkConf().setAppName("Spark MLLib MinHashLSH").set("spark.cores.max", "30")

    global sc
    sc = SparkContext(conf=spark_conf)

    global sql_context
    sql_context = SQLContext(sc)

    start_time = time.time()
    run_minhash_lsh()
    end_time = time.time()
    print(colored("Spark MLLib MinHashLSH run time (seconds): {0}".format(end_time - start_time), "magenta"))


if(__name__ == "__main__"):
    main()
