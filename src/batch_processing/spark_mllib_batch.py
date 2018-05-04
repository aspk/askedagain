import sys
import os
import boto3
import botocore

from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import SQLContext

from pyspark.ml.feature import MinHashLSH
from pyspark.ml.linalg import Vectors,VectorUDT
from pyspark.sql.functions import col
from pyspark.sql.types import *
from pyspark.sql.functions import udf
from pyspark.ml.feature import CountVectorizer

import time
from termcolor import colored

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/config")
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/lib")
import debug as config
import util

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.amazonaws:aws-java-sdk:1.10.34,org.apache.hadoop:hadoop-aws:2.6.0 pyspark-shell'


# Reads all JSON files from S3 bucket and returns as a dataframe
def read_all_from_bucket():
    bucket_name = config.S3_BUCKET_BATCH_PREPROCESSED
    bucket = util.get_bucket(bucket_name)
    fields = []
    if(config.LOG_DEBUG): print(colored("[BATCH]: Reading S3 files to master dataframe...", "green"))
    # specify schema to speed up read from s3 when there's time - currently infers schema
    # schema = StructType(fields)
    df = sql_context.read.json("s3n://{0}/*.json*".format(bucket_name))
    if(config.LOG_DEBUG): print(colored("[BATCH]: Created master dataframe for S3 files...", "green"))
    
    return df


def run_minhash_lsh():
    df = read_all_from_bucket()
    mh = MinHashLSH(inputCol="vectorized_cleaned_title", outputCol="hashes", numHashTables=config.LSH_NUM_BANDS)

    vectorizer = CountVectorizer(inputCol="cleaned_title",outputCol="vectorized_cleaned_title")
    vectorizer_transformer = vectorizer.fit(df)
    df = vectorizer_transformer.transform(df)

    if(config.LOG_DEBUG): print(colored("[MLLIB BATCH]: Fitting MinHashLSH model...","green"))
    model = mh.fit(df)
    model.transform(df).show()

    # approximate similarity join , threshold at 0.6
    model.approxSimilarityJoin(df, df, 0.6, "JaccardDistance")
        .select(col("df.vectorized_cleaned_title").alias("ct_a"),
        col("df.vectorized_cleaned_title").alias("ct_b"),
        col("JaccardDistance")).show()

    # find 2 nearest neighbors
    model.approxNearestNeighbors(df, key, 2).show()


def main():
    spark_conf = SparkConf().setAppName("Spark MLLib MinHashLSH").set("spark.cores.max", "30")
    
    global sc
    sc = SparkContext(conf=spark_conf)
    
    global sql_context
    sql_context = SQLContext(sc)

    start_time = time.time()
    run_minhash_lsh()
    end_time = time.time()
    print(colored("Spark MLLib MinHashLSH run time (seconds): {0}".format(end_time - start_time),"magenta"))

if(__name__ == "__main__"):
    main()