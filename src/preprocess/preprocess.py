import sys
import os
import boto3
import botocore

from pyspark.ml.feature import StopWordsRemover
from pyspark.ml.feature import Tokenizer
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import SQLContext

import time
from termcolor import colored
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/config")
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/lib")
import debug as config
import util

# For preprocessing a single question for streaming data
# def preprocess(data_point):

# Write preprocessed data back to file
def write_aws_s3(bucket_name, file, df):
    df.write.save("s3n://{0}/{1}".format(bucket_name, file), format="json", mode="overwrite")

# Preprocess a data file and upload it
def preprocess_file(bucket_name, file, sql_context):
    data = sql_context.read.json("s3n://{0}/{1}".format(bucket_name,file))

    # Tokenize question title
    tokenizer = Tokenizer(inputCol="title", outputCol="tokenized_title")
    tokenized_data = tokenizer.transform(data)

    # Remove stop words
    stop_words_remover = StopWordsRemover(inputCol="tokenized_title", outputCol="cleaned_title")
    cleaned_data = stop_words_remover.transform(tokenized_data)

    # Do Word2Vec synonym matching, etc. when there's time

    # Write to AWS
    cleaned_data.registerTempTable("cleaned_data")
    preprocessed_data = sql_context.sql("SELECT title, tokenized_title, cleaned_title, post_type_id, tags, score, comment_count, view_count from cleaned_data")
    write_aws_s3(config.S3_BUCKET_BATCH_PREPROCESSED, file, preprocessed_data)


def preprocess_all(sql_context):
    client = boto3.client('s3')
    bucket = util.get_bucket(config.S3_BUCKET_BATCH_RAW)
    for csv_obj in bucket.objects.all():
        preprocess_file(config.S3_BUCKET_BATCH_RAW, csv_obj.key, sql_context)
        print(colored("Finished preprocessing file s3://{0}/{1}".format(config.S3_BUCKET_BATCH_RAW,csv_obj.key),"green"))


def main():
    # Initialize Spark
    spark_conf = SparkConf().setAppName("Text Preprocesser").set("spark.cores.max", "30")

    global sc
    sc = SparkContext(conf=spark_conf)

    global sql_context
    sql_context = SQLContext(sc)

    start_time = time.time()
    preprocess_all(sql_context)
    end_time = time.time()
    print(colored("Preprocessing run time (seconds): {0}".format(end_time - start_time),"magenta"))


if(__name__ == "__main__"):
    main()
