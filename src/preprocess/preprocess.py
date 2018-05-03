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
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/config")
import debug as config


def get_bucket(bucket_name):
        s3 = boto3.resource('s3')
        try:
            s3.meta.client.head_bucket(Bucket=bucket_name)
        except botocore.exceptions.ClientError as e:
            return None
        else:
            return s3.Bucket(bucket_name)

# For preprocessing a single question for streaming data
# def preprocess(data_point):

# Write resulting preprocessed to data frame
def write_aws_s3(bucket_name, file, df):
    df.write.save("s3://{0}/{1}".format(bucket_name, file), format="json")

def write_local(path, file, df):
    df.write.save("{0}/{1}".format(path,file), format="json")

# Preprocess a data file and upload it
def preprocess_file(bucket_name, file, sql_context):
    data = sql_context.read.json("s3://{0}/{1}".format(bucket_name,file))

    # Tokenize question title
    tokenizer = Tokenizer(inputCol="title", outputCol="tokenized_title")
    tokenized_data = tokenizer.transform(data)

    # Remove stop words
    stop_words_remover = StopWordsRemover(inputCol="tokenized_title", outputCol="cleaned_title")
    cleaned_data = stop_words_remover.transform(tokenized_data)

    # Remove punctuation
    cleaned_data.registerTempTable("cleaned_data")
    preprocessed_data = sql_context.sql("SELECT title, tokenized_title, cleaned_title, post_type_id, tags, score, comment_count, view_count from cleaned_data")
    write_aws_s3(config.S3_BUCKET_PREPROCESSED, file, preprocessed_data)
    # extract relevant features that we want here


def preprocess_all(sql_context):
    client = boto3.client('s3')
    bucket = get_bucket(config.S3_BUCKET_RAW)
    for csv_obj in bucket.objects.all():
        preprocess_file(config.S3_BUCKET_RAW, csv_obj.key, sql_context)
        print("--- Finished preprocessing file s3://{0}/{1}".format(config.S3_BUCKET_RAW,csv_obj.key))


def main():
    # Initialize Spark
    spark_conf = SparkConf().setAppName("Text Preprocesser").set("spark.cores.max", "30")
    spark_context = SparkContext(conf=spark_conf)
    sql_context = SQLContext(spark_context)

    start_time = time.time()
    preprocess_all(sql_context)
    end_time = time.time()
    print("Preprocessing run time(s): {0}".format(end_time - start_time))


if(__name__ == "__name__"):
    main()
