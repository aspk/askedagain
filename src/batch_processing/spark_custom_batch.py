import sys
import os
import boto3
import botocore

import time
from termcolor import colored

from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import SQLContext

import redis

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/config")
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/lib")
import debug as config
import util
from min_hash import MinHash
from lsh import LSH


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

def upload_lsh_hashes_to_redis(redis_db, lsh_hashes):
	# Add hashes to redis
	for lsh_hash in lsh_hashes:
		bucket = redis_db.get(lsh_hash[1])
		if bucket == "None" or bucket == None or len(bucket) = 0:
			bucket = []
		bucket.append(lsh_hash[0])

		redis_db.set(lsh_hash[1], bucket)


def run_minhash_lsh():

	df = read_all_from_bucket()

	# Consider Broadcasting these variables 
	
	# Create and save MinHash and LSH or load them from file
	if(os.path.isfile(config.MIN_HASH_PICKLE) == False and os.path.isfile(config.LSH_PICKLE) == False):
		mh = MinHash(config.MIN_HASH_K_VALUE)
		lsh = LSH(config.LSH_NUM_BANDS, config.LSH_BAND_WIDTH, config.LSH_NUM_BUCKETS)

		util.save_pickle_file(mh, config.MIN_HASH_PICKLE)
		util.save_pickle_file(lsh, config.LSH_PICKLE)
	else:
		mh = util.load_pickle_file(config.MIN_HASH_PICKLE)
		lsh = util.load_pickle_file(config.LSH_PICKLE)

	# Create min hashes for all titles in df

	# Create locality sensitive hashes with document ID and store in redis
	
	
def main():
	spark_conf = SparkConf().setAppName("Spark Custom MinHashLSH").set("spark.cores.max", "30")

	global sc
	sc = SparkContext(conf=spark_conf)

	global sql_context
	sql_context = SQLContext(sc)

	start_time = time.time()
	run_minhash_lsh()
	end_time = time.time()
	print(colored("Spark Custom MinHashLSH run time (seconds): {0}".format(end_time - start_time),"magenta"))


if(__name__ == "__main__"):
	main()