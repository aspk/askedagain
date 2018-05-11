import sys
import os
import re
import time
from termcolor import colored

from pyspark.ml.feature import StopWordsRemover
from pyspark.ml.feature import Tokenizer

from pyspark.sql.types import ArrayType, StringType, StructType, StructField
from pyspark.sql.functions import udf, concat, col, lit

from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import SQLContext

from itertools import chain
import nltk
nltk.download("wordnet")
from nltk.corpus import wordnet
from nltk.stem import WordNetLemmatizer

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/config")
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/lib")
import config
import util

'''Master preprocessing script + General preprocessing functions'''


def write_aws_s3(bucket_name, file_name, df):
    df.write.save("s3a://{0}/{1}".format(bucket_name, file_name), format="json", mode="overwrite")


# Adds closest synonyms to list of tokens
# def find_all_synonyms(tokens):
#     final = []
#     for token in tokens:
#         synonyms = wordnet.synsets(token)
#         lemmas = list(set(chain.from_iterable([word.lemma_names() for word in synonyms])))
#         cleaned_lemmas = [lemma for lemma in lemmas if not lemma.contains("_")]  # Remove multi-word synonyms
#         final = final + cleaned_lemmas[:config.NUM_SYNONYMS] + [token]
#     return tuple(final)


# Stems words
def lemmatize(tokens):
    wordnet_lemmatizer = WordNetLemmatizer()
    stems = [wordnet_lemmatizer.lemmatize(token) for token in tokens if len(token) > 1]
    return tuple(stems)


# Removes code snippets and other irregular sections from question body, returns cleaned string
def filter_body(body):
    remove_code = re.sub('<[^>]+>', '', body)
    remove_punctuation = re.sub(r"[^\w\s]", " ", remove_code)
    remove_spaces = remove_punctuation.replace("\n", " ")
    return remove_spaces.encode('ascii', 'ignore')


# Preprocess a data file and upload it
def preprocess_file(bucket_name, file_name):
    # schema = StructType([
    #     StructField("TICKET", StringType(), True),
    #     StructField("TRANFERRED", StringType(), True),
    #     StructField("ACCOUNT", StringType(), True),
    # ])

    # raw_data = sql_context.read.json("s3a://{0}/{1}".format(bucket_name, file_name), schema)
    raw_data = sql_context.read.json("s3a://{0}/{1}".format(bucket_name, file_name))

    # Clean question body
    if(config.LOG_DEBUG): print(colored("[PROCESSING]: Cleaning question body...", "green"))
    clean_body = udf(lambda body: filter_body(body), StringType())
    partially_cleaned_data = raw_data.withColumn("cleaned_body", clean_body("body"))

    # Concat cleaned question body and question title to form question vector
    if (config.LOG_DEBUG): print(colored("[PROCESSING]: Concating question body and question title...", "green"))
    data = partially_cleaned_data.withColumn("text_body", concat(col("title"), lit(" "), col("body")))

    # Tokenize question title
    if (config.LOG_DEBUG): print(colored("[PROCESSING]: Tokenizing text vector...", "green"))
    tokenizer = Tokenizer(inputCol="text_body", outputCol="text_body_tokenized")
    tokenized_data = tokenizer.transform(data)

    # Remove stop words
    if (config.LOG_DEBUG): print(colored("[PROCESSING]: Removing stop words...", "green"))
    stop_words_remover = StopWordsRemover(inputCol="text_body_tokenized", outputCol="text_body_stop_words_removed")
    stop_words_removed_data = stop_words_remover.transform(tokenized_data)

    # Stem words
    if (config.LOG_DEBUG): print(colored("[PROCESSING]: Stemming tokenized vector...", "green"))
    stem = udf(lambda tokens: lemmatize(tokens), ArrayType(StringType()))
    stemmed_data = stop_words_removed_data.withColumn("text_body_stemmed", stem("text_body_stop_words_removed"))

    # # Add synonyms for related words
    # if (config.LOG_DEBUG): print(colored("[PROCESSING]: Adding closest synonyms...", "green"))
    # add_synonyms = udf(lambda tokens: find_all_synonyms(tokens), ArrayType(StringType()))
    # synonymed_data = stemmed_data.withColumn("text_body_synonymed", add_synonyms("text_body_stemmed"))

    # Extract data that we want
    final_data = stemmed_data
    final_data.registerTempTable("final_data")

    preprocessed_data = sql_context.sql(
        "SELECT title, body, text_body, text_body_stemmed, text_body_synonymed, post_type_id, tags, score, comment_count, view_count, id from final_data"
    )

    # Write to AWS
    if (config.LOG_DEBUG): print(colored("[UPLOAD]: Writing preprocessed data to AWS...", "green"))
    write_aws_s3(config.S3_BUCKET_BATCH_PREPROCESSED, file_name, preprocessed_data)


def preprocess_all():
    bucket = util.get_bucket(config.S3_BUCKET_BATCH_RAW)
    for csv_obj in bucket.objects.all():
        preprocess_file(config.S3_BUCKET_BATCH_RAW, csv_obj.key)
        print(colored("Finished preprocessing file s3a://{0}/{1}".format(config.S3_BUCKET_BATCH_RAW, csv_obj.key), "green"))


def main():
    spark_conf = SparkConf().setAppName("Text Preprocesser").set("spark.cores.max", "30")

    global sc
    sc = SparkContext(conf=spark_conf)
    sc.setLogLevel("ERROR")
    sc.addFile(os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/lib/util.py")
    sc.addFile(os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/config/config.py")

    global sql_context
    sql_context = SQLContext(sc)

    start_time = time.time()
    preprocess_all()
    end_time = time.time()
    print(colored("Preprocessing run time (seconds): {0}".format(end_time - start_time), "magenta"))


if(__name__ == "__main__"):
    main()
