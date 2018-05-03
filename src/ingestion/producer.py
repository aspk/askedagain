import os
import sys
import boto3
import botocore
import smart_open
import kafka
import time
import threading
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


class Producer(threading.Thread):
    def run(self):
        producer = kafka.KafkaProducer(bootstrap_servers=config.KAFKA_SERVER)

        bucket = get_bucket(config.S3_BUCKET_RAW)

        for json_obj in bucket.objects.all():
            json_file = "s3://{0}/{1}".format(config.S3_BUCKET_RAW, json_obj.key)
            for line in smart_open.smart_open(json_file):
                    producer.send(config.KAFKA_TOPIC, line)


def main():
    producer = Producer()
    producer.daemon = True
    producer.start()
    while True:
        time.sleep(config.KAFKA_PRODUCER_RATE)


if __name__ == "__main__":
    main()
