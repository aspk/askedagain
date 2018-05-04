import threading
import time
import kafka
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/config")
import debug as config


class Consumer(threading.Thread):
    def run(self):
        consumer = kafka.KafkaConsumer(bootstrap_servers=config.KAFKA_SERVER)
        consumer.subscribe([config.KAFKA_TOPIC])

        if config.LOG_DEBUG:
            for message in consumer:
                print("{0}\n\n".format(message))


def main():
    thread = Consumer()
    thread.daemon = True
    while True:
        if not thread.isAlive():
            print("Starting Kafka Consumer...")
            thread.start()
        else:
            print("Listening for topic: {0}...".format(config.KAFKA_TOPIC))
            time.sleep(config.KAFKA_CONSUMER_REFRESH)


if __name__ == "__main__":
    main()
