from confluent_kafka import Consumer, KafkaError
from pymongo import MongoClient
import time
import logging

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class MongoDBConnector:
    def __init__(self, mongodb_uri, database_name, collection_name):
        self.client = MongoClient(mongodb_uri)
        self.db = self.client[database_name]
        self.collection_name = collection_name

    def create_collection(self):
        # Check if the collection already exists
        if self.collection_name not in self.db.list_collection_names():
            self.db.create_collection(self.collection_name)
            logger.info(f"Created collection: {self.collection_name}")
        else:
            logger.warning(f"Collection {self.collection_name} already exists")

    def insert_data(self, email, otp):
        document = {
            "email": email,
            "otp": otp
        }
        self.db[self.collection_name].insert_one(document)

    def close(self):
        self.client.close()

class KafkaConsumerWrapperMongoDB:
    def __init__(self, kafka_config, topics, mongodb_connector):
        self.consumer = Consumer(kafka_config)
        self.consumer.subscribe(topics)
        self.mongodb_connector = mongodb_connector

    def consume_and_insert_messages(self):
        start_time = time.time()
        try:
            while True:
                elapsed_time = time.time() - start_time
                if elapsed_time >= 30:
                    break
                msg = self.consumer.poll(1.0)

                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        logger.info('Reached end of partition')
                    else:
                        logger.warning('Error: {}'.format(msg.error()))
                else:
                    email = msg.key().decode('utf-8')
                    otp = msg.value().decode('utf-8')

                    existing_document = self.mongodb_connector.db[self.mongodb_connector.collection_name].find_one({"email": email, "otp": otp})
                    if existing_document:
                        logger.warning(f"Document with Email={email}, OTP={otp} already exists in the collection.")
                    else:
                        self.mongodb_connector.insert_data(email, otp)
                        logger.info(f'Received and inserted: Email={email}, OTP={otp}')

        except KeyboardInterrupt:
            logger.info("Received KeyboardInterrupt. Closing consumer.")
        finally:
            self.mongodb_connector.close()

    def close(self):
        self.consumer.close()

mongodb_uri = 'mongodb://root:root@172.19.0.5:27017/'
database_name = 'email_database'
collection_name = 'email_collection'
mongodb_connector = MongoDBConnector(mongodb_uri, database_name, collection_name)

def kafka_consumer_mongodb_main():
    mongodb_connector.create_collection()

    kafka_config = {
        'bootstrap.servers': 'localhost:9092,localhost:9093,localhost:9094', 
        'group.id': 'consumer_group',
        'auto.offset.reset': 'earliest'
    }

    topics = ['email_topic']

    kafka_consumer = KafkaConsumerWrapperMongoDB(kafka_config, topics, mongodb_connector)
    kafka_consumer.consume_and_insert_messages()

if __name__ == '__main__':
    kafka_consumer_mongodb_main()
