import sys
import os
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator

# Adjust the Python path to include the workspace root

from check_mongodb import check_mongodb_main
from kafka_producer import kafka_producer_main
from check_cassandra import check_cassandra_main
from kafka_create_topic import kafka_create_topic_main
from kafka_consumer_mongodb import kafka_consumer_mongodb_main
from kafka_consumer_cassandra import kafka_consumer_cassandra_main



start_date = datetime(2022, 10, 19, 12, 20)

default_args = {
    'owner': 'airflow',
    'start_date': start_date,
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}

email_cassandra = check_cassandra_main()['email']
otp_cassandra = check_cassandra_main()['otp']
email_mongodb = check_mongodb_main()['email']
otp_mongodb = check_mongodb_main()['otp']

def decide_branch():
    create_topic = kafka_create_topic_main()
    if create_topic == "Created":
        return "topic_created"
    else:
        return "topic_already_exists"


with DAG('airflow_kafka_cassandra_mongodb', default_args=default_args, schedule_interval='@daily', catchup=False) as dag:

    create_new_topic = BranchPythonOperator(task_id='create_new_topic', python_callable=decide_branch)
    
    kafka_consumer_cassandra = PythonOperator(task_id='kafka_consumer_cassandra', python_callable=kafka_consumer_cassandra_main,
                             retries=2, retry_delay=timedelta(seconds=10),
                             execution_timeout=timedelta(seconds=45))
    
    kafka_consumer_mongodb = PythonOperator(task_id='kafka_consumer_mongodb', python_callable=kafka_consumer_mongodb_main,
                             retries=2, retry_delay=timedelta(seconds=10),
                             execution_timeout=timedelta(seconds=45))
    
    kafka_producer = PythonOperator(task_id='kafka_producer', python_callable=kafka_producer_main,
                             retries=2, retry_delay=timedelta(seconds=10),
                             execution_timeout=timedelta(seconds=45))
    
    check_cassandra = PythonOperator(task_id='check_cassandra', python_callable=check_cassandra_main,
                             retries=2, retry_delay=timedelta(seconds=10),
                             execution_timeout=timedelta(seconds=45))
    
    check_mongodb = PythonOperator(task_id='check_mongodb', python_callable=check_mongodb_main,
                             retries=2, retry_delay=timedelta(seconds=10),
                             execution_timeout=timedelta(seconds=45))

    topic_created = DummyOperator(task_id="topic_created")

    topic_already_exists = DummyOperator(task_id="topic_already_exists")

    create_new_topic >> [topic_created, topic_already_exists] >> kafka_producer
    kafka_consumer_cassandra >> check_cassandra
    kafka_consumer_mongodb >> check_mongodb