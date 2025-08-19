from airflow import DAG
from airflow.operators.python import PythonOperator
from kafka import KafkaProducer
import datetime
import requests
import json
import logging
import time

default_args = {
    'owner': 'ayub',
    'start_date': datetime.datetime(2025, 8, 17, 10, 00)
}

def get_data():
    res = requests.get('https://randomuser.me/api/')
    res.raise_for_status() 
    
    results = res.json().get('results')
    if not results:
        logging.warning("API returned no results")
        return None

    res_json = results[0]
    
    user_name = res_json['name']["first"] + " " + res_json['name']["last"]
    user_email = res_json['email']
    user_gender = res_json['gender']
    user_city = res_json['location']['city']
    user_state = res_json['location']['state']
    user_country = res_json['location']['country']
    user_location = res_json['location']['street']['name'] + " " + str(res_json['location']['street']['number'])
    user_phone = res_json['phone']
    user_cell = res_json['cell']
    
    return {
        "name": user_name,
        "email": user_email,
        "gender": user_gender,
        "city": user_city,
        "state": user_state,
        "country": user_country,
        "location": user_location,
        "phone": user_phone,
        "cell": user_cell
    }

def stream_data_to_kafka():
    producer = None
    start_time = time.time()
    try:
        producer = KafkaProducer(bootstrap_servers='kafka:9093', api_version=(0, 10, 1))
        while time.time() - start_time < 120:
            data = get_data()
            if data:
                logging.info(f"Sending data to Kafka: {data}")
                producer.send('user_data', json.dumps(data).encode('utf-8'))
                producer.flush()
                logging.info("Data sent to Kafka successfully.")
            time.sleep(1)
    except Exception as e:
        logging.error(f"Error sending data to Kafka: {e}")
        raise
    finally:
        if producer:
            producer.close()
            logging.info("Kafka producer closed.")

with DAG('user_automation',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:

    streaming_task = PythonOperator(
        task_id='stream_data_from_api',
        python_callable=stream_data_to_kafka
    )