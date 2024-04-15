from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import json
import requests
from kafka import KafkaProducer
import time


default_args = {
    'owner': 'airscholar',
    'start_date': datetime(2024, 4, 12, 11, 30, 00)
}


def get_data():
    url= 'https://randomuser.me/api/'
    response = requests.get(url)
    response = response.json()
    result = response['results'][0]
    return result


def format_data(res):
    data = {}
    location = res['location']
    data['first_name'] = res['name']['first'] 
    data['last_name'] = res['name']['last'] 
    data['gender'] = res['gender']

    data['address'] = f"{str(location['street']['number'])} {location['street']['name']}, " + \
                    f"{location['city']}, {location['state']}, {location['country']}"
    data['postcode'] = location['postcode']
    data['email'] = res['email']
    data['username'] = res['login']['username']
    data['dob'] = res['dob']['date']
    data['registered_date'] = res['registered']['date']
    data['phone'] = res['phone']
    data['picture'] = res['picture']['medium']

    return data


def stream_data():
    res = get_data()
    data = format_data(res)
    print(json.dumps(data, indent=3))

    producer = KafkaProducer(bootstrap_servers=['localhost:9092'], max_block_ms=5000)

    producer.send('users_created', json.dumps(data).encode('utf-8'))
    

with DAG('user_automation',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:
    
    streaming_task = PythonOperator(
        task_id = 'stream_data_from_api',
        python_callable=stream_data
    )

stream_data()