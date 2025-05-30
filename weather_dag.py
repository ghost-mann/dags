from airflow import DAG
from datetime import datetime,timedelta
from airflow.operators.python import PythonOperator
import os
import requests
import psycopg2

default_args = {
    'owner' : 'Austin',
    'depends_on_past': False,
    'start_date': datetime(2025, 5, 20),
    'email': ['austinbenjamin128@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

def fetch_weather_data(**kwargs):
    api_key = os.getenv('OPENWEATHER_API_KEY')
    url = f"https://api.openweathermap.org/data/2.5/weather?q=Nairobi&appid={api_key}&units=metric"


    response = requests.get(url)
    response.raise_for_status()
    weather = response.json()


    kwargs['ti'].xcom_push(key='weather_data', value=weather)

def save_to_aiven_db(**kwargs):
    weather = kwargs['ti'].xcom_pull(key='weather_data', task_ids='fetch_weather')

    conn = psycopg2.connect(
        dbname=os.getenv('AIVEN_DB'),
        user=os.getenv('AIVEN_USER'),
        password=os.getenv('AIVEN_PASSWORD'),
        host=os.getenv('AIVEN_HOST'),
        port=os.getenv('AIVEN_PORT')
    )
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS weather (
            city TEXT,
            temperature REAL,
            description TEXT,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')

    city = weather['name']
    temperature = weather['main']['temp']
    description = weather['weather'][0]['description']

    cursor.execute('''
        INSERT INTO weather (city, temperature, description)
        VALUES (%s, %s, %s)
    ''', (city, temperature, description))

    conn.commit()
    cursor.close()
    conn.close()

with DAG(
    dag_id='weather_dag',
    schedule_interval='@daily',
    default_args=default_args,
    catchup=False,
    tags=['weather','simple']
) as dag:
    
    fetch_weather = PythonOperator(
        task_id='fetch_weather',
        python_callable=fetch_weather_data,
        provide_context=True
    )

    store_weather = PythonOperator(
        task_id='store_weather',
        python_callable=save_to_aiven_db,
        provide_context=True
    )

    fetch_weather >> store_weather