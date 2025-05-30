from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator

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

with DAG(
    dag_id='forex_dag',
    schedule_interval='@daily',
    default_args=default_args,
    catchup=False,
    tags=['simple','forex']
) as dag:
    
    say_hi = BashOperator(
        task_id='say_hi',
        bash_command="echo 'Fetching EURUSD Forex data...'"
    )

    fetch_forex = BashOperator(
        task_id = 'get_forex_data',
        bash_command=(
             "curl -s 'https://api.twelvedata.com/time_series?"
            "symbol=EUR/USD&interval=1min&apikey=9563a3afd2834e2ca1a52252e40bfca4' "
            "> /tmp/forex_data.json"
        )
    )

    done = BashOperator(
        task_id = 'done_message',
        bash_command="echo 'Forex data saved to /tmp/forex_data.json'"
    )

    say_hi >> fetch_forex >> done