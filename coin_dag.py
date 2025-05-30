from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator

default_args = {
    'owner' : 'Austin',
    'depends_on_past' : False,
    'start_date' : datetime(2025,5,20),
    'email' : ['austinbenjamin128@gmail.com'],
    'email_on_retry' : True,
    'email_on_failure' : True,
    'retries' : 5,
    'retry_delay' : 2
}

with DAG(
    dag_id='coin_dag',
    schedule_interval='@hourly',
    default_args=default_args,
    catchup=False,
    tags= ['coin','coins']
) as dag:
    
    hello = BashOperator (
        task_id = 'print_hi',
        bash_command="echo 'Hello you!'"
    )
    
    coin_price = BashOperator (
        task_id = 'extract_price',
        bash_command='python3 /home/austin/airflow/dags/coin_price.py'
    )