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