from airflow import DAG
from datetime import datetime,timedelta
from airflow.operators.bash import BashOperator

default_args = {
    'owner' : 'admin', # who's responsible for the dag?
    'depends_on_past' : False, # means each task is independent, if true task will run based on previous
    'start_date' : datetime(2025,5,20), # when the dag will start running 
    'email' : ['austinbenjamin128@gmail.com'], # for user notifications
    'email_on_failure' : True,
    'email_on_retry' : True,
'retries' : 5, # number of times airflow retries before marking workflow event failed
    'retry_delay' : timedelta(minutes=2) # delay between the retries
}

with DAG(
    dag_id='main_dag',
    schedule_interval='@daily',
    default_args=default_args,
    catchup=False,
    tags=['simple', 'test']    
)as dag:
    
    hi = BashOperator(
        task_id = 'print_Hello',
        bash_command = "echo 'Hello world!'"
    )
    bye = BashOperator(
        task_id = 'print_Bye',
        bash_command = "echo 'Hello world!'"
    )

    hi >> bye