from airflow import DAG
from datetime import datetime,timedelta
from airflow.operators.bash import BashOperator

default_args = {
    'owner' : 'Austin',
    'depends_on_past' : False,
    'start_date' : datetime(2025,5,20),
    'email' : ['austinbenjamin128@gmail.com'],
    'email_on_failure' : True,
    'email_on_retry' : True,
    'retries' : 5,
    'retry_delay' : timedelta(minutes=2)
}

with DAG (
    dag_id= 'austin_gas_prices_dag',
    schedule_interval='@daily',
    default_args=default_args,
    catchup=False,
    tags= ['gas_prices', 'gas']

) as dag:
    
    starter = BashOperator(
        task_id = 'activate_venv',
        bash_command='source /airflowenv/bin/activate'
    )

    run_script = BashOperator(
        task_id = 'run_gas_prices',
        bash_command='python3 /root/austin/gas_prices.py'
    )

    
starter >> run_script
    



