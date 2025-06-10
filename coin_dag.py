from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
import requests
import psycopg2
from psycopg2.extras import execute_values


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

coin_list = [
    'bitcoin', 'ethereum', 'tether', 'xrp', 'binancecoin', 'solana', 'usd-coin', 'dogecoin', 'cardano', 'tron'
    'avalanche-2', 'polkadot', 'chainlink', 'litecoin', 'stellar'
]

def fetch_and_store_coin_prices():
    url = 'https://api.coingecko.com/api/v3/coins/markets'
    params = {
        'ids' : ','.join(coin_list),
        'vs_currency' : 'USD'
    }
    
    response = requests.get(url, params=params)
    
    if response.status_code == 200:
        data = response.json()
        rows = []
        for coin in data:
            rows.append((
                coin['name'],
                coin['symbol'].upper(),
                coin['current_price'],
                coin['market_cap'],
                coin['total_volume'],
                coin['last_updated']
            ))
        
        conn = psycopg2.connect(
            dbname = 'defaultdb',
            user = 'avnadmin',
            password='AVNS_0qHtQCLVTc7PYqYM6i2',
            host='pg-3e06a654-austin-ace9.e.aivencloud.com',
            port=15559
        )
        
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS crypto_prices (
                name TEXT,
                symbol TEXT,
                price_usd NUMERIC,
                market_cap NUMERIC,
                volume NUMERIC,
                last_updated TIMESTAMPTZ
            )    
        """)
        insert_query = """
            INSERT INTO crypto_prices (name, symbol, price_usd, market_cap, volume, last_updated)
            VALUES %s
        """
        execute_values(cur, insert_query, rows)
        conn.commit()
        cur.close()
        conn.close()
        print("Data fetched and stored in PostgreSQL successfully.")
    else:
        raise Exception(f"Error fetching data: {response.status_code}")
 
with DAG(
    dag_id='coin_dag',
    schedule_interval='@hourly',
    default_args=default_args,
    catchup=False,
    tags= ['coin','coins']
) as dag:
    
    fetch_prices = PythonOperator(
        task_id = 'fetch_and_store_prices',
        python_callable=fetch_and_store_coin_prices
    )
    
    fetch_prices
    
