import requests
import psycopg2
from psycopg2.extras import execute_values
import pandas as pd
import matplotlib.pyplot as plt


# list of coins
coin_list = [
    'bitcoin', 'ethereum', 'tether', 'xrp', 'binancecoin',
    'solana', 'usd-coin', 'dogecoin', 'cardano', 'tron',
    'avalanche-2', 'polkadot', 'chainlink', 'litecoin', 'stellar'
]

# Fetch data from API
url = "https://api.coingecko.com/api/v3/coins/markets"
params = {
    'ids': ','.join(coin_list),
    'vs_currency': 'USD'
}

response = requests.get(url, params=params)

if response.status_code == 200:
    data = response.json()

    print(f"Number of coins fetched: {len(data)}")
    print(data[0])


    # Prepare data for insertion
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
    print(f"Rows prepared for insertion: {len(rows)}")

    # Connect to PostgreSQL using psycopg2
    conn = psycopg2.connect(
        dbname='defaultdb',
        user='avnadmin',
        password='AVNS_0qHtQCLVTc7PYqYM6i2',
        host='pg-3e06a654-austin-ace9.e.aivencloud.com',
        port=15559
    )
    cur = conn.cursor()

    # Create table if not exists
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

    # Insert data using execute_values for performance
    insert_query = """
        INSERT INTO crypto_prices (name, symbol, price_usd, market_cap, volume, last_updated)
        VALUES %s
    """
    execute_values(cur, insert_query, rows)

    # Commit and close
    conn.commit()
    cur.close()
    conn.close()
    

    print("Data fetched and stored in PostgreSQL successfully.")

else:
    print(f"Error fetching data: {response.status_code}")
    