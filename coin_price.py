import requests
import json
import os
from dotenv import load_dotenv
import sqlalchemy
load_dotenv()

coin_list = [
    'bitcoin', 'ethereum', 'tether', 'xrp', 'binancecoin',
    'solana', 'usd-coin', 'dogecoin', 'cardano', 'tron',
    'avalanche-2', 'polkadot', 'chainlink', 'litecoin', 'stellar'
]
url = f"https://api.coingecko.com/api/v3/coins/markets"

params = {
    'ids': ','.join(coin_list),
    'vs_currency' : 'USD'
}

response = requests.get(url, params=params)
print(response.json())