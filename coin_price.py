import requests
import json
import os
from dotenv import load_dotenv
load_dotenv()

coin_list = ['bitcoin', 'ethereum','usdt','bnb','solana','doge']
url = f"https://api.coingecko.com/api/v3/coins/markets"

params = {
    'ids': ','.join(coin_list),
    'vs_currency' : 'USD'
}

response = requests.get(url, params=params)
print(response.json())