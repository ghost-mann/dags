import requests
import sqlalchemy

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

if response.status_code == 200:
    data = response.json()
    for coin in data:
        print(f"{coin['name']} ({coin['symbol'].upper()}): ${coin['current_price']}")
    else:
        print(f"Error fetching data: {response.status_code}")
        