from airflow import DAG
import os
import psycopg2
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from dotenv import load_dotenv
from googleapiclient.discovery import build

# loading .env file
load_dotenv()

YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")
CHANNEL_ID = os.getenv("CHANNEL_ID")

AIVEN_DB_CONFIG = {
    'host':os.getenv('AIVEN_DB_HOST'),
    'database_name':os.getenv('AIVEN_DB_NAME'),
    'user':os.getenv('AIVEN_USER'),
    'password':os.getenv('AIVEN_PASSWORD'),
    'port':os.getenv('AIVEN_PORT')
}

# yt api connection
youtube = build('youtube', 'v3', developerKey=YOUTUBE_API_KEY)

request = youtube.search().list(
    part='snippet',
    q='programming tutorials',
    maxResults=5,
    type='video'
)

response = request.execute()

for item in response['items']:
    print(f"{item['snippet']['title']} - https://www.youtube.com/watch?v={item['id']['videoId']}")